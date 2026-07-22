use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use consensus::{
    AsyncVoteAggregator, CertificateKind, ConsensusError, Proposal, Replica, Validator,
    ValidatorSet, Vote, VoteCollector, VotePhase, verify_certificate,
};
use crypto::{AggregateSignatureScheme, Bls12381Scheme};
use types::Hash;

struct Fixture {
    validators: ValidatorSet,
    scheme: Arc<dyn AggregateSignatureScheme>,
    keys: BTreeMap<Hash, Vec<u8>>,
    ids: Vec<Hash>,
}

impl Fixture {
    fn new(stakes: &[u64]) -> Self {
        let scheme: Arc<dyn AggregateSignatureScheme> = Arc::new(Bls12381Scheme);
        let mut keys = BTreeMap::new();
        let mut ids = Vec::new();
        let validators = stakes
            .iter()
            .enumerate()
            .map(|(index, stake)| {
                let mut id_bytes = [0_u8; 32];
                id_bytes[31] = u8::try_from(index).unwrap();
                let id = Hash::from_bytes(id_bytes);
                let key = vec![u8::try_from(index + 1).unwrap(); 32];
                keys.insert(id, key.clone());
                ids.push(id);
                Validator {
                    id,
                    stake: *stake,
                    public_key: scheme.public_key(&key).unwrap(),
                    proof_of_possession: scheme.proof_of_possession(&key).unwrap(),
                }
            })
            .collect();
        Self {
            validators: ValidatorSet::new(validators, scheme.as_ref()).unwrap(),
            scheme,
            keys,
            ids,
        }
    }

    fn vote(&self, index: usize, block: Hash, phase: VotePhase) -> Vote {
        let id = self.ids[index];
        Vote::sign(
            id,
            &self.keys[&id],
            7,
            0,
            block,
            phase,
            self.scheme.as_ref(),
        )
        .unwrap()
    }

    fn collect(
        &self,
        kind: CertificateKind,
        block: Hash,
        signers: &[usize],
    ) -> Option<consensus::QuorumCertificate> {
        let mut collector = VoteCollector::new(
            &self.validators,
            Arc::clone(&self.scheme),
            kind,
            7,
            0,
            block,
        );
        let phase = match kind {
            CertificateKind::Fast | CertificateKind::Prepare => VotePhase::Order,
            CertificateKind::Commit => VotePhase::Commit,
            CertificateKind::Timeout => VotePhase::Timeout,
        };
        let target = if phase == VotePhase::Timeout {
            Hash::default()
        } else {
            block
        };
        let mut certificate = None;
        for signer in signers {
            certificate = collector
                .add_vote(self.vote(*signer, target, phase))
                .unwrap();
        }
        certificate
    }
}

#[test]
fn fast_path_finalizes_at_eighty_percent_in_one_round() {
    let fixture = Fixture::new(&[20, 20, 20, 20, 20]);
    let block = Hash::digest(b"fast");
    assert!(
        fixture
            .collect(CertificateKind::Fast, block, &[0, 1, 2])
            .is_none()
    );
    let certificate = fixture
        .collect(CertificateKind::Fast, block, &[0, 1, 2, 3])
        .unwrap();
    assert_eq!(certificate.signed_stake, 80);
    verify_certificate(&certificate, &fixture.validators, fixture.scheme.as_ref()).unwrap();
}

#[test]
fn bls_aggregation_runs_behind_a_nonblocking_worker_boundary() {
    let fixture = Fixture::new(&[20, 20, 20, 20, 20]);
    let block = Hash::digest(b"async-fast");
    let aggregator = AsyncVoteAggregator::new(
        fixture.validators.clone(),
        Arc::clone(&fixture.scheme),
        CertificateKind::Fast,
        7,
        0,
        block,
    )
    .unwrap();
    for signer in 0..4 {
        aggregator
            .submit(fixture.vote(signer, block, VotePhase::Order))
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(certificate) = aggregator.try_certificate().unwrap() {
            assert_eq!(certificate.signed_stake, 80);
            break;
        }
        assert!(Instant::now() < deadline, "aggregation worker timed out");
        std::thread::yield_now();
    }
}

#[test]
fn fallback_requires_prepare_and_distinct_commit_rounds() {
    let fixture = Fixture::new(&[20, 20, 20, 20, 20]);
    let block = Hash::digest(b"fallback");
    let prepare = fixture
        .collect(CertificateKind::Prepare, block, &[0, 1, 2])
        .unwrap();
    let commit = fixture
        .collect(CertificateKind::Commit, block, &[0, 1, 2])
        .unwrap();
    assert_eq!(prepare.signed_stake, 60);
    assert_eq!(commit.signed_stake, 60);
    assert_ne!(prepare.aggregate_signature, commit.aggregate_signature);
    verify_certificate(&prepare, &fixture.validators, fixture.scheme.as_ref()).unwrap();
    verify_certificate(&commit, &fixture.validators, fixture.scheme.as_ref()).unwrap();
}

#[test]
fn malformed_and_under_threshold_certificates_are_rejected() {
    let fixture = Fixture::new(&[20, 20, 20, 20, 20]);
    let block = Hash::digest(b"certificate-validation");
    let valid = fixture
        .collect(CertificateKind::Prepare, block, &[0, 1, 2])
        .unwrap();

    let mut under_threshold = valid.clone();
    under_threshold.kind = CertificateKind::Fast;
    assert!(matches!(
        verify_certificate(
            &under_threshold,
            &fixture.validators,
            fixture.scheme.as_ref()
        ),
        Err(ConsensusError::InsufficientStake { .. })
    ));

    let mut wrong_stake = valid.clone();
    wrong_stake.signed_stake += 1;
    assert_eq!(
        verify_certificate(&wrong_stake, &fixture.validators, fixture.scheme.as_ref()),
        Err(ConsensusError::IncorrectSignedStake)
    );

    let mut duplicate = valid.clone();
    duplicate.signers.insert(1, duplicate.signers[0]);
    assert_eq!(
        verify_certificate(&duplicate, &fixture.validators, fixture.scheme.as_ref()),
        Err(ConsensusError::InvalidSignerSet)
    );

    let mut tampered = valid;
    tampered.aggregate_signature[0] ^= 1;
    assert!(verify_certificate(&tampered, &fixture.validators, fixture.scheme.as_ref()).is_err());
}

#[test]
fn prepared_lock_rejects_a_conflicting_proposal() {
    let fixture = Fixture::new(&[20, 20, 20, 20, 20]);
    let id = fixture.ids[4];
    let parent = Hash::digest(b"parent");
    let mut replica = Replica::new(
        id,
        fixture.keys[&id].clone(),
        fixture.validators.clone(),
        Arc::clone(&fixture.scheme),
        7,
        parent,
    )
    .unwrap();
    let prepared_block = Hash::digest(b"prepared");
    let prepare = fixture
        .collect(CertificateKind::Prepare, prepared_block, &[0, 1, 2])
        .unwrap();
    replica.vote_to_commit(&prepare).unwrap();

    let conflicting = Proposal::new(
        7,
        0,
        parent,
        replica.leader(),
        vec![Hash::digest(b"conflict")],
        None,
    );
    assert_eq!(
        replica.vote_for_proposal(&conflicting),
        Err(ConsensusError::LockedOnDifferentBlock)
    );
}

#[test]
fn honest_replica_refuses_same_view_equivocation() {
    let fixture = Fixture::new(&[20, 20, 20, 20, 20]);
    let id = fixture.ids[0];
    let mut replica = Replica::new(
        id,
        fixture.keys[&id].clone(),
        fixture.validators.clone(),
        Arc::clone(&fixture.scheme),
        7,
        Hash::digest(b"parent"),
    )
    .unwrap();
    let leader = replica.leader();
    let first = Proposal::new(
        7,
        0,
        Hash::digest(b"parent"),
        leader,
        vec![Hash::digest(b"first")],
        None,
    );
    let second = Proposal::new(
        7,
        0,
        Hash::digest(b"parent"),
        leader,
        vec![Hash::digest(b"second")],
        None,
    );
    replica.vote_for_proposal(&first).unwrap();
    assert_eq!(
        replica.vote_for_proposal(&second),
        Err(ConsensusError::LocalDoubleVote)
    );
}

#[test]
fn identified_same_view_attack_is_prevented_below_twenty_percent() {
    let fixture = Fixture::new(&[19, 21, 20, 20, 20]);
    fixture.validators.validate_fault_budget(19, 0).unwrap();
    let left = Hash::digest(b"equivocated-left");
    let right = Hash::digest(b"equivocated-right");

    // The 19% Byzantine validator votes for both blocks. Honest stake is
    // partitioned 41/40 and votes only once. One side reaches 60%; the other
    // reaches 59%, so two conflicting prepare certificates cannot exist.
    assert!(
        fixture
            .collect(CertificateKind::Prepare, left, &[0, 1, 2])
            .is_some()
    );
    assert!(
        fixture
            .collect(CertificateKind::Prepare, right, &[0, 3, 4])
            .is_none()
    );
}

#[test]
fn same_view_attack_exists_at_twenty_percent_and_is_out_of_model() {
    let fixture = Fixture::new(&[20, 20, 20, 20, 20]);
    assert_eq!(
        fixture.validators.validate_fault_budget(20, 0),
        Err(ConsensusError::ByzantineBudgetExceeded)
    );
    let left = Hash::digest(b"boundary-left");
    let right = Hash::digest(b"boundary-right");
    assert!(
        fixture
            .collect(CertificateKind::Prepare, left, &[0, 1, 2])
            .is_some()
    );
    assert!(
        fixture
            .collect(CertificateKind::Prepare, right, &[0, 3, 4])
            .is_some()
    );
}

#[test]
fn quorum_intersection_is_exhaustive_for_integer_stake_units() {
    for byzantine in 0_u128..20 {
        for left_honest in 0_u128..=(100 - byzantine) {
            let right_honest = 100 - byzantine - left_honest;
            assert!(
                byzantine + left_honest < 60 || byzantine + right_honest < 60,
                "two 60% quorums formed with {byzantine}% Byzantine stake"
            );
        }
    }
}

#[test]
fn sixty_percent_timeout_rotates_away_from_failed_leader() {
    let fixture = Fixture::new(&[20, 20, 20, 20, 20]);
    let id = fixture.ids[0];
    let mut replica = Replica::new(
        id,
        fixture.keys[&id].clone(),
        fixture.validators.clone(),
        Arc::clone(&fixture.scheme),
        7,
        Hash::digest(b"parent"),
    )
    .unwrap();
    let old_leader = replica.leader();
    let timeout = fixture
        .collect(CertificateKind::Timeout, Hash::default(), &[0, 1, 2])
        .unwrap();
    replica.advance_view(&timeout).unwrap();
    assert_eq!(replica.view(), 1);
    assert_ne!(replica.leader(), old_leader);
}

#[test]
fn fast_certificate_signers_cannot_abandon_the_view_and_finalize_a_conflict() {
    let fixture = Fixture::new(&[19, 21, 20, 20, 20]);
    fixture.validators.validate_fault_budget(19, 0).unwrap();
    let parent = Hash::digest(b"fast-parent");
    let fast_block = Proposal::new(
        7,
        0,
        parent,
        fixture.validators.leader(7, 0).id,
        vec![Hash::digest(b"fast-a")],
        None,
    );
    let mut replicas = fixture
        .ids
        .iter()
        .map(|id| {
            Replica::new(
                *id,
                fixture.keys[id].clone(),
                fixture.validators.clone(),
                Arc::clone(&fixture.scheme),
                7,
                parent,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    let mut fast = VoteCollector::new(
        &fixture.validators,
        Arc::clone(&fixture.scheme),
        CertificateKind::Fast,
        7,
        0,
        fast_block.block_id,
    );
    let mut fast_certificate = None;
    for replica in replicas.iter_mut().take(4) {
        fast_certificate = fast
            .add_vote(replica.vote_for_proposal(&fast_block).unwrap())
            .unwrap();
    }
    let fast_certificate = fast_certificate.unwrap();
    replicas[1].finalize(&fast_certificate).unwrap();

    let mut timeout = VoteCollector::new(
        &fixture.validators,
        Arc::clone(&fixture.scheme),
        CertificateKind::Timeout,
        7,
        0,
        Hash::default(),
    );
    let byzantine_timeout = fixture.vote(0, Hash::default(), VotePhase::Timeout);
    assert!(timeout.add_vote(byzantine_timeout).unwrap().is_none());
    for replica in &mut replicas[2..4] {
        assert!(replica.local_timeout().unwrap().is_none());
    }
    let only_non_signer = replicas[4].local_timeout().unwrap().unwrap();
    assert!(timeout.add_vote(only_non_signer).unwrap().is_none());
}

#[test]
fn timeout_vote_prevents_a_later_order_vote_in_the_same_view() {
    let fixture = Fixture::new(&[19, 21, 20, 20, 20]);
    let id = fixture.ids[1];
    let parent = Hash::digest(b"timeout-parent");
    let mut replica = Replica::new(
        id,
        fixture.keys[&id].clone(),
        fixture.validators.clone(),
        Arc::clone(&fixture.scheme),
        7,
        parent,
    )
    .unwrap();
    assert!(replica.local_timeout().unwrap().is_some());
    let proposal = Proposal::new(
        7,
        0,
        parent,
        replica.leader(),
        vec![Hash::digest(b"late-proposal")],
        None,
    );
    assert_eq!(
        replica.vote_for_proposal(&proposal),
        Err(ConsensusError::ConflictingFirstRoundVote)
    );
}
