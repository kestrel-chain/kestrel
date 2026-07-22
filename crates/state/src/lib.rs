//! Deterministic Merkle state and storage-rent lifecycle for Kestrel.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use types::{Epoch, Hash, Object, ObjectId, Owner};

const EMPTY_DOMAIN: &[u8] = b"kestrel/state/empty/v1";
const LEAF_DOMAIN: &[u8] = b"kestrel/state/leaf/v1";
const BRANCH_DOMAIN: &[u8] = b"kestrel/state/branch/v1";
const ROOT_DOMAIN: &[u8] = b"kestrel/state/root/v1";
const STATE_SNAPSHOT_FORMAT_VERSION: u16 = 1;

/// Genesis parameters governing Phase 1 storage rent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StateConfig {
    /// Fixed rent charged to every active object at each epoch transition.
    pub rent_per_object_per_epoch: u64,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            rent_per_object_per_epoch: 1,
        }
    }
}

/// Historical record retained after an object exhausts its rent.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExpiredObject {
    /// Object after its final rent charge (normally with zero rent balance).
    pub object: Object,
    pub expired_at: Epoch,
    /// Root that still contained the object immediately before expiry charging.
    pub last_active_root: Hash,
    /// Exact object bytes committed by `last_active_root` before the final charge.
    pub last_active_object: Object,
    /// Compact binary-trie inclusion proof against `last_active_root`.
    pub last_active_proof: MerkleProof,
}

/// One branch needed to reconstruct a compressed binary Merkle path.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MerkleStep {
    pub depth: u16,
    pub sibling: Hash,
    pub sibling_on_left: bool,
}

/// Inclusion witness ordered from root branch to object leaf.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MerkleProof {
    pub object_id: ObjectId,
    pub steps: Vec<MerkleStep>,
}

/// Portable proof package accepted by stateful and stateless validators.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResurrectionWitness {
    pub object: Object,
    pub expired_at: Epoch,
    pub last_active_root: Hash,
    pub proof: MerkleProof,
}

/// State transition produced after consuming a valid resurrection witness.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResurrectionOutcome {
    pub object: Object,
}

/// Summary of an epoch transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RentOutcome {
    pub epoch: Epoch,
    pub expired: Vec<ObjectId>,
    pub state_root: Hash,
}

/// Canonical, portable application-state checkpoint persisted at a finalized block.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StateSnapshot {
    pub format_version: u16,
    pub config: StateConfig,
    pub epoch: Epoch,
    pub active_objects: Vec<Object>,
    pub expired_objects: Vec<ExpiredObject>,
    pub state_root: Hash,
}

/// Object keys observed by one state transition.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StateAccesses {
    pub reads: BTreeSet<ObjectId>,
    pub writes: BTreeSet<ObjectId>,
}

/// Deterministic active-state patch produced by speculative execution.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StateDelta {
    changes: BTreeMap<ObjectId, Option<Object>>,
    expired_changes: BTreeMap<ObjectId, Option<ExpiredObject>>,
}

/// Immutable base plus a transaction-local version overlay.
///
/// Clones share both maps. The first write detaches only the overlay, while
/// speculative snapshots compact the current view into one shared immutable
/// base. Deltas compare overlay keys when both views share that base, avoiding
/// full-state scans for ordinary transaction execution.
#[derive(Clone, Debug)]
struct VersionedMap<T> {
    base: Arc<BTreeMap<ObjectId, T>>,
    changes: Arc<BTreeMap<ObjectId, Option<T>>>,
}

impl<T> Default for VersionedMap<T> {
    fn default() -> Self {
        Self {
            base: Arc::new(BTreeMap::new()),
            changes: Arc::new(BTreeMap::new()),
        }
    }
}

impl<T: Clone + PartialEq> VersionedMap<T> {
    fn get(&self, id: &ObjectId) -> Option<&T> {
        self.changes
            .get(id)
            .map_or_else(|| self.base.get(id), Option::as_ref)
    }

    fn get_mut(&mut self, id: &ObjectId) -> Option<&mut T> {
        if !self.changes.contains_key(id) {
            let value = self.base.get(id)?.clone();
            Arc::make_mut(&mut self.changes).insert(*id, Some(value));
        }
        Arc::make_mut(&mut self.changes)
            .get_mut(id)
            .and_then(Option::as_mut)
    }

    fn contains_key(&self, id: &ObjectId) -> bool {
        self.get(id).is_some()
    }

    fn insert(&mut self, id: ObjectId, value: T) {
        Arc::make_mut(&mut self.changes).insert(id, Some(value));
    }

    fn remove(&mut self, id: &ObjectId) -> Option<T> {
        let value = self.get(id)?.clone();
        Arc::make_mut(&mut self.changes).insert(*id, None);
        Some(value)
    }

    fn values(&self) -> impl Iterator<Item = &T> {
        self.base
            .iter()
            .filter_map(|(id, value)| (!self.changes.contains_key(id)).then_some(value))
            .chain(self.changes.values().filter_map(Option::as_ref))
    }

    fn keys(&self) -> impl Iterator<Item = &ObjectId> {
        self.base
            .keys()
            .filter(|id| !self.changes.contains_key(*id))
            .chain(
                self.changes
                    .iter()
                    .filter_map(|(id, value)| value.as_ref().map(|_| id)),
            )
    }

    /// Iterates the logical map in canonical key order without materializing
    /// or cloning its values.
    fn entries(&self) -> impl Iterator<Item = (ObjectId, &T)> {
        let mut base = self.base.iter().peekable();
        let mut changes = self.changes.iter().peekable();
        std::iter::from_fn(move || {
            loop {
                match (base.peek(), changes.peek()) {
                    (Some((base_id, _)), Some((change_id, _))) => match base_id.cmp(change_id) {
                        std::cmp::Ordering::Less => {
                            let (id, value) = base.next().expect("peeked base entry exists");
                            return Some((*id, value));
                        }
                        std::cmp::Ordering::Equal => {
                            let _ = base.next();
                            let (id, value) = changes.next().expect("peeked overlay entry exists");
                            if let Some(value) = value {
                                return Some((*id, value));
                            }
                        }
                        std::cmp::Ordering::Greater => {
                            let (id, value) = changes.next().expect("peeked overlay entry exists");
                            if let Some(value) = value {
                                return Some((*id, value));
                            }
                        }
                    },
                    (Some(_), None) => {
                        let (id, value) = base.next().expect("peeked base entry exists");
                        return Some((*id, value));
                    }
                    (None, Some(_)) => {
                        let (id, value) = changes.next().expect("peeked overlay entry exists");
                        if let Some(value) = value {
                            return Some((*id, value));
                        }
                    }
                    (None, None) => return None,
                }
            }
        })
    }

    fn materialized(&self) -> BTreeMap<ObjectId, T> {
        let mut values = self.base.as_ref().clone();
        for (id, value) in self.changes.iter() {
            if let Some(value) = value {
                values.insert(*id, value.clone());
            } else {
                values.remove(id);
            }
        }
        values
    }

    fn snapshot(&self) -> Self {
        if self.changes.is_empty() {
            return Self {
                base: Arc::clone(&self.base),
                changes: Arc::new(BTreeMap::new()),
            };
        }
        Self {
            base: Arc::new(self.materialized()),
            changes: Arc::new(BTreeMap::new()),
        }
    }

    fn delta_from(&self, base: &Self) -> BTreeMap<ObjectId, Option<T>> {
        if Arc::ptr_eq(&self.base, &base.base) && base.changes.is_empty() {
            return self
                .changes
                .iter()
                .filter(|(id, value)| value.as_ref() != base.base.get(id))
                .map(|(id, value)| (*id, value.clone()))
                .collect();
        }
        let keys = if Arc::ptr_eq(&self.base, &base.base) {
            self.changes
                .keys()
                .chain(base.changes.keys())
                .copied()
                .collect::<BTreeSet<_>>()
        } else {
            self.keys()
                .chain(base.keys())
                .copied()
                .collect::<BTreeSet<_>>()
        };
        keys.into_iter()
            .filter_map(|id| {
                let current = self.get(&id);
                (current != base.get(&id)).then(|| (id, current.cloned()))
            })
            .collect()
    }
}

impl StateDelta {
    /// Builds a validated active-object patch for an atomic host commit.
    ///
    /// Callers must validate object lifecycle and version invariants before
    /// constructing the patch. Applying the returned delta is infallible.
    #[must_use]
    pub fn from_active_changes(changes: BTreeMap<ObjectId, Option<Object>>) -> Self {
        Self {
            changes,
            expired_changes: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn write_set(&self) -> BTreeSet<ObjectId> {
        self.changes
            .keys()
            .chain(self.expired_changes.keys())
            .copied()
            .collect()
    }

    /// Iterates object IDs changed by this patch without allocating a set.
    pub fn write_ids(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.changes
            .keys()
            .chain(self.expired_changes.keys())
            .copied()
    }
}

/// State transition failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StateError {
    #[error("rent per object per epoch must be greater than zero")]
    InvalidRentRate,
    #[error("object {0} already exists in active or expired state")]
    ObjectAlreadyExists(ObjectId),
    #[error("object {0} was not found in active state")]
    ObjectNotFound(ObjectId),
    #[error("object {id} version mismatch: expected {expected}, actual {actual}")]
    VersionMismatch {
        id: ObjectId,
        expected: u64,
        actual: u64,
    },
    #[error("replacement object ID differs from the existing object ID")]
    ObjectIdChanged,
    #[error("object version overflow")]
    VersionOverflow,
    #[error("rent balance overflow")]
    RentOverflow,
    #[error("resurrection rent credit must be greater than zero")]
    InvalidResurrectionRent,
    #[error("resurrection witness is malformed or does not match its root")]
    InvalidResurrectionWitness,
    #[error("resurrection witness does not match retained expiry history")]
    ResurrectionHistoryMismatch,
    #[error("cannot move epoch backwards from {current:?} to {requested:?}")]
    EpochRegression { current: Epoch, requested: Epoch },
    #[error("canonical state encoding failed: {0}")]
    Encoding(String),
    #[error("unsupported state snapshot format version {0}")]
    UnsupportedSnapshotFormat(u16),
    #[error("state snapshot contains duplicate, overlapping, or inconsistent objects")]
    InvalidSnapshotObjects,
    #[error("state snapshot root does not match its contents")]
    SnapshotRootMismatch,
}

/// In-memory Phase 1 state backed by deterministic Merkle prefix tries.
///
/// Active and expired objects live in separate logical trees. [`StateTree::root`]
/// commits to both trees, the current epoch, and the rent configuration.
#[derive(Clone, Debug)]
pub struct StateTree {
    config: StateConfig,
    epoch: Epoch,
    active: VersionedMap<Object>,
    expired: VersionedMap<ExpiredObject>,
    access_tracker: Option<Arc<Mutex<StateAccesses>>>,
}

impl StateTree {
    /// Creates empty state at epoch zero.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::InvalidRentRate`] when the configured rate is zero.
    pub fn new(config: StateConfig) -> Result<Self, StateError> {
        if config.rent_per_object_per_epoch == 0 {
            return Err(StateError::InvalidRentRate);
        }
        Ok(Self {
            config,
            epoch: Epoch(0),
            active: VersionedMap::default(),
            expired: VersionedMap::default(),
            access_tracker: None,
        })
    }

    #[must_use]
    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    #[must_use]
    pub const fn config(&self) -> StateConfig {
        self.config
    }

    /// Creates an immutable shared base for transaction-local version overlays.
    #[must_use]
    pub fn speculative_snapshot(&self) -> Self {
        Self {
            config: self.config,
            epoch: self.epoch,
            active: self.active.snapshot(),
            expired: self.expired.snapshot(),
            access_tracker: None,
        }
    }

    /// Materializes a canonical, root-bound checkpoint for durable persistence.
    ///
    /// # Errors
    ///
    /// Returns an encoding error if the state root cannot be computed.
    pub fn durable_snapshot(&self) -> Result<StateSnapshot, StateError> {
        Ok(StateSnapshot {
            format_version: STATE_SNAPSHOT_FORMAT_VERSION,
            config: self.config,
            epoch: self.epoch,
            active_objects: self
                .active
                .entries()
                .map(|(_, object)| object.clone())
                .collect(),
            expired_objects: self
                .expired
                .entries()
                .map(|(_, object)| object.clone())
                .collect(),
            state_root: self.root()?,
        })
    }

    /// Restores a checkpoint only after validating object identity, retained
    /// expiry witnesses, and the complete state root.
    ///
    /// # Errors
    ///
    /// Rejects unsupported formats, malformed object sets, invalid retained
    /// witnesses, invalid rent configuration, or a root mismatch.
    pub fn from_durable_snapshot(snapshot: StateSnapshot) -> Result<Self, StateError> {
        if snapshot.format_version != STATE_SNAPSHOT_FORMAT_VERSION {
            return Err(StateError::UnsupportedSnapshotFormat(
                snapshot.format_version,
            ));
        }
        let mut state = Self::new(snapshot.config)?;
        state.epoch = snapshot.epoch;

        let mut active = BTreeMap::new();
        for object in snapshot.active_objects {
            if active.insert(object.id, object).is_some() {
                return Err(StateError::InvalidSnapshotObjects);
            }
        }
        let mut expired = BTreeMap::new();
        for record in snapshot.expired_objects {
            let id = record.object.id;
            if active.contains_key(&id)
                || record.last_active_object.id != id
                || record.last_active_proof.object_id != id
                || record.expired_at > snapshot.epoch
                || expired.insert(id, record.clone()).is_some()
            {
                return Err(StateError::InvalidSnapshotObjects);
            }
            verify_resurrection_witness(&ResurrectionWitness {
                object: record.last_active_object,
                expired_at: record.expired_at,
                last_active_root: record.last_active_root,
                proof: record.last_active_proof,
            })?;
        }
        state.active = VersionedMap {
            base: Arc::new(active),
            changes: Arc::new(BTreeMap::new()),
        };
        state.expired = VersionedMap {
            base: Arc::new(expired),
            changes: Arc::new(BTreeMap::new()),
        };
        if state.root()? != snapshot.state_root {
            return Err(StateError::SnapshotRootMismatch);
        }
        Ok(state)
    }

    #[must_use]
    pub fn object(&self, id: &ObjectId) -> Option<&Object> {
        self.record_read(*id);
        self.active.get(id)
    }

    #[must_use]
    pub fn expired_object(&self, id: &ObjectId) -> Option<&ExpiredObject> {
        self.expired.get(id)
    }

    /// Returns the portable inclusion witness retained when an object expired.
    #[must_use]
    pub fn resurrection_witness(&self, id: &ObjectId) -> Option<ResurrectionWitness> {
        self.expired.get(id).map(|expired| ResurrectionWitness {
            object: expired.last_active_object.clone(),
            expired_at: expired.expired_at,
            last_active_root: expired.last_active_root,
            proof: expired.last_active_proof.clone(),
        })
    }

    pub fn active_objects(&self) -> impl Iterator<Item = &Object> {
        self.active.values()
    }

    pub fn expired_objects(&self) -> impl Iterator<Item = &ExpiredObject> {
        self.expired.values()
    }

    /// Inserts a newly created object.
    ///
    /// # Errors
    ///
    /// Returns an error if the ID is already present in either state tree.
    pub fn create_object(&mut self, object: Object) -> Result<(), StateError> {
        self.record_write(object.id);
        if self.active.contains_key(&object.id) || self.expired.contains_key(&object.id) {
            return Err(StateError::ObjectAlreadyExists(object.id));
        }
        self.active.insert(object.id, object);
        Ok(())
    }

    /// Replaces an object and increments its version exactly once.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing object, stale version, changed ID, or overflow.
    pub fn mutate_object(
        &mut self,
        id: ObjectId,
        expected_version: u64,
        mut replacement: Object,
    ) -> Result<(), StateError> {
        self.record_read(id);
        self.record_write(id);
        let existing = self.active.get(&id).ok_or(StateError::ObjectNotFound(id))?;
        if existing.version != expected_version {
            return Err(StateError::VersionMismatch {
                id,
                expected: expected_version,
                actual: existing.version,
            });
        }
        if replacement.id != id {
            return Err(StateError::ObjectIdChanged);
        }
        replacement.version = expected_version
            .checked_add(1)
            .ok_or(StateError::VersionOverflow)?;
        self.active.insert(id, replacement);
        Ok(())
    }

    /// Removes an active object and returns its final value.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing object or stale version.
    pub fn delete_object(
        &mut self,
        id: ObjectId,
        expected_version: u64,
    ) -> Result<Object, StateError> {
        self.record_read(id);
        self.record_write(id);
        let existing = self.active.get(&id).ok_or(StateError::ObjectNotFound(id))?;
        if existing.version != expected_version {
            return Err(StateError::VersionMismatch {
                id,
                expected: expected_version,
                actual: existing.version,
            });
        }
        self.active
            .remove(&id)
            .ok_or(StateError::ObjectNotFound(id))
    }

    /// Transfers an object and increments its version.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing object, stale version, or version overflow.
    pub fn transfer_object(
        &mut self,
        id: ObjectId,
        expected_version: u64,
        new_owner: Owner,
    ) -> Result<(), StateError> {
        self.record_read(id);
        self.record_write(id);
        let existing = self
            .active
            .get(&id)
            .ok_or(StateError::ObjectNotFound(id))?
            .clone();
        let mut replacement = existing;
        replacement.owner = new_owner;
        self.mutate_object(id, expected_version, replacement)
    }

    /// Adds rent credit without changing the object's version.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing object or balance overflow.
    pub fn top_up_rent(&mut self, id: ObjectId, amount: u64) -> Result<(), StateError> {
        self.record_read(id);
        self.record_write(id);
        let object = self
            .active
            .get_mut(&id)
            .ok_or(StateError::ObjectNotFound(id))?;
        object.rent_balance = object
            .rent_balance
            .checked_add(amount)
            .ok_or(StateError::RentOverflow)?;
        Ok(())
    }

    /// Advances epoch-by-epoch, charging rent and moving exhausted objects to
    /// expired state at the exact epoch where their balance reaches zero.
    ///
    /// # Errors
    ///
    /// Returns an error if epoch regresses or canonical state encoding fails.
    pub fn advance_to_epoch(&mut self, requested: Epoch) -> Result<RentOutcome, StateError> {
        if requested < self.epoch {
            return Err(StateError::EpochRegression {
                current: self.epoch,
                requested,
            });
        }

        let mut expired_ids = Vec::new();
        while self.epoch < requested {
            let last_active_root = self.active_root()?;
            let active_before_charge = self.active.materialized();
            let expiring: Vec<_> = active_before_charge
                .iter()
                .filter(|(_, object)| object.rent_balance <= self.config.rent_per_object_per_epoch)
                .map(|(id, object)| {
                    Ok((
                        *id,
                        object.clone(),
                        merkle_proof_for_objects(&active_before_charge, *id)?,
                    ))
                })
                .collect::<Result<_, StateError>>()?;
            self.epoch.0 += 1;
            let rate = self.config.rent_per_object_per_epoch;
            let active_ids = self.active.keys().copied().collect::<Vec<_>>();
            for id in active_ids {
                let object = self
                    .active
                    .get_mut(&id)
                    .ok_or(StateError::ObjectNotFound(id))?;
                object.rent_balance = object.rent_balance.saturating_sub(rate);
            }

            for (id, last_active_object, last_active_proof) in expiring {
                let object = self
                    .active
                    .remove(&id)
                    .ok_or(StateError::ObjectNotFound(id))?;
                self.expired.insert(
                    id,
                    ExpiredObject {
                        object,
                        expired_at: self.epoch,
                        last_active_root,
                        last_active_object,
                        last_active_proof,
                    },
                );
                expired_ids.push(id);
            }
        }

        Ok(RentOutcome {
            epoch: self.epoch,
            expired: expired_ids,
            state_root: self.root()?,
        })
    }

    /// Verifies and consumes a retained expiry witness, restoring the object
    /// with fresh rent and a new version so the witness cannot authorize stale writes.
    ///
    /// # Errors
    ///
    /// Rejects zero rent, invalid proofs, mismatched history, replay, or overflow.
    pub fn resurrect(
        &mut self,
        witness: &ResurrectionWitness,
        rent_credit: u64,
    ) -> Result<ResurrectionOutcome, StateError> {
        if rent_credit == 0 {
            return Err(StateError::InvalidResurrectionRent);
        }
        verify_resurrection_witness(witness)?;
        let retained = self
            .expired
            .get(&witness.object.id)
            .ok_or(StateError::ResurrectionHistoryMismatch)?;
        self.record_read(witness.object.id);
        if retained.expired_at != witness.expired_at
            || retained.last_active_root != witness.last_active_root
            || retained.last_active_object != witness.object
            || retained.last_active_proof != witness.proof
        {
            return Err(StateError::ResurrectionHistoryMismatch);
        }
        let mut object = retained.object.clone();
        object.version = object
            .version
            .checked_add(1)
            .ok_or(StateError::VersionOverflow)?;
        object.rent_balance = rent_credit;
        self.expired.remove(&object.id);
        self.active.insert(object.id, object.clone());
        self.record_write(object.id);
        Ok(ResurrectionOutcome { object })
    }

    /// Starts recording object reads and writes on this state and clones derived
    /// from it. Starting a new recording discards any previous recording.
    pub fn start_access_tracking(&mut self) {
        self.access_tracker = Some(Arc::new(Mutex::new(StateAccesses::default())));
    }

    /// Stops access recording and returns the accumulated sets.
    #[must_use]
    pub fn finish_access_tracking(&mut self) -> StateAccesses {
        let Some(tracker) = self.access_tracker.take() else {
            return StateAccesses::default();
        };
        match Arc::try_unwrap(tracker) {
            Ok(tracker) => tracker
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Err(tracker) => tracker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }

    /// Computes the exact active-object patch between two states.
    #[must_use]
    pub fn delta_from(&self, base: &Self) -> StateDelta {
        StateDelta {
            changes: self.active.delta_from(&base.active),
            expired_changes: self.expired.delta_from(&base.expired),
        }
    }

    /// Applies a previously validated speculative patch.
    pub fn apply_delta(&mut self, delta: &StateDelta) {
        for (id, object) in &delta.changes {
            self.record_write(*id);
            if let Some(object) = object {
                self.active.insert(*id, object.clone());
            } else {
                self.active.remove(id);
            }
        }
        for (id, object) in &delta.expired_changes {
            self.record_write(*id);
            if let Some(object) = object {
                self.expired.insert(*id, object.clone());
            } else {
                self.expired.remove(id);
            }
        }
    }

    /// Applies a validated speculative patch by moving its object values.
    ///
    /// This is equivalent to [`Self::apply_delta`] but avoids cloning values
    /// when the caller no longer needs the patch.
    pub fn apply_delta_owned(&mut self, delta: StateDelta) {
        for (id, object) in delta.changes {
            self.record_write(id);
            if let Some(object) = object {
                self.active.insert(id, object);
            } else {
                self.active.remove(&id);
            }
        }
        for (id, object) in delta.expired_changes {
            self.record_write(id);
            if let Some(object) = object {
                self.expired.insert(id, object);
            } else {
                self.expired.remove(&id);
            }
        }
    }

    /// Computes the active-object Merkle prefix-trie root.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical object encoding fails.
    pub fn active_root(&self) -> Result<Hash, StateError> {
        merkle_root_for_entries(self.active.entries())
    }

    /// Computes the expired-object Merkle prefix-trie root.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical expired-object encoding fails.
    pub fn expired_root(&self) -> Result<Hash, StateError> {
        merkle_root_for_entries(self.expired.entries())
    }

    /// Computes the canonical root committing to all Phase 1 state.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical value encoding fails.
    pub fn root(&self) -> Result<Hash, StateError> {
        let active = self.active_root()?;
        let expired = self.expired_root()?;
        Ok(hash_parts(&[
            ROOT_DOMAIN,
            active.as_bytes(),
            expired.as_bytes(),
            &self.epoch.0.to_be_bytes(),
            &self.config.rent_per_object_per_epoch.to_be_bytes(),
        ]))
    }

    fn record_read(&self, id: ObjectId) {
        if let Some(tracker) = &self.access_tracker {
            tracker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .reads
                .insert(id);
        }
    }

    fn record_write(&self, id: ObjectId) {
        if let Some(tracker) = &self.access_tracker {
            tracker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .writes
                .insert(id);
        }
    }
}

/// Verifies a resurrection witness using only its object, path, and historical root.
///
/// # Errors
///
/// Rejects malformed path directions/depths or a root mismatch.
pub fn verify_resurrection_witness(witness: &ResurrectionWitness) -> Result<(), StateError> {
    if witness.proof.object_id != witness.object.id {
        return Err(StateError::InvalidResurrectionWitness);
    }
    let mut current = encode_leaf(witness.object.id, &witness.object)?.1;
    let mut previous_depth = u16::MAX;
    for step in witness.proof.steps.iter().rev() {
        if step.depth >= previous_depth
            || step.sibling_on_left != bit_at(&witness.object.id, usize::from(step.depth))
        {
            return Err(StateError::InvalidResurrectionWitness);
        }
        let depth = step.depth.to_be_bytes();
        current = if step.sibling_on_left {
            hash_parts(&[
                BRANCH_DOMAIN,
                &depth,
                step.sibling.as_bytes(),
                current.as_bytes(),
            ])
        } else {
            hash_parts(&[
                BRANCH_DOMAIN,
                &depth,
                current.as_bytes(),
                step.sibling.as_bytes(),
            ])
        };
        previous_depth = step.depth;
    }
    if current == witness.last_active_root {
        Ok(())
    } else {
        Err(StateError::InvalidResurrectionWitness)
    }
}

fn encode_leaf<T: Serialize>(id: ObjectId, value: &T) -> Result<(Hash, Hash), StateError> {
    let encoded = bcs::to_bytes(value).map_err(|error| StateError::Encoding(error.to_string()))?;
    Ok((id, hash_parts(&[LEAF_DOMAIN, id.as_bytes(), &encoded])))
}

fn merkle_root_for_entries<'a, T>(
    entries: impl IntoIterator<Item = (ObjectId, &'a T)>,
) -> Result<Hash, StateError>
where
    T: Serialize + 'a,
{
    let mut encoded = Vec::new();
    let mut leaves = Vec::new();
    for (id, value) in entries {
        encoded.clear();
        bcs::serialize_into(&mut encoded, value)
            .map_err(|error| StateError::Encoding(error.to_string()))?;
        leaves.push((id, hash_parts(&[LEAF_DOMAIN, id.as_bytes(), &encoded])));
    }
    Ok(subtree_root(&leaves, 0))
}

fn merkle_proof_for_objects(
    objects: &BTreeMap<ObjectId, Object>,
    id: ObjectId,
) -> Result<MerkleProof, StateError> {
    let leaves = objects
        .iter()
        .map(|(object_id, object)| encode_leaf(*object_id, object))
        .collect::<Result<Vec<_>, _>>()?;
    let mut steps = Vec::new();
    if collect_merkle_steps(&leaves, 0, id, &mut steps) {
        Ok(MerkleProof {
            object_id: id,
            steps,
        })
    } else {
        Err(StateError::ObjectNotFound(id))
    }
}

fn collect_merkle_steps(
    leaves: &[(Hash, Hash)],
    depth: usize,
    id: ObjectId,
    steps: &mut Vec<MerkleStep>,
) -> bool {
    if leaves.is_empty() {
        return false;
    }
    if leaves.len() == 1 || depth == 256 {
        return leaves[0].0 == id;
    }
    let split = leaves.partition_point(|(key, _)| !bit_at(key, depth));
    let go_right = bit_at(&id, depth);
    let (selected, sibling) = if go_right {
        (&leaves[split..], &leaves[..split])
    } else {
        (&leaves[..split], &leaves[split..])
    };
    steps.push(MerkleStep {
        depth: u16::try_from(depth).unwrap_or(u16::MAX),
        sibling: subtree_root(sibling, depth + 1),
        sibling_on_left: go_right,
    });
    if collect_merkle_steps(selected, depth + 1, id, steps) {
        true
    } else {
        steps.pop();
        false
    }
}

fn subtree_root(leaves: &[(Hash, Hash)], depth: usize) -> Hash {
    let encoded_depth = u16::try_from(depth).unwrap_or(u16::MAX).to_be_bytes();
    if leaves.is_empty() {
        return hash_parts(&[EMPTY_DOMAIN, &encoded_depth]);
    }
    if leaves.len() == 1 || depth == 256 {
        return leaves[0].1;
    }

    let split = leaves.partition_point(|(key, _)| !bit_at(key, depth));
    let left = subtree_root(&leaves[..split], depth + 1);
    let right = subtree_root(&leaves[split..], depth + 1);
    hash_parts(&[
        BRANCH_DOMAIN,
        &encoded_depth,
        left.as_bytes(),
        right.as_bytes(),
    ])
}

fn bit_at(hash: &Hash, depth: usize) -> bool {
    let byte = hash.as_bytes()[depth / 8];
    byte & (1 << (7 - depth % 8)) != 0
}

fn hash_parts(parts: &[&[u8]]) -> Hash {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(part);
    }
    Hash::from_bytes(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use types::{Address, Epoch, Hash, Object, Owner};

    use super::{StateConfig, StateError, StateTree, verify_resurrection_witness};

    fn object(seed: u8, rent_balance: u64) -> Object {
        Object {
            id: Hash::digest([seed]),
            owner: Owner::Single(Address::from_bytes([seed; 32])),
            type_tag: "test::Object".to_owned(),
            version: 0,
            data: vec![seed],
            rent_balance,
        }
    }

    #[test]
    fn object_lifecycle_checks_versions() {
        let mut state = StateTree::new(StateConfig::default()).unwrap();
        let original = object(1, 10);
        let id = original.id;
        state.create_object(original.clone()).unwrap();

        let mut replacement = original;
        replacement.data = b"changed".to_vec();
        state.mutate_object(id, 0, replacement).unwrap();
        assert_eq!(state.object(&id).unwrap().version, 1);
        assert!(matches!(
            state.delete_object(id, 0),
            Err(StateError::VersionMismatch { .. })
        ));
        state.delete_object(id, 1).unwrap();
        assert!(state.object(&id).is_none());
    }

    #[test]
    fn access_tracking_and_delta_capture_exact_object_keys() {
        let mut base = StateTree::new(StateConfig::default()).unwrap();
        let original = object(3, 10);
        let id = original.id;
        base.create_object(original.clone()).unwrap();
        let mut candidate = base.speculative_snapshot();
        candidate.start_access_tracking();
        assert!(candidate.object(&id).is_some());
        let mut replacement = original;
        replacement.data = vec![99];
        candidate.mutate_object(id, 0, replacement).unwrap();
        let accesses = candidate.finish_access_tracking();
        assert_eq!(accesses.reads, std::collections::BTreeSet::from([id]));
        assert_eq!(accesses.writes, std::collections::BTreeSet::from([id]));
        assert_eq!(base.object(&id).unwrap().version, 0);

        let delta = candidate.delta_from(&base);
        let mut applied = base;
        applied.apply_delta(&delta);
        assert_eq!(applied.root().unwrap(), candidate.root().unwrap());
    }

    #[test]
    fn object_expires_at_exact_epoch_and_is_retained() {
        let mut state = StateTree::new(StateConfig {
            rent_per_object_per_epoch: 2,
        })
        .unwrap();
        let object = object(9, 5);
        let id = object.id;
        state.create_object(object).unwrap();

        assert!(state.advance_to_epoch(Epoch(2)).unwrap().expired.is_empty());
        assert_eq!(state.object(&id).unwrap().rent_balance, 1);
        let outcome = state.advance_to_epoch(Epoch(3)).unwrap();
        assert_eq!(outcome.expired, vec![id]);
        assert!(state.object(&id).is_none());
        let expired = state.expired_object(&id).unwrap();
        assert_eq!(expired.expired_at, Epoch(3));
        assert_eq!(expired.object.rent_balance, 0);
    }

    #[test]
    fn expired_object_resurrects_from_a_statelessly_verified_witness() {
        let mut state = StateTree::new(StateConfig {
            rent_per_object_per_epoch: 2,
        })
        .unwrap();
        let expiring = object(10, 2);
        let surviving = object(11, 20);
        let id = expiring.id;
        state.create_object(expiring).unwrap();
        state.create_object(surviving).unwrap();
        let before_expiry = state.active_root().unwrap();
        state.advance_to_epoch(Epoch(1)).unwrap();
        let expired_root = state.root().unwrap();
        let mut replay = state.clone();

        let witness = state.resurrection_witness(&id).unwrap();
        assert_eq!(witness.last_active_root, before_expiry);
        verify_resurrection_witness(&witness).unwrap();
        let resurrected = state.resurrect(&witness, 10).unwrap();
        assert_eq!(resurrected.object.version, 1);
        assert_eq!(resurrected.object.rent_balance, 10);

        let mut replacement = resurrected.object.clone();
        replacement.data = b"mutated after resurrection".to_vec();
        state.mutate_object(id, 1, replacement).unwrap();
        assert_eq!(state.object(&id).unwrap().version, 2);
        assert_ne!(state.root().unwrap(), expired_root);

        let replay_resurrected = replay.resurrect(&witness, 10).unwrap();
        let mut replay_replacement = replay_resurrected.object;
        replay_replacement.data = b"mutated after resurrection".to_vec();
        replay.mutate_object(id, 1, replay_replacement).unwrap();
        assert_eq!(state.root().unwrap(), replay.root().unwrap());
        assert_eq!(
            state.resurrect(&witness, 10),
            Err(StateError::ResurrectionHistoryMismatch)
        );
    }

    #[test]
    fn stateless_verifier_rejects_tampered_resurrection_witness() {
        let mut state = StateTree::new(StateConfig::default()).unwrap();
        let expiring = object(12, 1);
        let id = expiring.id;
        state.create_object(expiring).unwrap();
        state.advance_to_epoch(Epoch(1)).unwrap();
        let mut witness = state.resurrection_witness(&id).unwrap();
        witness.object.data.push(99);
        assert_eq!(
            verify_resurrection_witness(&witness),
            Err(StateError::InvalidResurrectionWitness)
        );
    }

    #[test]
    fn durable_snapshot_restores_exact_root_and_rejects_tampering() {
        let mut state = StateTree::new(StateConfig::default()).unwrap();
        state.create_object(object(20, 10)).unwrap();
        state.create_object(object(21, 1)).unwrap();
        state.advance_to_epoch(Epoch(1)).unwrap();
        let snapshot = state.durable_snapshot().unwrap();
        let restored = StateTree::from_durable_snapshot(snapshot.clone()).unwrap();
        assert_eq!(restored.root().unwrap(), state.root().unwrap());
        assert_eq!(restored.epoch(), Epoch(1));

        let mut tampered = snapshot;
        tampered.state_root = Hash::digest(b"tampered checkpoint");
        assert_eq!(
            StateTree::from_durable_snapshot(tampered).unwrap_err(),
            StateError::SnapshotRootMismatch
        );
    }

    #[test]
    fn resurrection_witness_remains_small_with_many_objects() {
        let mut state = StateTree::new(StateConfig::default()).unwrap();
        for seed in 0_u16..256 {
            let mut value = object(u8::try_from(seed).unwrap(), 100);
            value.id = Hash::digest(seed.to_be_bytes());
            if seed == 137 {
                value.rent_balance = 1;
            }
            state.create_object(value).unwrap();
        }
        state.advance_to_epoch(Epoch(1)).unwrap();
        let id = Hash::digest(137_u16.to_be_bytes());
        let witness = state.resurrection_witness(&id).unwrap();
        verify_resurrection_witness(&witness).unwrap();
        assert!(witness.proof.steps.len() < 32);
        assert!(bcs::to_bytes(&witness).unwrap().len() < 2_048);
    }

    proptest! {
        #[test]
        fn insertion_order_does_not_change_root(seeds in prop::collection::btree_set(any::<u8>(), 0..64)) {
            let mut forward = StateTree::new(StateConfig::default()).unwrap();
            let mut reverse = StateTree::new(StateConfig::default()).unwrap();
            let objects: Vec<_> = seeds.into_iter().map(|seed| object(seed, 10)).collect();
            for value in &objects {
                forward.create_object(value.clone()).unwrap();
            }
            for value in objects.iter().rev() {
                reverse.create_object(value.clone()).unwrap();
            }
            prop_assert_eq!(forward.root().unwrap(), reverse.root().unwrap());
        }
    }
}
