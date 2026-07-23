//! Localized fee ordering and deterministic application sequencing hooks.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, VecDeque, btree_map::Entry},
    sync::Arc,
};

use thiserror::Error;
use types::{Address, Hash, ObjectId};

/// Congestion and ordering are isolated to one object or one sender account.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FeeScope {
    Object(ObjectId),
    Account(Address),
}

/// Transaction metadata admitted by the localized market.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmittedTransaction {
    pub id: Hash,
    pub sender: Address,
    pub scope: FeeScope,
    pub touched_objects: BTreeSet<ObjectId>,
    pub compute_limit: u64,
    pub max_fee_per_compute: u128,
    pub priority_fee_per_compute: u128,
    pub arrival_sequence: u64,
    pub policy_data: Vec<u8>,
}

/// Accepted transaction with the local base price fixed at admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingTransaction {
    pub transaction: SubmittedTransaction,
    pub local_base_fee_per_compute: u128,
}

impl PendingTransaction {
    /// Extracts just the fields [`FeeLedger::settle`] needs.
    #[must_use]
    pub fn settlement(&self) -> Settlement {
        Settlement {
            transaction_id: self.transaction.id,
            payer: self.transaction.sender,
            compute_limit: self.transaction.compute_limit,
            local_base_fee_per_compute: self.local_base_fee_per_compute,
            priority_fee_per_compute: self.transaction.priority_fee_per_compute,
        }
    }
}

/// Application-supplied deterministic ordering rule for one target scope.
pub trait OrderingPolicy: Send + Sync {
    /// Returns the preferred order. Implementations must be total and deterministic.
    fn compare(&self, left: &PendingTransaction, right: &PendingTransaction) -> Ordering;
}

/// Default priority order: higher tip first, then canonical arrival and ID.
#[derive(Clone, Copy, Debug, Default)]
pub struct PriorityFeePolicy;

impl OrderingPolicy for PriorityFeePolicy {
    fn compare(&self, left: &PendingTransaction, right: &PendingTransaction) -> Ordering {
        right
            .transaction
            .priority_fee_per_compute
            .cmp(&left.transaction.priority_fee_per_compute)
            .then_with(|| {
                left.transaction
                    .arrival_sequence
                    .cmp(&right.transaction.arrival_sequence)
            })
            .then_with(|| left.transaction.id.cmp(&right.transaction.id))
    }
}

/// Admission quote exposing only the transaction's own congestion scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeeQuote {
    pub local_base_fee_per_compute: u128,
    pub effective_fee_per_compute: u128,
}

/// Fair block selection result and deterministic work counter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockSelection {
    pub transactions: Vec<PendingTransaction>,
    pub scope_visits: usize,
}

/// Object/account-local queues with per-scope capacity in each block.
#[derive(Clone)]
pub struct LocalizedMempool {
    base_fee_per_compute: u128,
    congestion_increment: u128,
    per_scope_block_limit: usize,
    queues: BTreeMap<FeeScope, VecDeque<PendingTransaction>>,
    policies: BTreeMap<FeeScope, Arc<dyn OrderingPolicy>>,
    transaction_ids: BTreeSet<Hash>,
}

impl LocalizedMempool {
    /// Creates a localized market.
    ///
    /// # Errors
    ///
    /// Rejects a zero per-scope block limit.
    pub fn new(
        base_fee_per_compute: u128,
        congestion_increment: u128,
        per_scope_block_limit: usize,
    ) -> Result<Self, MempoolError> {
        if per_scope_block_limit == 0 {
            return Err(MempoolError::ZeroScopeLimit);
        }
        Ok(Self {
            base_fee_per_compute,
            congestion_increment,
            per_scope_block_limit,
            queues: BTreeMap::new(),
            policies: BTreeMap::new(),
            transaction_ids: BTreeSet::new(),
        })
    }

    /// Registers one application ordering hook for its object/account scope.
    ///
    /// # Errors
    ///
    /// Rejects replacement so policy changes require an explicit epoch transition.
    pub fn register_policy(
        &mut self,
        scope: FeeScope,
        policy: Arc<dyn OrderingPolicy>,
    ) -> Result<(), MempoolError> {
        match self.policies.entry(scope) {
            Entry::Vacant(entry) => {
                entry.insert(policy);
                Ok(())
            }
            Entry::Occupied(_) => Err(MempoolError::PolicyAlreadyRegistered),
        }
    }

    /// Prices and inserts a transaction only within its declared local scope.
    ///
    /// # Errors
    ///
    /// Rejects invalid scope declarations, duplicate IDs, zero compute limits,
    /// overflow, or a max price below the local base plus priority fee.
    pub fn submit(&mut self, transaction: SubmittedTransaction) -> Result<FeeQuote, MempoolError> {
        validate_scope(&transaction)?;
        if transaction.compute_limit == 0 {
            return Err(MempoolError::ZeroComputeLimit);
        }
        if self.transaction_ids.contains(&transaction.id) {
            return Err(MempoolError::DuplicateTransaction);
        }
        let depth = self.queues.get(&transaction.scope).map_or(0, VecDeque::len);
        let depth = u128::try_from(depth).map_err(|_| MempoolError::FeeOverflow)?;
        let local_base_fee_per_compute = self
            .congestion_increment
            .checked_mul(depth)
            .and_then(|increment| self.base_fee_per_compute.checked_add(increment))
            .ok_or(MempoolError::FeeOverflow)?;
        let effective_fee_per_compute = local_base_fee_per_compute
            .checked_add(transaction.priority_fee_per_compute)
            .ok_or(MempoolError::FeeOverflow)?;
        if transaction.max_fee_per_compute < effective_fee_per_compute {
            return Err(MempoolError::FeeCapTooLow {
                required: effective_fee_per_compute,
                offered: transaction.max_fee_per_compute,
            });
        }
        self.transaction_ids.insert(transaction.id);
        let pending = PendingTransaction {
            transaction,
            local_base_fee_per_compute,
        };
        let scope = pending.transaction.scope;
        let policy = self
            .policies
            .get(&scope)
            .map_or(&PriorityFeePolicy as &dyn OrderingPolicy, Arc::as_ref);
        let queue = self.queues.entry(scope).or_default();
        let position = queue
            .make_contiguous()
            .binary_search_by(|existing| policy.compare(existing, &pending))
            .unwrap_or_else(|position| position);
        queue.insert(position, pending);
        Ok(FeeQuote {
            local_base_fee_per_compute,
            effective_fee_per_compute,
        })
    }

    /// Selects scopes round-robin and caps each scope's contribution.
    ///
    /// A hot queue therefore cannot increase the number of queue visits before
    /// an unrelated scope is considered.
    #[must_use]
    pub fn select_block(&mut self, maximum_transactions: usize) -> BlockSelection {
        let scopes = self.queues.keys().copied().collect::<Vec<_>>();
        let mut selected = Vec::new();
        let mut per_scope = BTreeMap::<FeeScope, usize>::new();
        let mut scope_visits = 0;
        while selected.len() < maximum_transactions {
            let mut progressed = false;
            for scope in &scopes {
                if selected.len() == maximum_transactions {
                    break;
                }
                scope_visits += 1;
                let count = per_scope.entry(*scope).or_default();
                if *count >= self.per_scope_block_limit {
                    continue;
                }
                let Some(queue) = self.queues.get_mut(scope) else {
                    continue;
                };
                if queue.is_empty() {
                    continue;
                }
                let Some(pending) = queue.pop_front() else {
                    continue;
                };
                self.transaction_ids.remove(&pending.transaction.id);
                selected.push(pending);
                *count += 1;
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
        self.queues.retain(|_, queue| !queue.is_empty());
        BlockSelection {
            transactions: selected,
            scope_visits,
        }
    }

    /// Previews canonical transaction IDs without mutating queue state.
    #[must_use]
    pub fn preview_block(&self, maximum_transactions: usize) -> Vec<Hash> {
        let mut positions = self
            .queues
            .keys()
            .copied()
            .map(|scope| (scope, 0_usize))
            .collect::<BTreeMap<_, _>>();
        let mut selected = Vec::new();
        while selected.len() < maximum_transactions {
            let mut progressed = false;
            for (scope, position) in &mut positions {
                if selected.len() == maximum_transactions {
                    break;
                }
                if *position >= self.per_scope_block_limit {
                    continue;
                }
                let Some(pending) = self.queues[scope].get(*position) else {
                    continue;
                };
                selected.push(pending.transaction.id);
                *position += 1;
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
        selected
    }

    /// Removes specific transactions after another leader's canonical block is
    /// finalized or a local reservation is invalidated.
    pub fn remove_transactions(&mut self, transaction_ids: &BTreeSet<Hash>) -> usize {
        let before = self.transaction_ids.len();
        for queue in self.queues.values_mut() {
            queue.retain(|pending| !transaction_ids.contains(&pending.transaction.id));
        }
        self.queues.retain(|_, queue| !queue.is_empty());
        for id in transaction_ids {
            self.transaction_ids.remove(id);
        }
        before.saturating_sub(self.transaction_ids.len())
    }

    #[must_use]
    pub fn scope_depth(&self, scope: FeeScope) -> usize {
        self.queues.get(&scope).map_or(0, VecDeque::len)
    }
}

/// Conserved fee balances. Every charged unit is credited to the validator.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FeeLedger {
    balances: BTreeMap<Address, u128>,
}

impl FeeLedger {
    /// Restores a ledger from durably persisted or genesis-seeded balances.
    #[must_use]
    pub const fn from_balances(balances: BTreeMap<Address, u128>) -> Self {
        Self { balances }
    }

    /// Returns the full balance table for durable persistence.
    #[must_use]
    pub fn balances(&self) -> &BTreeMap<Address, u128> {
        &self.balances
    }

    /// Credits `amount` to `address`.
    ///
    /// # Errors
    ///
    /// Returns [`MempoolError::FeeOverflow`] if the resulting balance exceeds
    /// `u128::MAX`.
    pub fn credit(&mut self, address: Address, amount: u128) -> Result<(), MempoolError> {
        let balance = self.balances.entry(address).or_default();
        *balance = balance
            .checked_add(amount)
            .ok_or(MempoolError::FeeOverflow)?;
        Ok(())
    }

    #[must_use]
    pub fn balance(&self, address: Address) -> u128 {
        self.balances.get(&address).copied().unwrap_or_default()
    }

    /// Charges actual—not reserved—compute and transfers the full fee to the validator.
    ///
    /// # Errors
    ///
    /// Rejects excess compute, arithmetic overflow, or insufficient payer balance.
    pub fn settle(
        &mut self,
        settlement: &Settlement,
        actual_compute: u64,
        validator: Address,
    ) -> Result<FeeReceipt, MempoolError> {
        if actual_compute > settlement.compute_limit {
            return Err(MempoolError::ComputeLimitExceeded);
        }
        let unit_price = settlement
            .local_base_fee_per_compute
            .checked_add(settlement.priority_fee_per_compute)
            .ok_or(MempoolError::FeeOverflow)?;
        let charged = unit_price
            .checked_mul(u128::from(actual_compute))
            .ok_or(MempoolError::FeeOverflow)?;
        let payer = settlement.payer;
        let payer_balance = self
            .balances
            .get(&payer)
            .copied()
            .ok_or(MempoolError::InsufficientBalance)?;
        let debited = payer_balance
            .checked_sub(charged)
            .ok_or(MempoolError::InsufficientBalance)?;
        if payer != validator {
            let validator_balance = self.balance(validator);
            let credited = validator_balance
                .checked_add(charged)
                .ok_or(MempoolError::FeeOverflow)?;
            self.balances.insert(payer, debited);
            self.balances.insert(validator, credited);
        }
        Ok(FeeReceipt {
            transaction_id: settlement.transaction_id,
            payer,
            validator,
            actual_compute,
            unit_price,
            charged,
        })
    }
}

/// Minimal per-transaction data needed to settle a metered fee, decoupled from
/// [`PendingTransaction`] (the mempool's own admission-scoped record) so
/// callers that never build one — e.g. `node::BlockLifecycle`, which learns
/// the certified base fee from a committed block rather than from live
/// mempool admission — can settle directly from the fields this actually
/// uses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Settlement {
    pub transaction_id: Hash,
    pub payer: Address,
    pub compute_limit: u64,
    pub local_base_fee_per_compute: u128,
    pub priority_fee_per_compute: u128,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeeReceipt {
    pub transaction_id: Hash,
    pub payer: Address,
    pub validator: Address,
    pub actual_compute: u64,
    pub unit_price: u128,
    pub charged: u128,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MempoolError {
    #[error("per-scope block limit must be nonzero")]
    ZeroScopeLimit,
    #[error("object-scoped transaction must declare that object as touched")]
    InvalidScope,
    #[error("compute limit must be nonzero")]
    ZeroComputeLimit,
    #[error("transaction already exists")]
    DuplicateTransaction,
    #[error("application ordering policy is already registered for this scope")]
    PolicyAlreadyRegistered,
    #[error("fee arithmetic overflow")]
    FeeOverflow,
    #[error("fee cap too low: required {required}, offered {offered}")]
    FeeCapTooLow { required: u128, offered: u128 },
    #[error("actual compute exceeded the transaction limit")]
    ComputeLimitExceeded,
    #[error("payer has insufficient balance")]
    InsufficientBalance,
}

fn validate_scope(transaction: &SubmittedTransaction) -> Result<(), MempoolError> {
    if let FeeScope::Object(object) = transaction.scope
        && !transaction.touched_objects.contains(&object)
    {
        return Err(MempoolError::InvalidScope);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, collections::BTreeSet, sync::Arc};

    use types::{Address, Hash};

    use super::{
        FeeLedger, FeeScope, LocalizedMempool, MempoolError, OrderingPolicy, PendingTransaction,
        PriorityFeePolicy, SubmittedTransaction,
    };

    struct ApplicationSequence;

    impl OrderingPolicy for ApplicationSequence {
        fn compare(&self, left: &PendingTransaction, right: &PendingTransaction) -> Ordering {
            left.transaction
                .policy_data
                .cmp(&right.transaction.policy_data)
                .then_with(|| left.transaction.id.cmp(&right.transaction.id))
        }
    }

    #[test]
    fn hot_object_does_not_delay_unrelated_scope() {
        let hot = Hash::digest(b"hot");
        let cold = Hash::digest(b"cold");
        let mut pool = LocalizedMempool::new(1, 1, 4).unwrap();
        for index in 0_u64..1_000 {
            pool.submit(transaction(index, hot, 1)).unwrap();
        }
        let cold_quote = pool.submit(transaction(2_000, cold, 1)).unwrap();
        assert_eq!(cold_quote.local_base_fee_per_compute, 1);
        let selection = pool.select_block(8);
        let cold_position = selection
            .transactions
            .iter()
            .position(|pending| pending.transaction.scope == FeeScope::Object(cold))
            .unwrap();
        assert!(cold_position <= 1);
        assert_eq!(selection.scope_visits, 10);
        assert_eq!(pool.scope_depth(FeeScope::Object(cold)), 0);
        assert_eq!(pool.scope_depth(FeeScope::Object(hot)), 996);
    }

    #[test]
    fn application_policy_overrides_fee_order_only_for_its_scope() {
        let object = Hash::digest(b"application");
        let mut pool = LocalizedMempool::new(1, 0, 10).unwrap();
        pool.register_policy(FeeScope::Object(object), Arc::new(ApplicationSequence))
            .unwrap();
        let mut later = transaction(1, object, 100);
        later.policy_data = vec![2];
        let mut earlier = transaction(2, object, 1);
        earlier.policy_data = vec![1];
        pool.submit(later).unwrap();
        pool.submit(earlier).unwrap();
        let selected = pool.select_block(2);
        assert_eq!(selected.transactions[0].transaction.policy_data, vec![1]);
    }

    #[test]
    fn rejected_policy_replacement_preserves_the_registered_policy() {
        let object = Hash::digest(b"immutable-application-policy");
        let scope = FeeScope::Object(object);
        let mut pool = LocalizedMempool::new(1, 0, 10).unwrap();
        pool.register_policy(scope, Arc::new(ApplicationSequence))
            .unwrap();
        assert_eq!(
            pool.register_policy(scope, Arc::new(PriorityFeePolicy)),
            Err(MempoolError::PolicyAlreadyRegistered)
        );

        let mut first_by_application = transaction(1, object, 1);
        first_by_application.policy_data = vec![1];
        let mut first_by_fee = transaction(2, object, 100);
        first_by_fee.policy_data = vec![2];
        pool.submit(first_by_fee).unwrap();
        pool.submit(first_by_application).unwrap();
        let selected = pool.select_block(2);
        assert_eq!(selected.transactions[0].transaction.policy_data, vec![1]);
    }

    #[test]
    fn actual_compute_fee_is_fully_transferred_without_burn() {
        let object = Hash::digest(b"fees");
        let payer = Address::from_bytes([1; 32]);
        let validator = Address::from_bytes([2; 32]);
        let mut pool = LocalizedMempool::new(2, 0, 10).unwrap();
        let pending = pool.submit_and_select(transaction_with_sender(1, object, 3, payer));
        let mut ledger = FeeLedger::default();
        ledger.credit(payer, 1_000).unwrap();
        let receipt = ledger.settle(&pending.settlement(), 10, validator).unwrap();
        assert_eq!(receipt.unit_price, 5);
        assert_eq!(receipt.charged, 50);
        assert_eq!(ledger.balance(payer), 950);
        assert_eq!(ledger.balance(validator), 50);
        assert_eq!(ledger.balance(payer) + ledger.balance(validator), 1_000);
    }

    #[test]
    fn failed_validator_credit_does_not_partially_debit_payer() {
        let object = Hash::digest(b"atomic-fees");
        let payer = Address::from_bytes([3; 32]);
        let validator = Address::from_bytes([4; 32]);
        let mut pool = LocalizedMempool::new(2, 0, 10).unwrap();
        let pending = pool.submit_and_select(transaction_with_sender(1, object, 3, payer));
        let mut ledger = FeeLedger::default();
        ledger.credit(payer, 1_000).unwrap();
        ledger.credit(validator, u128::MAX).unwrap();

        assert_eq!(
            ledger.settle(&pending.settlement(), 10, validator),
            Err(MempoolError::FeeOverflow)
        );
        assert_eq!(ledger.balance(payer), 1_000);
        assert_eq!(ledger.balance(validator), u128::MAX);
    }

    impl LocalizedMempool {
        fn submit_and_select(&mut self, transaction: SubmittedTransaction) -> PendingTransaction {
            self.submit(transaction).unwrap();
            self.select_block(1).transactions.remove(0)
        }
    }

    fn transaction(index: u64, object: Hash, priority: u128) -> SubmittedTransaction {
        transaction_with_sender(index, object, priority, Address::from_bytes([9; 32]))
    }

    fn transaction_with_sender(
        index: u64,
        object: Hash,
        priority: u128,
        sender: Address,
    ) -> SubmittedTransaction {
        SubmittedTransaction {
            id: Hash::digest(index.to_be_bytes()),
            sender,
            scope: FeeScope::Object(object),
            touched_objects: BTreeSet::from([object]),
            compute_limit: 100,
            max_fee_per_compute: 10_000,
            priority_fee_per_compute: priority,
            arrival_sequence: index,
            policy_data: Vec::new(),
        }
    }
}
