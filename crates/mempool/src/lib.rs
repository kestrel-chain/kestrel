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
    reservations: BTreeMap<Hash, FeeReservation>,
    reserved_by_payer: BTreeMap<Address, u128>,
    reserved_validator_credits: BTreeMap<Address, u128>,
}

impl FeeLedger {
    /// Restores a ledger from durably persisted or genesis-seeded balances.
    #[must_use]
    pub const fn from_balances(balances: BTreeMap<Address, u128>) -> Self {
        Self {
            balances,
            reservations: BTreeMap::new(),
            reserved_by_payer: BTreeMap::new(),
            reserved_validator_credits: BTreeMap::new(),
        }
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

    /// Returns funds not locked by admitted transactions.
    #[must_use]
    pub fn available_balance(&self, address: Address) -> u128 {
        self.balance(address).saturating_sub(
            self.reserved_by_payer
                .get(&address)
                .copied()
                .unwrap_or_default(),
        )
    }

    /// Locks the maximum amount the signed envelope permits the transaction to
    /// pay. Repeating the identical reservation is idempotent so gossip,
    /// reconstructed payloads, and restart replay can converge safely.
    ///
    /// # Errors
    ///
    /// Rejects arithmetic overflow, conflicting reuse of a transaction ID, or
    /// a payer whose unreserved balance cannot cover the maximum charge.
    pub fn reserve_maximum(
        &mut self,
        transaction_id: Hash,
        payer: Address,
        compute_limit: u64,
        max_fee_per_compute: u128,
    ) -> Result<(), MempoolError> {
        let amount = max_fee_per_compute
            .checked_mul(u128::from(compute_limit))
            .ok_or(MempoolError::FeeOverflow)?;
        let reservation = FeeReservation {
            transaction_id,
            payer,
            compute_limit,
            max_fee_per_compute,
            amount,
            settlement: None,
        };
        if let Some(existing) = self.reservations.get(&transaction_id) {
            return if existing.payer == payer
                && existing.compute_limit == compute_limit
                && existing.max_fee_per_compute == max_fee_per_compute
            {
                Ok(())
            } else {
                Err(MempoolError::ReservationMismatch)
            };
        }
        let already_reserved = self
            .reserved_by_payer
            .get(&payer)
            .copied()
            .unwrap_or_default();
        let total_reserved = already_reserved
            .checked_add(amount)
            .ok_or(MempoolError::FeeOverflow)?;
        if total_reserved > self.balance(payer) {
            return Err(MempoolError::InsufficientBalance);
        }
        self.reserved_by_payer.insert(payer, total_reserved);
        self.reservations.insert(transaction_id, reservation);
        Ok(())
    }

    /// Binds an existing maximum reservation to the certified unit price and
    /// block leader. Reserving the leader's worst-case credit here guarantees
    /// that settlement cannot overflow after execution.
    ///
    /// # Errors
    ///
    /// Rejects missing/mismatched reservations, a certified price above the
    /// signed cap, or insufficient credit capacity at the validator.
    pub fn bind_settlement(
        &mut self,
        settlement: Settlement,
        validator: Address,
    ) -> Result<(), MempoolError> {
        let unit_price = settlement
            .local_base_fee_per_compute
            .checked_add(settlement.priority_fee_per_compute)
            .ok_or(MempoolError::FeeOverflow)?;
        let maximum_charge = unit_price
            .checked_mul(u128::from(settlement.compute_limit))
            .ok_or(MempoolError::FeeOverflow)?;
        let reservation = self
            .reservations
            .get(&settlement.transaction_id)
            .ok_or(MempoolError::MissingReservation)?;
        if reservation.payer != settlement.payer
            || reservation.compute_limit != settlement.compute_limit
            || unit_price > reservation.max_fee_per_compute
            || maximum_charge > reservation.amount
        {
            return Err(MempoolError::ReservationMismatch);
        }
        let binding = SettlementBinding {
            settlement,
            validator,
            maximum_charge,
        };
        if let Some(existing) = reservation.settlement {
            return if existing == binding {
                Ok(())
            } else {
                Err(MempoolError::ReservationMismatch)
            };
        }
        if settlement.payer != validator {
            let reserved_credit = self
                .reserved_validator_credits
                .get(&validator)
                .copied()
                .unwrap_or_default();
            let total_credit = reserved_credit
                .checked_add(maximum_charge)
                .ok_or(MempoolError::FeeOverflow)?;
            self.balance(validator)
                .checked_add(total_credit)
                .ok_or(MempoolError::FeeOverflow)?;
            self.reserved_validator_credits
                .insert(validator, total_credit);
        }
        let reservation = self
            .reservations
            .get_mut(&settlement.transaction_id)
            .ok_or(MempoolError::MissingReservation)?;
        reservation.settlement = Some(binding);
        Ok(())
    }

    /// Releases a noncanonical admission and makes its maximum charge available
    /// to the payer again.
    ///
    /// # Errors
    ///
    /// Returns an accounting mismatch if an internal aggregate reservation no
    /// longer contains the amount recorded for this transaction.
    pub fn release(&mut self, transaction_id: Hash) -> Result<bool, MempoolError> {
        let Some(reservation) = self.reservations.remove(&transaction_id) else {
            return Ok(false);
        };
        subtract_accounting(
            &mut self.reserved_by_payer,
            reservation.payer,
            reservation.amount,
        )?;
        if let Some(binding) = reservation.settlement
            && binding.validator != reservation.payer
        {
            subtract_accounting(
                &mut self.reserved_validator_credits,
                binding.validator,
                binding.maximum_charge,
            )?;
        }
        Ok(true)
    }

    /// Charges actual compute from a prior reservation, credits the certified
    /// validator, and releases the unused maximum back to the payer.
    ///
    /// # Errors
    ///
    /// Rejects a missing/mismatched reservation or an executor result above the
    /// signed compute limit. Arithmetic and balance failures are prevented by
    /// `reserve_maximum` plus `bind_settlement`.
    pub fn settle(
        &mut self,
        settlement: &Settlement,
        actual_compute: u64,
        validator: Address,
    ) -> Result<FeeReceipt, MempoolError> {
        if actual_compute > settlement.compute_limit {
            return Err(MempoolError::ComputeLimitExceeded);
        }
        let reservation = self
            .reservations
            .get(&settlement.transaction_id)
            .copied()
            .ok_or(MempoolError::MissingReservation)?;
        let binding = reservation
            .settlement
            .ok_or(MempoolError::MissingSettlementBinding)?;
        if binding.settlement != *settlement || binding.validator != validator {
            return Err(MempoolError::ReservationMismatch);
        }
        let unit_price = settlement
            .local_base_fee_per_compute
            .checked_add(settlement.priority_fee_per_compute)
            .ok_or(MempoolError::FeeOverflow)?;
        let charged = unit_price
            .checked_mul(u128::from(actual_compute))
            .ok_or(MempoolError::FeeOverflow)?;
        if charged > reservation.amount || charged > binding.maximum_charge {
            return Err(MempoolError::ReservationMismatch);
        }
        let payer = settlement.payer;
        let payer_balance = self.balance(payer);
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
        self.release(settlement.transaction_id)?;
        Ok(FeeReceipt {
            transaction_id: settlement.transaction_id,
            payer,
            validator,
            actual_compute,
            unit_price,
            charged,
            reserved: reservation.amount,
            refunded: reservation.amount - charged,
        })
    }
}

fn subtract_accounting(
    accounting: &mut BTreeMap<Address, u128>,
    address: Address,
    amount: u128,
) -> Result<(), MempoolError> {
    // Zero-priced transactions create valid zero-value reservations. Multiple
    // such reservations may share an address without creating positive
    // aggregate accounting, so releasing one must not require a map entry that
    // another zero-value release may already have removed.
    if amount == 0 {
        return Ok(());
    }
    let current = accounting
        .get(&address)
        .copied()
        .ok_or(MempoolError::ReservationMismatch)?;
    let remaining = current
        .checked_sub(amount)
        .ok_or(MempoolError::ReservationMismatch)?;
    if remaining == 0 {
        accounting.remove(&address);
    } else {
        accounting.insert(address, remaining);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FeeReservation {
    transaction_id: Hash,
    payer: Address,
    compute_limit: u64,
    max_fee_per_compute: u128,
    amount: u128,
    settlement: Option<SettlementBinding>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SettlementBinding {
    settlement: Settlement,
    validator: Address,
    maximum_charge: u128,
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
    pub reserved: u128,
    pub refunded: u128,
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
    #[error("fee reservation is missing")]
    MissingReservation,
    #[error("fee reservation is not yet bound to a certified settlement")]
    MissingSettlementBinding,
    #[error("fee reservation does not match the transaction or certified settlement")]
    ReservationMismatch,
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
        PriorityFeePolicy, Settlement, SubmittedTransaction,
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
        let mut transaction = transaction_with_sender(1, object, 3, payer);
        transaction.max_fee_per_compute = 10;
        let pending = pool.submit_and_select(transaction);
        let mut ledger = FeeLedger::default();
        ledger.credit(payer, 1_000).unwrap();
        let settlement = pending.settlement();
        ledger
            .reserve_maximum(
                settlement.transaction_id,
                payer,
                settlement.compute_limit,
                10,
            )
            .unwrap();
        assert_eq!(ledger.available_balance(payer), 0);
        ledger.bind_settlement(settlement, validator).unwrap();
        let receipt = ledger.settle(&settlement, 10, validator).unwrap();
        assert_eq!(receipt.unit_price, 5);
        assert_eq!(receipt.charged, 50);
        assert_eq!(receipt.reserved, 1_000);
        assert_eq!(receipt.refunded, 950);
        assert_eq!(ledger.balance(payer), 950);
        assert_eq!(ledger.available_balance(payer), 950);
        assert_eq!(ledger.balance(validator), 50);
        assert_eq!(ledger.balance(payer) + ledger.balance(validator), 1_000);
    }

    #[test]
    fn validator_credit_overflow_is_rejected_before_execution() {
        let object = Hash::digest(b"atomic-fees");
        let payer = Address::from_bytes([3; 32]);
        let validator = Address::from_bytes([4; 32]);
        let mut pool = LocalizedMempool::new(2, 0, 10).unwrap();
        let mut transaction = transaction_with_sender(1, object, 3, payer);
        transaction.max_fee_per_compute = 10;
        let pending = pool.submit_and_select(transaction);
        let mut ledger = FeeLedger::default();
        ledger.credit(payer, 1_000).unwrap();
        ledger.credit(validator, u128::MAX).unwrap();
        let settlement = pending.settlement();
        ledger
            .reserve_maximum(
                settlement.transaction_id,
                payer,
                settlement.compute_limit,
                10,
            )
            .unwrap();

        assert_eq!(
            ledger.bind_settlement(settlement, validator),
            Err(MempoolError::FeeOverflow)
        );
        assert_eq!(ledger.balance(payer), 1_000);
        assert_eq!(ledger.available_balance(payer), 0);
        assert_eq!(ledger.balance(validator), u128::MAX);
    }

    #[test]
    fn competing_reservations_cannot_spend_the_same_balance() {
        let payer = Address::from_bytes([5; 32]);
        let first = Hash::digest(b"first");
        let second = Hash::digest(b"second");
        let mut ledger = FeeLedger::default();
        ledger.credit(payer, 100).unwrap();

        ledger.reserve_maximum(first, payer, 10, 6).unwrap();
        assert_eq!(ledger.available_balance(payer), 40);
        assert_eq!(
            ledger.reserve_maximum(second, payer, 10, 5),
            Err(MempoolError::InsufficientBalance)
        );
        ledger.release(first).unwrap();
        assert_eq!(ledger.available_balance(payer), 100);
        ledger.reserve_maximum(second, payer, 10, 5).unwrap();
    }

    #[test]
    fn reservation_identity_is_idempotent_but_cannot_be_rebound() {
        let payer = Address::from_bytes([6; 32]);
        let transaction = Hash::digest(b"idempotent");
        let mut ledger = FeeLedger::default();
        ledger.credit(payer, 100).unwrap();

        ledger.reserve_maximum(transaction, payer, 10, 5).unwrap();
        ledger.reserve_maximum(transaction, payer, 10, 5).unwrap();
        assert_eq!(ledger.available_balance(payer), 50);
        assert_eq!(
            ledger.reserve_maximum(transaction, payer, 10, 6),
            Err(MempoolError::ReservationMismatch)
        );
    }

    #[test]
    fn multiple_zero_price_reservations_settle_independently() {
        let payer = Address::from_bytes([7; 32]);
        let validator = Address::from_bytes([8; 32]);
        let mut ledger = FeeLedger::default();

        for label in [b"zero-one".as_slice(), b"zero-two".as_slice()] {
            let transaction_id = Hash::digest(label);
            let settlement = Settlement {
                transaction_id,
                payer,
                compute_limit: 10,
                local_base_fee_per_compute: 0,
                priority_fee_per_compute: 0,
            };
            ledger
                .reserve_maximum(transaction_id, payer, 10, 0)
                .unwrap();
            ledger.bind_settlement(settlement, validator).unwrap();
        }
        for label in [b"zero-one".as_slice(), b"zero-two".as_slice()] {
            let settlement = Settlement {
                transaction_id: Hash::digest(label),
                payer,
                compute_limit: 10,
                local_base_fee_per_compute: 0,
                priority_fee_per_compute: 0,
            };
            let receipt = ledger.settle(&settlement, 5, validator).unwrap();
            assert_eq!(receipt.charged, 0);
        }
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
