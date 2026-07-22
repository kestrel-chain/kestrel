//! Deterministic sequential and optimistic parallel transaction execution.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    thread::{self, JoinHandle},
};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use state::{ResurrectionWitness, StateAccesses, StateDelta, StateSnapshot, StateTree};
use thiserror::Error;
use types::{Address, Epoch, Hash, Object, ObjectId, Owner};
use vm_move::{MoveCall, MoveHostError, MoveVmHost};

/// An ordered operation understood by the execution layer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MoveOperation {
    PublishModule {
        sender: Address,
        module_bytes: Vec<u8>,
    },
    EntryFunction(MoveCall),
    CreateObject {
        sender: Address,
        object: Object,
    },
    MutateObject {
        sender: Address,
        id: ObjectId,
        expected_version: u64,
        replacement: Object,
    },
    DeleteObject {
        sender: Address,
        id: ObjectId,
        expected_version: u64,
    },
    TransferObject {
        sender: Address,
        id: ObjectId,
        expected_version: u64,
        new_owner: Owner,
    },
    ResurrectObject {
        sender: Address,
        witness: ResurrectionWitness,
        rent_credit: u64,
    },
}

impl MoveOperation {
    #[must_use]
    pub const fn sender(&self) -> Address {
        match self {
            Self::PublishModule { sender, .. }
            | Self::CreateObject { sender, .. }
            | Self::MutateObject { sender, .. }
            | Self::DeleteObject { sender, .. }
            | Self::TransferObject { sender, .. }
            | Self::ResurrectObject { sender, .. } => *sender,
            Self::EntryFunction(call) => call.sender,
        }
    }
}

/// Access mode declared by a transaction before execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AccessMode {
    Read,
    Write,
}

/// Static object reference used for scheduling and fast-path eligibility.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeclaredObjectRef {
    pub id: ObjectId,
    pub owner: Owner,
    pub access: AccessMode,
}

/// Operation plus its complete, statically declared object references.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutableTransaction {
    pub operation: MoveOperation,
    pub object_references: Vec<DeclaredObjectRef>,
    /// Compute budget for this operation. Move operations meter real Move
    /// bytecode against this limit; native object operations are charged a
    /// flat [`NATIVE_OPERATION_COMPUTE_COST`] and fail closed if it is not
    /// affordable. Exceeding the limit aborts the operation atomically.
    pub compute_limit: u64,
}

/// Flat compute charge for native object primitives, which bypass the Move
/// VM entirely and therefore have no per-bytecode cost to meter.
pub const NATIVE_OPERATION_COMPUTE_COST: u64 = 100;

/// Scheduler path which produced a committed result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionPath {
    Sequential,
    StructuralFastPath,
    Optimistic,
    Reexecuted,
}

/// Block commitment and access metadata produced by one operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionReceipt {
    pub transaction_index: usize,
    /// Final state root of the block containing this transaction.
    ///
    /// Kestrel computes the canonical commitment once after ordered commit;
    /// every receipt in the block therefore carries the same root.
    pub state_root: Hash,
    pub event_count: usize,
    pub compute_used: u64,
    pub read_set: BTreeSet<ObjectId>,
    pub write_set: BTreeSet<ObjectId>,
    pub path: ExecutionPath,
    pub attempts: usize,
}

/// Scheduler counters for a completed block.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SchedulerStats {
    pub speculative_executions: usize,
    pub structural_fast_path: usize,
    pub optimistic_commits: usize,
    pub aborts: usize,
    pub reexecutions: usize,
}

/// Output of ordered block execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockExecutionResult {
    pub receipts: Vec<ExecutionReceipt>,
    pub state_root: Hash,
    pub scheduler: SchedulerStats,
}

/// A finalized ordering decision paired with the locally available executable payloads.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OrderedExecutionBlock {
    pub height: u64,
    pub consensus_block_id: Hash,
    pub transaction_ids: Vec<Hash>,
    pub transactions: Vec<ExecutableTransaction>,
}

/// Result emitted by the execution stage without feeding effects back into ordering.
#[derive(Debug)]
pub struct DeferredExecutionResult {
    pub height: u64,
    pub consensus_block_id: Hash,
    pub result: Result<BlockExecutionResult, ExecutionError>,
    /// Root-bound state materialization emitted only after successful execution.
    pub state_snapshot: Option<StateSnapshot>,
}

/// Errors at the asynchronous consensus-to-execution boundary.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DeferredExecutionError {
    #[error("ordered block contains a different number of transaction IDs and payloads")]
    TransactionCountMismatch,
    #[error("execution is already one ordered block behind")]
    LagLimitReached,
    #[error("deferred execution worker stopped")]
    WorkerStopped,
}

enum ExecutionCommand {
    Execute(OrderedExecutionBlock),
    Shutdown,
}

/// Bounded asynchronous stage connecting finalized order to Phase 2 execution.
///
/// One queued block is permitted while the worker executes the previous block.
/// Consensus never waits for an execution result; once that one-block pipeline
/// slot is occupied, submission reports backpressure to the node coordinator.
pub struct DeferredExecutor {
    commands: SyncSender<ExecutionCommand>,
    results: Receiver<DeferredExecutionResult>,
    worker: Option<JoinHandle<()>>,
}

impl DeferredExecutor {
    /// Starts a state-owning execution worker with a one-block ordering buffer.
    ///
    /// `blocks_per_epoch` deterministically maps each block's height to its
    /// epoch (`height / blocks_per_epoch`); before executing a block's
    /// transactions the worker advances state to that epoch, charging storage
    /// rent and expiring exhausted objects, so the reported state root already
    /// reflects the epoch's rent accounting.
    ///
    /// # Errors
    ///
    /// Returns scheduler initialization failures before spawning the worker,
    /// or if `blocks_per_epoch` is zero.
    pub fn new(
        state: StateTree,
        rent_balance: u64,
        worker_count: usize,
        blocks_per_epoch: u64,
    ) -> Result<Self, ExecutionError> {
        if blocks_per_epoch == 0 {
            return Err(ExecutionError::SchedulerInitialization(
                "blocks per epoch must be greater than zero".to_owned(),
            ));
        }
        let executor = ParallelExecutor::new(rent_balance, worker_count)?;
        let (command_sender, command_receiver) = mpsc::sync_channel(1);
        let (result_sender, result_receiver) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("kestrel-deferred-execution".to_owned())
            .spawn(move || {
                let mut state = state;
                while let Ok(command) = command_receiver.recv() {
                    match command {
                        ExecutionCommand::Execute(block) => {
                            let target_epoch = Epoch(block.height / blocks_per_epoch);
                            let mut result = state
                                .advance_to_epoch(target_epoch)
                                .map_err(|error| ExecutionError::EpochAdvance(error.to_string()))
                                .and_then(|_| {
                                    executor.execute_block(&mut state, &block.transactions)
                                });
                            let state_snapshot = if result.is_ok() {
                                match state.durable_snapshot() {
                                    Ok(snapshot) => Some(snapshot),
                                    Err(error) => {
                                        result =
                                            Err(ExecutionError::StateSnapshot(error.to_string()));
                                        None
                                    }
                                }
                            } else {
                                None
                            };
                            if result_sender
                                .send(DeferredExecutionResult {
                                    height: block.height,
                                    consensus_block_id: block.consensus_block_id,
                                    result,
                                    state_snapshot,
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                        ExecutionCommand::Shutdown => break,
                    }
                }
            })
            .map_err(|error| ExecutionError::SchedulerInitialization(error.to_string()))?;
        Ok(Self {
            commands: command_sender,
            results: result_receiver,
            worker: Some(worker),
        })
    }

    /// Enqueues finalized order without waiting for execution.
    ///
    /// # Errors
    ///
    /// Rejects payload mismatches, a full one-block buffer, or a stopped worker.
    pub fn submit(&self, block: OrderedExecutionBlock) -> Result<(), DeferredExecutionError> {
        if block.transaction_ids.len() != block.transactions.len() {
            return Err(DeferredExecutionError::TransactionCountMismatch);
        }
        match self.commands.try_send(ExecutionCommand::Execute(block)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(DeferredExecutionError::LagLimitReached),
            Err(TrySendError::Disconnected(_)) => Err(DeferredExecutionError::WorkerStopped),
        }
    }

    /// Polls a completed block without blocking the consensus task.
    ///
    /// # Errors
    ///
    /// Returns an error if the worker disconnected before producing another result.
    pub fn try_result(&self) -> Result<Option<DeferredExecutionResult>, DeferredExecutionError> {
        match self.results.try_recv() {
            Ok(result) => Ok(Some(result)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(DeferredExecutionError::WorkerStopped),
        }
    }
}

impl Drop for DeferredExecutor {
    fn drop(&mut self) {
        let _ = self.commands.send(ExecutionCommand::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Execution failures include the canonical transaction index for diagnosis.
#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("transaction {index} failed: {source}")]
    Transaction {
        index: usize,
        #[source]
        source: MoveHostError,
    },
    #[error("transaction {index} accessed undeclared objects: {objects:?}")]
    UndeclaredAccess {
        index: usize,
        objects: BTreeSet<ObjectId>,
    },
    #[error("parallel scheduler initialization failed: {0}")]
    SchedulerInitialization(String),
    #[error("durable state snapshot failed: {0}")]
    StateSnapshot(String),
    #[error("epoch advancement failed: {0}")]
    EpochAdvance(String),
}

/// Phase 1 reference executor. This implementation remains single-threaded.
pub struct SequentialExecutor {
    host: MoveVmHost,
}

impl SequentialExecutor {
    /// Creates an executor whose newly created Move objects receive `rent_balance`.
    ///
    /// # Errors
    ///
    /// Returns an error if the embedded Move VM cannot initialize.
    pub fn new(rent_balance: u64) -> Result<Self, MoveHostError> {
        Ok(Self {
            host: MoveVmHost::new(rent_balance)?,
        })
    }

    /// Applies operations strictly in list order to a single state view.
    ///
    /// Every individual operation is atomic. Successfully completed operations
    /// before a later failure remain committed. This entry point takes bare
    /// operations with no attached compute limit (used only as a sequential
    /// reference oracle by benches/equivalence tests, never by the live
    /// consensus-to-execution pipeline), so every operation is metered against
    /// a generous fixed [`LEGACY_OPERATION_COMPUTE_LIMIT`].
    ///
    /// # Errors
    ///
    /// Returns the index and source error of the first rejected operation.
    pub fn execute_block(
        &self,
        state: &mut StateTree,
        operations: &[MoveOperation],
    ) -> Result<BlockExecutionResult, ExecutionError> {
        let mut pending_receipts = Vec::with_capacity(operations.len());
        for (index, operation) in operations.iter().enumerate() {
            state.start_access_tracking();
            let result = self.execute_operation(state, operation, LEGACY_OPERATION_COMPUTE_LIMIT);
            let accesses = state.finish_access_tracking();
            let result = result.map_err(|source| ExecutionError::Transaction { index, source })?;
            pending_receipts.push(pending_receipt(
                index,
                result,
                accesses,
                ExecutionPath::Sequential,
                1,
            ));
        }

        let state_root = block_root(state, operations.len())?;
        Ok(BlockExecutionResult {
            receipts: finalize_receipts(pending_receipts, state_root),
            state_root,
            scheduler: SchedulerStats::default(),
        })
    }

    fn execute_operation(
        &self,
        state: &mut StateTree,
        operation: &MoveOperation,
        compute_limit: u64,
    ) -> Result<OperationResult, MoveHostError> {
        let result = match operation {
            MoveOperation::PublishModule {
                sender,
                module_bytes,
            } => self
                .host
                .publish_module(state, *sender, module_bytes.clone(), compute_limit)?,
            MoveOperation::EntryFunction(call) => {
                self.host
                    .execute_entry_function(state, call, compute_limit)?
            }
            MoveOperation::CreateObject { object, .. } => {
                charge_native_operation(compute_limit)?;
                self.host.create_object(state, object.clone())?;
                return Ok(native_result());
            }
            MoveOperation::MutateObject {
                sender,
                id,
                expected_version,
                replacement,
            } => {
                charge_native_operation(compute_limit)?;
                self.host.mutate_object(
                    state,
                    *sender,
                    *id,
                    *expected_version,
                    replacement.clone(),
                )?;
                return Ok(native_result());
            }
            MoveOperation::DeleteObject {
                sender,
                id,
                expected_version,
            } => {
                charge_native_operation(compute_limit)?;
                self.host
                    .delete_object(state, *sender, *id, *expected_version)?;
                return Ok(native_result());
            }
            MoveOperation::TransferObject {
                sender,
                id,
                expected_version,
                new_owner,
            } => {
                charge_native_operation(compute_limit)?;
                self.host.transfer_object(
                    state,
                    *sender,
                    *id,
                    *expected_version,
                    new_owner.clone(),
                )?;
                return Ok(native_result());
            }
            MoveOperation::ResurrectObject {
                witness,
                rent_credit,
                ..
            } => {
                charge_native_operation(compute_limit)?;
                state
                    .resurrect(witness, *rent_credit)
                    .map_err(MoveHostError::State)?;
                return Ok(native_result());
            }
        };
        Ok(OperationResult {
            event_count: result.event_count,
            compute_used: result.compute_used,
        })
    }
}

/// Legacy compute budget for [`SequentialExecutor::execute_block`]'s bare-operation
/// entry point, which has no `ExecutableTransaction` to carry a real limit.
const LEGACY_OPERATION_COMPUTE_LIMIT: u64 = 1_000_000_000;

fn charge_native_operation(compute_limit: u64) -> Result<(), MoveHostError> {
    if compute_limit < NATIVE_OPERATION_COMPUTE_COST {
        return Err(MoveHostError::OutOfGas {
            gas_limit: compute_limit,
        });
    }
    Ok(())
}

/// Phase 2 executor using parallel speculation and canonical ordered commit.
pub struct ParallelExecutor {
    pool: rayon::ThreadPool,
    rent_balance: u64,
}

impl ParallelExecutor {
    /// Creates a fixed-size execution pool.
    ///
    /// # Errors
    ///
    /// Returns an error if `worker_count` is zero or the pool cannot be built.
    pub fn new(rent_balance: u64, worker_count: usize) -> Result<Self, ExecutionError> {
        if worker_count == 0 {
            return Err(ExecutionError::SchedulerInitialization(
                "worker count must be greater than zero".to_owned(),
            ));
        }
        MoveVmHost::new(rent_balance).map_err(|error| {
            ExecutionError::SchedulerInitialization(format!("Move VM: {error}"))
        })?;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(worker_count)
            .build()
            .map_err(|error| ExecutionError::SchedulerInitialization(error.to_string()))?;
        Ok(Self { pool, rent_balance })
    }

    /// Speculates transactions in parallel, validates their accesses, and commits
    /// effects strictly in canonical transaction order.
    ///
    /// Conflicting speculative attempts are discarded and deterministically
    /// re-executed against the latest canonical state.
    ///
    /// # Errors
    ///
    /// Returns the first canonical transaction failure or an invalid declaration.
    pub fn execute_block(
        &self,
        state: &mut StateTree,
        transactions: &[ExecutableTransaction],
    ) -> Result<BlockExecutionResult, ExecutionError> {
        let base = state.speculative_snapshot();
        let fast_owners = classify_fast_path(&base, transactions);
        let tasks = build_tasks(transactions, &fast_owners);
        let minimum_grain = tasks
            .len()
            .div_ceil(self.pool.current_num_threads().saturating_mul(4))
            .max(1);
        let rent_balance = self.rent_balance;
        let mut speculative = self.pool.install(|| {
            tasks
                .into_par_iter()
                .with_min_len(minimum_grain)
                .map_init(
                    || SequentialExecutor::new(rent_balance),
                    |executor, task| Self::execute_task(executor, &base, task),
                )
                .flatten_iter()
                .collect::<Vec<_>>()
        });
        speculative.sort_unstable_by_key(|attempt| attempt.index);

        let reference = SequentialExecutor::new(self.rent_balance).map_err(|error| {
            ExecutionError::SchedulerInitialization(format!("Move VM: {error}"))
        })?;
        let mut pending_receipts = Vec::with_capacity(transactions.len());
        let mut committed_writes = BTreeSet::new();
        let mut scheduler_stats = SchedulerStats {
            speculative_executions: speculative.len(),
            ..SchedulerStats::default()
        };

        for (index, (transaction, attempt)) in
            transactions.iter().zip(speculative.into_iter()).enumerate()
        {
            debug_assert_eq!(attempt.index, index);
            ensure_declared(index, transaction, &attempt.accesses)?;
            // Structural lanes are disjoint from every optimistic transaction
            // by construction, and transactions for one owner execute serially
            // inside their lane. They therefore need neither conflict-set scans
            // nor insertion into the optimistic validator's committed set.
            let conflicts = !attempt.fast
                && conflicts_with_committed_writes(&attempt.accesses, &committed_writes);
            if (attempt.fast || !conflicts)
                && let Ok(success) = attempt.result
            {
                if !attempt.fast {
                    committed_writes.extend(attempt.accesses.writes.iter().copied());
                }
                state.apply_delta_owned(success.delta);
                let path = if attempt.fast {
                    scheduler_stats.structural_fast_path += 1;
                    ExecutionPath::StructuralFastPath
                } else {
                    scheduler_stats.optimistic_commits += 1;
                    ExecutionPath::Optimistic
                };
                pending_receipts.push(PendingReceipt {
                    transaction_index: index,
                    event_count: success.event_count,
                    compute_used: success.compute_used,
                    read_set: attempt.accesses.reads,
                    write_set: attempt.accesses.writes,
                    path,
                    attempts: 1,
                });
                continue;
            }

            scheduler_stats.aborts += 1;
            scheduler_stats.reexecutions += 1;
            state.start_access_tracking();
            let result = reference.execute_operation(
                state,
                &transaction.operation,
                transaction.compute_limit,
            );
            let accesses = state.finish_access_tracking();
            ensure_declared(index, transaction, &accesses)?;
            let result = result.map_err(|source| ExecutionError::Transaction { index, source })?;
            committed_writes.extend(accesses.writes.iter().copied());
            pending_receipts.push(pending_receipt(
                index,
                result,
                accesses,
                ExecutionPath::Reexecuted,
                2,
            ));
        }

        let state_root = block_root(state, transactions.len())?;
        Ok(BlockExecutionResult {
            receipts: finalize_receipts(pending_receipts, state_root),
            state_root,
            scheduler: scheduler_stats,
        })
    }

    fn execute_task(
        executor: &Result<SequentialExecutor, MoveHostError>,
        base: &StateTree,
        task: SpeculationTask<'_>,
    ) -> Vec<SpeculativeAttempt> {
        let executor = match executor {
            Ok(executor) => executor,
            Err(error) => {
                return task
                    .transactions()
                    .map(|(index, _)| SpeculativeAttempt {
                        index,
                        accesses: StateAccesses::default(),
                        result: Err(error.to_string()),
                        fast: false,
                    })
                    .collect();
            }
        };
        match task {
            SpeculationTask::Optimistic(index, transaction) => {
                vec![speculate(executor, base, index, transaction, false)]
            }
            SpeculationTask::FastLane(transactions) => {
                let mut lane_state = base.clone();
                transactions
                    .into_iter()
                    .map(|(index, transaction)| {
                        let attempt = speculate(executor, &lane_state, index, transaction, true);
                        if let Ok(success) = &attempt.result {
                            lane_state.apply_delta(&success.delta);
                        }
                        attempt
                    })
                    .collect()
            }
        }
    }
}

/// Returns whether a speculative attempt observed or wrote an object changed
/// by an earlier canonical commit.
///
/// This pure validation rule is shared by the production commit loop and the
/// Loom completion-order model so concurrency tests cannot silently duplicate
/// and diverge from scheduler semantics.
#[must_use]
pub fn conflicts_with_committed_writes(
    accesses: &StateAccesses,
    committed_writes: &BTreeSet<ObjectId>,
) -> bool {
    !accesses.reads.is_disjoint(committed_writes) || !accesses.writes.is_disjoint(committed_writes)
}

#[derive(Clone, Copy)]
struct OperationResult {
    event_count: usize,
    compute_used: u64,
}

struct PendingReceipt {
    transaction_index: usize,
    event_count: usize,
    compute_used: u64,
    read_set: BTreeSet<ObjectId>,
    write_set: BTreeSet<ObjectId>,
    path: ExecutionPath,
    attempts: usize,
}

struct SpeculativeSuccess {
    delta: StateDelta,
    event_count: usize,
    compute_used: u64,
}

struct SpeculativeAttempt {
    index: usize,
    accesses: StateAccesses,
    result: Result<SpeculativeSuccess, String>,
    fast: bool,
}

enum SpeculationTask<'a> {
    Optimistic(usize, &'a ExecutableTransaction),
    FastLane(Vec<(usize, &'a ExecutableTransaction)>),
}

impl<'a> SpeculationTask<'a> {
    fn transactions(&'a self) -> Box<dyn Iterator<Item = (usize, &'a ExecutableTransaction)> + 'a> {
        match self {
            Self::Optimistic(index, transaction) => {
                Box::new(std::iter::once((*index, *transaction)))
            }
            Self::FastLane(transactions) => Box::new(transactions.iter().copied()),
        }
    }
}

fn speculate(
    executor: &SequentialExecutor,
    base: &StateTree,
    index: usize,
    transaction: &ExecutableTransaction,
    fast: bool,
) -> SpeculativeAttempt {
    let mut candidate = base.clone();
    candidate.start_access_tracking();
    let result = executor.execute_operation(
        &mut candidate,
        &transaction.operation,
        transaction.compute_limit,
    );
    let accesses = candidate.finish_access_tracking();
    let result = result
        .map(|result| SpeculativeSuccess {
            delta: candidate.delta_from(base),
            event_count: result.event_count,
            compute_used: result.compute_used,
        })
        .map_err(|error| error.to_string());
    SpeculativeAttempt {
        index,
        accesses,
        result,
        fast,
    }
}

fn build_tasks<'a>(
    transactions: &'a [ExecutableTransaction],
    fast_owners: &[Option<Address>],
) -> Vec<SpeculationTask<'a>> {
    let mut lanes: BTreeMap<Address, Vec<(usize, &ExecutableTransaction)>> = BTreeMap::new();
    let mut tasks = Vec::new();
    for (index, (transaction, owner)) in transactions.iter().zip(fast_owners).enumerate() {
        if let Some(owner) = owner {
            lanes.entry(*owner).or_default().push((index, transaction));
        } else {
            tasks.push(SpeculationTask::Optimistic(index, transaction));
        }
    }
    tasks.extend(lanes.into_values().map(SpeculationTask::FastLane));
    tasks
}

fn classify_fast_path(
    state: &StateTree,
    transactions: &[ExecutableTransaction],
) -> Vec<Option<Address>> {
    let mut candidates: Vec<_> = transactions
        .iter()
        .map(|transaction| fast_path_owner(state, transaction))
        .collect();
    let mut unsafe_ids: BTreeSet<_> = transactions
        .iter()
        .zip(&candidates)
        .filter(|(_, owner)| owner.is_none())
        .flat_map(|(transaction, _)| {
            transaction
                .object_references
                .iter()
                .map(|reference| reference.id)
        })
        .collect();
    let mut fast_owners_by_id = BTreeMap::new();
    for (transaction, owner) in transactions.iter().zip(&candidates) {
        let Some(owner) = owner else {
            continue;
        };
        for reference in &transaction.object_references {
            if fast_owners_by_id
                .insert(reference.id, *owner)
                .is_some_and(|existing| existing != *owner)
            {
                unsafe_ids.insert(reference.id);
            }
        }
    }
    for (transaction, owner) in transactions.iter().zip(&mut candidates) {
        if transaction
            .object_references
            .iter()
            .any(|reference| unsafe_ids.contains(&reference.id))
        {
            *owner = None;
        }
    }
    candidates
}

fn fast_path_owner(state: &StateTree, transaction: &ExecutableTransaction) -> Option<Address> {
    let first = transaction.object_references.first()?;
    let Owner::Single(owner) = first.owner else {
        return None;
    };
    if owner != transaction.operation.sender()
        || transaction
            .object_references
            .iter()
            .any(|reference| reference.owner != Owner::Single(owner))
        || transaction.object_references.iter().any(|reference| {
            state
                .object(&reference.id)
                .is_some_and(|object| object.owner != reference.owner)
        })
        || operation_changes_owner(&transaction.operation, owner)
    {
        return None;
    }
    Some(owner)
}

fn operation_changes_owner(operation: &MoveOperation, owner: Address) -> bool {
    match operation {
        MoveOperation::MutateObject { replacement, .. }
        | MoveOperation::CreateObject {
            object: replacement,
            ..
        } => replacement.owner != Owner::Single(owner),
        MoveOperation::TransferObject { new_owner, .. } => *new_owner != Owner::Single(owner),
        MoveOperation::PublishModule { .. }
        | MoveOperation::EntryFunction(_)
        | MoveOperation::DeleteObject { .. } => false,
        MoveOperation::ResurrectObject { witness, .. } => {
            witness.object.owner != Owner::Single(owner)
        }
    }
}

fn ensure_declared(
    index: usize,
    transaction: &ExecutableTransaction,
    accesses: &StateAccesses,
) -> Result<(), ExecutionError> {
    let mut undeclared = BTreeSet::new();
    for id in &accesses.reads {
        if !transaction
            .object_references
            .iter()
            .any(|reference| reference.id == *id)
        {
            undeclared.insert(*id);
        }
    }
    for id in &accesses.writes {
        if !transaction
            .object_references
            .iter()
            .any(|reference| reference.id == *id && reference.access == AccessMode::Write)
        {
            undeclared.insert(*id);
        }
    }
    if undeclared.is_empty() {
        Ok(())
    } else {
        Err(ExecutionError::UndeclaredAccess {
            index,
            objects: undeclared,
        })
    }
}

const fn native_result() -> OperationResult {
    OperationResult {
        event_count: 0,
        compute_used: NATIVE_OPERATION_COMPUTE_COST,
    }
}

fn pending_receipt(
    transaction_index: usize,
    result: OperationResult,
    accesses: StateAccesses,
    path: ExecutionPath,
    attempts: usize,
) -> PendingReceipt {
    PendingReceipt {
        transaction_index,
        event_count: result.event_count,
        compute_used: result.compute_used,
        read_set: accesses.reads,
        write_set: accesses.writes,
        path,
        attempts,
    }
}

fn finalize_receipts(
    pending_receipts: Vec<PendingReceipt>,
    state_root: Hash,
) -> Vec<ExecutionReceipt> {
    pending_receipts
        .into_iter()
        .map(|pending| ExecutionReceipt {
            transaction_index: pending.transaction_index,
            state_root,
            event_count: pending.event_count,
            compute_used: pending.compute_used,
            read_set: pending.read_set,
            write_set: pending.write_set,
            path: pending.path,
            attempts: pending.attempts,
        })
        .collect()
}

fn block_root(state: &StateTree, index: usize) -> Result<Hash, ExecutionError> {
    state.root().map_err(|source| ExecutionError::Transaction {
        index,
        source: MoveHostError::State(source),
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use move_binary_format::file_format::CompiledModule;
    use move_compiler::{Compiler, compiled_unit::AnnotatedCompiledUnit};
    use proptest::prelude::*;
    use state::{StateConfig, StateTree};
    use tempfile::TempDir;
    use types::Address;
    use vm_move::{MoveArgument, MoveCall, MoveModuleId};

    use super::{MoveOperation, SequentialExecutor};

    proptest! {
        #[test]
        fn identical_transaction_lists_produce_identical_roots(
            minted in 1_u64..1_000_000,
            transfer in 0_u64..500_000,
        ) {
            let transfer = transfer.min(minted);
            let owner = Address::from_bytes([0x44; 32]);
            let recipient = Address::from_bytes([0x55; 32]);
            let module = compile_module(&token_source(owner));
            let operations = operations(owner, recipient, module, minted, transfer);

            let mut first_state = StateTree::new(StateConfig::default()).unwrap();
            let mut second_state = StateTree::new(StateConfig::default()).unwrap();
            let first = SequentialExecutor::new(100).unwrap()
                .execute_block(&mut first_state, &operations)
                .unwrap();
            let second = SequentialExecutor::new(100).unwrap()
                .execute_block(&mut second_state, &operations)
                .unwrap();

            prop_assert_eq!(first.state_root, second.state_root);
            prop_assert_eq!(first_state.root().unwrap(), second_state.root().unwrap());
        }
    }

    fn operations(
        owner: Address,
        recipient: Address,
        module_bytes: Vec<u8>,
        minted: u64,
        transfer: u64,
    ) -> Vec<MoveOperation> {
        let module = MoveModuleId {
            address: owner,
            name: "Token".to_owned(),
        };
        vec![
            MoveOperation::PublishModule {
                sender: owner,
                module_bytes,
            },
            MoveOperation::EntryFunction(MoveCall {
                sender: owner,
                module: module.clone(),
                function: "mint".to_owned(),
                arguments: vec![MoveArgument::Signer, MoveArgument::U64(minted)],
            }),
            MoveOperation::EntryFunction(MoveCall {
                sender: recipient,
                module: module.clone(),
                function: "mint".to_owned(),
                arguments: vec![MoveArgument::Signer, MoveArgument::U64(0)],
            }),
            MoveOperation::EntryFunction(MoveCall {
                sender: owner,
                module,
                function: "transfer".to_owned(),
                arguments: vec![
                    MoveArgument::Address(owner),
                    MoveArgument::Address(recipient),
                    MoveArgument::U64(transfer),
                ],
            }),
        ]
    }

    fn token_source(address: Address) -> String {
        include_str!("../../vm-move/tests/fixtures/token.move")
            .replace("__KESTREL_PUBLISHER__", &address.to_string())
    }

    fn compile_module(source: &str) -> Vec<u8> {
        let directory = TempDir::new().unwrap();
        let source_path = directory.path().join("token.move");
        fs::write(&source_path, source).unwrap();
        let (_, units) = Compiler::from_files(
            vec![source_path.to_string_lossy().into_owned()],
            Vec::new(),
            BTreeMap::<String, move_compiler::shared::NumericalAddress>::new(),
        )
        .build_and_report()
        .unwrap();
        let module = units
            .into_iter()
            .find_map(|unit| match unit {
                AnnotatedCompiledUnit::Module(module) => Some(module.named_module.module),
                AnnotatedCompiledUnit::Script(_) => None,
            })
            .unwrap();
        let mut bytes = Vec::new();
        CompiledModule::serialize(&module, &mut bytes).unwrap();
        bytes
    }
}
