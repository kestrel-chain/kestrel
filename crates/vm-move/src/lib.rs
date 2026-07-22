//! Move VM host and object/resource primitives for Kestrel.

use move_core_types::{
    account_address::AccountAddress,
    effects::{ChangeSet, Op},
    identifier::Identifier,
    language_storage::{ModuleId, StructTag},
    resolver::{ModuleResolver, ResourceResolver},
    value::MoveValue,
    vm_status::StatusCode,
};
use move_vm_runtime::{move_vm::MoveVM, session::SerializedReturnValues};
use move_vm_test_utils::gas_schedule::{Gas, GasStatus, INITIAL_COST_SCHEDULE};
use serde::{Deserialize, Serialize};
use state::{StateDelta, StateError, StateTree};
use thiserror::Error;
use types::{Address, Hash, Object, ObjectId, Owner};

const MODULE_ID_DOMAIN: &[u8] = b"kestrel/move/module/v1";
const RESOURCE_ID_DOMAIN: &[u8] = b"kestrel/move/resource/v1";

/// Stable public representation of a Move module identifier.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MoveModuleId {
    pub address: Address,
    pub name: String,
}

/// Supported canonical Move transaction arguments.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MoveArgument {
    /// A signer manufactured by the adapter and always bound to the transaction sender.
    Signer,
    Address(Address),
    Bool(bool),
    U8(u8),
    U64(u64),
    U128(u128),
    Bytes(Vec<u8>),
}

/// A Move entry-function invocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MoveCall {
    pub sender: Address,
    pub module: MoveModuleId,
    pub function: String,
    pub arguments: Vec<MoveArgument>,
}

/// Committed result of a Move VM session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MoveExecutionResult {
    pub return_values: Vec<Vec<u8>>,
    pub event_count: usize,
    /// Compute consumed, denominated in the same units as the caller's `gas_limit`.
    pub compute_used: u64,
}

/// Move host and object-primitive failures.
#[derive(Debug, Error)]
pub enum MoveHostError {
    #[error("Move VM initialization failed: {0}")]
    VmInitialization(String),
    #[error("Move VM execution failed: {0}")]
    Vm(String),
    #[error("invalid Move identifier: {0}")]
    InvalidIdentifier(String),
    #[error("object {0} is not owned by transaction sender")]
    NotObjectOwner(ObjectId),
    #[error("Move execution exceeded its gas limit of {gas_limit} compute units")]
    OutOfGas { gas_limit: u64 },
    #[error(transparent)]
    State(#[from] StateError),
}

/// Single-node Move execution adapter.
pub struct MoveVmHost {
    vm: MoveVM,
    new_object_rent_balance: u64,
}

impl MoveVmHost {
    /// Creates a Move VM with no chain-specific native functions.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying VM cannot initialize.
    pub fn new(new_object_rent_balance: u64) -> Result<Self, MoveHostError> {
        let vm = MoveVM::new([]).map_err(|error| {
            MoveHostError::VmInitialization(format!("{:?}", error.into_vm_status()))
        })?;
        Ok(Self {
            vm,
            new_object_rent_balance,
        })
    }

    /// Verifies and publishes one compiled Move module atomically.
    ///
    /// `gas_limit` bounds Move bytecode execution against the deterministic
    /// reference cost schedule; exceeding it aborts with
    /// [`MoveHostError::OutOfGas`] and leaves state unchanged.
    ///
    /// # Errors
    ///
    /// Returns an error if verification, publication, gas exhaustion, or state
    /// application fails. State is unchanged on error.
    pub fn publish_module(
        &self,
        state: &mut StateTree,
        sender: Address,
        module_bytes: Vec<u8>,
        gas_limit: u64,
    ) -> Result<MoveExecutionResult, MoveHostError> {
        let resolver = StateResolver { state };
        let mut session = self.vm.new_session(&resolver);
        let mut gas_status = GasStatus::new(&INITIAL_COST_SCHEDULE, Gas::new(gas_limit));
        session
            .publish_module(module_bytes, to_move_address(sender), &mut gas_status)
            .map_err(|error| vm_error(error, gas_limit))?;
        let compute_used = compute_used(gas_limit, &gas_status);
        let (changes, events) = session
            .finish()
            .map_err(|error| vm_error(error, gas_limit))?;
        self.commit_changes(state, changes, Vec::new(), events.len(), compute_used)
    }

    /// Executes a public Move entry function atomically.
    ///
    /// The host, not the caller, materializes signer arguments. This prevents a
    /// transaction from forging a signer for an address other than `call.sender`.
    /// `gas_limit` bounds Move bytecode execution against the deterministic
    /// reference cost schedule; exceeding it aborts with
    /// [`MoveHostError::OutOfGas`] and leaves state unchanged.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identifiers, rejected VM execution, gas
    /// exhaustion, or invalid state effects. State is unchanged on error.
    pub fn execute_entry_function(
        &self,
        state: &mut StateTree,
        call: &MoveCall,
        gas_limit: u64,
    ) -> Result<MoveExecutionResult, MoveHostError> {
        let module = module_id(&call.module)?;
        let function = Identifier::new(call.function.clone())
            .map_err(|_| MoveHostError::InvalidIdentifier(call.function.clone()))?;
        let args = call
            .arguments
            .iter()
            .map(|argument| serialize_argument(call.sender, argument))
            .collect();

        let resolver = StateResolver { state };
        let mut session = self.vm.new_session(&resolver);
        let mut gas_status = GasStatus::new(&INITIAL_COST_SCHEDULE, Gas::new(gas_limit));
        let values = session
            .execute_entry_function(&module, &function, Vec::new(), args, &mut gas_status)
            .map_err(|error| vm_error(error, gas_limit))?;
        let compute_used = compute_used(gas_limit, &gas_status);
        let (changes, events) = session
            .finish()
            .map_err(|error| vm_error(error, gas_limit))?;
        self.commit_changes(
            state,
            changes,
            flatten_returns(values),
            events.len(),
            compute_used,
        )
    }

    /// Creates an arbitrary native object through the VM ownership boundary.
    ///
    /// # Errors
    ///
    /// Returns a state error for duplicate IDs.
    pub fn create_object(
        &self,
        state: &mut StateTree,
        object: Object,
    ) -> Result<(), MoveHostError> {
        state.create_object(object)?;
        Ok(())
    }

    /// Mutates an object after enforcing its ownership rule.
    ///
    /// # Errors
    ///
    /// Returns an error for unauthorized access or an invalid state transition.
    pub fn mutate_object(
        &self,
        state: &mut StateTree,
        sender: Address,
        id: ObjectId,
        expected_version: u64,
        replacement: Object,
    ) -> Result<(), MoveHostError> {
        Self::require_owner(state, sender, id)?;
        state.mutate_object(id, expected_version, replacement)?;
        Ok(())
    }

    /// Deletes an object after enforcing its ownership rule.
    ///
    /// # Errors
    ///
    /// Returns an error for unauthorized access or an invalid state transition.
    pub fn delete_object(
        &self,
        state: &mut StateTree,
        sender: Address,
        id: ObjectId,
        expected_version: u64,
    ) -> Result<Object, MoveHostError> {
        Self::require_owner(state, sender, id)?;
        Ok(state.delete_object(id, expected_version)?)
    }

    /// Transfers an object after enforcing its ownership rule.
    ///
    /// # Errors
    ///
    /// Returns an error for unauthorized access or an invalid state transition.
    pub fn transfer_object(
        &self,
        state: &mut StateTree,
        sender: Address,
        id: ObjectId,
        expected_version: u64,
        new_owner: Owner,
    ) -> Result<(), MoveHostError> {
        Self::require_owner(state, sender, id)?;
        state.transfer_object(id, expected_version, new_owner)?;
        Ok(())
    }

    fn require_owner(
        state: &StateTree,
        sender: Address,
        id: ObjectId,
    ) -> Result<(), MoveHostError> {
        let object = state.object(&id).ok_or(StateError::ObjectNotFound(id))?;
        match object.owner {
            Owner::Single(owner) if owner != sender => Err(MoveHostError::NotObjectOwner(id)),
            Owner::Single(_) | Owner::Shared => Ok(()),
        }
    }

    fn commit_changes(
        &self,
        state: &mut StateTree,
        changes: ChangeSet,
        return_values: Vec<Vec<u8>>,
        event_count: usize,
        compute_used: u64,
    ) -> Result<MoveExecutionResult, MoveHostError> {
        let delta = prepare_change_set(state, changes, self.new_object_rent_balance)?;
        state.apply_delta(&delta);
        Ok(MoveExecutionResult {
            return_values,
            event_count,
            compute_used,
        })
    }
}

struct StateResolver<'a> {
    state: &'a StateTree,
}

impl ModuleResolver for StateResolver<'_> {
    type Error = StateError;

    fn get_module(&self, id: &ModuleId) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self
            .state
            .object(&module_object_id(id))
            .map(|object| object.data.clone()))
    }
}

impl ResourceResolver for StateResolver<'_> {
    type Error = StateError;

    fn get_resource(
        &self,
        address: &AccountAddress,
        tag: &StructTag,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self
            .state
            .object(&resource_object_id(address, tag))
            .map(|object| object.data.clone()))
    }
}

fn prepare_change_set(
    state: &StateTree,
    changes: ChangeSet,
    new_object_rent_balance: u64,
) -> Result<StateDelta, StateError> {
    let mut prepared = std::collections::BTreeMap::new();
    for (account, account_changes) in changes.into_inner() {
        let (modules, resources) = account_changes.into_inner();
        for (name, operation) in modules {
            let module = ModuleId::new(account, name);
            let id = module_object_id(&module);
            prepare_operation(
                state,
                &mut prepared,
                id,
                Owner::Shared,
                format!("move::module::{module}"),
                operation,
                new_object_rent_balance,
            )?;
        }
        for (tag, operation) in resources {
            let id = resource_object_id(&account, &tag);
            prepare_operation(
                state,
                &mut prepared,
                id,
                Owner::Single(from_move_address(account)),
                format!("move::resource::{tag}"),
                operation,
                new_object_rent_balance,
            )?;
        }
    }
    Ok(StateDelta::from_active_changes(prepared))
}

fn prepare_operation(
    state: &StateTree,
    prepared: &mut std::collections::BTreeMap<ObjectId, Option<Object>>,
    id: ObjectId,
    owner: Owner,
    type_tag: String,
    operation: Op<Vec<u8>>,
    new_object_rent_balance: u64,
) -> Result<(), StateError> {
    match operation {
        Op::New(data) => {
            if state.object(&id).is_some() || state.expired_object(&id).is_some() {
                return Err(StateError::ObjectAlreadyExists(id));
            }
            prepared.insert(
                id,
                Some(Object {
                    id,
                    owner,
                    type_tag,
                    version: 0,
                    data,
                    rent_balance: new_object_rent_balance,
                }),
            );
            Ok(())
        }
        Op::Modify(data) => {
            let existing = state.object(&id).ok_or(StateError::ObjectNotFound(id))?;
            let mut replacement = existing.clone();
            replacement.data = data;
            replacement.version = replacement
                .version
                .checked_add(1)
                .ok_or(StateError::VersionOverflow)?;
            prepared.insert(id, Some(replacement));
            Ok(())
        }
        Op::Delete => {
            state.object(&id).ok_or(StateError::ObjectNotFound(id))?;
            prepared.insert(id, None);
            Ok(())
        }
    }
}

fn serialize_argument(sender: Address, argument: &MoveArgument) -> Vec<u8> {
    let value = match argument {
        MoveArgument::Signer => MoveValue::Signer(to_move_address(sender)),
        MoveArgument::Address(address) => MoveValue::Address(to_move_address(*address)),
        MoveArgument::Bool(value) => MoveValue::Bool(*value),
        MoveArgument::U8(value) => MoveValue::U8(*value),
        MoveArgument::U64(value) => MoveValue::U64(*value),
        MoveArgument::U128(value) => MoveValue::U128(*value),
        MoveArgument::Bytes(value) => MoveValue::vector_u8(value.clone()),
    };
    value
        .simple_serialize()
        .expect("supported Move arguments have canonical layouts")
}

fn flatten_returns(values: SerializedReturnValues) -> Vec<Vec<u8>> {
    values
        .return_values
        .into_iter()
        .map(|(bytes, _layout)| bytes)
        .collect()
}

fn module_id(id: &MoveModuleId) -> Result<ModuleId, MoveHostError> {
    let name = Identifier::new(id.name.clone())
        .map_err(|_| MoveHostError::InvalidIdentifier(id.name.clone()))?;
    Ok(ModuleId::new(to_move_address(id.address), name))
}

fn module_object_id(id: &ModuleId) -> ObjectId {
    hash_identifier(MODULE_ID_DOMAIN, id.address(), id.name().as_str())
}

fn resource_object_id(address: &AccountAddress, tag: &StructTag) -> ObjectId {
    hash_identifier(RESOURCE_ID_DOMAIN, address, &tag.to_string())
}

fn hash_identifier(domain: &[u8], address: &AccountAddress, name: &str) -> Hash {
    let mut bytes = Vec::with_capacity(domain.len() + AccountAddress::LENGTH + 8 + name.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(&address.into_bytes());
    bytes.extend_from_slice(&(name.len() as u64).to_be_bytes());
    bytes.extend_from_slice(name.as_bytes());
    Hash::digest(bytes)
}

fn to_move_address(address: Address) -> AccountAddress {
    AccountAddress::new(*address.as_bytes())
}

fn from_move_address(address: AccountAddress) -> Address {
    Address::from_bytes(address.into_bytes())
}

fn vm_error(error: move_binary_format::errors::VMError, gas_limit: u64) -> MoveHostError {
    let status = error.into_vm_status();
    if status.status_code() == StatusCode::OUT_OF_GAS {
        MoveHostError::OutOfGas { gas_limit }
    } else {
        MoveHostError::Vm(format!("{status:?}"))
    }
}

/// Compute consumed so far, in the same external units as `gas_limit`.
fn compute_used(gas_limit: u64, gas_status: &GasStatus<'_>) -> u64 {
    gas_limit.saturating_sub(u64::from(gas_status.remaining_gas()))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use move_binary_format::file_format::CompiledModule;
    use move_compiler::{Compiler, compiled_unit::AnnotatedCompiledUnit};
    use state::{StateConfig, StateTree};
    use tempfile::TempDir;
    use types::{Address, Hash, Object, Owner};

    use super::{MoveArgument, MoveCall, MoveHostError, MoveModuleId, MoveVmHost};

    const DEFAULT_TEST_GAS_LIMIT: u64 = 1_000_000;

    #[test]
    fn deploy_mint_and_transfer_move_resource_end_to_end() {
        let alice = Address::from_bytes([0x0a; 32]);
        let bob = Address::from_bytes([0x0b; 32]);
        let module = compile_module(&token_source(alice));
        let mut state = StateTree::new(StateConfig::default()).unwrap();
        let host = MoveVmHost::new(100).unwrap();

        // Module publication does no metered bytecode execution (the reference
        // `GasMeter` trait has no publish-specific charge hook), so it always
        // reports zero compute regardless of gas_limit.
        let publish_result = host
            .publish_module(&mut state, alice, module, DEFAULT_TEST_GAS_LIMIT)
            .unwrap();
        assert_eq!(publish_result.compute_used, 0);
        for (sender, amount) in [(alice, 100_u64), (bob, 10_u64)] {
            let result = host
                .execute_entry_function(
                    &mut state,
                    &MoveCall {
                        sender,
                        module: MoveModuleId {
                            address: alice,
                            name: "Token".to_owned(),
                        },
                        function: "mint".to_owned(),
                        arguments: vec![MoveArgument::Signer, MoveArgument::U64(amount)],
                    },
                    DEFAULT_TEST_GAS_LIMIT,
                )
                .unwrap();
            assert!(result.compute_used > 0);
            assert!(result.compute_used < DEFAULT_TEST_GAS_LIMIT);
        }

        host.execute_entry_function(
            &mut state,
            &MoveCall {
                sender: alice,
                module: MoveModuleId {
                    address: alice,
                    name: "Token".to_owned(),
                },
                function: "transfer".to_owned(),
                arguments: vec![
                    MoveArgument::Address(alice),
                    MoveArgument::Address(bob),
                    MoveArgument::U64(40),
                ],
            },
            DEFAULT_TEST_GAS_LIMIT,
        )
        .unwrap();

        let resource_objects: Vec<_> = state
            .active_objects()
            .filter(|object| object.type_tag.contains("move::resource"))
            .collect();
        assert_eq!(resource_objects.len(), 2);
        let mut balances: Vec<_> = resource_objects
            .iter()
            .map(|object| u64::from_le_bytes(object.data.as_slice().try_into().unwrap()))
            .collect();
        balances.sort_unstable();
        assert_eq!(balances, vec![50, 60]);
    }

    #[test]
    fn insufficient_gas_aborts_atomically_without_mutating_state() {
        let alice = Address::from_bytes([0x0c; 32]);
        let module = compile_module(&token_source(alice));
        let mut state = StateTree::new(StateConfig::default()).unwrap();
        let host = MoveVmHost::new(100).unwrap();

        host.publish_module(&mut state, alice, module, DEFAULT_TEST_GAS_LIMIT)
            .unwrap();
        let root_after_publish = state.root().unwrap();
        let out_of_gas_call = MoveCall {
            sender: alice,
            module: MoveModuleId {
                address: alice,
                name: "Token".to_owned(),
            },
            function: "mint".to_owned(),
            arguments: vec![MoveArgument::Signer, MoveArgument::U64(1)],
        };

        // A generous budget succeeds, establishing this call is normally payable.
        let affordable = host
            .execute_entry_function(&mut state, &out_of_gas_call, DEFAULT_TEST_GAS_LIMIT)
            .is_ok();
        assert!(affordable);
        let root_after_mint = state.root().unwrap();
        assert_ne!(root_after_publish, root_after_mint);

        // The same call with a zero budget must run out of gas on its very
        // first charge and leave state exactly as it was (no partial mutation).
        let error = host
            .execute_entry_function(&mut state, &out_of_gas_call, 0)
            .unwrap_err();
        assert!(matches!(error, MoveHostError::OutOfGas { gas_limit: 0 }));
        assert_eq!(state.root().unwrap(), root_after_mint);
    }

    #[test]
    fn native_object_primitives_enforce_owner_and_versions() {
        let owner = Address::from_bytes([1; 32]);
        let stranger = Address::from_bytes([2; 32]);
        let id = Hash::digest(b"owned");
        let object = Object {
            id,
            owner: Owner::Single(owner),
            type_tag: "test::Owned".to_owned(),
            version: 0,
            data: vec![],
            rent_balance: 10,
        };
        let host = MoveVmHost::new(10).unwrap();
        let mut state = StateTree::new(StateConfig::default()).unwrap();
        host.create_object(&mut state, object.clone()).unwrap();
        assert!(
            host.mutate_object(&mut state, stranger, id, 0, object.clone())
                .is_err()
        );
        host.transfer_object(&mut state, owner, id, 0, Owner::Single(stranger))
            .unwrap();
        assert_eq!(state.object(&id).unwrap().version, 1);
        assert_eq!(state.object(&id).unwrap().owner, Owner::Single(stranger));
    }

    fn token_source(address: Address) -> String {
        include_str!("../tests/fixtures/token.move")
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
