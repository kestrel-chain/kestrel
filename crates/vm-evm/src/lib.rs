//! Co-resident `revm` execution with an atomic native-object precompile bridge.

use std::sync::{Arc, Mutex};

use revm::{
    Context, ExecuteCommitEvm, MainBuilder, MainContext,
    context::TxEnv,
    context_interface::{Cfg, ContextTr},
    database::InMemoryDB,
    handler::{EthPrecompiles, PrecompileProvider},
    interpreter::{CallInputs, Gas, InstructionResult, InterpreterResult},
    primitives::{Address as RevmAddress, Bytes, TxKind, U256, hardfork::SpecId},
    state::AccountInfo,
};
use state::StateTree;
use thiserror::Error;
use types::{Hash, ObjectId};

const BRIDGE_GAS_BASE: u64 = 1_000;
const BRIDGE_GAS_PER_BYTE: u64 = 4;

/// Public EVM address type used by the host API.
pub type EvmAddress = RevmAddress;

/// Reserved native-object precompile address.
#[must_use]
pub fn native_object_precompile_address() -> EvmAddress {
    let mut bytes = [0_u8; 20];
    bytes[18..].copy_from_slice(&0x0f00_u16.to_be_bytes());
    EvmAddress::from(bytes)
}

type BridgeState = Arc<Mutex<StateTree>>;

#[derive(Clone, Debug)]
struct NativeObjectPrecompiles {
    ethereum: EthPrecompiles,
}

impl NativeObjectPrecompiles {
    fn new() -> Self {
        Self {
            ethereum: EthPrecompiles::new(SpecId::PRAGUE),
        }
    }
}

impl<CTX> PrecompileProvider<CTX> for NativeObjectPrecompiles
where
    CTX: ContextTr<Chain = BridgeState>,
{
    type Output = InterpreterResult;

    fn set_spec(&mut self, spec: <CTX::Cfg as Cfg>::Spec) -> bool {
        <EthPrecompiles as PrecompileProvider<CTX>>::set_spec(&mut self.ethereum, spec)
    }

    fn run(
        &mut self,
        context: &mut CTX,
        inputs: &CallInputs,
    ) -> Result<Option<Self::Output>, String> {
        if inputs.bytecode_address != native_object_precompile_address() {
            return <EthPrecompiles as PrecompileProvider<CTX>>::run(
                &mut self.ethereum,
                context,
                inputs,
            );
        }
        let input = inputs.input.bytes(context);
        let output = execute_bridge_call(context.chain_mut(), &input, inputs.is_static)?;
        let input_len = u64::try_from(input.len()).map_err(|_| "bridge input too large")?;
        let output_len = u64::try_from(output.len()).map_err(|_| "bridge output too large")?;
        let cost = BRIDGE_GAS_BASE
            .checked_add(
                input_len
                    .checked_add(output_len)
                    .and_then(|bytes| bytes.checked_mul(BRIDGE_GAS_PER_BYTE))
                    .ok_or("bridge gas overflow")?,
            )
            .ok_or("bridge gas overflow")?;
        let mut gas = Gas::new(inputs.gas_limit);
        if !gas.record_cost(cost) {
            return Ok(Some(InterpreterResult {
                result: InstructionResult::PrecompileOOG,
                output: Bytes::new(),
                gas,
            }));
        }
        Ok(Some(InterpreterResult {
            result: InstructionResult::Return,
            output: Bytes::from(output),
            gas,
        }))
    }

    fn warm_addresses(&self) -> Box<impl Iterator<Item = EvmAddress>> {
        Box::new(
            self.ethereum
                .warm_addresses()
                .chain(std::iter::once(native_object_precompile_address())),
        )
    }

    fn contains(&self, address: &EvmAddress) -> bool {
        *address == native_object_precompile_address() || self.ethereum.contains(address)
    }
}

fn execute_bridge_call(
    state: &BridgeState,
    input: &[u8],
    is_static: bool,
) -> Result<Vec<u8>, String> {
    let (&operation, body) = input.split_first().ok_or("missing bridge operation")?;
    let id_bytes: [u8; 32] = body
        .get(..32)
        .ok_or("missing object ID")?
        .try_into()
        .map_err(|_| "invalid object ID")?;
    let id = Hash::from_bytes(id_bytes);
    let mut state = state.lock().map_err(|_| "native state lock poisoned")?;
    match operation {
        0 => state
            .object(&id)
            .map(|object| object.data.clone())
            .ok_or_else(|| "native object not found".to_owned()),
        1 => {
            if is_static {
                return Err("native object write attempted from STATICCALL".to_owned());
            }
            let version_bytes: [u8; 8] = body
                .get(32..40)
                .ok_or("missing expected object version")?
                .try_into()
                .map_err(|_| "invalid expected object version")?;
            let expected_version = u64::from_be_bytes(version_bytes);
            let mut replacement = state
                .object(&id)
                .cloned()
                .ok_or("native object not found")?;
            replacement.data = body.get(40..).ok_or("missing object data")?.to_vec();
            state
                .mutate_object(id, expected_version, replacement)
                .map_err(|error| error.to_string())?;
            Ok(Vec::new())
        }
        _ => Err("unknown native-object bridge operation".to_owned()),
    }
}

/// Stateful `revm` host sharing one atomic object state with the Move executor.
pub struct EvmHost {
    database: InMemoryDB,
    caller: EvmAddress,
    nonce: u64,
}

impl EvmHost {
    #[must_use]
    pub fn new(caller: EvmAddress) -> Self {
        let mut database = InMemoryDB::default();
        database.insert_account_info(
            caller,
            AccountInfo {
                balance: U256::MAX,
                ..AccountInfo::default()
            },
        );
        Self {
            database,
            caller,
            nonce: 0,
        }
    }

    /// Deploys a minimal EVM contract that forwards calldata to the native bridge.
    ///
    /// # Errors
    ///
    /// Returns an error if EVM creation fails, reverts, or does not return an address.
    pub fn deploy_bridge_contract(
        &mut self,
        state: &mut StateTree,
    ) -> Result<EvmAddress, EvmHostError> {
        let runtime = bridge_forwarder_runtime();
        let init_code = init_code(&runtime)?;
        let transaction = TxEnv::builder()
            .caller(self.caller)
            .gas_limit(1_000_000)
            .gas_price(0)
            .nonce(self.nonce)
            .kind(TxKind::Create)
            .data(Bytes::from(init_code))
            .build()
            .map_err(|error| EvmHostError::Transaction(error.to_string()))?;
        let result = self.execute(state, transaction)?;
        result
            .created_address()
            .ok_or(EvmHostError::DeploymentFailed)
    }

    /// Executes contract calldata and atomically commits native-object effects.
    ///
    /// # Errors
    ///
    /// Returns an error for EVM validation, halt, revert, or bridge failure.
    pub fn call(
        &mut self,
        state: &mut StateTree,
        contract: EvmAddress,
        input: Vec<u8>,
    ) -> Result<EvmReceipt, EvmHostError> {
        let transaction = TxEnv::builder()
            .caller(self.caller)
            .gas_limit(1_000_000)
            .gas_price(0)
            .nonce(self.nonce)
            .kind(TxKind::Call(contract))
            .data(Bytes::from(input))
            .build()
            .map_err(|error| EvmHostError::Transaction(error.to_string()))?;
        let result = self.execute(state, transaction)?;
        if !result.is_success() {
            return Err(EvmHostError::ExecutionFailed);
        }
        Ok(EvmReceipt {
            gas_used: result.gas_used(),
            output: result
                .output()
                .map_or_else(Vec::new, |bytes| bytes.to_vec()),
        })
    }

    fn execute(
        &mut self,
        state: &mut StateTree,
        transaction: TxEnv,
    ) -> Result<revm::context_interface::result::ExecutionResult, EvmHostError> {
        let candidate = Arc::new(Mutex::new(state.clone()));
        let context = Context::mainnet()
            .with_db(&mut self.database)
            .with_chain(Arc::clone(&candidate));
        let mut evm = context
            .build_mainnet()
            .with_precompiles(NativeObjectPrecompiles::new());
        let result = evm
            .transact_commit(transaction)
            .map_err(|error| EvmHostError::Transaction(error.to_string()))?;
        drop(evm);
        self.nonce = self
            .nonce
            .checked_add(1)
            .ok_or(EvmHostError::NonceOverflow)?;
        if result.is_success() {
            *state = candidate
                .lock()
                .map_err(|_| EvmHostError::StateLock)?
                .clone();
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmReceipt {
    pub gas_used: u64,
    pub output: Vec<u8>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum EvmHostError {
    #[error("EVM transaction failed: {0}")]
    Transaction(String),
    #[error("EVM contract deployment did not return an address")]
    DeploymentFailed,
    #[error("EVM execution reverted or halted")]
    ExecutionFailed,
    #[error("EVM caller nonce overflowed")]
    NonceOverflow,
    #[error("native state lock was poisoned")]
    StateLock,
    #[error("bridge runtime is too large")]
    RuntimeTooLarge,
}

fn init_code(runtime: &[u8]) -> Result<Vec<u8>, EvmHostError> {
    let length = u8::try_from(runtime.len()).map_err(|_| EvmHostError::RuntimeTooLarge)?;
    let mut init = vec![
        0x60, length, 0x60, 0x0c, 0x60, 0x00, 0x39, 0x60, length, 0x60, 0x00, 0xf3,
    ];
    init.extend_from_slice(runtime);
    Ok(init)
}

fn bridge_forwarder_runtime() -> Vec<u8> {
    let address = native_object_precompile_address();
    let mut code = vec![
        0x36, 0x60, 0x00, 0x60, 0x00, 0x37, // calldata -> memory
        0x61, 0x10, 0x00, // output size
        0x60, 0x00, // output offset
        0x36, // input size
        0x60, 0x00, // input offset
        0x60, 0x00, // value
        0x73, // PUSH20 bridge address
    ];
    code.extend_from_slice(address.as_slice());
    code.extend_from_slice(&[
        0x5a, 0xf1, 0x50, // GAS CALL POP
        0x3d, 0x60, 0x00, 0x60, 0x00, 0x3e, // copy returndata
        0x3d, 0x60, 0x00, 0xf3, // return returndata
    ]);
    code
}

/// Encodes a native-object read for the bridge contract.
#[must_use]
pub fn encode_object_read(id: ObjectId) -> Vec<u8> {
    let mut input = Vec::with_capacity(33);
    input.push(0);
    input.extend_from_slice(id.as_bytes());
    input
}

/// Encodes a version-checked native-object write for the bridge contract.
#[must_use]
pub fn encode_object_write(id: ObjectId, expected_version: u64, data: &[u8]) -> Vec<u8> {
    let mut input = Vec::with_capacity(41 + data.len());
    input.push(1);
    input.extend_from_slice(id.as_bytes());
    input.extend_from_slice(&expected_version.to_be_bytes());
    input.extend_from_slice(data);
    input
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use execution::{MoveOperation, SequentialExecutor};
    use move_binary_format::file_format::CompiledModule;
    use move_compiler::{Compiler, compiled_unit::AnnotatedCompiledUnit};
    use state::{StateConfig, StateTree};
    use tempfile::TempDir;
    use types::{Address, ObjectId, Owner};
    use vm_move::{MoveArgument, MoveCall, MoveModuleId};

    use super::{EvmAddress, EvmHost, encode_object_read, encode_object_write};

    #[test]
    fn deployed_evm_contract_and_move_entry_functions_share_resource_state() {
        let publisher = Address::from_bytes([7; 32]);
        let recipient = Address::from_bytes([8; 32]);
        let module = MoveModuleId {
            address: publisher,
            name: "Token".to_owned(),
        };
        let module_bytes = compile_module(&token_source(publisher));
        let mut state = StateTree::new(StateConfig::default()).unwrap();
        SequentialExecutor::new(100)
            .unwrap()
            .execute_block(
                &mut state,
                &[
                    MoveOperation::PublishModule {
                        sender: publisher,
                        module_bytes,
                    },
                    MoveOperation::EntryFunction(MoveCall {
                        sender: publisher,
                        module: module.clone(),
                        function: "mint".to_owned(),
                        arguments: vec![MoveArgument::Signer, MoveArgument::U64(100)],
                    }),
                    MoveOperation::EntryFunction(MoveCall {
                        sender: recipient,
                        module: module.clone(),
                        function: "mint".to_owned(),
                        arguments: vec![MoveArgument::Signer, MoveArgument::U64(0)],
                    }),
                ],
            )
            .unwrap();
        let resource_id = state
            .active_objects()
            .find(|object| {
                object.owner == Owner::Single(publisher)
                    && object.type_tag.contains("move::resource")
            })
            .unwrap()
            .id;
        let initial_version = state.object(&resource_id).unwrap().version;

        let mut evm = EvmHost::new(EvmAddress::from([0x11; 20]));
        let contract = evm.deploy_bridge_contract(&mut state).unwrap();
        let read = evm
            .call(&mut state, contract, encode_object_read(resource_id))
            .unwrap();
        assert_eq!(u64::from_le_bytes(read.output.try_into().unwrap()), 100);

        evm.call(
            &mut state,
            contract,
            encode_object_write(resource_id, initial_version, &90_u64.to_le_bytes()),
        )
        .unwrap();
        assert_eq!(resource_balance(&state, resource_id), 90);
        let after_evm_write = state.root().unwrap();
        assert!(
            evm.call(
                &mut state,
                contract,
                encode_object_write(resource_id, initial_version, &1_u64.to_le_bytes()),
            )
            .is_err()
        );
        assert_eq!(state.root().unwrap(), after_evm_write);

        SequentialExecutor::new(100)
            .unwrap()
            .execute_block(
                &mut state,
                &[MoveOperation::EntryFunction(MoveCall {
                    sender: publisher,
                    module,
                    function: "transfer".to_owned(),
                    arguments: vec![
                        MoveArgument::Address(publisher),
                        MoveArgument::Address(recipient),
                        MoveArgument::U64(10),
                    ],
                })],
            )
            .unwrap();
        let balances = state
            .active_objects()
            .filter_map(|object| match object.owner {
                Owner::Single(owner) if object.type_tag.contains("move::resource") => Some((
                    owner,
                    u64::from_le_bytes(object.data.as_slice().try_into().unwrap()),
                )),
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(balances[&publisher], 80);
        assert_eq!(balances[&recipient], 10);
    }

    fn token_source(address: Address) -> String {
        include_str!("../../vm-move/tests/fixtures/token.move")
            .replace("__KESTREL_PUBLISHER__", &address.to_string())
    }

    fn resource_balance(state: &StateTree, id: ObjectId) -> u64 {
        u64::from_le_bytes(
            state
                .object(&id)
                .unwrap()
                .data
                .as_slice()
                .try_into()
                .unwrap(),
        )
    }

    fn compile_module(source: &str) -> Vec<u8> {
        let directory = TempDir::new().unwrap();
        let source_path = directory.path().join("cross-vm.move");
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
