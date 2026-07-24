use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use libp2p::{Multiaddr, PeerId, identity};
use network::{ConfiguredPeer, GossipConfig, NetworkFaults, NetworkNode};
use node::{
    BlockLifecycle, ConsensusCoordinator, CoordinatorConfig, CoordinatorFaults, GenesisDocument,
    Stage2Pipeline, Stage2PipelineConfig,
};
use rpc::{NodeStatus, RpcConfig, RpcService, TransactionSubmitter};
use state::StateTree;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;
use types::Hash;

const NEW_OBJECT_RENT_BALANCE: u64 = 100;

#[tokio::main]
#[allow(clippy::too_many_lines)] // Keep operator flag validation and startup wiring in one auditable path.
async fn main() -> Result<()> {
    init_tracing()?;
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.first().map(String::as_str) != Some("run") {
        println!(concat!(
            "node ",
            env!("CARGO_PKG_VERSION"),
            "\nusage: node run --genesis PATH [--rpc ADDRESS] [--allow-public-rpc] ",
            "[--validator-id HEX --validator-key PATH --gossip-key PATH --data-dir PATH]"
        ));
        return Ok(());
    }
    let genesis_path =
        value_after(&arguments, "--genesis").context("--genesis PATH is required")?;
    let rpc_address = value_after(&arguments, "--rpc").unwrap_or("127.0.0.1:8899");
    let rpc_address = rpc_address
        .parse::<SocketAddr>()
        .context("invalid --rpc socket address")?;
    if !rpc_address.ip().is_loopback()
        && !arguments.iter().any(|value| value == "--allow-public-rpc")
    {
        bail!("refusing a public RPC bind without --allow-public-rpc");
    }

    let genesis = GenesisDocument::load_json(genesis_path).context("genesis validation failed")?;
    let validated = genesis.validate()?;
    let status = Arc::new(RwLock::new(NodeStatus {
        chain_id: genesis.chain_id.clone(),
        genesis_hash: validated.genesis_hash,
        finalized_height: 0,
        committed_height: 0,
        finalized_block: validated.genesis_hash,
        state_root: validated.state_root,
        peer_count: 0,
        ready: false,
        finality_latency_ms: None,
        view_changes: 0,
    }));

    let validator_identity = (
        value_after(&arguments, "--validator-id"),
        value_after(&arguments, "--validator-key"),
        value_after(&arguments, "--gossip-key"),
        value_after(&arguments, "--data-dir"),
    );
    let submitter: Option<Arc<dyn TransactionSubmitter>>;
    let rpc_state: Arc<RwLock<StateTree>>;
    if let (Some(id), Some(key_path), Some(gossip_key_path), Some(data_directory)) =
        validator_identity
    {
        let id = parse_hash(id)?;
        let private_key = hex::decode(
            std::fs::read_to_string(key_path)
                .context("failed to read validator key")?
                .trim(),
        )
        .context("validator key must be hexadecimal")?;
        let gossip_identity = identity::Keypair::from_protobuf_encoding(
            &std::fs::read(gossip_key_path).context("failed to read gossip key")?,
        )
        .context("gossip key must be a valid protobuf-encoded libp2p keypair")?;
        let local_entry = genesis
            .validators
            .iter()
            .find(|entry| entry.validator.id == id)
            .context("--validator-id is absent from genesis")?;
        if gossip_identity.public().to_peer_id().to_string() != local_entry.gossip_peer_id {
            bail!("--gossip-key does not match this validator's genesis gossip peer ID");
        }
        let listen_address = local_entry
            .gossip_address
            .parse::<Multiaddr>()
            .context("genesis gossip address is not a valid multiaddr")?;
        let configured_peers = genesis
            .validators
            .iter()
            .filter(|entry| entry.validator.id != id)
            .map(|entry| {
                Ok(ConfiguredPeer {
                    peer_id: entry.gossip_peer_id.parse::<PeerId>()?,
                    address: entry.gossip_address.parse::<Multiaddr>()?,
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()
            .context("invalid peer gossip identity or address in genesis")?;
        let validator_peers = genesis
            .validators
            .iter()
            .map(|entry| Ok((entry.validator.id, entry.gossip_peer_id.parse::<PeerId>()?)))
            .collect::<Result<BTreeMap<_, _>, anyhow::Error>>()
            .context("invalid peer gossip identity in genesis")?;

        let data_directory = std::path::Path::new(data_directory);
        let shared_state = Arc::new(RwLock::new(StateTree::new(genesis.state_config)?));
        let lifecycle = BlockLifecycle::open(
            &genesis,
            data_directory.join("application"),
            Arc::clone(&status),
            Arc::clone(&shared_state),
            NEW_OBJECT_RENT_BALANCE,
            std::thread::available_parallelism().map_or(4, std::num::NonZero::get),
        )?;
        let network_faults = NetworkFaults {
            outbound_delay: Duration::from_millis(
                parse_optional(&arguments, "--gossip-delay-ms")?.unwrap_or(0),
            ),
            transaction_drop_basis_points: parse_optional(&arguments, "--tx-drop-bps")?
                .unwrap_or(0),
            shred_drop_basis_points: parse_optional(&arguments, "--shred-drop-bps")?.unwrap_or(0),
            shred_outage: Duration::from_millis(
                parse_optional(&arguments, "--shred-outage-ms")?.unwrap_or(0),
            ),
        };
        let network_node = NetworkNode::spawn(
            gossip_identity,
            GossipConfig {
                listen_address,
                configured_peers,
                faults: network_faults,
                ..GossipConfig::default()
            },
        )?;
        let (finalized_order_sender, finalized_order_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (pipeline, handle) = Stage2Pipeline::new(
            &genesis,
            network_node,
            lifecycle,
            validator_peers,
            finalized_order_receiver,
            Stage2PipelineConfig::default(),
        )?;
        let proposal_source = pipeline.proposal_source();

        let faults = CoordinatorFaults {
            withhold_votes: arguments.iter().any(|value| value == "--withhold-votes"),
            corrupt_votes: arguments.iter().any(|value| value == "--corrupt-votes"),
            equivocate_when_leader: arguments.iter().any(|value| value == "--equivocate"),
            blocked_peers: value_after(&arguments, "--blocked-peers")
                .map(parse_hash_list)
                .transpose()?
                .unwrap_or_default(),
            outbound_delay: Duration::from_millis(
                parse_optional(&arguments, "--delay-ms")?.unwrap_or(0),
            ),
            drop_basis_points: parse_optional(&arguments, "--drop-bps")?.unwrap_or(0),
            proposal_delay: Duration::from_millis(
                parse_optional(&arguments, "--proposal-delay-ms")?.unwrap_or(0),
            ),
        };
        let config = CoordinatorConfig {
            stop_after_height: parse_optional(&arguments, "--stop-after-height")?,
            ..CoordinatorConfig::default()
        };
        let coordinator = ConsensusCoordinator::bind_with_pipeline(
            &genesis,
            id,
            private_key,
            data_directory.join("consensus"),
            Arc::clone(&status),
            config,
            faults,
            proposal_source,
            finalized_order_sender,
        )
        .await?;
        let genesis_unix_ms = genesis.genesis_unix_ms;
        tokio::spawn(async move {
            match coordinator.run(genesis_unix_ms).await {
                Ok(outcome) => info!(
                    height = outcome.finalized_height,
                    block = %outcome.finalized_block,
                    latency_ms = outcome.finality_latency_ms,
                    view_changes = outcome.view_changes,
                    "consensus coordinator reached its configured height bound"
                ),
                Err(error) => tracing::error!(%error, "consensus coordinator stopped"),
            }
        });
        tokio::spawn(async move {
            if let Err(error) = pipeline.run().await {
                tracing::error!(%error, "Stage 2 pipeline stopped");
            }
        });
        submitter = Some(Arc::new(handle));
        rpc_state = shared_state;
    } else if validator_identity.0.is_some()
        || validator_identity.1.is_some()
        || validator_identity.2.is_some()
        || validator_identity.3.is_some()
    {
        bail!(
            "--validator-id, --validator-key, --gossip-key, and --data-dir must be supplied together"
        );
    } else {
        let mut state = StateTree::new(genesis.state_config)?;
        for object in &genesis.initial_objects {
            state.create_object(object.clone())?;
        }
        if let Ok(mut current) = status.write() {
            current.ready = true;
        }
        submitter = None;
        rpc_state = Arc::new(RwLock::new(state));
    }

    let service = RpcService::new(
        RpcConfig::default(),
        Arc::clone(&status),
        rpc_state,
        submitter,
    )?;

    let listener = TcpListener::bind(rpc_address).await?;
    info!(%rpc_address, genesis = %validated.genesis_hash, "validator RPC ready");
    service
        .serve(listener, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

fn parse_hash(encoded: &str) -> Result<Hash> {
    let bytes = hex::decode(encoded.strip_prefix("0x").unwrap_or(encoded))
        .context("validator ID must be hexadecimal")?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("validator ID must contain exactly 32 bytes"))?;
    Ok(Hash::from_bytes(bytes))
}

fn parse_hash_list(encoded: &str) -> Result<BTreeSet<Hash>> {
    encoded
        .split(',')
        .filter(|item| !item.is_empty())
        .map(parse_hash)
        .collect()
}

fn parse_optional<T>(arguments: &[String], flag: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value_after(arguments, flag)
        .map(|value| {
            value
                .parse::<T>()
                .with_context(|| format!("invalid value for {flag}"))
        })
        .transpose()
}

fn value_after<'a>(arguments: &'a [String], flag: &str) -> Option<&'a str> {
    arguments
        .windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].as_str())
}

fn init_tracing() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .map_err(|error| anyhow::anyhow!("tracing initialization failed: {error}"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn binary_identifies_as_kestrel() {
        assert_eq!(env!("CARGO_PKG_NAME"), "node");
    }
}
