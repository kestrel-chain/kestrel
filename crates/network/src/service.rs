use std::time::Duration;

use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
    allow_block_list::{self, BlockedPeers},
    gossipsub::{self, IdentTopic, MessageAuthenticity},
    identify, identity, mdns, noise, request_response,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle, time::sleep};
use types::Hash;

use crate::Shred;

const TRANSACTION_TOPIC: &str = "kestrel/transactions/1";
const SHRED_PROTOCOL: &str = "/kestrel/shreds/1";

/// Validator peer with a stable libp2p identity and dial address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredPeer {
    pub peer_id: PeerId,
    pub address: Multiaddr,
}

/// Operator-controlled fault injection on this node's libp2p transaction and
/// shred paths, mirroring `node::CoordinatorFaults` for the raw-TCP consensus
/// path (TD-003). All fields are disabled by default. Drops are deterministic
/// in the message payload, so a given transaction or shred is either always or
/// never dropped by a given node, keeping fault scenarios reproducible.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NetworkFaults {
    /// Delay applied before each outbound transaction publish and shred send on
    /// this node, modelling added link latency. Because a single task drives
    /// the swarm, this also stalls that node's other network processing for the
    /// delay — an intentional "slow node" model, not a per-link queue.
    pub outbound_delay: Duration,
    /// Fraction of this node's outbound transaction publishes dropped, in basis
    /// points (0..=10000). 10000 silences its transaction gossip entirely.
    pub transaction_drop_basis_points: u16,
    /// Fraction of this node's outbound shred sends dropped, in basis points
    /// (0..=10000). 10000 models a fully dead relay/leader shred path on it.
    ///
    /// Drops are deterministic in the message, so a blocked path never reopens.
    /// That is the right model for a persistently dead relay, but it cannot
    /// exercise anything that retries — use `shred_outage` for that.
    pub shred_drop_basis_points: u16,
    /// Drops every outbound shred for this long after the node starts, then
    /// delivers normally: a transient outage rather than a permanent one, which
    /// is the case payload repair exists to recover from.
    pub shred_outage: Duration,
}

/// Independent bandwidth/queue settings for transaction and shred propagation.
#[derive(Clone, Debug)]
pub struct GossipConfig {
    pub listen_address: Multiaddr,
    pub configured_peers: Vec<ConfiguredPeer>,
    pub transaction_queue_capacity: usize,
    pub shred_queue_capacity: usize,
    pub inbound_transaction_capacity: usize,
    pub inbound_shred_capacity: usize,
    pub transaction_max_bytes: usize,
    pub shred_max_bytes: usize,
    pub heartbeat_interval: Duration,
    pub faults: NetworkFaults,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            listen_address: "/ip4/0.0.0.0/tcp/0"
                .parse()
                .expect("the default multiaddress is static and valid"),
            configured_peers: Vec::new(),
            transaction_queue_capacity: 4_096,
            shred_queue_capacity: 16_384,
            inbound_transaction_capacity: 4_096,
            inbound_shred_capacity: 16_384,
            transaction_max_bytes: 512 * 1024,
            shred_max_bytes: 2 * 1024 * 1024,
            // Profiling the real pipeline (docs/TECH_DEBT.md TD-003 tooling)
            // found the first block after a burst of admissions was reliably
            // empty in 15/15 trials: with round-robin leadership, the next
            // height after submission is almost never led by the validator
            // that admitted the transaction, so it depends on gossip to reach
            // the other validators' mempools before their proposal is built.
            // 100ms was large enough to reliably miss that window; the test
            // suite already exercises 25ms successfully.
            heartbeat_interval: Duration::from_millis(25),
            faults: NetworkFaults::default(),
        }
    }
}

/// Transaction received from the dedicated mempool gossipsub behaviour.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundTransaction {
    pub source: Option<PeerId>,
    pub bytes: Vec<u8>,
}

/// Shred received directly from one leader or relay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundShred {
    pub source: PeerId,
    pub shred: Shred,
    /// True only on the leader-to-relay leg. A relay must fan this shred out
    /// directly to validators with the flag cleared, preventing multi-hop trees.
    pub relay_requested: bool,
}

/// Network construction and queue failures.
#[derive(Debug, Error)]
pub enum GossipError {
    #[error("network setup failed: {0}")]
    Setup(String),
    #[error("network listen failed: {0}")]
    Listen(String),
    #[error("transaction gossip queue is full or closed")]
    TransactionQueueUnavailable,
    #[error("shred propagation queue is full or closed")]
    ShredQueueUnavailable,
    #[error("transaction exceeds its configured maximum size")]
    TransactionTooLarge,
    #[error("shred exceeds its configured maximum size")]
    ShredTooLarge,
    #[error("peer ban queue is full or closed")]
    BanQueueUnavailable,
}

/// Cloneable sender side of the independent transaction and shred paths.
#[derive(Clone, Debug)]
pub struct NetworkHandle {
    transaction_sender: mpsc::Sender<Vec<u8>>,
    shred_sender: mpsc::Sender<(PeerId, Shred, bool)>,
    repair_sender: mpsc::Sender<(PeerId, u64)>,
    ban_sender: mpsc::Sender<PeerId>,
    transaction_max_bytes: usize,
    shred_max_bytes: usize,
}

impl NetworkHandle {
    /// Queues a transaction without waiting for shred capacity.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction exceeds its budget or its queue is unavailable.
    pub fn try_publish_transaction(&self, bytes: Vec<u8>) -> Result<(), GossipError> {
        if bytes.len() > self.transaction_max_bytes {
            return Err(GossipError::TransactionTooLarge);
        }
        self.transaction_sender
            .try_send(bytes)
            .map_err(|_| GossipError::TransactionQueueUnavailable)
    }

    /// Queues a shred for one explicit relay or validator. The transport never
    /// forwards it automatically, preserving `KestrelCast`'s single relay layer.
    ///
    /// # Errors
    ///
    /// Returns an error if the shred exceeds its budget or its queue is unavailable.
    pub fn try_send_shred(&self, peer: PeerId, shred: Shred) -> Result<(), GossipError> {
        self.try_send_shred_inner(peer, shred, false)
    }

    /// Queues the leader-to-relay leg of a `KestrelCast` shred. The receiving
    /// relay is instructed to perform exactly one direct validator fanout.
    ///
    /// # Errors
    ///
    /// Returns an error if the shred exceeds its budget or its queue is unavailable.
    pub fn try_send_relay_shred(&self, peer: PeerId, shred: Shred) -> Result<(), GossipError> {
        self.try_send_shred_inner(peer, shred, true)
    }

    fn try_send_shred_inner(
        &self,
        peer: PeerId,
        shred: Shred,
        relay_requested: bool,
    ) -> Result<(), GossipError> {
        if shred.payload.len() > self.shred_max_bytes {
            return Err(GossipError::ShredTooLarge);
        }
        self.shred_sender
            .try_send((peer, shred, relay_requested))
            .map_err(|_| GossipError::ShredQueueUnavailable)
    }

    /// Asks `peer` for the shreds of `height`, for a validator that has a
    /// certified order it cannot execute because the payload never arrived.
    ///
    /// `KestrelCast` delivery is fire-and-forget, so a single lost send would
    /// otherwise strand that validator's execution permanently: it holds the
    /// order, and every transaction named in it, but cannot rebuild the payload
    /// because the leader's per-transaction base fees exist nowhere else and are
    /// bound by the certified fee commitment.
    ///
    /// # Errors
    ///
    /// Returns an error if the repair queue is full or the network task stopped.
    pub fn try_request_payload(&self, peer: PeerId, height: u64) -> Result<(), GossipError> {
        self.repair_sender
            .try_send((peer, height))
            .map_err(|_| GossipError::ShredQueueUnavailable)
    }

    /// Blocks all present and future connections to and from `peer` and closes
    /// any connection to it immediately. Intended for a peer that has
    /// repeatedly sent invalid gossip; there is no automatic unban.
    ///
    /// # Errors
    ///
    /// Returns an error if the ban queue is full or the network task stopped.
    pub fn ban_peer(&self, peer: PeerId) -> Result<(), GossipError> {
        self.ban_sender
            .try_send(peer)
            .map_err(|_| GossipError::BanQueueUnavailable)
    }
}

/// Running libp2p node and independent inbound propagation streams.
pub struct NetworkNode {
    pub local_peer_id: PeerId,
    pub handle: NetworkHandle,
    pub inbound_transactions: mpsc::Receiver<InboundTransaction>,
    pub inbound_shreds: mpsc::Receiver<InboundShred>,
    /// `(peer, height)` pairs from peers that could not reconstruct that
    /// height's payload. Answered by re-sending its shreds, which only the
    /// pipeline can do because only it maps heights to payloads.
    pub inbound_repair_requests: mpsc::Receiver<(PeerId, u64)>,
    pub task: JoinHandle<()>,
}

impl NetworkNode {
    /// Builds a TCP/Noise/Yamux node with mDNS discovery, transaction gossipsub,
    /// and targeted request/response shred delivery.
    ///
    /// # Errors
    ///
    /// Returns an error if transport, behaviour, or listening setup fails.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime because the network loop is spawned.
    pub fn spawn(identity: identity::Keypair, config: GossipConfig) -> Result<Self, GossipError> {
        let local_peer_id = identity.public().to_peer_id();
        let mut swarm = build_swarm(identity, &config)?;
        swarm
            .listen_on(config.listen_address)
            .map_err(|error| GossipError::Listen(error.to_string()))?;
        for peer in &config.configured_peers {
            if peer.peer_id == local_peer_id {
                continue;
            }
            swarm
                .behaviour_mut()
                .transaction_gossip
                .add_explicit_peer(&peer.peer_id);
            swarm
                .dial(
                    peer.address
                        .clone()
                        .with(libp2p::multiaddr::Protocol::P2p(peer.peer_id)),
                )
                .map_err(|error| GossipError::Setup(error.to_string()))?;
        }

        let (transaction_sender, transaction_receiver) =
            mpsc::channel(config.transaction_queue_capacity);
        let (shred_sender, shred_receiver) = mpsc::channel(config.shred_queue_capacity);
        let (ban_sender, ban_receiver) = mpsc::channel(64);
        let (repair_sender, repair_receiver) = mpsc::channel(config.shred_queue_capacity);
        let (inbound_repair_sender, inbound_repair_requests) = mpsc::channel(64);
        let (inbound_transaction_sender, inbound_transactions) =
            mpsc::channel(config.inbound_transaction_capacity);
        let (inbound_shred_sender, inbound_shreds) = mpsc::channel(config.inbound_shred_capacity);
        let handle = NetworkHandle {
            transaction_sender,
            shred_sender,
            repair_sender,
            ban_sender,
            transaction_max_bytes: config.transaction_max_bytes,
            shred_max_bytes: config.shred_max_bytes,
        };
        let task = tokio::spawn(run(
            swarm,
            transaction_receiver,
            shred_receiver,
            ban_receiver,
            repair_receiver,
            InboundSenders {
                transactions: inbound_transaction_sender,
                shreds: inbound_shred_sender,
                repair_requests: inbound_repair_sender,
            },
            RunConfig {
                shred_max_bytes: config.shred_max_bytes,
                faults: config.faults,
            },
        ));
        Ok(Self {
            local_peer_id,
            handle,
            inbound_transactions,
            inbound_shreds,
            inbound_repair_requests,
            task,
        })
    }
}

fn build_swarm(
    identity: identity::Keypair,
    config: &GossipConfig,
) -> Result<Swarm<Behaviour>, GossipError> {
    let local_peer_id = identity.public().to_peer_id();
    let behaviour = Behaviour::new(&identity, local_peer_id, config)?;
    let builder = SwarmBuilder::with_existing_identity(identity)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|error| GossipError::Setup(error.to_string()))?
        .with_behaviour(|_| behaviour)
        .map_err(|error| GossipError::Setup(error.to_string()))?;
    Ok(builder.build())
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    transaction_gossip: gossipsub::Behaviour,
    shred_exchange: request_response::cbor::Behaviour<ShredRequest, ShredResponse>,
    discovery: mdns::tokio::Behaviour,
    identify: identify::Behaviour,
    block_list: allow_block_list::Behaviour<BlockedPeers>,
}

impl Behaviour {
    fn new(
        identity: &identity::Keypair,
        local_peer_id: PeerId,
        config: &GossipConfig,
    ) -> Result<Self, GossipError> {
        let transaction_config = gossipsub::ConfigBuilder::default()
            .protocol_id_prefix("/kestrel/transactions")
            .heartbeat_interval(config.heartbeat_interval)
            .max_transmit_size(config.transaction_max_bytes)
            .validation_mode(gossipsub::ValidationMode::Strict)
            .build()
            .map_err(|error| GossipError::Setup(error.to_string()))?;
        let authenticity = MessageAuthenticity::Signed(identity.clone());
        let mut transaction_gossip = gossipsub::Behaviour::new(authenticity, transaction_config)
            .map_err(|error| GossipError::Setup(error.to_string()))?;
        transaction_gossip
            .subscribe(&IdentTopic::new(TRANSACTION_TOPIC))
            .map_err(|error| GossipError::Setup(error.to_string()))?;
        let shred_exchange = request_response::cbor::Behaviour::new(
            [(
                StreamProtocol::new(SHRED_PROTOCOL),
                request_response::ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        );
        let discovery = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)
            .map_err(|error| GossipError::Setup(error.to_string()))?;
        let identify_behaviour = identify::Behaviour::new(identify::Config::new(
            "/kestrel/identify/1".to_owned(),
            identity.public(),
        ));
        Ok(Self {
            transaction_gossip,
            shred_exchange,
            discovery,
            identify: identify_behaviour,
            block_list: allow_block_list::Behaviour::default(),
        })
    }
}

/// Static per-run delivery settings for the network loop, grouped to keep
/// `run`'s parameter list within bounds.
struct RunConfig {
    shred_max_bytes: usize,
    faults: NetworkFaults,
}

/// The loop's inbound publication points, grouped to keep `run` within bounds.
struct InboundSenders {
    transactions: mpsc::Sender<InboundTransaction>,
    shreds: mpsc::Sender<InboundShred>,
    repair_requests: mpsc::Sender<(PeerId, u64)>,
}

#[allow(clippy::too_many_lines)] // Keep the whole event loop visible in one place.
async fn run(
    mut swarm: Swarm<Behaviour>,
    mut transaction_receiver: mpsc::Receiver<Vec<u8>>,
    mut shred_receiver: mpsc::Receiver<(PeerId, Shred, bool)>,
    mut ban_receiver: mpsc::Receiver<PeerId>,
    mut repair_receiver: mpsc::Receiver<(PeerId, u64)>,
    inbound: InboundSenders,
    config: RunConfig,
) {
    let InboundSenders {
        transactions: inbound_transaction_sender,
        shreds: inbound_shred_sender,
        repair_requests: inbound_repair_sender,
    } = inbound;
    let RunConfig {
        shred_max_bytes,
        faults,
    } = config;
    let transaction_topic = IdentTopic::new(TRANSACTION_TOPIC);
    let local_bytes = swarm.local_peer_id().to_bytes();
    let started = std::time::Instant::now();
    loop {
        tokio::select! {
            Some(transaction) = transaction_receiver.recv() => {
                if deterministic_drop(
                    faults.transaction_drop_basis_points,
                    &[&local_bytes, &transaction],
                ) {
                    continue;
                }
                if !faults.outbound_delay.is_zero() {
                    sleep(faults.outbound_delay).await;
                }
                let _ = swarm.behaviour_mut().transaction_gossip.publish(transaction_topic.clone(), transaction);
            }
            Some((peer, shred, relay_requested)) = shred_receiver.recv() => {
                if started.elapsed() < faults.shred_outage
                    || deterministic_drop(
                        faults.shred_drop_basis_points,
                        &[&local_bytes, &peer.to_bytes(), &shred.payload],
                    )
                {
                    continue;
                }
                if !faults.outbound_delay.is_zero() {
                    sleep(faults.outbound_delay).await;
                }
                swarm.behaviour_mut().shred_exchange.send_request(
                    &peer,
                    ShredRequest::Deliver { shred, relay_requested },
                );
            }
            Some((peer, height)) = repair_receiver.recv() => {
                swarm
                    .behaviour_mut()
                    .shred_exchange
                    .send_request(&peer, ShredRequest::RepairPayload { height });
            }
            Some(peer) = ban_receiver.recv() => {
                swarm.behaviour_mut().block_list.block_peer(peer);
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(BehaviourEvent::Discovery(mdns::Event::Discovered(peers))) => {
                    for (peer, _) in peers {
                        swarm.behaviour_mut().transaction_gossip.add_explicit_peer(&peer);
                    }
                }
                SwarmEvent::Behaviour(BehaviourEvent::Discovery(mdns::Event::Expired(peers))) => {
                    for (peer, _) in peers {
                        swarm.behaviour_mut().transaction_gossip.remove_explicit_peer(&peer);
                    }
                }
                SwarmEvent::Behaviour(BehaviourEvent::TransactionGossip(
                    gossipsub::Event::Message { propagation_source, message, .. }
                )) => {
                    let _ = inbound_transaction_sender.try_send(InboundTransaction {
                        source: message.source.or(Some(propagation_source)),
                        bytes: message.data,
                    });
                }
                SwarmEvent::Behaviour(BehaviourEvent::ShredExchange(
                    request_response::Event::Message {
                        peer,
                        message: request_response::Message::Request { request, channel, .. },
                        ..
                    }
                )) => {
                    match request {
                        ShredRequest::Deliver { shred, relay_requested } => {
                            if shred.payload.len() <= shred_max_bytes {
                                let _ = inbound_shred_sender.try_send(InboundShred {
                                    source: peer,
                                    shred,
                                    relay_requested,
                                });
                            }
                        }
                        // Only the pipeline maps heights to payloads, so the
                        // request is handed up rather than answered here. Its
                        // reply travels back over the ordinary shred path.
                        ShredRequest::RepairPayload { height } => {
                            let _ = inbound_repair_sender.try_send((peer, height));
                        }
                    }
                    let _ = swarm.behaviour_mut().shred_exchange.send_response(channel, ShredResponse);
                }
                _ => {}
            },
            else => break,
        }
    }
}

/// Deterministically decides whether to drop a message, sampling a stable hash
/// of the seed parts so the same message is either always or never dropped by a
/// given node. Mirrors `ConsensusCoordinator::should_drop` for the raw-TCP path.
fn deterministic_drop(basis_points: u16, seed: &[&[u8]]) -> bool {
    if basis_points == 0 {
        return false;
    }
    let mut bytes = b"kestrel/network/drop/v1".to_vec();
    for part in seed {
        bytes.extend_from_slice(part);
    }
    let digest = Hash::digest(bytes);
    let sample = u16::from_be_bytes([digest.as_bytes()[0], digest.as_bytes()[1]]) % 10_000;
    sample < basis_points
}

/// A shred being delivered, or a request for the shreds of a height this peer
/// could not reconstruct.
///
/// Repair is keyed by height rather than by block id: a validator that never
/// received a payload cannot know its hash, since shreds are keyed by the hash
/// of the payload itself. Height is the only identifier a stuck validator
/// already has, from the certified order it is waiting to execute.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum ShredRequest {
    Deliver { shred: Shred, relay_requested: bool },
    RepairPayload { height: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ShredResponse;

#[cfg(test)]
mod tests {
    use std::{
        net::TcpListener,
        time::{Duration, Instant},
    };

    use tokio::sync::mpsc;
    use types::Hash;

    use crate::Shred;

    use super::{
        ConfiguredPeer, GossipConfig, GossipError, NetworkFaults, NetworkHandle, NetworkNode,
    };

    /// Upper bound on a message crossing a loopback libp2p connection, which
    /// takes milliseconds on an idle machine. Deliberately generous, because
    /// the value only matters when the machine is not idle. The windows that
    /// assert a message is *never* delivered stay short by design: a slow
    /// machine only makes non-delivery more likely, so they cannot fail
    /// spuriously the way a positive wait can.
    const DELIVERY_BOUND: Duration = Duration::from_secs(15);

    #[test]
    fn full_shred_queue_does_not_block_transaction_queue() {
        let (transaction_sender, mut transaction_receiver) = mpsc::channel(1);
        let (shred_sender, _shred_receiver) = mpsc::channel(1);
        let (ban_sender, _ban_receiver) = mpsc::channel(1);
        let (repair_sender, _repair_receiver) = mpsc::channel(1);
        let handle = NetworkHandle {
            transaction_sender,
            shred_sender,
            repair_sender,
            ban_sender,
            transaction_max_bytes: 1024,
            shred_max_bytes: 1024,
        };
        let shred = Shred {
            block_id: Hash::digest(b"block"),
            index: 0,
            data_shards: 1,
            parity_shards: 1,
            original_len: 1,
            payload: vec![1],
        };
        let peer = libp2p::PeerId::random();
        handle.try_send_shred(peer, shred.clone()).unwrap();
        assert!(matches!(
            handle.try_send_shred(peer, shred),
            Err(GossipError::ShredQueueUnavailable)
        ));
        handle.try_publish_transaction(vec![7]).unwrap();
        assert_eq!(transaction_receiver.try_recv().unwrap(), vec![7]);
    }

    #[test]
    fn full_transaction_queue_does_not_block_shred_queue() {
        let (transaction_sender, _transaction_receiver) = mpsc::channel(1);
        let (shred_sender, mut shred_receiver) = mpsc::channel(1);
        let (ban_sender, _ban_receiver) = mpsc::channel(1);
        let (repair_sender, _repair_receiver) = mpsc::channel(1);
        let handle = NetworkHandle {
            transaction_sender,
            shred_sender,
            repair_sender,
            ban_sender,
            transaction_max_bytes: 1024,
            shred_max_bytes: 1024,
        };
        handle.try_publish_transaction(vec![1]).unwrap();
        assert!(matches!(
            handle.try_publish_transaction(vec![2]),
            Err(GossipError::TransactionQueueUnavailable)
        ));
        let peer = libp2p::PeerId::random();
        let shred = Shred {
            block_id: Hash::digest(b"other block"),
            index: 0,
            data_shards: 1,
            parity_shards: 1,
            original_len: 1,
            payload: vec![9],
        };
        handle.try_send_shred(peer, shred.clone()).unwrap();
        assert_eq!(shred_receiver.try_recv().unwrap(), (peer, shred, false));
    }

    #[tokio::test]
    async fn tcp_noise_yamux_swarm_builds() {
        let swarm = super::build_swarm(
            libp2p::identity::Keypair::generate_ed25519(),
            &super::GossipConfig::default(),
        )
        .unwrap();
        drop(swarm);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn configured_peer_carries_independent_transaction_and_shred_paths() {
        let first_port = reserve_port();
        let first_identity = libp2p::identity::Keypair::generate_ed25519();
        let first_peer = first_identity.public().to_peer_id();
        let mut first = NetworkNode::spawn(
            first_identity,
            GossipConfig {
                listen_address: format!("/ip4/127.0.0.1/tcp/{first_port}").parse().unwrap(),
                heartbeat_interval: Duration::from_millis(25),
                ..GossipConfig::default()
            },
        )
        .unwrap();
        let second = NetworkNode::spawn(
            libp2p::identity::Keypair::generate_ed25519(),
            GossipConfig {
                listen_address: "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
                configured_peers: vec![ConfiguredPeer {
                    peer_id: first_peer,
                    address: format!("/ip4/127.0.0.1/tcp/{first_port}").parse().unwrap(),
                }],
                heartbeat_interval: Duration::from_millis(25),
                ..GossipConfig::default()
            },
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(300)).await;
        second
            .handle
            .try_publish_transaction(b"signed-transaction".to_vec())
            .unwrap();
        let received = tokio::time::timeout(DELIVERY_BOUND, first.inbound_transactions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.bytes, b"signed-transaction");

        let shred = Shred {
            block_id: Hash::digest(b"configured-peer-block"),
            index: 0,
            data_shards: 1,
            parity_shards: 1,
            original_len: 1,
            payload: vec![42],
        };
        second
            .handle
            .try_send_shred(first_peer, shred.clone())
            .unwrap();
        let received = tokio::time::timeout(DELIVERY_BOUND, first.inbound_shreds.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.source, second.local_peer_id);
        assert_eq!(received.shred, shred);
        assert!(!received.relay_requested);

        first.task.abort();
        second.task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn banning_a_peer_closes_its_connection_and_blocks_reconnection() {
        let first_port = reserve_port();
        let first_identity = libp2p::identity::Keypair::generate_ed25519();
        let first_peer = first_identity.public().to_peer_id();
        let mut first = NetworkNode::spawn(
            first_identity,
            GossipConfig {
                listen_address: format!("/ip4/127.0.0.1/tcp/{first_port}").parse().unwrap(),
                heartbeat_interval: Duration::from_millis(25),
                ..GossipConfig::default()
            },
        )
        .unwrap();
        let second_identity = libp2p::identity::Keypair::generate_ed25519();
        let second_peer = second_identity.public().to_peer_id();
        let second = NetworkNode::spawn(
            second_identity,
            GossipConfig {
                listen_address: "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
                configured_peers: vec![ConfiguredPeer {
                    peer_id: first_peer,
                    address: format!("/ip4/127.0.0.1/tcp/{first_port}").parse().unwrap(),
                }],
                heartbeat_interval: Duration::from_millis(25),
                ..GossipConfig::default()
            },
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(300)).await;
        second
            .handle
            .try_publish_transaction(b"before-ban".to_vec())
            .unwrap();
        let received = tokio::time::timeout(DELIVERY_BOUND, first.inbound_transactions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.bytes, b"before-ban");

        first.handle.ban_peer(second_peer).unwrap();
        // Banning closes the live connection; give the swarm time to act on it
        // and let the second node's periodic reconnection attempts (if any)
        // observe the ban rather than racing this assertion.
        tokio::time::sleep(Duration::from_millis(300)).await;

        second
            .handle
            .try_publish_transaction(b"after-ban".to_vec())
            .unwrap();
        let never_received = tokio::time::timeout(
            Duration::from_millis(500),
            first.inbound_transactions.recv(),
        )
        .await;
        assert!(
            never_received.is_err(),
            "a banned peer's messages must never be delivered after the ban"
        );

        first.task.abort();
        second.task.abort();
    }

    /// Spawns a receiver and a sender that dials it as a configured peer,
    /// applying `sender_faults` to the sender, and waits for the connection to
    /// establish. Returns `(receiver, sender)`.
    async fn configured_pair(sender_faults: NetworkFaults) -> (NetworkNode, NetworkNode) {
        let receiver_port = reserve_port();
        let receiver_identity = libp2p::identity::Keypair::generate_ed25519();
        let receiver_peer = receiver_identity.public().to_peer_id();
        let receiver = NetworkNode::spawn(
            receiver_identity,
            GossipConfig {
                listen_address: format!("/ip4/127.0.0.1/tcp/{receiver_port}")
                    .parse()
                    .unwrap(),
                heartbeat_interval: Duration::from_millis(25),
                ..GossipConfig::default()
            },
        )
        .unwrap();
        let sender = NetworkNode::spawn(
            libp2p::identity::Keypair::generate_ed25519(),
            GossipConfig {
                listen_address: "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
                configured_peers: vec![ConfiguredPeer {
                    peer_id: receiver_peer,
                    address: format!("/ip4/127.0.0.1/tcp/{receiver_port}")
                        .parse()
                        .unwrap(),
                }],
                heartbeat_interval: Duration::from_millis(25),
                faults: sender_faults,
                ..GossipConfig::default()
            },
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        (receiver, sender)
    }

    fn sample_shred() -> Shred {
        Shred {
            block_id: Hash::digest(b"fault-injection-block"),
            index: 0,
            data_shards: 1,
            parity_shards: 1,
            original_len: 1,
            payload: vec![7],
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transaction_drop_basis_points_can_silence_a_nodes_gossip() {
        // Control: no fault, so the receiver does get the transaction. This
        // proves the mesh is live, so the silence under fault below is the drop
        // and not merely an unformed mesh (i.e. the fault assertion is real).
        let (mut receiver, sender) = configured_pair(NetworkFaults::default()).await;
        sender
            .handle
            .try_publish_transaction(b"gossiped-transaction".to_vec())
            .unwrap();
        let delivered = tokio::time::timeout(DELIVERY_BOUND, receiver.inbound_transactions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivered.bytes, b"gossiped-transaction");
        receiver.task.abort();
        sender.task.abort();

        // Fault: 100% outbound transaction loss. `publish` is never reached, so
        // nothing enters the sender's gossip cache and nothing is ever delivered.
        let (mut receiver, sender) = configured_pair(NetworkFaults {
            transaction_drop_basis_points: 10_000,
            ..NetworkFaults::default()
        })
        .await;
        sender
            .handle
            .try_publish_transaction(b"gossiped-transaction".to_vec())
            .unwrap();
        let silenced = tokio::time::timeout(
            Duration::from_millis(750),
            receiver.inbound_transactions.recv(),
        )
        .await;
        assert!(
            silenced.is_err(),
            "a fully-dropped transaction path must deliver nothing"
        );
        receiver.task.abort();
        sender.task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shred_drop_basis_points_models_a_dead_relay_path() {
        // Control: a healthy shred path delivers.
        let (mut receiver, sender) = configured_pair(NetworkFaults::default()).await;
        sender
            .handle
            .try_send_shred(receiver.local_peer_id, sample_shred())
            .unwrap();
        let delivered = tokio::time::timeout(DELIVERY_BOUND, receiver.inbound_shreds.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivered.shred, sample_shred());
        receiver.task.abort();
        sender.task.abort();

        // Fault: 100% outbound shred loss models a node whose relay/leader shred
        // path is dead — it forwards nothing.
        let (mut receiver, sender) = configured_pair(NetworkFaults {
            shred_drop_basis_points: 10_000,
            ..NetworkFaults::default()
        })
        .await;
        sender
            .handle
            .try_send_shred(receiver.local_peer_id, sample_shred())
            .unwrap();
        let silenced =
            tokio::time::timeout(Duration::from_millis(750), receiver.inbound_shreds.recv()).await;
        assert!(
            silenced.is_err(),
            "a fully-dropped shred path must deliver nothing"
        );
        receiver.task.abort();
        sender.task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outbound_delay_postpones_shred_delivery_without_losing_it() {
        // A 400ms outbound delay must push delivery well past the sub-10ms
        // loopback baseline the other tests deliver at, while still arriving.
        let (mut receiver, sender) = configured_pair(NetworkFaults {
            outbound_delay: Duration::from_millis(400),
            ..NetworkFaults::default()
        })
        .await;
        let sent_at = Instant::now();
        sender
            .handle
            .try_send_shred(receiver.local_peer_id, sample_shred())
            .unwrap();
        let delivered = tokio::time::timeout(DELIVERY_BOUND, receiver.inbound_shreds.recv())
            .await
            .unwrap()
            .unwrap();
        let elapsed = sent_at.elapsed();
        assert_eq!(delivered.shred, sample_shred());
        assert!(
            elapsed >= Duration::from_millis(300),
            "outbound delay of 400ms must postpone delivery (took {elapsed:?})"
        );
        receiver.task.abort();
        sender.task.abort();
    }

    fn reserve_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }
}
