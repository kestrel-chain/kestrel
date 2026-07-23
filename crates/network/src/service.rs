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
use tokio::{sync::mpsc, task::JoinHandle};

use crate::Shred;

const TRANSACTION_TOPIC: &str = "kestrel/transactions/1";
const SHRED_PROTOCOL: &str = "/kestrel/shreds/1";

/// Validator peer with a stable libp2p identity and dial address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredPeer {
    pub peer_id: PeerId,
    pub address: Multiaddr,
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
            heartbeat_interval: Duration::from_millis(100),
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
        let (inbound_transaction_sender, inbound_transactions) =
            mpsc::channel(config.inbound_transaction_capacity);
        let (inbound_shred_sender, inbound_shreds) = mpsc::channel(config.inbound_shred_capacity);
        let handle = NetworkHandle {
            transaction_sender,
            shred_sender,
            ban_sender,
            transaction_max_bytes: config.transaction_max_bytes,
            shred_max_bytes: config.shred_max_bytes,
        };
        let task = tokio::spawn(run(
            swarm,
            transaction_receiver,
            shred_receiver,
            ban_receiver,
            inbound_transaction_sender,
            inbound_shred_sender,
            config.shred_max_bytes,
        ));
        Ok(Self {
            local_peer_id,
            handle,
            inbound_transactions,
            inbound_shreds,
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

async fn run(
    mut swarm: Swarm<Behaviour>,
    mut transaction_receiver: mpsc::Receiver<Vec<u8>>,
    mut shred_receiver: mpsc::Receiver<(PeerId, Shred, bool)>,
    mut ban_receiver: mpsc::Receiver<PeerId>,
    inbound_transaction_sender: mpsc::Sender<InboundTransaction>,
    inbound_shred_sender: mpsc::Sender<InboundShred>,
    shred_max_bytes: usize,
) {
    let transaction_topic = IdentTopic::new(TRANSACTION_TOPIC);
    loop {
        tokio::select! {
            Some(transaction) = transaction_receiver.recv() => {
                let _ = swarm.behaviour_mut().transaction_gossip.publish(transaction_topic.clone(), transaction);
            }
            Some((peer, shred, relay_requested)) = shred_receiver.recv() => {
                swarm.behaviour_mut().shred_exchange.send_request(
                    &peer,
                    ShredRequest { shred, relay_requested },
                );
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
                    if request.shred.payload.len() <= shred_max_bytes {
                        let _ = inbound_shred_sender.try_send(InboundShred {
                            source: peer,
                            shred: request.shred,
                            relay_requested: request.relay_requested,
                        });
                    }
                    let _ = swarm.behaviour_mut().shred_exchange.send_response(channel, ShredResponse);
                }
                _ => {}
            },
            else => break,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ShredRequest {
    shred: Shred,
    relay_requested: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ShredResponse;

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, time::Duration};

    use tokio::sync::mpsc;
    use types::Hash;

    use crate::Shred;

    use super::{ConfiguredPeer, GossipConfig, GossipError, NetworkHandle, NetworkNode};

    #[test]
    fn full_shred_queue_does_not_block_transaction_queue() {
        let (transaction_sender, mut transaction_receiver) = mpsc::channel(1);
        let (shred_sender, _shred_receiver) = mpsc::channel(1);
        let (ban_sender, _ban_receiver) = mpsc::channel(1);
        let handle = NetworkHandle {
            transaction_sender,
            shred_sender,
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
        let handle = NetworkHandle {
            transaction_sender,
            shred_sender,
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
        let received =
            tokio::time::timeout(Duration::from_secs(3), first.inbound_transactions.recv())
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
        let received = tokio::time::timeout(Duration::from_secs(3), first.inbound_shreds.recv())
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
        let received =
            tokio::time::timeout(Duration::from_secs(3), first.inbound_transactions.recv())
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

    fn reserve_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }
}
