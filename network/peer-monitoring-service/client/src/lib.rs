// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

use crate::logging::{LogEntry, LogEvent, LogSchema};
use aptos_config::{config::PeerMonitoringServiceConfig, network_id::PeerNetworkId};
use aptos_logger::{info, warn};
use aptos_network::{
    application::{
        interface::{NetworkClient, NetworkClientInterface},
        storage::PeersAndMetadata,
    },
    protocols::network::RpcError,
};
use aptos_peer_monitoring_service_types::{
    PeerMonitoringServiceError, PeerMonitoringServiceMessage, PeerMonitoringServiceRequest,
    PeerMonitoringServiceResponse,
};
use aptos_time_service::TimeService;
use futures::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use std::ops::Add;
use thiserror::Error;

mod logging;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Error from remote monitoring service: {0}")]
    PeerMonitoringServiceError(#[from] PeerMonitoringServiceError),

    #[error("Aptos network rpc error: {0}")]
    RpcError(#[from] RpcError),
}

/// The interface for sending peer monitoring service requests and querying
/// peer information.
#[derive(Clone, Debug)]
pub struct PeerMonitoringServiceClient<NetworkClient> {
    network_client: NetworkClient,
}

impl<NetworkClient: NetworkClientInterface<PeerMonitoringServiceMessage>>
    PeerMonitoringServiceClient<NetworkClient>
{
    pub fn new(network_client: NetworkClient) -> Self {
        Self { network_client }
    }

    pub async fn send_request(
        &self,
        recipient: PeerNetworkId,
        request: PeerMonitoringServiceRequest,
        timeout: Duration,
    ) -> Result<PeerMonitoringServiceResponse, Error> {
        let response = self
            .network_client
            .send_to_peer_rpc(
                PeerMonitoringServiceMessage::Request(request),
                timeout,
                recipient,
            )
            .await
            .map_err(|error| Error::NetworkError(error.to_string()))?;
        match response {
            PeerMonitoringServiceMessage::Response(Ok(response)) => Ok(response),
            PeerMonitoringServiceMessage::Response(Err(err)) => {
                Err(Error::PeerMonitoringServiceError(err))
            },
            PeerMonitoringServiceMessage::Request(request) => Err(Error::NetworkError(format!(
                "Got peer monitoring request instead of response! Request: {:?}",
                request
            ))),
        }
    }

    pub fn get_available_peers(&self) -> Result<Vec<PeerNetworkId>, Error> {
        self.network_client
            .get_available_peers()
            .map_err(|error| Error::NetworkError(error.to_string()))
    }

    pub fn get_peers_and_metadata(&self) -> Arc<PeersAndMetadata> {
        self.network_client.get_peers_and_metadata()
    }
}

/// Runs the peer monitor that continuously monitors
/// the state of the peers.
pub async fn start_peer_monitor(
    peer_monitoring_config: PeerMonitoringServiceConfig,
    peer_monitoring_network_client: NetworkClient<PeerMonitoringServiceMessage>,
    time_service: TimeService,
) {
    // Create an interval ticker for the monitor loop
    let ticker = time_service.interval(peer_monitoring_config.peer_monitor_loop_interval_secs);
    futures::pin_mut!(ticker);

    // Create a new peer monitor state
    let peer_monitor_state = Arc::new(RwLock::new(PeerMonitorState::new()));

    // Start the monitor loop
    info!(
        (LogSchema::new(LogEntry::PeerMonitorLoop)
            .event(LogEvent::StartedPeerMonitorLoop)
            .message("Starting the peer monitor!"))
    );
    loop {
        // Wait for the next round before pinging peers
        ticker.next().await;

        // Get all connected peers
        let peers_and_metadata = peer_monitoring_network_client.get_peers_and_metadata();
        let connected_peers_and_metadata =
            match peers_and_metadata.get_connected_peers_and_metadata() {
                Ok(connected_peers_and_metadata) => connected_peers_and_metadata,
                Err(error) => {
                    warn!(
                        (LogSchema::new(LogEntry::PeerMonitorLoop)
                            .event(LogEvent::UnexpectedErrorEncountered)
                            .error(&error)
                            .message("Failed to run the peer monitor loop!"))
                    );
                    continue; // Move to the new loop iteration
                },
            };

        // Ping the peers that need to be refreshed
        for peer_network_id in connected_peers_and_metadata.keys() {
            let latency_ping_interval_secs = peer_monitoring_config.latency_monitoring.latency_ping_interval_secs;
            let peer_needs_latency_ping = peer_monitor_state
                .read()
                .needs_new_latency_ping(peer_network_id, latency_ping_interval_secs);
            let in_flight_latency_ping = peer_monitor_state
                .read()
                .in_flight_latency_ping(peer_network_id);

            // Ping the peer only if it needs to be pinged
            if peer_needs_latency_ping && !in_flight_latency_ping {

            }
        }

        // If last ping time is too long ago

        // If there's no in-flight poll

        // Send a new poll
    }
}

/// A simple container that holds the state of the peer monitor
struct PeerMonitorState {
    in_flight_latency_pings: HashSet<PeerNetworkId>, // The latency pings currently in-flight
    last_latency_ping: HashMap<PeerNetworkId, Instant>, // The last latency ping timestamp per peer
    num_consecutive_latency_ping_failures: HashMap<PeerNetworkId, u64>, // The num of consecutive latency ping failures per peer
}

impl PeerMonitorState {
    pub fn new() -> Self {
        Self {
            in_flight_latency_pings: HashSet::new(),
            last_latency_ping: HashMap::new(),
            num_consecutive_latency_ping_failures: HashMap::new(),
        }
    }

    /// Returns true iff there is a latency ping currently
    /// in-flight for the specified peer.
    fn in_flight_latency_ping(&self, peer_network_id: &PeerNetworkId) -> bool {
        self.in_flight_latency_pings.contains(peer_network_id)
    }

    /// Returns true iff the given peer needs to have a new
    /// latency ping sent (based on the latest ping time).
    fn needs_new_latency_ping(&self, peer_network_id: &PeerNetworkId, latency_ping_interval_secs: u64) -> bool {
        match self.last_latency_ping.get(peer_network_id) {
            Some(last_latency_ping) => {
                Instant::now() > last_latency_ping.add(Duration::from_secs(latency_ping_interval_secs))
            },
            None => true,
        }
    }
}
