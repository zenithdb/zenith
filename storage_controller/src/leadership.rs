use std::sync::Arc;

use hyper::Uri;
use tokio_util::sync::CancellationToken;

use crate::{
    peer_client::{GlobalObservedState, PeerClient},
    persistence::{ControllerPersistence, DatabaseError, DatabaseResult, Persistence},
    service::Config,
};

/// Helper for storage controller leadership acquisition
pub(crate) struct Leadership {
    persistence: Arc<Persistence>,
    config: Config,
    cancel: CancellationToken,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum LeadershipError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

pub(crate) type LeadershipResult<T> = Result<T, LeadershipError>;

impl Leadership {
    pub(crate) fn new(
        persistence: Arc<Persistence>,
        config: Config,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            persistence,
            config,
            cancel,
        }
    }

    /// Find the current leader in the database and request it to step down if required.
    /// Should be called early on in within the start-up sequence.
    ///
    /// Returns a tuple of two optionals: the current leader and its observed state
    pub(crate) async fn prologue(
        &self,
    ) -> LeadershipResult<(Option<ControllerPersistence>, Option<GlobalObservedState>)> {
        let leader = self.get_leader().await?;
        let leader_step_down_state = if let Some(ref leader) = leader {
            if self.config.start_as_candidate {
                self.request_step_down(leader).await
            } else {
                None
            }
        } else {
            tracing::info!("No leader found to request step down from. Will build observed state.");
            None
        };

        Ok((leader, leader_step_down_state))
    }

    /// Mark the current storage controller instance as the leader in the database
    pub(crate) async fn epilogue(
        &self,
        current_leader: Option<ControllerPersistence>,
    ) -> LeadershipResult<()> {
        if let Some(address_for_peers) = &self.config.address_for_peers {
            // TODO: `address-for-peers` can become a mandatory cli arg
            // after we update the k8s setup
            let proposed_leader = ControllerPersistence {
                address: address_for_peers.to_string(),
                started_at: chrono::Utc::now(),
            };

            self.persistence
                .update_leader(current_leader, proposed_leader)
                .await
                .map_err(LeadershipError::Database)
        } else {
            tracing::info!("No address-for-peers provided. Skipping leader persistence.");
            Ok(())
        }
    }

    async fn get_leader(&self) -> DatabaseResult<Option<ControllerPersistence>> {
        self.persistence.get_leader().await
    }

    /// Request step down from the currently registered leader in the database
    ///
    /// If such an entry is persisted, the success path returns the observed
    /// state and details of the leader. Otherwise, None is returned indicating
    /// there is no leader currently.
    async fn request_step_down(
        &self,
        leader: &ControllerPersistence,
    ) -> Option<GlobalObservedState> {
        tracing::info!("Sending step down request to {leader:?}");

        // TODO: jwt token
        let client = PeerClient::new(
            Uri::try_from(leader.address.as_str()).expect("Failed to build leader URI"),
            self.config.jwt_token.clone(),
        );
        let state = client.step_down(&self.cancel).await;
        match state {
            Ok(state) => Some(state),
            Err(err) => {
                // TODO: Make leaders periodically update a timestamp field in the
                // database and, if the leader is not reachable from the current instance,
                // but inferred as alive from the timestamp, abort start-up. This avoids
                // a potential scenario in which we have two controllers acting as leaders.
                tracing::error!(
                    "Leader ({}) did not respond to step-down request: {}",
                    leader.address,
                    err
                );

                None
            }
        }
    }
}
