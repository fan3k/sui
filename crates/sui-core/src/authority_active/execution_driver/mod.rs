// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::ActiveAuthority;
use crate::authority::AuthorityState;
use crate::authority_client::AuthorityAPI;
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use sui_metrics::spawn_monitored_task;
use sui_types::{
    error::{SuiError, SuiResult},
    messages::VerifiedCertificate,
};
use tokio::{
    sync::Semaphore,
    time::{sleep, timeout},
};
use tracing::{debug, error, info, warn};

#[cfg(test)]
pub(crate) mod tests;

// Maximum number of certificates to be executed concurrently.
// TODO: make this configurable.
const MAX_EXECUTION_CONCURRENCY: usize = 20;
const MAX_EXECUTION_TIMEOUT: Duration = Duration::from_secs(30);

#[async_trait]
pub trait PendCertificateForExecution {
    async fn add_pending_certificates(&self, certs: Vec<VerifiedCertificate>) -> SuiResult<()>;
}

#[async_trait]
impl PendCertificateForExecution for &AuthorityState {
    async fn add_pending_certificates(&self, certs: Vec<VerifiedCertificate>) -> SuiResult<()> {
        AuthorityState::add_pending_certificates(self, certs).await
    }
}

/// A no-op PendCertificateForExecution that we use for testing, when
/// we do not care about certificates actually being executed.
pub struct PendCertificateForExecutionNoop;

#[async_trait]
impl PendCertificateForExecution for PendCertificateForExecutionNoop {
    async fn add_pending_certificates(&self, _certs: Vec<VerifiedCertificate>) -> SuiResult<()> {
        Ok(())
    }
}

/// When a notification that a new pending transaction is received we activate
/// processing the transaction in a loop.
pub async fn execution_process<A>(active_authority: Arc<ActiveAuthority<A>>)
where
    A: AuthorityAPI + Send + Sync + 'static + Clone,
{
    info!("Starting pending certificates execution process.");

    // Rate limit, similiar to handle_messages() in node_state.rs.
    let limit = Arc::new(Semaphore::new(MAX_EXECUTION_CONCURRENCY));

    // Loop whenever there is a signal that a new transactions is ready to process.
    loop {
        let certificate = if let Some(cert) = active_authority.state.next_ready_certificate().await
        {
            cert
        } else {
            // Should not happen. Only possible if the AuthorityState has shut down.
            warn!("Ready digest stream from authority state is broken. Retrying in 10s ...");
            sleep(std::time::Duration::from_secs(10)).await;
            continue;
        };
        let digest = *certificate.digest();
        debug!(?digest, "Pending certificate execution activated.");

        // Process any tx that failed to commit.
        if let Err(err) = active_authority.state.process_tx_recovery_log(None).await {
            tracing::error!("Error processing tx recovery log: {:?}", err);
        }

        let limit = limit.clone();
        // hold semaphore permit until task completes. unwrap ok because we never close
        // the semaphore in this context.
        let permit = limit.acquire_owned().await.unwrap();
        let authority = active_authority.clone();

        spawn_monitored_task!(async move {
            let _ = &permit;
            let res = timeout(
                MAX_EXECUTION_TIMEOUT,
                authority.state.handle_certificate(&certificate),
            )
            .await
            .map_err(|_| SuiError::TimeoutError);
            if let Err(e) = res {
                error!(?digest, "Failed to execute certified transaction! {e}");
            }

            // Remove the pending certificate regardless of execution status. It can be retried
            // from a higher level.
            let _ = authority
                .state
                .node_sync_store
                .cleanup_cert(certificate.epoch(), &digest);
        });
    }
}
