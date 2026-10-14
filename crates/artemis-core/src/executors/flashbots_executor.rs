use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use ethers::{
    providers::Middleware, signers::Signer, types::transaction::eip2718::TypedTransaction,
};
use ethers_flashbots::{BundleRequest, FlashbotsMiddleware, SimulatedBundle};
use reqwest::Url;

use crate::types::Executor;

/// A Flashbots executor that sends transactions to the Flashbots relay.
pub struct FlashbotsExecutor<M, S> {
    /// The Flashbots middleware.
    fb_client: FlashbotsMiddleware<Arc<M>, S>,

    /// The signer to sign transactions before sending to the relay.
    tx_signer: S,
}

/// A bundle of transactions to send to the Flashbots relay.
pub type FlashbotsBundle = Vec<TypedTransaction>;

impl<M: Middleware, S: Signer> FlashbotsExecutor<M, S> {
    pub fn new(client: Arc<M>, tx_signer: S, relay_signer: S, relay_url: impl Into<Url>) -> Self {
        let fb_client = FlashbotsMiddleware::new(client, relay_url, relay_signer);
        Self {
            fb_client,
            tx_signer,
        }
    }
}

#[async_trait]
impl<M, S> Executor<FlashbotsBundle> for FlashbotsExecutor<M, S>
where
    M: Middleware + 'static,
    M::Error: 'static,
    S: Signer + 'static,
{
    /// Send a bundle to transactions to the Flashbots relay.
    async fn execute(&self, action: FlashbotsBundle) -> Result<()> {
        // Add txs to bundle.
        let mut bundle = BundleRequest::new();

        // Sign each transaction in bundle.
        for tx in action {
            let signature = self.tx_signer.sign_transaction(&tx).await?;
            bundle.add_transaction(tx.rlp_signed(&signature));
        }

        // Simulate bundle.
        let block_number = self.fb_client.get_block_number().await?;
        let bundle = bundle
            .set_block(block_number + 1)
            .set_simulation_block(block_number)
            .set_simulation_timestamp(0);

        let simulation = self
            .fb_client
            .simulate_bundle(&bundle)
            .await
            .context("Error simulating bundle")?;
        validate_simulation(&simulation)?;

        // Send bundle.
        self.fb_client
            .send_bundle(&bundle)
            .await
            .context("Error sending bundle")?;

        Ok(())
    }
}

fn validate_simulation(bundle: &SimulatedBundle) -> Result<()> {
    for transaction in &bundle.transactions {
        if let Some(reason) = transaction.error.as_ref().or(transaction.revert.as_ref()) {
            bail!(
                "Bundle simulation failed for {}: {}",
                transaction.hash,
                reason
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers_flashbots::SimulatedTransaction;

    fn simulation(error: Option<String>, revert: Option<String>) -> SimulatedBundle {
        SimulatedBundle {
            hash: Default::default(),
            coinbase_diff: Default::default(),
            coinbase_tip: Default::default(),
            gas_price: Default::default(),
            gas_used: Default::default(),
            gas_fees: Default::default(),
            simulation_block: Default::default(),
            transactions: vec![SimulatedTransaction {
                hash: Default::default(),
                coinbase_diff: Default::default(),
                coinbase_tip: Default::default(),
                gas_price: Default::default(),
                gas_used: Default::default(),
                gas_fees: Default::default(),
                from: Default::default(),
                to: None,
                value: None,
                error,
                revert,
            }],
        }
    }

    #[test]
    fn rejects_simulated_transaction_errors_and_reverts() {
        assert!(validate_simulation(&simulation(Some("out of gas".into()), None)).is_err());
        assert!(validate_simulation(&simulation(None, Some("execution reverted".into()))).is_err());
        assert!(validate_simulation(&simulation(None, None)).is_ok());
    }
}
