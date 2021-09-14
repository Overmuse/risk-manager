mod input;
mod risk_manager;
mod settings;
pub use crate::risk_manager::{DenyReason, Price, RiskCheckResponse, RiskManager, Shares};
use alpaca::Client;
use anyhow::{anyhow, Result};
pub use input::Lot;
use kafka_settings::{consumer, producer};
use rdkafka::producer::FutureRecord;
pub use settings::Settings;
use tracing::{info, trace};

pub async fn run(settings: Settings) -> Result<()> {
    info!("Running RiskManager");
    let consumer = consumer(&settings.kafka)?;
    let producer = producer(&settings.kafka)?;
    let client = Client::from_env()?;
    let mut risk_manager = RiskManager::new();
    risk_manager.bind_consumer(consumer);
    risk_manager.bind_alpaca_client(client);
    risk_manager.initialize().await?;
    loop {
        let message = risk_manager.receive_message().await?;
        match message {
            input::Input::Lot(lot) => {
                trace!("Lot received");
                risk_manager.update_holdings(lot.ticker, Shares(lot.shares), Price(lot.price));
            }
            input::Input::TradeIntent(trade_intent) => {
                trace!("TradeIntent received");
                let response = risk_manager.risk_check(&trade_intent);
                let payload = serde_json::to_string(&response)?;
                let record = FutureRecord::to("risk-check-response")
                    .key(&trade_intent.ticker)
                    .payload(&payload);
                producer
                    .send(record, std::time::Duration::from_secs(0))
                    .await
                    .map_err(|(e, m)| anyhow!("{} - {:?}", e, m))?;
            }
        }
    }
}
