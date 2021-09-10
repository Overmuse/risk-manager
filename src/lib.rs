mod input;
mod risk_manager;
mod settings;
pub use crate::risk_manager::{Price, RiskManager, Shares};
use anyhow::{anyhow, Result};
use kafka_settings::{consumer, producer};
use rdkafka::producer::FutureRecord;
pub use settings::Settings;

pub async fn run(settings: Settings) -> Result<()> {
    let consumer = consumer(&settings.kafka)?;
    let producer = producer(&settings.kafka)?;
    let mut risk_manager = RiskManager::new();
    risk_manager.bind_consumer(consumer);
    loop {
        let message = risk_manager.receive_message().await?;
        match message {
            input::Input::Lot(lot) => {
                risk_manager.update_holdings(lot.ticker, Shares(lot.shares), Price(lot.price));
            }
            input::Input::TradeIntent(trade_intent) => {
                let response = risk_manager.risk_check(&trade_intent);
                let payload = serde_json::to_string(&response)?;
                let record = FutureRecord::to("risk")
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
