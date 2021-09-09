mod input;
mod risk_manager;
mod settings;
pub use crate::risk_manager::{Price, RiskManager, Shares};
use anyhow::Result;
use kafka_settings::consumer;
pub use settings::Settings;

pub async fn run(settings: Settings) -> Result<()> {
    let consumer = consumer(&settings.kafka)?;
    let mut risk_manager = RiskManager::new();
    risk_manager.bind_consumer(consumer);
    loop {
        let message = risk_manager.receive_message().await?;
        match message {
            input::Input::Lot(lot) => {
                risk_manager.update_holdings(lot.ticker, Shares(lot.shares), Price(lot.price));
            }
            input::Input::TradeIntent(trade_intent) => {
                risk_manager.risk_check(trade_intent);
            }
        }
    }
}
