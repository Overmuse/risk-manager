use crate::RiskManager;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use rdkafka::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::debug;
use trading_base::TradeIntent;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum Input {
    Lot(Lot),
    TradeIntent(TradeIntent),
}

#[derive(Clone, Debug, Deserialize)]
pub struct Lot {
    pub id: Uuid,
    pub order_id: Uuid,
    pub ticker: String,
    pub fill_time: DateTime<Utc>,
    pub price: Decimal,
    pub shares: Decimal,
}

impl RiskManager {
    #[tracing::instrument(skip(self))]
    pub async fn receive_message(&mut self) -> Result<Input> {
        match self.kafka_consumer.as_ref() {
            Some(consumer) => {
                let message = consumer.recv().await?;
                debug!("Message received from kafka");
                let payload = message.payload().ok_or_else(|| anyhow!("Empty payload"))?;
                Ok(serde_json::from_slice(payload)?)
            }
            None => Err(anyhow!("Consumer not initialized")),
        }
    }
}
