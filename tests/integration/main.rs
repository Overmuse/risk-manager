use rdkafka::producer::FutureRecord;
use rdkafka::Message;
use risk_manager::{DenyReason, RiskCheckResponse};
use rust_decimal::Decimal;
use setup::setup;
use teardown::teardown;
use trading_base::{OrderType, TradeIntent};
mod setup;
mod teardown;

#[tokio::test]
async fn main() {
    let (admin, admin_options, consumer, producer) = setup().await;
    let intent = TradeIntent::new("AAPL", 2).order_type(OrderType::Limit {
        limit_price: Decimal::new(100, 0),
    });
    let payload = serde_json::to_string(&intent).unwrap();
    let record = FutureRecord::to("risk-check-request")
        .key(&intent.ticker)
        .payload(&payload);
    producer
        .send(record, std::time::Duration::from_secs(0))
        .await
        .map_err(|(e, _)| e)
        .unwrap();

    let response = consumer.recv().await.unwrap();
    let message: RiskCheckResponse = serde_json::from_slice(&response.payload().unwrap()).unwrap();
    assert_eq!(
        message,
        RiskCheckResponse::Denied {
            intent,
            reason: DenyReason::InsufficientBuyingPower {
                buying_power: Decimal::ZERO
            }
        }
    );

    teardown(&admin, &admin_options).await;
}
