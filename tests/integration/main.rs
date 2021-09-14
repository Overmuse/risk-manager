use chrono::Utc;
use rdkafka::producer::FutureRecord;
use rdkafka::Message;
use risk_manager::{DenyReason, Lot, RiskCheckResponse};
use rust_decimal::Decimal;
use setup::setup;
use teardown::teardown;
use trading_base::{OrderType, TradeIntent};
use uuid::Uuid;
mod setup;
mod teardown;

#[tokio::test]
async fn main() {
    let (admin, admin_options, consumer, producer) = setup().await;
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    tracing::info!("Test 1");
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
    assert_eq!(message, RiskCheckResponse::Granted { intent });

    let lot = Lot {
        id: Uuid::new_v4(),
        order_id: Uuid::new_v4(),
        ticker: "AAPL".into(),
        fill_time: Utc::now(),
        shares: Decimal::new(2, 0),
        price: Decimal::new(100, 0),
    };
    let payload = serde_json::to_string(&lot).unwrap();
    let record = FutureRecord::to("lots").key(&lot.ticker).payload(&payload);
    producer
        .send(record, std::time::Duration::from_secs(0))
        .await
        .map_err(|(e, _)| e)
        .unwrap();

    tracing::info!("Test 2");
    let intent = TradeIntent::new("AAPL", 20000).order_type(OrderType::Limit {
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
                buying_power: Decimal::new(1999800, 0)
            }
        }
    );

    teardown(&admin, &admin_options).await;
}
