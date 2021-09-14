use mockito::mock;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::FutureProducer,
    ClientConfig,
};
use risk_manager::{run, Settings};
use tracing::{debug, subscriber::set_global_default};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use uuid::Uuid;

pub async fn setup() -> (
    AdminClient<DefaultClientContext>,
    AdminOptions,
    StreamConsumer,
    FutureProducer,
) {
    debug!("Initializing test infrastructure");
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .set("security.protocol", "PLAINTEXT")
        .create()
        .unwrap();
    let admin_options = AdminOptions::new();
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    set_global_default(subscriber).unwrap();
    debug!("Creating topics");
    admin
        .create_topics(
            &[
                NewTopic::new("lots", 1, TopicReplication::Fixed(1)),
                NewTopic::new("risk-check-request", 1, TopicReplication::Fixed(1)),
                NewTopic::new("risk-check-response", 1, TopicReplication::Fixed(1)),
            ],
            &admin_options,
        )
        .await
        .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .set("security.protocol", "PLAINTEXT")
        .create()
        .unwrap();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .set("security.protocol", "PLAINTEXT")
        .set("group.id", "test-consumer")
        .create()
        .unwrap();

    debug!("Subscribing to topics");
    consumer.subscribe(&[&"risk-check-response"]).unwrap();
    consumer
        .subscription()
        .unwrap()
        .set_all_offsets(rdkafka::topic_partition_list::Offset::End)
        .unwrap();

    tokio::spawn(async move {
        let _m_account = mock("GET", "/account")
            .match_header("apca-api-key-id", "APCA_API_KEY_ID")
            .match_header("apca-api-secret-key", "APCA_API_SECRET_KEY")
            .with_body(
                r#"{
		  "account_blocked": false,
		  "account_number": "010203ABCD",
		  "buying_power": "4000000",
		  "cash": "1000000.0",
		  "created_at": "2019-06-12T22:47:07.99658Z",
		  "currency": "USD",
		  "daytrade_count": 0,
		  "daytrading_buying_power": "4000000",
		  "equity": "1000000",
		  "id": "e6fe16f3-64a4-4921-8928-cadf02f92f98",
		  "initial_margin": "0",
		  "last_equity": "1000000",
		  "last_maintenance_margin": "0",
		  "long_market_value": "0",
		  "maintenance_margin": "0",
		  "multiplier": "4",
		  "pattern_day_trader": false,
		  "portfolio_value": "0",
		  "regt_buying_power": "2000000",
		  "short_market_value": "0",
		  "shorting_enabled": true,
		  "sma": "0",
		  "status": "ACTIVE",
		  "trade_suspended_by_user": false,
		  "trading_blocked": false,
		  "transfers_blocked": false
		}"#,
            )
            .create();
        let _m_positions = mock("GET", "/positions")
            .match_header("apca-api-key-id", "APCA_API_KEY_ID")
            .match_header("apca-api-secret-key", "APCA_API_SECRET_KEY")
            .with_body("[]")
            .create();
        std::env::set_var("KAFKA__BOOTSTRAP_SERVER", "localhost:9094");
        std::env::set_var("KAFKA__GROUP_ID", Uuid::new_v4().to_string());
        std::env::set_var("KAFKA__INPUT_TOPICS", "lots,risk-check-request");
        std::env::set_var("KAFKA__BOOTSTRAP_SERVERS", "localhost:9094");
        std::env::set_var("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT");
        std::env::set_var("KAFKA__ACKS", "0");
        std::env::set_var("KAFKA__RETRIES", "0");
        std::env::set_var("WEBSERVER__PORT", "0");
        std::env::set_var("APCA_API_BASE_URL", mockito::server_url());
        std::env::set_var("APCA_API_KEY_ID", "APCA_API_KEY_ID");
        std::env::set_var("APCA_API_SECRET_KEY", "APCA_API_SECRET_KEY");
        let settings = Settings::new();
        tracing::debug!("{:?}", settings);
        let res = run(settings.unwrap()).await;
        tracing::error!("{:?}", res);
    });

    (admin, admin_options, consumer, producer)
}
