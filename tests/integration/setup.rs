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
    // Drop database, don't care if it fails
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
        std::env::set_var("KAFKA__BOOTSTRAP_SERVER", "localhost:9094");
        std::env::set_var("KAFKA__GROUP_ID", Uuid::new_v4().to_string());
        std::env::set_var("KAFKA__INPUT_TOPICS", "risk-check-request");
        std::env::set_var("KAFKA__BOOTSTRAP_SERVERS", "localhost:9094");
        std::env::set_var("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT");
        std::env::set_var("KAFKA__ACKS", "0");
        std::env::set_var("KAFKA__RETRIES", "0");
        std::env::set_var("WEBSERVER__PORT", "0");
        let settings = Settings::new();
        tracing::debug!("{:?}", settings);
        let res = run(settings.unwrap()).await;
        tracing::error!("{:?}", res);
    });

    (admin, admin_options, consumer, producer)
}
