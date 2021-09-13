use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use tracing::debug;

pub async fn teardown(admin: &AdminClient<DefaultClientContext>, admin_options: &AdminOptions) {
    debug!("Deleting topics");
    admin
        .delete_topics(
            &["risk-check-request", "risk-check-response"],
            &admin_options,
        )
        .await
        .unwrap();
}
