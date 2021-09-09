use config::{Config, ConfigError, Environment};
use kafka_settings::KafkaSettings;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppSettings {
    pub order_manager_url: String,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub app: AppSettings,
    pub kafka: KafkaSettings,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(Environment::new().separator("__"))?;
        s.try_into()
    }
}
