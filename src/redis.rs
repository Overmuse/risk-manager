use crate::settings::RedisSettings;
use anyhow::Result;
use redis::{Client, Commands, Connection, FromRedisValue};
use rust_decimal::prelude::*;

#[derive(Clone)]
pub struct Redis {
    client: Client,
}

impl Redis {
    pub fn new(settings: RedisSettings) -> Result<Self> {
        let client = redis::Client::open(settings.url)?;
        Ok(Self { client })
    }

    fn get_connection(&self) -> Result<Connection> {
        self.client.get_connection().map_err(Into::into)
    }

    pub fn get<T: Send + FromRedisValue>(&self, key: &str) -> Result<T> {
        let mut con = self.get_connection()?;
        Ok(con.get::<&str, T>(key)?)
    }

    pub fn get_latest_price(&self, ticker: &str) -> Result<Option<Decimal>> {
        Ok(self
            .get::<Option<f64>>(&format!("price/{}", ticker))?
            .map(Decimal::from_f64)
            .flatten())
    }
}
