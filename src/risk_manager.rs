use alpaca::{rest::account::GetAccount, rest::positions::GetPositions, Client};
use anyhow::{anyhow, Context, Result};
use num_traits::sign::Signed;
use rdkafka::consumer::StreamConsumer;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, trace};
use trading_base::{OrderType, TradeIntent};

#[derive(Copy, Clone)]
pub struct Shares(pub Decimal);

#[derive(Copy, Clone)]
pub struct Price(pub Decimal);

#[derive(Default)]
pub struct RiskManager {
    pub(super) kafka_consumer: Option<StreamConsumer>,
    alpaca_client: Option<Client>,
    cash: Decimal,
    holdings: HashMap<String, (Shares, Price)>,
    is_pattern_day_trader: bool,
    last_equity: Decimal,
    last_maintenance_margin: Decimal,
    datastore_url: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DenyReason {
    InsufficientBuyingPower { buying_power: Decimal },
    ChangeInPositionSide,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum RiskCheckResponse {
    Granted {
        intent: TradeIntent,
    },
    Denied {
        intent: TradeIntent,
        reason: DenyReason,
    },
}

impl RiskManager {
    pub fn new(datastore_url: String) -> Self {
        Self {
            kafka_consumer: None,
            alpaca_client: None,
            cash: Decimal::ZERO,
            holdings: HashMap::new(),
            is_pattern_day_trader: false,
            last_equity: Decimal::ZERO,
            last_maintenance_margin: Decimal::ZERO,
            datastore_url,
        }
    }

    pub async fn initialize(&mut self) -> Result<()> {
        if let Some(client) = self.alpaca_client.as_ref() {
            let account = client.send(GetAccount).await?;
            let holdings = client
                .send(GetPositions)
                .await?
                .into_iter()
                .map(|pos| {
                    let shares = Decimal::from_i32(pos.qty).unwrap();
                    (pos.symbol, (Shares(shares), Price(pos.avg_entry_price)))
                })
                .collect();
            self.cash = account.cash;
            self.holdings = holdings;
            self.is_pattern_day_trader = account.pattern_day_trader;
            self.last_equity = account.last_equity;
            self.last_maintenance_margin = account.last_maintenance_margin;
            Ok(())
        } else {
            Err(anyhow!("Alpaca client not initialized"))
        }
    }

    pub fn bind_alpaca_client(&mut self, client: Client) {
        self.alpaca_client = Some(client)
    }

    pub fn bind_consumer(&mut self, consumer: StreamConsumer) {
        self.kafka_consumer = Some(consumer)
    }

    #[tracing::instrument(skip(self, cash))]
    pub fn update_cash(&mut self, cash: Decimal) {
        trace!(%cash, "Updating cash");
        self.cash = cash
    }

    #[tracing::instrument(skip(self, ticker, price))]
    pub fn update_price<T: ToString + std::fmt::Display>(&mut self, ticker: T, price: Price) {
        trace!(%ticker, price = %price.0, "Updating price");
        self.holdings
            .entry(ticker.to_string())
            .and_modify(|(_, p)| *p = price);
    }

    #[tracing::instrument(skip(self, ticker, shares, price))]
    pub fn update_holdings<T: ToString + std::fmt::Display>(
        &mut self,
        ticker: T,
        shares: Shares,
        price: Price,
    ) {
        trace!(%ticker, shares = %shares.0, price = %price.0, "Updating holdings");
        self.holdings
            .entry(ticker.to_string())
            .and_modify(|(s, p)| {
                *s = Shares(s.0 + shares.0);
                *p = price
            })
            .or_insert((shares, price));
        self.cash -= shares.0 * price.0;
    }

    pub fn long_market_exposure(&self) -> Decimal {
        self.holdings
            .values()
            .filter(|(s, _)| s.0.is_sign_positive())
            .fold(Decimal::ZERO, |state, (shares, price)| {
                state + shares.0 * price.0
            })
    }

    pub fn short_market_exposure(&self) -> Decimal {
        self.holdings
            .values()
            .filter(|(s, _)| s.0.is_sign_negative())
            .fold(Decimal::ZERO, |state, (shares, price)| {
                state + -shares.0 * price.0
            })
    }

    pub fn gross_market_exposure(&self) -> Decimal {
        self.holdings
            .values()
            .fold(Decimal::ZERO, |state, (shares, price)| {
                state + shares.0.abs() * price.0
            })
    }

    pub fn net_market_exposure(&self) -> Decimal {
        self.holdings
            .values()
            .fold(Decimal::ZERO, |state, (shares, price)| {
                state + shares.0 * price.0
            })
    }

    pub fn equity(&self) -> Decimal {
        self.net_market_exposure() + self.cash
    }

    pub fn initial_margin(&self) -> Decimal {
        self.holdings
            .values()
            .fold(Decimal::ZERO, |state, (shares, price)| {
                state + shares.0.abs() * price.0 * Decimal::new(5, 1)
            })
    }

    pub fn maintenance_margin(&self) -> Decimal {
        self.holdings
            .values()
            .fold(Decimal::ZERO, |state, (shares, price)| {
                let factor = if shares.0.is_sign_positive() {
                    if price.0 >= Decimal::new(25, 1) {
                        Decimal::new(3, 1)
                    } else {
                        Decimal::ONE
                    }
                } else if price.0 >= Decimal::new(5, 0) {
                    Decimal::new(3, 1)
                } else {
                    Decimal::ONE
                };
                state + shares.0.abs() * price.0 * factor
            })
    }

    pub fn multiplier(&self) -> Decimal {
        let equity = self.equity();
        if self.is_pattern_day_trader {
            if equity < Decimal::new(2000, 0) {
                Decimal::ONE
            } else if equity < Decimal::new(25000, 0) {
                Decimal::new(2, 0)
            } else {
                Decimal::new(4, 0)
            }
        } else if equity > Decimal::new(25000, 0) {
            Decimal::new(2, 0)
        } else {
            Decimal::new(1, 0)
        }
    }

    pub fn regt_buying_power(&self) -> Decimal {
        ((self.equity() - self.initial_margin()) * Decimal::new(2, 0)).max(Decimal::ZERO)
    }

    pub fn daytrading_buying_power(&self) -> Decimal {
        ((self.last_equity - self.last_maintenance_margin) * self.multiplier()
            - self.gross_market_exposure())
        .max(Decimal::ZERO)
    }

    pub fn buying_power(&self) -> Decimal {
        self.regt_buying_power().max(self.daytrading_buying_power())
    }

    #[tracing::instrument(skip(self, trade_intent), fields(id = %trade_intent.id))]
    pub fn risk_check(&self, trade_intent: &TradeIntent) -> Result<RiskCheckResponse> {
        debug!("Running risk_check");
        let owned_shares = self.holdings.get(&trade_intent.ticker);
        if let Some((shares, _)) = owned_shares {
            let qty = Decimal::from_isize(trade_intent.qty)
                .context("Failed to convert isize to Decimal")?;
            if (qty.signum() * shares.0.signum()) == Decimal::new(-1, 0) {
                // This is a closing trade
                if qty.abs() > shares.0.abs() {
                    trace!("Change in position, risk check denied");
                    return Ok(RiskCheckResponse::Denied {
                        intent: trade_intent.clone(),
                        reason: DenyReason::ChangeInPositionSide,
                    });
                } else {
                    trace!("Closing trade, risk check granted");
                    return Ok(RiskCheckResponse::Granted {
                        intent: trade_intent.clone(),
                    });
                }
            }
        }
        let required_buying_power = match trade_intent.order_type {
            OrderType::Limit { limit_price } => {
                limit_price
                    * Decimal::from_isize(trade_intent.qty.abs())
                        .context("Failed to convert isize to Decimal")?
            }
            OrderType::Market => {
                let url = format!("{}/last/{}", self.datastore_url, trade_intent.ticker);
                let price: Decimal = reqwest::blocking::get(url).unwrap().json().unwrap();
                price
                    * Decimal::new(103, 2)
                    * Decimal::from_isize(trade_intent.qty.abs())
                        .context("Failed to convert isize to Decimal")?
            }
            _ => {
                return Err(anyhow!(
                    "Risk manager can only deal with Market and Limit orders currently"
                ))
            }
        };
        let buying_power = self.buying_power();
        trace!(?buying_power, ?required_buying_power);

        if buying_power > required_buying_power {
            debug!("Risk-check granted");
            Ok(RiskCheckResponse::Granted {
                intent: trade_intent.clone(),
            })
        } else {
            debug!("Insufficient buying power, risk check denied");
            Ok(RiskCheckResponse::Denied {
                intent: trade_intent.clone(),
                reason: DenyReason::InsufficientBuyingPower { buying_power },
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn realistic_equity_calculations() {
        let mut manager = RiskManager {
            kafka_consumer: None,
            alpaca_client: None,
            cash: Decimal::ZERO,
            holdings: HashMap::new(),
            is_pattern_day_trader: true,
            last_equity: Decimal::new(99791448, 2),
            last_maintenance_margin: Decimal::ZERO,
            datastore_url: String::new(),
        };

        manager.update_holdings(
            "TRP",
            Shares(Decimal::new(5118, 0)),
            Price(Decimal::new(4869, 2)),
        );
        manager.update_holdings(
            "MPLX",
            Shares(Decimal::new(8825, 0)),
            Price(Decimal::new(2812, 2)),
        );
        manager.update_holdings(
            "E",
            Shares(Decimal::new(19539, 0)),
            Price(Decimal::new(2556, 2)),
        );
        manager.update_holdings(
            "DVN",
            Shares(Decimal::new(-8146, 0)),
            Price(Decimal::new(3058, 2)),
        );
        manager.update_holdings(
            "CVE",
            Shares(Decimal::new(-27716, 0)),
            Price(Decimal::new(910, 2)),
        );
        manager.update_holdings(
            "COP",
            Shares(Decimal::new(-4005, 0)),
            Price(Decimal::new(6243, 2)),
        );
        manager.update_holdings(
            "BKR",
            Shares(Decimal::new(10527, 0)),
            Price(Decimal::new(2368, 2)),
        );
        manager.update_holdings(
            "APA",
            Shares(Decimal::new(-24754, 0)),
            Price(Decimal::new(2033, 2)),
        );
        manager.update_cash(Decimal::new(99283298, 2));
        assert_eq!(manager.long_market_exposure(), Decimal::new(124605062, 2));
        assert_eq!(manager.short_market_exposure(), Decimal::new(125460125, 2));
        assert_eq!(manager.gross_market_exposure(), Decimal::new(250065187, 2));
        assert_eq!(manager.net_market_exposure(), Decimal::new(-855063, 2));
        assert_eq!(manager.equity(), Decimal::new(98428235, 2));
        assert_eq!(manager.initial_margin(), Decimal::new(1250325935, 3));
        assert_eq!(manager.maintenance_margin(), Decimal::new(750195561, 3));
        assert_eq!(manager.regt_buying_power(), Decimal::ZERO);
        assert_eq!(
            manager.daytrading_buying_power(),
            Decimal::new(149100605, 2)
        );
        assert_eq!(manager.buying_power(), Decimal::new(149100605, 2));
    }

    #[test]
    fn equity_calculations() {
        let mut manager = RiskManager {
            kafka_consumer: None,
            alpaca_client: None,
            cash: Decimal::ZERO,
            holdings: HashMap::new(),
            is_pattern_day_trader: true,
            last_equity: Decimal::ZERO,
            last_maintenance_margin: Decimal::ZERO,
            datastore_url: String::new(),
        };

        manager.update_holdings("AAPL", Shares(Decimal::ONE), Price(Decimal::new(100, 0)));
        manager.update_cash(Decimal::new(300, 0));
        assert_eq!(manager.long_market_exposure(), Decimal::new(100, 0));
        assert_eq!(manager.short_market_exposure(), Decimal::ZERO);
        assert_eq!(manager.gross_market_exposure(), Decimal::new(100, 0));
        assert_eq!(manager.net_market_exposure(), Decimal::new(100, 0));
        assert_eq!(manager.equity(), Decimal::new(400, 0));
        assert_eq!(manager.initial_margin(), Decimal::new(50, 0));
        assert_eq!(manager.maintenance_margin(), Decimal::new(30, 0));
        assert_eq!(manager.regt_buying_power(), Decimal::new(700, 0));
        assert_eq!(manager.daytrading_buying_power(), Decimal::ZERO);
        assert_eq!(manager.buying_power(), Decimal::new(700, 0));

        manager.update_holdings(
            "TSLA",
            Shares(Decimal::new(-2, 0)),
            Price(Decimal::new(80, 0)),
        );
        assert_eq!(manager.long_market_exposure(), Decimal::new(100, 0));
        assert_eq!(manager.short_market_exposure(), Decimal::new(160, 0));
        assert_eq!(manager.gross_market_exposure(), Decimal::new(260, 0));
        assert_eq!(manager.net_market_exposure(), Decimal::new(-60, 0));
        assert_eq!(manager.equity(), Decimal::new(400, 0));
        assert_eq!(manager.initial_margin(), Decimal::new(130, 0));
        assert_eq!(manager.maintenance_margin(), Decimal::new(78, 0));
        assert_eq!(manager.regt_buying_power(), Decimal::new(540, 0));
        assert_eq!(manager.daytrading_buying_power(), Decimal::ZERO);
        assert_eq!(manager.buying_power(), Decimal::new(540, 0));

        manager.update_holdings(
            "TSLA",
            Shares(Decimal::new(-1, 0)),
            Price(Decimal::new(100, 0)),
        );
        assert_eq!(manager.long_market_exposure(), Decimal::new(100, 0));
        assert_eq!(manager.short_market_exposure(), Decimal::new(300, 0));
        assert_eq!(manager.gross_market_exposure(), Decimal::new(400, 0));
        assert_eq!(manager.net_market_exposure(), Decimal::new(-200, 0));
        assert_eq!(manager.equity(), Decimal::new(360, 0));
        assert_eq!(manager.initial_margin(), Decimal::new(200, 0));
        assert_eq!(manager.maintenance_margin(), Decimal::new(120, 0));
        assert_eq!(manager.regt_buying_power(), Decimal::new(320, 0));
        assert_eq!(manager.daytrading_buying_power(), Decimal::ZERO);
        assert_eq!(manager.buying_power(), Decimal::new(320, 0));

        manager.update_holdings(
            "TSLA",
            Shares(Decimal::new(3, 0)),
            Price(Decimal::new(90, 0)),
        );
        assert_eq!(manager.long_market_exposure(), Decimal::new(100, 0));
        assert_eq!(manager.short_market_exposure(), Decimal::ZERO);
        assert_eq!(manager.gross_market_exposure(), Decimal::new(100, 0));
        assert_eq!(manager.net_market_exposure(), Decimal::new(100, 0));
        assert_eq!(manager.equity(), Decimal::new(390, 0));
        assert_eq!(manager.initial_margin(), Decimal::new(50, 0));
        assert_eq!(manager.maintenance_margin(), Decimal::new(30, 0));
        assert_eq!(manager.regt_buying_power(), Decimal::new(680, 0));
        assert_eq!(manager.daytrading_buying_power(), Decimal::ZERO);
        assert_eq!(manager.buying_power(), Decimal::new(680, 0));
    }

    #[test]
    fn risk_check() {
        let mut manager = RiskManager {
            kafka_consumer: None,
            alpaca_client: None,
            cash: Decimal::ZERO,
            holdings: HashMap::new(),
            is_pattern_day_trader: true,
            last_equity: Decimal::ZERO,
            last_maintenance_margin: Decimal::ZERO,
            datastore_url: String::new(),
        };

        manager.update_holdings("AAPL", Shares(Decimal::ONE), Price(Decimal::new(100, 0)));
        manager.update_holdings(
            "TSLA",
            Shares(Decimal::new(-2, 0)),
            Price(Decimal::new(80, 0)),
        );
        manager.update_cash(Decimal::new(300, 0));

        let trade_intent = TradeIntent::new("AAPL", 1).order_type(OrderType::Limit {
            limit_price: Decimal::new(100, 0),
        });
        let response = manager.risk_check(&trade_intent).unwrap();
        assert_eq!(
            response,
            RiskCheckResponse::Granted {
                intent: trade_intent
            }
        );

        let trade_intent = TradeIntent::new("AAPL", 1).order_type(OrderType::Limit {
            limit_price: Decimal::new(240, 0),
        });
        let response = manager.risk_check(&trade_intent).unwrap();
        assert_eq!(
            response,
            RiskCheckResponse::Denied {
                intent: trade_intent,
                reason: DenyReason::InsufficientBuyingPower {
                    buying_power: Decimal::new(220, 0)
                }
            }
        );

        let trade_intent = TradeIntent::new("AAPL", -2).order_type(OrderType::Limit {
            limit_price: Decimal::new(120, 0),
        });
        let response = manager.risk_check(&trade_intent).unwrap();
        assert_eq!(
            response,
            RiskCheckResponse::Denied {
                intent: trade_intent,
                reason: DenyReason::ChangeInPositionSide
            }
        );

        let trade_intent = TradeIntent::new("AAPL", -1).order_type(OrderType::Limit {
            limit_price: Decimal::new(120, 0),
        });
        let response = manager.risk_check(&trade_intent).unwrap();
        assert_eq!(
            response,
            RiskCheckResponse::Granted {
                intent: trade_intent,
            }
        )
    }
}
