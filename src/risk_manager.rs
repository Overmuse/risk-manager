use rdkafka::consumer::StreamConsumer;
use rust_decimal::prelude::*;
use std::collections::HashMap;
use trading_base::{OrderType, TradeIntent};

#[derive(Copy, Clone)]
pub struct Shares(pub Decimal);

#[derive(Copy, Clone)]
pub struct Price(pub Decimal);

pub struct RiskManager {
    pub(super) kafka_consumer: Option<StreamConsumer>,
    cash: Decimal,
    holdings: HashMap<String, (Shares, Price, Price)>,
}

impl RiskManager {
    pub fn new() -> Self {
        Self {
            kafka_consumer: None,
            cash: Decimal::ZERO,
            holdings: HashMap::new(),
        }
    }

    pub fn bind_consumer(&mut self, consumer: StreamConsumer) {
        self.kafka_consumer = Some(consumer)
    }

    pub fn update_cash(&mut self, cash: Decimal) {
        self.cash = cash
    }

    pub fn update_price<T: ToString>(&mut self, ticker: T, price: Price) {
        self.holdings
            .entry(ticker.to_string())
            .and_modify(|(_, _, p)| *p = price);
    }

    pub fn update_holdings<T: ToString>(&mut self, ticker: T, shares: Shares, price: Price) {
        self.holdings
            .entry(ticker.to_string())
            .and_modify(|(s, i, p)| {
                *s = shares;
                *i = price;
                *p = price
            })
            .or_insert((shares, price, price));
    }

    pub fn long_market_exposure(&self) -> Decimal {
        self.holdings
            .values()
            .filter(|(s, _, _)| s.0.is_sign_positive())
            .fold(Decimal::ZERO, |state, (shares, _, price)| {
                state + shares.0 * price.0
            })
    }

    pub fn short_market_exposure(&self) -> Decimal {
        self.holdings
            .values()
            .filter(|(s, _, _)| s.0.is_sign_negative())
            .fold(Decimal::ZERO, |state, (shares, _, price)| {
                state + -shares.0 * price.0
            })
    }

    pub fn gross_market_exposure(&self) -> Decimal {
        self.holdings
            .values()
            .fold(Decimal::ZERO, |state, (shares, _, price)| {
                state + shares.0.abs() * price.0
            })
    }

    pub fn net_market_exposure(&self) -> Decimal {
        self.holdings
            .values()
            .fold(Decimal::ZERO, |state, (shares, _, price)| {
                state + shares.0 * price.0
            })
    }

    pub fn total_equity(&self) -> Decimal {
        self.net_market_exposure() + self.cash
    }

    pub fn initial_margin(&self) -> Decimal {
        self.holdings
            .values()
            .fold(Decimal::ZERO, |state, (shares, _, price)| {
                state + shares.0.abs() * price.0 * Decimal::new(5, 1)
            })
    }

    pub fn maintenance_margin(&self) -> Decimal {
        self.holdings
            .values()
            .fold(Decimal::ZERO, |state, (shares, _, price)| {
                let factor = if shares.0.is_sign_positive() {
                    if price.0 >= Decimal::new(25, 1) {
                        Decimal::new(3, 1)
                    } else {
                        Decimal::ONE
                    }
                } else {
                    if price.0 >= Decimal::new(5, 0) {
                        Decimal::new(3, 1)
                    } else {
                        Decimal::ONE
                    }
                };
                state + shares.0.abs() * price.0 * factor
            })
    }

    pub fn buying_power(&self) -> Decimal {
        (self.total_equity() - self.initial_margin()) * Decimal::new(2, 0)
    }

    pub fn risk_check(&self, trade_intent: TradeIntent) -> bool {
        let required_buying_power = match trade_intent.order_type {
            OrderType::Limit { limit_price } => {
                limit_price * Decimal::from_isize(trade_intent.qty.abs()).unwrap()
            }
            _ => todo!(),
        };

        self.buying_power() > required_buying_power
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn equity_calculations() {
        let mut manager = RiskManager::new();

        manager.update_holdings("AAPL", Shares(Decimal::ONE), Price(Decimal::new(100, 0)));
        manager.update_holdings(
            "TSLA",
            Shares(Decimal::new(-2, 0)),
            Price(Decimal::new(80, 0)),
        );
        manager.update_cash(Decimal::new(300, 0));

        assert_eq!(manager.long_market_exposure(), Decimal::new(100, 0));
        assert_eq!(manager.short_market_exposure(), Decimal::new(160, 0));
        assert_eq!(manager.gross_market_exposure(), Decimal::new(260, 0));
        assert_eq!(manager.net_market_exposure(), Decimal::new(-60, 0));
        assert_eq!(manager.total_equity(), Decimal::new(240, 0));
        assert_eq!(manager.initial_margin(), Decimal::new(130, 0));
        assert_eq!(manager.maintenance_margin(), Decimal::new(78, 0));
        assert_eq!(manager.buying_power(), Decimal::new(220, 0));
    }

    #[test]
    fn risk_check() {
        let mut manager = RiskManager::new();

        manager.update_holdings("AAPL", Shares(Decimal::ONE), Price(Decimal::new(100, 0)));
        manager.update_holdings(
            "TSLA",
            Shares(Decimal::new(-2, 0)),
            Price(Decimal::new(80, 0)),
        );
        manager.update_cash(Decimal::new(300, 0));

        let trade_intent = TradeIntent::new("AAPL", 1).order_type(OrderType::Limit {
            limit_price: Decimal::new(200, 0),
        });
        assert!(manager.risk_check(trade_intent));

        let trade_intent = TradeIntent::new("AAPL", -1).order_type(OrderType::Limit {
            limit_price: Decimal::new(230, 0),
        });
        assert!(!manager.risk_check(trade_intent));
    }
}
