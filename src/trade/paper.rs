//! Local paper matching. Fills against the live mark; nothing here signs.



pub const STARTING_CASH: f64 = 10_000.0;
pub const DEFAULT_LEVERAGE: f64 = 10.0;

#[derive(Clone, Debug)]
pub struct PaperPosition {
    pub symbol: String,
    pub size: f64,
    pub entry: f64,
    pub leverage: f64,
}

#[derive(Clone, Debug)]
pub struct PaperOrder {
    pub id: u64,
    pub symbol: String,
    pub is_long: bool,
    pub size: f64,
    pub price: f64,
}

#[derive(Clone, Debug)]
pub struct PaperFill {
    pub symbol: String,
    pub is_long: bool,
    pub size: f64,
    pub price: f64,
    pub realized: f64,
}

#[derive(Debug)]
pub struct PaperAccount {
    pub cash: f64,
    pub leverage: f64,
    pub positions: Vec<PaperPosition>,
    pub orders: Vec<PaperOrder>,
    pub fills: Vec<PaperFill>,
    next_id: u64,
}

impl Default for PaperAccount {
    fn default() -> Self {
        Self::new()
    }
}

impl PaperAccount {
    pub fn new() -> Self {
        Self {
            cash: STARTING_CASH,
            leverage: DEFAULT_LEVERAGE,
            positions: Vec::new(),
            orders: Vec::new(),
            fills: Vec::new(),
            next_id: 1,
        }
    }

    pub fn place(
        &mut self,
        symbol: &str,
        is_long: bool,
        is_limit: bool,
        size: f64,
        price: f64,
        mark: f64,
    ) -> Result<Option<PaperFill>, String> {
        if !size.is_finite() || size <= 0.0 {
            return Err("size must be greater than 0".into());
        }
        if !price.is_finite() || price <= 0.0 {
            return Err("price must be greater than 0".into());
        }
        if !mark.is_finite() || mark <= 0.0 {
            return Err("no mark yet".into());
        }
        let margin = required_margin(size, price, self.leverage);
        if margin > self.available() + 1e-9 {
            return Err(format!(
                "need {} USDC margin, available {}",
                round_usd(margin),
                round_usd(self.available())
            ));
        }

        let id = self.next_id;
        self.next_id += 1;
        let fill_now = !is_limit || limit_crosses(is_long, price, mark);
        if fill_now {
            let fill_price = if is_limit { price } else { mark };
            Ok(Some(self.fill(symbol, is_long, size, fill_price)))
        } else {
            self.orders.push(PaperOrder {
                id,
                symbol: symbol.to_owned(),
                is_long,
                size,
                price,
            });
            Ok(None)
        }
    }

    pub fn cancel(&mut self, id: u64) -> bool {
        let before = self.orders.len();
        self.orders.retain(|order| order.id != id);
        self.orders.len() != before
    }

    pub fn mark(&mut self, symbol: &str, mark: f64) -> Vec<PaperFill> {
        if !mark.is_finite() || mark <= 0.0 {
            return Vec::new();
        }
        let mut due = Vec::new();
        self.orders.retain(|order| {
            if order.symbol == symbol && limit_crosses(order.is_long, order.price, mark) {
                due.push(order.clone());
                false
            } else {
                true
            }
        });
        due.into_iter()
            .map(|order| {
                self.fill(
                    order.id,
                    &order.symbol,
                    order.is_long,
                    order.size,
                    order.price,
                )
            })
            .collect()
    }

    pub fn available(&self) -> f64 {
        let reserved = self
            .orders
            .iter()
            .map(|order| required_margin(order.size, order.price, self.leverage))
            .sum::<f64>();
        (self.cash - reserved).max(0.0)
    }

    pub fn equity(&self, marks: impl Fn(&str) -> Option<f64>) -> f64 {
        let upnl = self
            .positions
            .iter()
            .map(|position| {
                marks(&position.symbol)
                    .map(|mark| (mark - position.entry) * position.size)
                    .unwrap_or(0.0)
            })
            .sum::<f64>();
        self.cash + upnl
    }

    fn fill(&mut self, symbol: &str, is_long: bool, size: f64, price: f64) -> PaperFill {
        let signed = if is_long { size } else { -size };
        let realized = apply_fill(&mut self.positions, symbol, signed, price, self.leverage);
        self.cash += realized;
        let fill = PaperFill {
            symbol: symbol.to_owned(),
            is_long,
            size,
            price,
            realized,
        };
        self.fills.insert(0, fill.clone());
        self.fills.truncate(40);
        fill
    }
}

fn apply_fill(
    positions: &mut Vec<PaperPosition>,
    symbol: &str,
    signed_size: f64,
    price: f64,
    leverage: f64,
) -> f64 {
    let Some(index) = positions
        .iter()
        .position(|position| position.symbol == symbol)
    else {
        positions.push(PaperPosition {
            symbol: symbol.to_owned(),
            size: signed_size,
            entry: price,
            leverage,
        });
        return 0.0;
    };
    let current = positions[index].size;
    if current == 0.0 || current.signum() == signed_size.signum() {
        let next = current + signed_size;
        let entry = if current.abs() < f64::EPSILON {
            price
        } else {
            (positions[index].entry * current.abs() + price * signed_size.abs()) / next.abs()
        };
        positions[index].size = next;
        positions[index].entry = entry;
        return 0.0;
    }

    let closing = current.abs().min(signed_size.abs());
    let realized = (price - positions[index].entry) * closing * current.signum();
    let remainder = current + signed_size;
    if remainder.abs() < 1e-9 {
        positions.remove(index);
    } else if remainder.signum() == current.signum() {
        positions[index].size = remainder;
    } else {
        positions[index].size = remainder;
        positions[index].entry = price;
    }
    realized
}

fn limit_crosses(is_long: bool, limit: f64, mark: f64) -> bool {
    if is_long {
        mark <= limit
    } else {
        mark >= limit
    }
}

fn required_margin(size: f64, price: f64, leverage: f64) -> f64 {
    (size.abs() * price) / leverage.max(1.0)
}

pub fn round_usd(value: f64) -> String {
    format!("{value:.2}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_long_opens_and_close_realizes() {
        let mut account = PaperAccount::new();
        account
            .place("SOL", true, false, 2.0, 100.0, 100.0)
            .unwrap();
        assert_eq!(account.positions.len(), 1);
        assert_eq!(account.positions[0].size, 2.0);
        account
            .place("SOL", false, false, 2.0, 110.0, 110.0)
            .unwrap();
        assert!(account.positions.is_empty());
        assert!((account.cash - (STARTING_CASH + 20.0)).abs() < 1e-9);
    }

    #[test]
    fn resting_limit_fills_on_mark() {
        let mut account = PaperAccount::new();
        let fill = account.place("SOL", true, true, 1.0, 90.0, 100.0).unwrap();
        assert!(fill.is_none());
        assert_eq!(account.orders.len(), 1);
        let fills = account.mark("SOL", 89.0);
        assert_eq!(fills.len(), 1);
        assert!(account.orders.is_empty());
        assert_eq!(account.positions[0].entry, 90.0);
    }

    #[test]
    fn rejects_when_margin_exceeds_cash() {
        let mut account = PaperAccount::new();
        let error = account
            .place("SOL", true, false, 10_000.0, 100.0, 100.0)
            .unwrap_err();
        assert!(error.contains("margin"));
    }
}
