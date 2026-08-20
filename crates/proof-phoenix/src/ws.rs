//! Phoenix public WebSocket market-data client.
//!
//! Live orderbooks, trades, candles, and mark stats belong here. REST is only
//! for history seeds and slow reconciliation.

use std::net::TcpStream;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use serde_json::{Value, json};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket, connect};

use crate::{BookLevel, Candle, OrderBook, Timeframe, Trade};

pub const DEFAULT_WS_URL: &str = "wss://perp-api.phoenix.trade/v1/ws";

pub struct PhoenixWs {
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
    symbol: String,
    timeframe: Timeframe,
}

#[derive(Debug)]
pub enum WsEvent {
    Orderbook(OrderBook),
    Market(MarketTick),
    Trades(Vec<Trade>),
    Mids(Vec<(String, f64)>),
    Candle {
        timeframe: Timeframe,
        candle: Candle,
    },
}

#[derive(Clone, Debug, Default)]
pub struct MarketTick {
    pub mark: Option<f64>,
    pub index: Option<f64>,
    pub mid: Option<f64>,
    pub prev_day: Option<f64>,
    pub open_interest: Option<f64>,
    pub volume_24h: Option<f64>,
    pub change_24h_pct: Option<f64>,
    pub funding_pct: Option<f64>,
}

impl PhoenixWs {
    pub fn connect(symbol: &str, timeframe: Timeframe) -> anyhow::Result<Self> {
        install_crypto_provider();
        let (socket, _) =
            connect(DEFAULT_WS_URL).with_context(|| format!("phoenix ws {DEFAULT_WS_URL}"))?;
        set_timeouts(&socket)?;
        let mut client = Self {
            socket,
            symbol: symbol.to_owned(),
            timeframe,
        };
        client.subscribe_all()?;
        Ok(client)
    }

    pub fn set_selection(&mut self, symbol: &str, timeframe: Timeframe) -> anyhow::Result<()> {
        if self.symbol == symbol && self.timeframe == timeframe {
            return Ok(());
        }
        let _ = self.unsubscribe_all();
        self.symbol = symbol.to_owned();
        self.timeframe = timeframe;
        self.subscribe_all()
    }

    pub fn poll(&mut self, timeout: Duration) -> anyhow::Result<Option<WsEvent>> {
        set_read_timeout(&self.socket, timeout)?;
        match self.socket.read() {
            Ok(Message::Text(text)) => Ok(parse_event(text.as_str())),
            Ok(Message::Binary(bytes)) => Ok(parse_event(&String::from_utf8_lossy(&bytes))),
            Ok(Message::Ping(payload)) => {
                let _ = self.socket.send(Message::Pong(payload));
                Ok(None)
            }
            Ok(Message::Pong(_) | Message::Frame(_)) => Ok(None),
            Ok(Message::Close(_)) => bail!("phoenix ws closed"),
            Err(tungstenite::Error::Io(error))
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                Ok(None)
            }
            Err(error) => Err(anyhow!(error)),
        }
    }

    fn subscribe_all(&mut self) -> anyhow::Result<()> {
        for payload in self.subscriptions(true) {
            self.socket
                .send(Message::Text(payload.to_string().into()))
                .context("phoenix ws subscribe")?;
        }
        Ok(())
    }

    fn unsubscribe_all(&mut self) -> anyhow::Result<()> {
        for payload in self.subscriptions(false) {
            self.socket
                .send(Message::Text(payload.to_string().into()))
                .context("phoenix ws unsubscribe")?;
        }
        Ok(())
    }

    fn subscriptions(&self, subscribe: bool) -> Vec<Value> {
        let kind = if subscribe {
            "subscribe"
        } else {
            "unsubscribe"
        };
        let symbol = &self.symbol;
        vec![
            json!({ "type": kind, "subscription": { "channel": "allMids" } }),
            json!({ "type": kind, "subscription": { "channel": "orderbook", "symbol": symbol } }),
            json!({ "type": kind, "subscription": { "channel": "market", "symbol": symbol } }),
            json!({ "type": kind, "subscription": { "channel": "trades", "symbol": symbol } }),
            json!({ "type": kind, "subscription": { "channel": "fundingRate", "symbol": symbol } }),
            json!({
                "type": kind,
                "subscription": {
                    "channel": "candles",
                    "symbol": symbol,
                    "timeframe": self.timeframe.as_api()
                }
            }),
        ]
    }
}

fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn set_timeouts(socket: &WebSocket<MaybeTlsStream<TcpStream>>) -> anyhow::Result<()> {
    set_read_timeout(socket, Duration::from_millis(200))?;
    match socket.get_ref() {
        MaybeTlsStream::Plain(stream) => {
            stream.set_nodelay(true)?;
        }
        MaybeTlsStream::Rustls(stream) => {
            stream.get_ref().set_nodelay(true)?;
        }
        _ => {}
    }
    Ok(())
}

fn set_read_timeout(
    socket: &WebSocket<MaybeTlsStream<TcpStream>>,
    timeout: Duration,
) -> anyhow::Result<()> {
    match socket.get_ref() {
        MaybeTlsStream::Plain(stream) => stream.set_read_timeout(Some(timeout))?,
        MaybeTlsStream::Rustls(stream) => stream.get_ref().set_read_timeout(Some(timeout))?,
        _ => {}
    }
    Ok(())
}

fn parse_event(text: &str) -> Option<WsEvent> {
    let value: Value = serde_json::from_str(text).ok()?;
    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .or_else(|| value.get("type").and_then(Value::as_str))
        .unwrap_or_default();
    match channel {
        "orderbook" => parse_orderbook(&value).map(WsEvent::Orderbook),
        "market" => Some(WsEvent::Market(parse_market(&value))),
        "trades" => Some(WsEvent::Trades(parse_trades(&value))),
        "allMids" => Some(WsEvent::Mids(parse_mids(&value))),
        "candle" | "candles" => {
            let timeframe = value
                .get("timeframe")
                .and_then(Value::as_str)
                .and_then(Timeframe::from_api)?;
            let candle = value.get("candle").and_then(parse_candle)?;
            Some(WsEvent::Candle { timeframe, candle })
        }
        "fundingRate" => {
            let funding = number_field(&value, &["funding", "fundingRate"]).map(funding_to_percent);
            Some(WsEvent::Market(MarketTick {
                funding_pct: funding,
                ..MarketTick::default()
            }))
        }
        "subscriptionConfirmed" | "subscriptionStatus" | "subscriptionError" | "error" => None,
        _ => None,
    }
}

fn parse_orderbook(value: &Value) -> Option<OrderBook> {
    let book = value.get("orderbook").unwrap_or(value);
    let bids = levels(book.get("bids"));
    let asks = levels(book.get("asks"));
    if bids.is_empty() && asks.is_empty() {
        return None;
    }
    let mid = number_field(book, &["mid"])
        .or_else(|| number_field(value, &["mid"]))
        .or_else(|| match (bids.first(), asks.first()) {
            (Some(bid), Some(ask)) => Some((bid.price + ask.price) / 2.0),
            (Some(bid), None) => Some(bid.price),
            (None, Some(ask)) => Some(ask.price),
            (None, None) => None,
        });
    Some(OrderBook {
        symbol: value
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        bids,
        asks,
        mid,
    })
}

fn parse_market(value: &Value) -> MarketTick {
    let prev = number_field(value, &["prevDayPx", "prevDayPrice"]);
    let mark = number_field(value, &["markPx", "markPrice"]);
    let mid = number_field(value, &["midPx", "mid"]);
    let index = number_field(value, &["oraclePx", "indexPx", "indexPrice"]);
    let change_24h_pct = match (mark.or(mid), prev) {
        (Some(price), Some(prev)) if prev.abs() > f64::EPSILON => {
            Some(((price - prev) / prev) * 100.0)
        }
        _ => None,
    };
    MarketTick {
        mark: mark.or(mid),
        index,
        mid,
        prev_day: prev,
        open_interest: number_field(value, &["openInterest", "openInterestBase"]),
        volume_24h: number_field(value, &["dayNtlVlm", "volume24h", "dayVolume"]),
        change_24h_pct,
        funding_pct: number_field(value, &["funding", "fundingRate"]).map(funding_to_percent),
    }
}

fn funding_to_percent(value: f64) -> f64 {
    if value.abs() <= 0.05 {
        value * 100.0
    } else {
        value
    }
}

fn parse_mids(value: &Value) -> Vec<(String, f64)> {
    value
        .get("mids")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(symbol, price)| Some((symbol.clone(), parse_number(Some(price))?)))
        .collect()
}

fn parse_trades(value: &Value) -> Vec<Trade> {
    value
        .get("trades")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(parse_trade)
        .collect()
}

fn parse_trade(value: &Value) -> Option<Trade> {
    let price =
        number_field(value, &["price", "px"]).or_else(|| parse_number(value.get("price")))?;
    let size = number_field(value, &["size", "baseAmount", "baseQty", "sz"]).unwrap_or(0.0);
    let notional = number_field(value, &["notional", "quoteAmount", "quoteQty"]);
    let side = value.get("side").and_then(Value::as_str).unwrap_or("");
    let is_buy = matches!(side, "bid" | "b" | "buy" | "Buy");
    let timestamp = value
        .get("timestamp")
        .and_then(|field| {
            field
                .as_str()
                .map(str::to_owned)
                .or_else(|| field.as_i64().map(|n| n.to_string()))
        })
        .or_else(|| integer_field(value, &["time"]).map(|n| n.to_string()))
        .unwrap_or_default();
    Some(Trade {
        price,
        size: size.abs(),
        notional,
        is_buy: if size < 0.0 {
            false
        } else {
            is_buy || size > 0.0
        },
        timestamp,
    })
}

fn parse_candle(value: &Value) -> Option<Candle> {
    let time = integer_field(value, &["time"])?;
    let time_ms = if time > 10_000_000_000 {
        time
    } else {
        time * 1000
    };
    Some(Candle {
        time_ms,
        open: number_field(value, &["open"])?,
        high: number_field(value, &["high"])?,
        low: number_field(value, &["low"])?,
        close: number_field(value, &["close"])?,
        volume: number_field(value, &["volume"]).unwrap_or(0.0),
        volume_quote: number_field(value, &["volumeQuote"]),
        trade_count: integer_field(value, &["tradeCount"]).map(|value| value.max(0) as u32),
    })
}

fn levels(value: Option<&Value>) -> Vec<BookLevel> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let pair = entry.as_array()?;
            let price = parse_number(pair.first())?;
            let size = parse_number(pair.get(1))?;
            if price.is_finite() && size.is_finite() && size > 0.0 {
                Some(BookLevel { price, size })
            } else {
                None
            }
        })
        .collect()
}

fn number_field(value: &Value, names: &[&str]) -> Option<f64> {
    names.iter().find_map(|name| parse_number(value.get(*name)))
}

fn integer_field(value: &Value, names: &[&str]) -> Option<i64> {
    names.iter().find_map(|name| {
        let field = value.get(*name)?;
        field
            .as_i64()
            .or_else(|| field.as_u64().map(|value| value as i64))
            .or_else(|| field.as_f64().map(|value| value as i64))
            .or_else(|| field.as_str()?.parse().ok())
    })
}

fn parse_number(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|value| value as f64))
        .or_else(|| value.as_u64().map(|value| value as f64))
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|value| value.is_finite())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ws_candle_seconds() {
        let event = parse_event(
            r#"{"channel":"candle","symbol":"SOL","timeframe":"1m","candle":{"time":1747556460,"open":170.42,"high":170.5,"low":170.38,"close":170.45,"volume":92.1}}"#,
        );
        match event {
            Some(WsEvent::Candle { timeframe, candle }) => {
                assert_eq!(timeframe, Timeframe::OneMinute);
                assert_eq!(candle.time_ms, 1_747_556_460_000);
                assert_eq!(candle.close, 170.45);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parses_flat_orderbook_and_market_funding() {
        let book = parse_event(
            r#"{"channel":"orderbook","symbol":"SOL","bids":[[86.84,92.05]],"asks":[[86.86,89.6]],"mid":86.85}"#,
        );
        match book {
            Some(WsEvent::Orderbook(book)) => {
                assert_eq!(book.bids[0].price, 86.84);
                assert_eq!(book.asks[0].size, 89.6);
                assert_eq!(book.mid, Some(86.85));
            }
            other => panic!("{other:?}"),
        }

        let market = parse_event(
            r#"{"channel":"market","symbol":"SOL","markPx":86.83,"prevDayPx":77.48,"openInterest":29696.65,"dayNtlVlm":8796596.52,"funding":0.001936}"#,
        );
        match market {
            Some(WsEvent::Market(tick)) => {
                assert_eq!(tick.mark, Some(86.83));
                assert_eq!(tick.prev_day, Some(77.48));
                assert!((tick.funding_pct.unwrap() - 0.1936).abs() < 1e-6);
                assert!((tick.change_24h_pct.unwrap() - 12.0676).abs() < 1e-3);
            }
            other => panic!("{other:?}"),
        }
    }
}
