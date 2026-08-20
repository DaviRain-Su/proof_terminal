//! Phoenix Perps public REST adapter.
//!
//! P0 only reads market data. Nothing here signs, submits, or holds keys.
//! Callers must keep this off the UI thread.

mod candles;
mod ws;

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use serde::Deserialize;

pub use candles::{CandleStore, window_candles};
pub use ws::{MarketTick, PhoenixWs, WsEvent};

pub const DEFAULT_API_URL: &str = "https://perp-api.phoenix.trade";
pub const DEFAULT_SYMBOL: &str = "SOL";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Timeframe {
    OneSecond,
    FiveSeconds,
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    ThirtyMinutes,
    OneHour,
    FourHours,
    OneDay,
}

#[derive(Debug)]
pub struct RateLimit {
    pub retry_after: Duration,
}

impl std::fmt::Display for RateLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "phoenix rate limited, retry after {}s",
            self.retry_after.as_secs().max(1)
        )
    }
}

impl std::error::Error for RateLimit {}

pub fn retry_after(error: &anyhow::Error) -> Option<Duration> {
    error
        .downcast_ref::<RateLimit>()
        .map(|limit| limit.retry_after)
}

impl Timeframe {
    pub const ALL: [Self; 9] = [
        Self::OneSecond,
        Self::FiveSeconds,
        Self::OneMinute,
        Self::FiveMinutes,
        Self::FifteenMinutes,
        Self::ThirtyMinutes,
        Self::OneHour,
        Self::FourHours,
        Self::OneDay,
    ];

    pub fn as_api(self) -> &'static str {
        match self {
            Self::OneSecond => "1s",
            Self::FiveSeconds => "5s",
            Self::OneMinute => "1m",
            Self::FiveMinutes => "5m",
            Self::FifteenMinutes => "15m",
            Self::ThirtyMinutes => "30m",
            Self::OneHour => "1h",
            Self::FourHours => "4h",
            Self::OneDay => "1d",
        }
    }

    pub fn label(self) -> &'static str {
        self.as_api()
    }

    pub fn from_api(value: &str) -> Option<Self> {
        match value {
            "1s" => Some(Self::OneSecond),
            "5s" => Some(Self::FiveSeconds),
            "1m" => Some(Self::OneMinute),
            "5m" => Some(Self::FiveMinutes),
            "15m" => Some(Self::FifteenMinutes),
            "30m" => Some(Self::ThirtyMinutes),
            "1h" => Some(Self::OneHour),
            "4h" => Some(Self::FourHours),
            "1d" => Some(Self::OneDay),
            _ => None,
        }
    }

    pub fn duration_ms(self) -> i64 {
        match self {
            Self::OneSecond => 1_000,
            Self::FiveSeconds => 5_000,
            Self::OneMinute => 60_000,
            Self::FiveMinutes => 5 * 60_000,
            Self::FifteenMinutes => 15 * 60_000,
            Self::ThirtyMinutes => 30 * 60_000,
            Self::OneHour => 60 * 60_000,
            Self::FourHours => 4 * 60 * 60_000,
            Self::OneDay => 24 * 60 * 60_000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MarketListing {
    pub symbol: String,
    pub name: String,
    pub status: String,
    pub isolated_only: bool,
    pub max_leverage: f64,
    pub taker_fee: f64,
    pub maker_fee: f64,
    pub base_lots_decimals: u32,
    pub open_interest: f64,
    pub is_commodity: bool,
    pub mark: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct BookLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Clone, Debug)]
pub struct OrderBook {
    pub symbol: String,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub mid: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Candle {
    pub time_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub volume_quote: Option<f64>,
    pub trade_count: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct Trade {
    pub price: f64,
    pub size: f64,
    pub notional: Option<f64>,
    pub is_buy: bool,
    pub timestamp: String,
}

#[derive(Clone, Debug)]
pub struct DeskSnapshot {
    pub symbol: String,
    pub timeframe: Timeframe,
    pub name: String,
    pub status: String,
    pub mark: Option<f64>,
    pub index: Option<f64>,
    pub mid: Option<f64>,
    pub prev_day: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub change_24h: Option<f64>,
    pub change_24h_pct: Option<f64>,
    pub volume_24h: Option<f64>,
    pub open_interest: Option<f64>,
    pub funding_pct: Option<f64>,
    pub max_leverage: Option<f64>,
    pub taker_fee: Option<f64>,
    pub isolated_only: bool,
    pub book: OrderBook,
    pub candles: Arc<Vec<Candle>>,
    pub trades: Vec<Trade>,
    pub fetched_at: Instant,
}

#[derive(Clone, Debug)]
pub struct LiveQuote {
    pub listing: MarketListing,
    pub mark: Option<f64>,
    pub index: Option<f64>,
    pub mid: Option<f64>,
    pub prev_day: Option<f64>,
    pub book: OrderBook,
    pub trades: Vec<Trade>,
    pub funding_pct: Option<f64>,
    pub open_interest: Option<f64>,
    pub volume_24h: Option<f64>,
    pub change_24h_pct: Option<f64>,
    pub fetched_at: Instant,
}

pub fn desk_from_live(live: LiveQuote, timeframe: Timeframe, candles: Vec<Candle>) -> DeskSnapshot {
    let mark = live.mark.or(live.mid).or(live.book.mid);
    let prev_day = live.prev_day;
    let change_24h = match (mark, prev_day) {
        (Some(mark), Some(prev)) => Some(mark - prev),
        _ => None,
    };
    let change_24h_pct = live.change_24h_pct.or_else(|| match (mark, prev_day) {
        (Some(mark), Some(prev)) if prev.abs() > f64::EPSILON => {
            Some(((mark - prev) / prev) * 100.0)
        }
        _ => percent_change_from_candles(&candles),
    });
    let _ = timeframe;
    DeskSnapshot {
        symbol: live.listing.symbol.clone(),
        timeframe,
        name: live.listing.name.clone(),
        status: live.listing.status.clone(),
        mark,
        index: live.index,
        mid: live.mid.or(live.book.mid),
        prev_day,
        best_bid: live.book.bids.first().map(|level| level.price),
        best_ask: live.book.asks.first().map(|level| level.price),
        change_24h,
        change_24h_pct,
        volume_24h: live.volume_24h,
        open_interest: live
            .open_interest
            .or(Some(live.listing.open_interest).filter(|value| *value > 0.0)),
        funding_pct: live.funding_pct,
        max_leverage: Some(live.listing.max_leverage).filter(|value| *value > 0.0),
        taker_fee: Some(live.listing.taker_fee),
        isolated_only: live.listing.isolated_only,
        book: live.book,
        candles: Arc::new(candles),
        trades: live.trades,
        fetched_at: live.fetched_at,
    }
}

#[derive(Clone)]
pub struct PhoenixRest {
    agent: ureq::Agent,
    base: String,
}

impl PhoenixRest {
    pub fn new() -> Self {
        Self::with_base(DEFAULT_API_URL)
    }

    pub fn with_base(base: impl Into<String>) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(CONNECT_TIMEOUT)
            .timeout_read(READ_TIMEOUT)
            .timeout_write(CONNECT_TIMEOUT)
            .user_agent("proof-terminal/0.1")
            .build();
        Self {
            agent,
            base: base.into().trim_end_matches('/').to_owned(),
        }
    }

    pub fn list_markets(&self) -> anyhow::Result<Vec<MarketListing>> {
        let url = format!("{}/v1/view/exchange/markets", self.base);
        let raw: Vec<serde_json::Value> = self.get_json(&url)?;
        Ok(raw
            .into_iter()
            .filter_map(|value| serde_json::from_value::<ApiMarket>(value).ok())
            .map(MarketListing::from)
            .collect())
    }

    pub fn get_listing(&self, symbol: &str) -> anyhow::Result<MarketListing> {
        Ok(MarketListing::from(self.get_market(symbol)?))
    }

    pub fn get_orderbook(&self, symbol: &str) -> anyhow::Result<OrderBook> {
        let url = format!("{}/v1/view/orderbook/{symbol}", self.base);
        let raw: ApiOrderbook = self.get_json(&url)?;
        Ok(OrderBook::from(raw))
    }

    pub fn load_live(&self, symbol: &str) -> anyhow::Result<LiveQuote> {
        let symbol = symbol.trim();
        if symbol.is_empty() {
            bail!("market symbol is empty");
        }

        let listing = self.get_listing(symbol)?;
        let mut quote = LiveQuote::from_listing(listing);
        if let Ok(book) = self.get_orderbook(symbol) {
            quote.mark = book.mid;
            quote.book = book;
        }
        Ok(quote)
    }

    pub fn load_desk(&self, symbol: &str, timeframe: Timeframe) -> anyhow::Result<DeskSnapshot> {
        let live = self.load_live(symbol)?;
        let candles = self.get_candles(symbol, timeframe, 240).unwrap_or_default();
        Ok(desk_from_live(live, timeframe, candles))
    }

    pub fn get_candles(
        &self,
        symbol: &str,
        timeframe: Timeframe,
        limit: u32,
    ) -> anyhow::Result<Vec<Candle>> {
        let url = format!(
            "{}/v1/candles/{symbol}?timeframe={}&limit={limit}",
            self.base,
            timeframe.as_api()
        );
        self.candles_from_url(&url)
    }

    pub fn get_candles_after(
        &self,
        symbol: &str,
        timeframe: Timeframe,
        start_time_ms: i64,
        limit: u32,
    ) -> anyhow::Result<Vec<Candle>> {
        let url = format!(
            "{}/v1/candles/{symbol}?timeframe={}&startTime={start_time_ms}&limit={limit}",
            self.base,
            timeframe.as_api()
        );
        self.candles_from_url(&url)
    }

    fn candles_from_url(&self, url: &str) -> anyhow::Result<Vec<Candle>> {
        let raw: Vec<ApiCandle> = self.get_json(url)?;
        Ok(raw.into_iter().filter_map(Candle::from_api).collect())
    }

    fn get_market(&self, symbol: &str) -> anyhow::Result<ApiMarket> {
        let url = format!("{}/v1/view/exchange/market/{symbol}", self.base);
        self.get_json(&url)
    }

    fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> anyhow::Result<T> {
        let response = match self.agent.get(url).call() {
            Ok(response) => response,
            Err(ureq::Error::Status(429, response)) => {
                let retry_after = retry_after_header(&response);
                return Err(RateLimit { retry_after }.into());
            }
            Err(ureq::Error::Status(code, _)) => {
                bail!("phoenix GET {url} returned {code}");
            }
            Err(error) => bail!("phoenix GET {url}: {error}"),
        };
        if !(200..300).contains(&response.status()) {
            bail!("phoenix GET {url} returned {}", response.status());
        }
        let body = response
            .into_string()
            .with_context(|| format!("phoenix GET {url} body"))?;
        serde_json::from_str(&body).with_context(|| {
            let preview = body
                .chars()
                .take(120)
                .collect::<String>()
                .replace('\n', " ");
            format!("phoenix GET {url} JSON: {preview}")
        })
    }
}

fn retry_after_header(response: &ureq::Response) -> Duration {
    let secs = response
        .header("retry-after")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(8)
        .clamp(1, 60);
    Duration::from_secs(secs)
}

impl Default for PhoenixRest {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketListing {
    pub fn placeholder(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_owned(),
            name: format!("{symbol}-PERP"),
            status: "unknown".into(),
            isolated_only: false,
            max_leverage: 0.0,
            taker_fee: 0.0,
            maker_fee: 0.0,
            base_lots_decimals: 0,
            open_interest: 0.0,
            is_commodity: false,
            mark: None,
        }
    }
}

impl LiveQuote {
    pub fn from_listing(listing: MarketListing) -> Self {
        let symbol = listing.symbol.clone();
        Self {
            listing,
            mark: None,
            index: None,
            mid: None,
            prev_day: None,
            book: OrderBook {
                symbol,
                bids: Vec::new(),
                asks: Vec::new(),
                mid: None,
            },
            trades: Vec::new(),
            funding_pct: None,
            open_interest: None,
            volume_24h: None,
            change_24h_pct: None,
            fetched_at: Instant::now(),
        }
    }
}

impl From<ApiMarket> for MarketListing {
    fn from(market: ApiMarket) -> Self {
        let decimals = market.base_lots_decimals.unwrap_or(0);
        let open_interest = market
            .stats_snapshot
            .as_ref()
            .and_then(|stats| lots_to_base(stats.open_interest_base_lots.as_deref(), decimals))
            .unwrap_or(0.0);
        let max_leverage = market
            .leverage_tiers
            .first()
            .and_then(|tier| tier.max_leverage)
            .unwrap_or(0.0);
        let name = market
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.name.clone())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| market.symbol.clone());
        Self {
            symbol: market.symbol,
            name,
            status: market.market_status.unwrap_or_else(|| "unknown".into()),
            isolated_only: market.isolated_only.unwrap_or(false),
            max_leverage,
            taker_fee: market.taker_fee.unwrap_or(0.0),
            maker_fee: market.maker_fee.unwrap_or(0.0),
            base_lots_decimals: decimals,
            open_interest,
            is_commodity: market
                .commodity_metadata
                .as_ref()
                .and_then(|value| value.get("isCommodity"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            mark: None,
        }
    }
}

impl From<ApiOrderbook> for OrderBook {
    fn from(raw: ApiOrderbook) -> Self {
        let bids = levels_from_pairs(raw.bids);
        let asks = levels_from_pairs(raw.asks);
        let mid = mid_from_levels(&bids, &asks);
        Self {
            symbol: raw.symbol.unwrap_or_default(),
            bids,
            asks,
            mid,
        }
    }
}

impl Candle {
    fn from_api(raw: ApiCandle) -> Option<Self> {
        Some(Self {
            time_ms: raw.time?,
            open: raw.open?,
            high: raw.high?,
            low: raw.low?,
            close: raw.close?,
            volume: raw.volume.unwrap_or(0.0),
            volume_quote: raw.volume_quote,
            trade_count: raw.trade_count,
        })
    }
}

fn levels_from_pairs(pairs: Vec<[f64; 2]>) -> Vec<BookLevel> {
    pairs
        .into_iter()
        .filter_map(|pair| {
            let price = pair[0];
            let size = pair[1];
            if price.is_finite() && size.is_finite() && size > 0.0 {
                Some(BookLevel { price, size })
            } else {
                None
            }
        })
        .collect()
}

fn mid_from_levels(bids: &[BookLevel], asks: &[BookLevel]) -> Option<f64> {
    match (bids.first(), asks.first()) {
        (Some(bid), Some(ask)) => Some((bid.price + ask.price) / 2.0),
        (Some(bid), None) => Some(bid.price),
        (None, Some(ask)) => Some(ask.price),
        (None, None) => None,
    }
}

pub fn percent_change_from_candles(candles: &[Candle]) -> Option<f64> {
    let first = candles.first()?;
    let last = candles.last()?;
    if first.open.abs() < f64::EPSILON {
        return None;
    }
    Some(((last.close - first.open) / first.open) * 100.0)
}

fn lots_to_base(raw: Option<&str>, decimals: u32) -> Option<f64> {
    let lots = parse_f64(raw)?;
    Some(lots / 10_f64.powi(decimals as i32))
}

fn parse_f64(raw: Option<&str>) -> Option<f64> {
    raw?.parse::<f64>().ok().filter(|value| value.is_finite())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiMarket {
    symbol: String,
    #[serde(default)]
    market_status: Option<String>,
    #[serde(default)]
    isolated_only: Option<bool>,
    #[serde(default)]
    taker_fee: Option<f64>,
    #[serde(default)]
    maker_fee: Option<f64>,
    #[serde(default)]
    base_lots_decimals: Option<u32>,
    #[serde(default)]
    leverage_tiers: Vec<ApiLeverageTier>,
    #[serde(default)]
    metadata: Option<ApiMarketMetadata>,
    #[serde(default)]
    stats_snapshot: Option<ApiStatsSnapshot>,
    #[serde(default)]
    commodity_metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiLeverageTier {
    #[serde(default)]
    max_leverage: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiMarketMetadata {
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiStatsSnapshot {
    #[serde(default)]
    open_interest_base_lots: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiOrderbook {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    bids: Vec<[f64; 2]>,
    #[serde(default)]
    asks: Vec<[f64; 2]>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiCandle {
    #[serde(default)]
    time: Option<i64>,
    #[serde(default)]
    open: Option<f64>,
    #[serde(default)]
    high: Option<f64>,
    #[serde(default)]
    low: Option<f64>,
    #[serde(default)]
    close: Option<f64>,
    #[serde(default)]
    volume: Option<f64>,
    #[serde(default)]
    volume_quote: Option<f64>,
    #[serde(default)]
    trade_count: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_orderbook_and_mark_shapes() {
        let book: ApiOrderbook = serde_json::from_str(
            r#"{"slot":1,"symbol":"SOL","bids":[[87.67,86.33],[87.66,10.0]],"asks":[[87.68,12.0]]}"#,
        )
        .unwrap();
        let book = OrderBook::from(book);
        assert_eq!(book.bids[0].price, 87.67);
        assert_eq!(book.asks[0].size, 12.0);
        assert!((book.mid.unwrap() - 87.675).abs() < 1e-9);
    }

    #[test]
    fn twenty_four_hour_change_uses_first_open() {
        let candles = [
            Candle {
                time_ms: 0,
                open: 80.0,
                high: 81.0,
                low: 79.0,
                close: 80.5,
                volume: 1.0,
                volume_quote: None,
                trade_count: None,
            },
            Candle {
                time_ms: 1,
                open: 80.5,
                high: 88.0,
                low: 80.0,
                close: 88.0,
                volume: 2.0,
                volume_quote: None,
                trade_count: None,
            },
        ];
        let change = percent_change_from_candles(&candles).unwrap();
        assert!((change - 10.0).abs() < 1e-9);
    }
}
