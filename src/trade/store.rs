use std::collections::HashMap;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use proof_phoenix::{
    Candle, CandleStore, DeskSnapshot, LiveQuote, MarketListing, MarketTick, PhoenixRest,
    PhoenixWs, Timeframe, Trade, WsEvent, desk_from_live, retry_after,
};
use smol::channel::{Sender as EventSender, bounded as event_channel};

const WS_POLL: Duration = Duration::from_millis(80);
const MARKET_REFRESH: Duration = Duration::from_secs(120);
const HISTORY_LIMIT: usize = 720;
const TRADE_LIMIT: usize = 40;

type SeriesKey = (String, Timeframe);

pub enum FeedCommand {
    Select {
        symbol: String,
        timeframe: Timeframe,
    },
    Shutdown,
}

pub enum FeedEvent {
    Markets(Vec<MarketListing>),
    Desk(Box<DeskSnapshot>),
    Error(String),
}

pub fn spawn_feed() -> (Sender<FeedCommand>, smol::channel::Receiver<FeedEvent>) {
    let (command_tx, command_rx) = unbounded();
    let (event_tx, event_rx) = event_channel(8);
    thread::Builder::new()
        .name("proof-phoenix-feed".into())
        .spawn(move || run_feed(command_rx, event_tx))
        .expect("could not start Phoenix market-data thread");
    (command_tx, event_rx)
}

fn run_feed(commands: Receiver<FeedCommand>, events: EventSender<FeedEvent>) {
    let rest = PhoenixRest::new();
    let store = CandleStore::open_default().ok();
    let mut symbol = proof_phoenix::DEFAULT_SYMBOL.to_owned();
    let mut timeframe = Timeframe::OneMinute;
    let mut series: HashMap<SeriesKey, Vec<Candle>> = HashMap::new();
    let mut listings: HashMap<String, MarketListing> = HashMap::new();
    let mut markets_dirty = false;
    let mut live = LiveQuote::from_listing(MarketListing::placeholder(&symbol));
    let mut ws = PhoenixWs::connect(&symbol, timeframe).ok();
    let mut last_ws_attempt = Instant::now();
    let mut last_markets = Instant::now();
    let mut rest_cooldown_until = Instant::now();
    let mut backoff = Duration::from_secs(1);

    seed_default_markets(&events, &mut listings);
    seed_history(
        &rest,
        store.as_ref(),
        &mut series,
        &symbol,
        timeframe,
        &mut rest_cooldown_until,
    );
    if Instant::now() >= rest_cooldown_until {
        push_markets(&rest, &events, &mut listings, &mut rest_cooldown_until);
    }
    if let Some(listing) = listings.get(&symbol).cloned() {
        live.listing = listing;
        live.open_interest = Some(live.listing.open_interest).filter(|value| *value > 0.0);
    }
    emit_current(&live, &series, &events, &symbol, timeframe);

    loop {
        match commands.recv_timeout(WS_POLL) {
            Ok(FeedCommand::Shutdown) | Err(RecvTimeoutError::Disconnected) => break,
            Ok(FeedCommand::Select {
                symbol: next_symbol,
                timeframe: next_timeframe,
            }) => {
                let symbol_changed = next_symbol != symbol;
                symbol = next_symbol;
                timeframe = next_timeframe;
                if symbol_changed {
                    live = listing_quote(&listings, &symbol);
                }
                if let Some(cached) = load_cached(store.as_ref(), &series, &symbol, timeframe) {
                    series.insert((symbol.clone(), timeframe), cached);
                } else {
                    seed_history(
                        &rest,
                        store.as_ref(),
                        &mut series,
                        &symbol,
                        timeframe,
                        &mut rest_cooldown_until,
                    );
                }
                if let Some(socket) = ws.as_mut() {
                    if socket.set_selection(&symbol, timeframe).is_err() {
                        ws = None;
                    }
                }
                emit_current(&live, &series, &events, &symbol, timeframe);
            }
            Err(RecvTimeoutError::Timeout) => {
                let mut dirty = false;
                if let Some(socket) = ws.as_mut() {
                    for _ in 0..32 {
                        match socket.poll(Duration::from_millis(1)) {
                            Ok(Some(event)) => {
                                let (quote_dirty, listing_dirty) = apply_ws(
                                    event,
                                    store.as_ref(),
                                    &mut series,
                                    &mut live,
                                    &mut listings,
                                    &symbol,
                                    timeframe,
                                );
                                dirty |= quote_dirty;
                                markets_dirty |= listing_dirty;
                                backoff = Duration::from_secs(1);
                            }
                            Ok(None) => break,
                            Err(error) => {
                                send_event(&events, FeedEvent::Error(error.to_string()));
                                ws = None;
                                break;
                            }
                        }
                    }
                } else if last_ws_attempt.elapsed() >= backoff {
                    last_ws_attempt = Instant::now();
                    match PhoenixWs::connect(&symbol, timeframe) {
                        Ok(socket) => {
                            ws = Some(socket);
                            backoff = Duration::from_secs(1);
                        }
                        Err(error) => {
                            send_event(&events, FeedEvent::Error(error.to_string()));
                            backoff = (backoff * 2).min(Duration::from_secs(30));
                        }
                    }
                }
                if last_markets.elapsed() >= MARKET_REFRESH && Instant::now() >= rest_cooldown_until
                {
                    last_markets = Instant::now();
                    push_markets(&rest, &events, &mut listings, &mut rest_cooldown_until);
                    if let Some(listing) = listings.get(&symbol).cloned() {
                        live.listing = listing;
                        dirty = true;
                    }
                }
                if markets_dirty {
                    markets_dirty = false;
                    emit_markets(&listings, &events);
                }
                if dirty {
                    emit_current(&live, &series, &events, &symbol, timeframe);
                }
            }
        }
    }
}

fn apply_ws(
    event: WsEvent,
    store: Option<&CandleStore>,
    series: &mut HashMap<SeriesKey, Vec<Candle>>,
    live: &mut LiveQuote,
    listings: &mut HashMap<String, MarketListing>,
    symbol: &str,
    timeframe: Timeframe,
) -> (bool, bool) {
    match event {
        WsEvent::Orderbook(book) => {
            live.book = book;
            live.mid = live.book.mid.or(live.mid);
            live.mark = live.mark.or(live.mid);
            live.fetched_at = Instant::now();
            (true, false)
        }
        WsEvent::Market(tick) => {
            apply_tick(live, &tick);
            live.fetched_at = Instant::now();
            if let Some(mark) = tick.mark.or(tick.mid)
                && let Some(listing) = listings.get_mut(symbol)
            {
                listing.mark = Some(mark);
                return (true, true);
            }
            (true, false)
        }
        WsEvent::Trades(trades) => {
            prepend_trades(&mut live.trades, trades);
            live.fetched_at = Instant::now();
            (true, false)
        }
        WsEvent::Mids(mids) => {
            let mut changed = false;
            for (mid_symbol, price) in mids {
                if let Some(listing) = listings.get_mut(&mid_symbol) {
                    listing.mark = Some(price);
                    changed = true;
                }
                if mid_symbol == symbol {
                    live.mid = Some(price);
                    live.mark = live.mark.or(Some(price));
                }
            }
            (true, changed)
        }
        WsEvent::Candle {
            timeframe: candle_tf,
            candle,
        } => {
            if candle_tf != timeframe {
                return (false, false);
            }
            let key = (symbol.to_owned(), timeframe);
            let mut candles = series.remove(&key).unwrap_or_default();
            upsert_candle(&mut candles, candle);
            if let Some(store) = store {
                let _ = store.upsert(symbol, timeframe, std::slice::from_ref(&candle));
            }
            series.insert(key, candles);
            (true, false)
        }
    }
}

fn emit_markets(listings: &HashMap<String, MarketListing>, events: &EventSender<FeedEvent>) {
    let mut markets: Vec<MarketListing> = listings.values().cloned().collect();
    markets.sort_by(|left, right| {
        market_rank(&left.symbol)
            .cmp(&market_rank(&right.symbol))
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    send_event(events, FeedEvent::Markets(markets));
}

fn apply_tick(quote: &mut LiveQuote, tick: &MarketTick) {
    if tick.mark.is_some() {
        quote.mark = tick.mark;
    }
    if tick.index.is_some() {
        quote.index = tick.index;
    }
    if tick.mid.is_some() {
        quote.mid = tick.mid;
    }
    if tick.prev_day.is_some() {
        quote.prev_day = tick.prev_day;
    }
    if tick.funding_pct.is_some() {
        quote.funding_pct = tick.funding_pct;
    }
    if tick.open_interest.is_some() {
        quote.open_interest = tick.open_interest;
    }
    if tick.volume_24h.is_some() {
        quote.volume_24h = tick.volume_24h;
    }
    if tick.change_24h_pct.is_some() {
        quote.change_24h_pct = tick.change_24h_pct;
    }
}

fn prepend_trades(existing: &mut Vec<Trade>, incoming: Vec<Trade>) {
    for trade in incoming.into_iter().rev() {
        existing.insert(0, trade);
    }
    existing.truncate(TRADE_LIMIT);
}

fn upsert_candle(candles: &mut Vec<Candle>, candle: Candle) {
    if let Some(old) = candles
        .iter_mut()
        .rev()
        .find(|old| old.time_ms == candle.time_ms)
    {
        *old = candle;
        return;
    }
    candles.push(candle);
    candles.sort_by_key(|item| item.time_ms);
    candles.dedup_by_key(|item| item.time_ms);
    if candles.len() > HISTORY_LIMIT {
        let drop = candles.len() - HISTORY_LIMIT;
        candles.drain(..drop);
    }
}

fn seed_history(
    rest: &PhoenixRest,
    store: Option<&CandleStore>,
    series: &mut HashMap<SeriesKey, Vec<Candle>>,
    symbol: &str,
    timeframe: Timeframe,
    rest_cooldown_until: &mut Instant,
) {
    let key = (symbol.to_owned(), timeframe);
    if series.get(&key).is_some_and(|candles| !candles.is_empty()) {
        return;
    }
    if let Some(cached) = store.and_then(|store| store.load(symbol, timeframe, HISTORY_LIMIT).ok())
        && !cached.is_empty()
    {
        series.insert(key, cached);
        return;
    }
    if Instant::now() < *rest_cooldown_until {
        return;
    }
    match rest.get_candles(symbol, timeframe, HISTORY_LIMIT as u32) {
        Ok(incoming) if !incoming.is_empty() => {
            let merged = merge_candles(series.remove(&key).unwrap_or_default(), incoming);
            if let Some(store) = store {
                let _ = store.upsert(symbol, timeframe, &merged);
            }
            series.insert(key, merged);
        }
        Ok(_) => {}
        Err(error) => note_rest_error(&error, rest_cooldown_until, symbol, "candles"),
    }
}

fn listing_quote(listings: &HashMap<String, MarketListing>, symbol: &str) -> LiveQuote {
    LiveQuote::from_listing(
        listings
            .get(symbol)
            .cloned()
            .unwrap_or_else(|| MarketListing::placeholder(symbol)),
    )
}

fn emit_current(
    live: &LiveQuote,
    series: &HashMap<SeriesKey, Vec<Candle>>,
    events: &EventSender<FeedEvent>,
    symbol: &str,
    timeframe: Timeframe,
) {
    let candles = series
        .get(&(symbol.to_owned(), timeframe))
        .cloned()
        .unwrap_or_default();
    let snapshot = desk_from_live(live.clone(), timeframe, candles);
    send_event(events, FeedEvent::Desk(Box::new(snapshot)));
}

fn load_cached(
    store: Option<&CandleStore>,
    series: &HashMap<SeriesKey, Vec<Candle>>,
    symbol: &str,
    timeframe: Timeframe,
) -> Option<Vec<Candle>> {
    series
        .get(&(symbol.to_owned(), timeframe))
        .cloned()
        .or_else(|| store.and_then(|store| store.load(symbol, timeframe, HISTORY_LIMIT).ok()))
        .filter(|candles| !candles.is_empty())
}

fn merge_candles(mut existing: Vec<Candle>, incoming: Vec<Candle>) -> Vec<Candle> {
    for candle in incoming {
        upsert_candle(&mut existing, candle);
    }
    existing
}

fn seed_default_markets(
    events: &EventSender<FeedEvent>,
    listings: &mut HashMap<String, MarketListing>,
) {
    let defaults = ["SOL", "BTC", "ETH"].map(MarketListing::placeholder);
    for market in &defaults {
        listings.insert(market.symbol.clone(), market.clone());
    }
    send_event(events, FeedEvent::Markets(defaults.to_vec()));
}

fn push_markets(
    client: &PhoenixRest,
    events: &EventSender<FeedEvent>,
    listings: &mut HashMap<String, MarketListing>,
    rest_cooldown_until: &mut Instant,
) {
    match client.list_markets() {
        Ok(mut markets) => {
            markets.sort_by(|left, right| {
                market_rank(&left.symbol)
                    .cmp(&market_rank(&right.symbol))
                    .then_with(|| left.symbol.cmp(&right.symbol))
            });
            listings.clear();
            for market in &markets {
                listings.insert(market.symbol.clone(), market.clone());
            }
            send_event(events, FeedEvent::Markets(markets));
        }
        Err(error) => {
            note_rest_error(&error, rest_cooldown_until, "exchange", "markets");
            if listings.is_empty() {
                send_event(events, FeedEvent::Error(error.to_string()));
            }
        }
    }
}

fn note_rest_error(
    error: &anyhow::Error,
    rest_cooldown_until: &mut Instant,
    symbol: &str,
    kind: &str,
) {
    if let Some(retry) = retry_after(error) {
        *rest_cooldown_until = Instant::now() + retry;
    }
    eprintln!("phoenix {kind} {symbol}: {error:#}");
}

fn send_event(events: &EventSender<FeedEvent>, event: FeedEvent) {
    let _ = events.try_send(event);
}

fn market_rank(symbol: &str) -> u8 {
    match symbol {
        "SOL" => 0,
        "BTC" => 1,
        "ETH" => 2,
        _ => 10,
    }
}
