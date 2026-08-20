//! Local SQLite cache for Phoenix candles.
//!
//! History lives on disk so switching timeframes does not wait on the network
//! and live ticks only rewrite the forming bar.

use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use rusqlite::{Connection, params};

use crate::{Candle, Timeframe};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    time_ms INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    PRIMARY KEY (symbol, timeframe, time_ms)
);
";

pub struct CandleStore {
    connection: Connection,
}

impl CandleStore {
    pub fn open_default() -> anyhow::Result<Self> {
        Self::open(default_path()?)
    }

    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("could not create {}", parent.display()))?;
        }
        let connection = Connection::open(path.as_ref())
            .with_context(|| format!("could not open {}", path.as_ref().display()))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;",
        )?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self { connection })
    }

    pub fn load(
        &self,
        symbol: &str,
        timeframe: Timeframe,
        limit: usize,
    ) -> anyhow::Result<Vec<Candle>> {
        let mut statement = self.connection.prepare(
            "SELECT time_ms, open, high, low, close, volume
             FROM candles
             WHERE symbol = ?1 AND timeframe = ?2
             ORDER BY time_ms DESC
             LIMIT ?3",
        )?;
        let mut rows = statement.query(params![symbol, timeframe.as_api(), limit as i64])?;
        let mut candles = Vec::new();
        while let Some(row) = rows.next()? {
            candles.push(Candle {
                time_ms: row.get(0)?,
                open: row.get(1)?,
                high: row.get(2)?,
                low: row.get(3)?,
                close: row.get(4)?,
                volume: row.get(5)?,
            });
        }
        candles.reverse();
        Ok(candles)
    }

    pub fn upsert(
        &self,
        symbol: &str,
        timeframe: Timeframe,
        candles: &[Candle],
    ) -> anyhow::Result<()> {
        if candles.is_empty() {
            return Ok(());
        }
        let tx = self.connection.unchecked_transaction()?;
        {
            let mut statement = tx.prepare(
                "INSERT INTO candles (symbol, timeframe, time_ms, open, high, low, close, volume)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(symbol, timeframe, time_ms) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume",
            )?;
            for candle in candles {
                statement.execute(params![
                    symbol,
                    timeframe.as_api(),
                    candle.time_ms,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}

fn default_path() -> anyhow::Result<PathBuf> {
    let root =
        dirs::data_local_dir().ok_or_else(|| anyhow!("local data directory is unavailable"))?;
    Ok(root.join("Proof Terminal").join("phoenix-candles.sqlite"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_replaces_forming_bar() {
        let store = CandleStore::open(":memory:").unwrap();
        let first = Candle {
            time_ms: 1_000,
            open: 10.0,
            high: 11.0,
            low: 9.0,
            close: 10.5,
            volume: 1.0,
        };
        store.upsert("SOL", Timeframe::OneMinute, &[first]).unwrap();
        let updated = Candle {
            time_ms: 1_000,
            open: 10.0,
            high: 12.0,
            low: 9.0,
            close: 11.5,
            volume: 3.0,
        };
        store
            .upsert("SOL", Timeframe::OneMinute, &[updated])
            .unwrap();
        let loaded = store.load("SOL", Timeframe::OneMinute, 10).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].close, 11.5);
        assert_eq!(loaded[0].volume, 3.0);
    }

    #[test]
    fn window_keeps_latest_when_offset_is_zero() {
        let candles = (0..10)
            .map(|index| Candle {
                time_ms: index,
                open: index as f64,
                high: index as f64,
                low: index as f64,
                close: index as f64,
                volume: 1.0,
            })
            .collect::<Vec<_>>();
        let window = window_candles(&candles, 4, 0);
        assert_eq!(window.len(), 4);
        assert_eq!(window[0].time_ms, 6);
        assert_eq!(window[3].time_ms, 9);
        let panned = window_candles(&candles, 4, 3);
        assert_eq!(panned[0].time_ms, 3);
        assert_eq!(panned[3].time_ms, 6);
        let clamped = window_candles(&candles, 4, 99);
        assert_eq!(clamped[0].time_ms, 0);
    }
}

pub fn window_candles(candles: &[Candle], visible: usize, offset: usize) -> Vec<Candle> {
    if candles.is_empty() {
        return Vec::new();
    }
    let visible = visible.clamp(1, candles.len());
    let max_offset = candles.len().saturating_sub(visible);
    let offset = offset.min(max_offset);
    let start = candles.len() - visible - offset;
    candles[start..start + visible].to_vec()
}
