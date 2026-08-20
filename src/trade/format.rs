pub fn price(value: f64) -> String {
    if !value.is_finite() {
        return "—".into();
    }
    let abs = value.abs();
    if abs >= 1_000.0 {
        format!("{value:.2}")
    } else if abs >= 1.0 {
        format!("{value:.4}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_owned()
    } else {
        format!("{value:.6}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_owned()
    }
}

pub fn compact(value: f64) -> String {
    if !value.is_finite() {
        return "—".into();
    }
    let abs = value.abs();
    let sign = if value < 0.0 { "-" } else { "" };
    if abs >= 1_000_000_000.0 {
        format!("{sign}{:.2}B", abs / 1_000_000_000.0)
    } else if abs >= 1_000_000.0 {
        format!("{sign}{:.2}M", abs / 1_000_000.0)
    } else if abs >= 1_000.0 {
        format!("{sign}{:.2}K", abs / 1_000.0)
    } else {
        price(abs)
    }
}

pub fn signed_percent(value: f64) -> String {
    if !value.is_finite() {
        return "—".into();
    }
    format!("{value:+.2}%")
}

pub fn unsigned_percent(value: f64) -> String {
    if !value.is_finite() {
        return "—".into();
    }
    format!("{value:.4}%")
}

pub fn size(value: f64) -> String {
    if !value.is_finite() {
        return "—".into();
    }
    if value.abs() >= 100.0 {
        format!("{value:.2}")
    } else {
        format!("{value:.4}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_owned()
    }
}

pub fn clock_label(timestamp: &str) -> String {
    if let Some(label) = timestamp.get(11..19).filter(|label| !label.is_empty()) {
        return label.to_owned();
    }
    let digits = timestamp.trim();
    if let Ok(value) = digits.parse::<i64>() {
        let seconds = if value > 10_000_000_000 {
            value / 1000
        } else {
            value
        };
        let hours = ((seconds % 86_400) / 3_600 + 24) % 24;
        let minutes = (seconds % 3_600) / 60;
        let secs = seconds % 60;
        return format!("{hours:02}:{minutes:02}:{secs:02}");
    }
    timestamp.to_owned()
}
