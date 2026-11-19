//! Timestamp forms RevisionChest meets: the dump's `2026-05-31T23:59:55Z`
//! and the database's `2026-05-31 23:59:55`.

/// Normalizes either form to `YYYY-MM-DDTHH:MM:SSZ`. Returns `None` for
/// anything else. Pure string work; both inputs are already UTC.
#[must_use]
pub fn normalize_utc(ts: &str) -> Option<String> {
    let t = ts.trim();
    let t = t.strip_suffix('Z').unwrap_or(t);
    let (date, time) = t.split_once(['T', ' '])?;
    let ok = |s: &str, n: usize| {
        s.len() == n
            && s.bytes()
                .all(|b| b.is_ascii_digit() || b == b'-' || b == b':')
    };
    if !(ok(date, 10) && ok(time, 8)) {
        return None;
    }
    // light structural check: YYYY-MM-DD and HH:MM:SS
    let d: Vec<&str> = date.split('-').collect();
    let h: Vec<&str> = time.split(':').collect();
    if d.len() != 3
        || h.len() != 3
        || d[0].len() != 4
        || d[1].len() != 2
        || d[2].len() != 2
        || h.iter().any(|p| p.len() != 2)
    {
        return None;
    }
    Some(format!("{date}T{time}Z"))
}

#[cfg(test)]
mod tests {
    use super::normalize_utc;

    #[test]
    fn both_forms() {
        assert_eq!(
            normalize_utc("2026-05-31T23:59:55Z").as_deref(),
            Some("2026-05-31T23:59:55Z")
        );
        assert_eq!(
            normalize_utc("2026-05-31 23:59:55").as_deref(),
            Some("2026-05-31T23:59:55Z")
        );
        assert_eq!(normalize_utc("garbage"), None);
        assert_eq!(normalize_utc("2026-05-31"), None);
    }
}
