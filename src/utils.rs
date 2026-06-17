use chrono::{DateTime, NaiveDateTime, Utc};

pub fn sanitize_copy_text(s: &str) -> String {
    let mut sanitized = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '\t' {
            sanitized.push_str("\\t");
        } else if c == '\n' {
            sanitized.push_str("\\n");
        } else if c == '\r' {
            sanitized.push_str("\\r");
        } else if c == '\\' {
            sanitized.push_str("\\\\");
        } else if c == '\0' {
            // Null characters are not allowed in PostgreSQL text fields
            continue;
        } else {
            sanitized.push(c);
        }
    }
    sanitized
}

pub fn compute_url_hash(url: &str) -> String {
    let digest = md5::compute(url);
    format!("{:x}", digest)
}

pub fn parse_timestamp_utc(timestamp: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(timestamp)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            DateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M:%SZ")
                .map(|dt| dt.with_timezone(&Utc))
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S")
                .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
        })
}

pub fn normalize_timestamp_utc(timestamp: &str) -> Result<String, chrono::ParseError> {
    parse_timestamp_utc(timestamp).map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{normalize_timestamp_utc, parse_timestamp_utc};

    #[test]
    fn parses_db_style_timestamp_as_utc() {
        let parsed = parse_timestamp_utc("2026-05-31 23:59:55").unwrap();

        assert_eq!(parsed, Utc.with_ymd_and_hms(2026, 5, 31, 23, 59, 55).unwrap());
    }

    #[test]
    fn normalizes_supported_timestamp_formats() {
        let rfc3339 = normalize_timestamp_utc("2026-05-31T23:59:55Z").unwrap();
        let db_style = normalize_timestamp_utc("2026-05-31 23:59:55").unwrap();

        assert_eq!(rfc3339, "2026-05-31T23:59:55Z");
        assert_eq!(db_style, "2026-05-31T23:59:55Z");
    }
}
