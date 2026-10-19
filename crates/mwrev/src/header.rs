//! The `# key=value …` header line.

use std::fmt;

/// The metadata line that precedes a revision's text.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RevisionHeader {
    /// Page ID.
    pub page_id: u64,
    /// Namespace number.
    pub ns: i64,
    /// Revision ID.
    pub rev_id: u64,
    /// Parent revision ID; written empty when `None`.
    pub parent_rev_id: Option<u64>,
    /// Timestamp as the source gave it (`2004-01-01T00:00:00Z`).
    pub timestamp: String,
    /// Contributor's user ID; `0` for anonymous or hidden contributors.
    pub user_id: u64,
}

/// Why a header line did not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HeaderError {
    /// The line does not start with `#`.
    #[error("not a header line: {0:?}")]
    NotHeader(String),
    /// A required key is missing or not a number.
    #[error("header field {0} missing or invalid in {1:?}")]
    Field(&'static str, String),
}

impl RevisionHeader {
    /// Formats the header line without a trailing newline.
    #[must_use]
    pub fn to_line(&self) -> String {
        self.to_string()
    }

    /// Parses a header line (with or without the trailing newline).
    pub fn parse(line: &str) -> Result<Self, HeaderError> {
        let line = line.trim_end_matches(['\n', '\r']);
        let Some(rest) = line.strip_prefix('#') else {
            return Err(HeaderError::NotHeader(line.to_string()));
        };
        let mut h = Self::default();
        let mut seen_page = false;
        let mut seen_rev = false;
        let mut seen_ts = false;
        for part in rest.split_whitespace() {
            let Some((k, v)) = part.split_once('=') else {
                continue;
            };
            match k {
                "page_id" => {
                    h.page_id = v
                        .parse()
                        .map_err(|_| HeaderError::Field("page_id", line.to_string()))?;
                    seen_page = true;
                }
                "ns" => h.ns = v.parse().unwrap_or(0),
                "rev_id" => {
                    h.rev_id = v
                        .parse()
                        .map_err(|_| HeaderError::Field("rev_id", line.to_string()))?;
                    seen_rev = true;
                }
                "parent_rev_id" => {
                    h.parent_rev_id = if v.is_empty() { None } else { v.parse().ok() }
                }
                "timestamp" => {
                    h.timestamp = v.to_string();
                    seen_ts = true;
                }
                "user_id" => h.user_id = v.parse().unwrap_or(0),
                _ => {}
            }
        }
        if !seen_page {
            return Err(HeaderError::Field("page_id", line.to_string()));
        }
        if !seen_rev {
            return Err(HeaderError::Field("rev_id", line.to_string()));
        }
        if !seen_ts {
            return Err(HeaderError::Field("timestamp", line.to_string()));
        }
        Ok(h)
    }
}

impl fmt::Display for RevisionHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "# page_id={} ns={} rev_id={} parent_rev_id={} timestamp={} user_id={}",
            self.page_id,
            self.ns,
            self.rev_id,
            self.parent_rev_id
                .map(|p| p.to_string())
                .unwrap_or_default(),
            self.timestamp,
            self.user_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let h = RevisionHeader {
            page_id: 101,
            ns: 0,
            rev_id: 1002,
            parent_rev_id: Some(1001),
            timestamp: "2005-06-15T12:00:00Z".into(),
            user_id: 0,
        };
        let line = h.to_line();
        assert_eq!(
            line,
            "# page_id=101 ns=0 rev_id=1002 parent_rev_id=1001 timestamp=2005-06-15T12:00:00Z user_id=0"
        );
        assert_eq!(RevisionHeader::parse(&line).unwrap(), h);
        assert_eq!(RevisionHeader::parse(&format!("{line}\n")).unwrap(), h);
    }

    #[test]
    fn empty_parent_is_none() {
        let h = RevisionHeader::parse(
            "# page_id=1 ns=0 rev_id=2 parent_rev_id= timestamp=2004-01-01T00:00:00Z user_id=7",
        )
        .unwrap();
        assert_eq!(h.parent_rev_id, None);
        assert_eq!(h.user_id, 7);
        assert_eq!(
            h.to_line(),
            "# page_id=1 ns=0 rev_id=2 parent_rev_id= timestamp=2004-01-01T00:00:00Z user_id=7"
        );
    }

    #[test]
    fn rejects_non_header_and_missing_fields() {
        assert!(matches!(
            RevisionHeader::parse(" text"),
            Err(HeaderError::NotHeader(_))
        ));
        assert!(matches!(
            RevisionHeader::parse("# ns=0 rev_id=2 timestamp=x"),
            Err(HeaderError::Field("page_id", _))
        ));
    }
}
