//! The event reader.

use std::io::BufRead;

use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event as XmlEvent};

use crate::DumpError;

/// One namespace from `<siteinfo><namespaces>`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Namespace {
    /// The namespace number (`key` attribute).
    pub key: i64,
    /// The canonical name; empty for the main namespace.
    pub name: String,
}

/// The dump's `<siteinfo>`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SiteInfo {
    /// `<sitename>`.
    pub sitename: Option<String>,
    /// `<dbname>`, e.g. `enwiki`.
    pub dbname: Option<String>,
    /// `<base>`: the URL of the main page, from which the domain is taken.
    pub base: Option<String>,
    /// `<generator>`.
    pub generator: Option<String>,
    /// The `xml:lang` of the root element, when present.
    pub lang: Option<String>,
    /// The namespaces table.
    pub namespaces: Vec<Namespace>,
}

impl SiteInfo {
    /// The site's domain, taken from `<base>` (`https://en.wikipedia.org/wiki/Main_Page`
    /// → `en.wikipedia.org`).
    #[must_use]
    pub fn domain(&self) -> Option<String> {
        let base = self.base.as_deref()?;
        let rest = base.split_once("://")?.1;
        let host = rest.split(['/', '?', '#']).next()?;
        let host = host.rsplit('@').next()?; // userinfo, never present in practice
        let host = host.split(':').next()?;
        (!host.is_empty()).then(|| host.to_string())
    }
}

/// What a page carries besides its revisions.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PageHeader {
    /// The page ID (`<id>` directly under `<page>`).
    pub id: u64,
    /// The namespace number.
    pub ns: i64,
    /// The full title, namespace prefix included.
    pub title: String,
    /// The redirect target, when the page is a redirect.
    pub redirect: Option<String>,
}

/// One revision of a page.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Revision {
    /// The page this revision belongs to (repeated here so a consumer that
    /// only looks at revisions needs no state).
    pub page_id: u64,
    /// The page's namespace number.
    pub ns: i64,
    /// The revision ID.
    pub id: u64,
    /// The parent revision ID; `None` for a page's first revision.
    pub parent_id: Option<u64>,
    /// The timestamp exactly as the dump writes it (`2026-09-20T14:02:11Z`).
    pub timestamp: String,
    /// The contributor's user ID, for a registered user.
    pub user_id: Option<u64>,
    /// The contributor's user name, for a registered user.
    pub user_name: Option<String>,
    /// The contributor's IP address, for an anonymous edit.
    pub ip: Option<String>,
    /// The dump hid the contributor (`<contributor deleted="deleted" />`).
    pub user_hidden: bool,
    /// A minor edit.
    pub minor: bool,
    /// The edit summary; `None` when absent or hidden.
    pub comment: Option<String>,
    /// The content model (`wikitext`, `wikibase-item`, …).
    pub model: Option<String>,
    /// The content format (`text/x-wiki`, …).
    pub format: Option<String>,
    /// The revision text, verbatim (no trimming, entities decoded). Empty when
    /// the text is empty or hidden; see [`Revision::text_hidden`].
    pub text: String,
    /// The dump hid the text (`<text deleted="deleted" />`).
    pub text_hidden: bool,
    /// The `bytes` attribute of `<text>`, when present.
    pub text_bytes: Option<u64>,
    /// The dump's `<sha1>` of the text (MediaWiki's base-36 SHA-1).
    pub sha1: Option<String>,
}

/// What the reader yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The `<siteinfo>` block; comes first when the dump has one.
    SiteInfo(SiteInfo),
    /// A page begins. Its revisions follow.
    PageStart(PageHeader),
    /// One revision of the current page.
    Revision(Revision),
    /// The current page ends.
    PageEnd {
        /// The page ID, matching the preceding [`Event::PageStart`].
        id: u64,
    },
}

/// A streaming reader over a dump. Iterate it for [`Event`]s.
pub struct DumpReader<R: BufRead> {
    reader: Reader<R>,
    buf: Vec<u8>,
    /// Local names of the open elements, outermost first.
    path: Vec<String>,
    root_lang: Option<String>,
    siteinfo: Option<SiteInfo>,
    page: Option<PageHeader>,
    page_sent: bool,
    revision: Option<Revision>,
    /// Text accumulated for the innermost leaf element.
    text: String,
    /// A `PageEnd` queued behind a `PageStart` for a page with no revisions.
    pending_end: Option<Event>,
    done: bool,
}

impl<R: BufRead> DumpReader<R> {
    /// Wraps a decompressed XML stream.
    pub fn new(inner: R) -> Self {
        let mut reader = Reader::from_reader(inner);
        // Keep every byte of text: revision text may begin or end with whitespace.
        reader.config_mut().trim_text(false);
        reader.config_mut().expand_empty_elements = false;
        Self {
            reader,
            buf: Vec::with_capacity(64 * 1024),
            path: Vec::with_capacity(8),
            root_lang: None,
            siteinfo: None,
            page: None,
            page_sent: false,
            revision: None,
            text: String::new(),
            pending_end: None,
            done: false,
        }
    }

    fn xml_err(&self, source: quick_xml::Error) -> DumpError {
        DumpError::Xml {
            position: self.reader.buffer_position(),
            source,
        }
    }

    fn parent(&self) -> Option<&str> {
        self.path
            .len()
            .checked_sub(2)
            .map(|i| self.path[i].as_str())
    }

    fn attr(e: &BytesStart<'_>, name: &[u8]) -> Option<String> {
        e.attributes()
            .flatten()
            .find(|a| a.key.local_name().as_ref() == name)
            .and_then(|a| a.unescape_value().ok().map(|v| v.into_owned()))
    }

    /// Handles the start of an element (also called for empty elements).
    fn on_start(&mut self, e: &BytesStart<'_>, empty: bool) -> Result<Option<Event>, DumpError> {
        let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
        self.text.clear();
        let mut out = None;
        match name.as_str() {
            "mediawiki" if self.path.is_empty() => {
                self.root_lang = e
                    .attributes()
                    .flatten()
                    .find(|a| a.key.as_ref() == b"xml:lang")
                    .and_then(|a| a.unescape_value().ok().map(|v| v.into_owned()));
            }
            "siteinfo" => {
                self.siteinfo = Some(SiteInfo {
                    lang: self.root_lang.clone(),
                    ..SiteInfo::default()
                });
            }
            // `name` is not on the path yet, so the innermost open element is the parent.
            "namespace" if self.path.last().map(String::as_str) == Some("namespaces") => {
                if let Some(si) = self.siteinfo.as_mut() {
                    let key = Self::attr(e, b"key")
                        .and_then(|k| k.parse().ok())
                        .unwrap_or(0);
                    si.namespaces.push(Namespace {
                        key,
                        name: String::new(),
                    });
                }
            }
            "page" => {
                self.page = Some(PageHeader::default());
                self.page_sent = false;
            }
            "redirect" => {
                if let Some(p) = self.page.as_mut() {
                    p.redirect = Self::attr(e, b"title");
                }
            }
            "revision" => {
                // The page header is complete once its first revision starts.
                out = self.emit_page_start();
                let (page_id, ns) = self.page.as_ref().map_or((0, 0), |p| (p.id, p.ns));
                self.revision = Some(Revision {
                    page_id,
                    ns,
                    ..Revision::default()
                });
            }
            "contributor" => {
                if let Some(r) = self.revision.as_mut() {
                    if Self::attr(e, b"deleted").is_some() {
                        r.user_hidden = true;
                    }
                }
            }
            "minor" => {
                if let Some(r) = self.revision.as_mut() {
                    r.minor = true;
                }
            }
            "text" => {
                if let Some(r) = self.revision.as_mut() {
                    r.text_bytes = Self::attr(e, b"bytes").and_then(|b| b.parse().ok());
                    if Self::attr(e, b"deleted").is_some() {
                        r.text_hidden = true;
                    }
                }
            }
            "comment" => {
                if let Some(r) = self.revision.as_mut() {
                    if Self::attr(e, b"deleted").is_some() {
                        r.comment = None;
                    } else if empty {
                        r.comment = Some(String::new());
                    }
                }
            }
            _ => {}
        }
        if !empty {
            self.path.push(name);
        }
        Ok(out)
    }

    fn emit_page_start(&mut self) -> Option<Event> {
        if self.page_sent {
            return None;
        }
        let p = self.page.as_ref()?;
        self.page_sent = true;
        Some(Event::PageStart(p.clone()))
    }

    /// Handles the end of an element whose text has been accumulated.
    fn on_end(&mut self, name: &str) -> Option<Event> {
        let text = std::mem::take(&mut self.text);
        let parent = self.parent().map(str::to_owned);
        let in_revision = self.revision.is_some();
        let mut out = None;
        match (name, parent.as_deref()) {
            // siteinfo
            ("sitename", Some("siteinfo")) => self.si(|s| s.sitename = Some(text)),
            ("dbname", Some("siteinfo")) => self.si(|s| s.dbname = Some(text)),
            ("base", Some("siteinfo")) => self.si(|s| s.base = Some(text)),
            ("generator", Some("siteinfo")) => self.si(|s| s.generator = Some(text)),
            ("namespace", Some("namespaces")) => self.si(|s| {
                if let Some(ns) = s.namespaces.last_mut() {
                    ns.name = text;
                }
            }),
            ("siteinfo", _) => {
                if let Some(si) = self.siteinfo.take() {
                    out = Some(Event::SiteInfo(si));
                }
            }
            // page header
            ("title", Some("page")) => self.pg(|p| p.title = text),
            ("ns", Some("page")) => self.pg(|p| p.ns = text.trim().parse().unwrap_or(0)),
            ("id", Some("page")) => self.pg(|p| p.id = text.trim().parse().unwrap_or(0)),
            // revision
            ("id", Some("revision")) => self.rv(|r| r.id = text.trim().parse().unwrap_or(0)),
            ("parentid", Some("revision")) => self.rv(|r| r.parent_id = text.trim().parse().ok()),
            ("timestamp", Some("revision")) => self.rv(|r| r.timestamp = text.trim().to_string()),
            ("comment", Some("revision")) => self.rv(|r| r.comment = Some(text)),
            ("model", Some("revision")) => self.rv(|r| r.model = Some(text.trim().to_string())),
            ("format", Some("revision")) => self.rv(|r| r.format = Some(text.trim().to_string())),
            ("sha1", Some("revision")) => self.rv(|r| r.sha1 = Some(text.trim().to_string())),
            ("text", Some("revision")) => self.rv(|r| r.text = text),
            ("username", Some("contributor")) if in_revision => {
                self.rv(|r| r.user_name = Some(text))
            }
            ("id", Some("contributor")) if in_revision => {
                self.rv(|r| r.user_id = text.trim().parse().ok())
            }
            ("ip", Some("contributor")) if in_revision => {
                self.rv(|r| r.ip = Some(text.trim().to_string()))
            }
            ("revision", _) => {
                if let Some(r) = self.revision.take() {
                    out = Some(Event::Revision(r));
                }
            }
            ("page", _) => {
                // A page with no revisions still gets its start event.
                let start = self.emit_page_start();
                if let Some(p) = self.page.take() {
                    out = Some(match start {
                        // Both events are due; keep the start and queue the end.
                        Some(s) => {
                            self.pending_end = Some(Event::PageEnd { id: p.id });
                            s
                        }
                        None => Event::PageEnd { id: p.id },
                    });
                }
            }
            _ => {}
        }
        out
    }

    fn si(&mut self, f: impl FnOnce(&mut SiteInfo)) {
        if let Some(s) = self.siteinfo.as_mut() {
            f(s);
        }
    }
    fn pg(&mut self, f: impl FnOnce(&mut PageHeader)) {
        if let Some(p) = self.page.as_mut() {
            f(p);
        }
    }
    fn rv(&mut self, f: impl FnOnce(&mut Revision)) {
        if let Some(r) = self.revision.as_mut() {
            f(r);
        }
    }
}

impl<R: BufRead> DumpReader<R> {
    fn next_event(&mut self) -> Result<Option<Event>, DumpError> {
        if let Some(e) = self.pending_end.take() {
            return Ok(Some(e));
        }
        loop {
            self.buf.clear();
            // Owned so the event does not keep `self.buf` borrowed while we mutate state.
            let ev = match self.reader.read_event_into(&mut self.buf) {
                Ok(ev) => ev.into_owned(),
                Err(e) => return Err(self.xml_err(e)),
            };
            match ev {
                XmlEvent::Start(e) => {
                    if let Some(out) = self.on_start(&e, false)? {
                        return Ok(Some(out));
                    }
                }
                XmlEvent::Empty(e) => {
                    let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                    let out = self.on_start(&e, true)?;
                    // An empty element also "ends": `<text … />`, `<namespace key="0" />`.
                    self.path.push(name.clone());
                    let end = self.on_end(&name);
                    self.path.pop();
                    if let Some(out) = out.or(end) {
                        return Ok(Some(out));
                    }
                }
                XmlEvent::End(_) => {
                    let Some(name) = self.path.pop() else {
                        continue;
                    };
                    // on_end needs the element still on the path to find its parent.
                    self.path.push(name.clone());
                    let out = self.on_end(&name);
                    self.path.pop();
                    if let Some(out) = out {
                        return Ok(Some(out));
                    }
                }
                XmlEvent::Text(e) => {
                    if self.wants_text() {
                        let t = match e.unescape() {
                            Ok(t) => t.into_owned(),
                            Err(err) => return Err(self.xml_err(err)),
                        };
                        self.text.push_str(&t);
                    }
                }
                XmlEvent::CData(e) => {
                    if self.wants_text() {
                        self.text.push_str(&String::from_utf8_lossy(&e));
                    }
                }
                XmlEvent::Eof => {
                    self.done = true;
                    // quick-xml reports a truncated document as a plain end of
                    // input; an element still open at EOF means the dump was cut.
                    if let Some(open) = self.path.last() {
                        return Err(DumpError::Malformed(format!(
                            "unexpected end of input inside <{open}> at byte {}",
                            self.reader.buffer_position()
                        )));
                    }
                    return Ok(None);
                }
                XmlEvent::Decl(_)
                | XmlEvent::PI(_)
                | XmlEvent::Comment(_)
                | XmlEvent::DocType(_) => {}
            }
        }
    }

    /// Only leaf elements we read get their text accumulated; this keeps the
    /// indentation between elements out of the buffers.
    fn wants_text(&self) -> bool {
        matches!(
            self.path.last().map(String::as_str),
            Some(
                "sitename"
                    | "dbname"
                    | "base"
                    | "generator"
                    | "namespace"
                    | "title"
                    | "ns"
                    | "id"
                    | "parentid"
                    | "timestamp"
                    | "comment"
                    | "model"
                    | "format"
                    | "sha1"
                    | "text"
                    | "username"
                    | "ip"
            )
        )
    }
}

impl<R: BufRead> Iterator for DumpReader<R> {
    type Item = Result<Event, DumpError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.next_event() {
            Ok(Some(e)) => Some(Ok(e)),
            Ok(None) => None,
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}
