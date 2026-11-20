//! A page at a time: the reader's events collected into whole pages, for
//! callers that work on a page with all its revisions (an import, a talk
//! page's history, an export). A page holds every revision it has in memory,
//! so for a full-history dump of a large wiki the event stream
//! ([`DumpReader`](crate::DumpReader)) is the better fit.

use std::io::BufRead;

use crate::DumpError;
use crate::reader::{DumpReader, Event, PageHeader, Revision, SiteInfo};

/// A page with its revisions, in dump order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Page {
    /// The page's ID, namespace, title and redirect.
    pub header: PageHeader,
    /// Its revisions, in dump order.
    pub revisions: Vec<Revision>,
}

impl Page {
    /// A page with these revisions.
    #[must_use]
    pub fn new(header: PageHeader, revisions: Vec<Revision>) -> Self {
        Self { header, revisions }
    }

    /// The newest revision: the highest revision ID.
    #[must_use]
    pub fn latest(&self) -> Option<&Revision> {
        self.revisions.iter().max_by_key(|r| r.id)
    }
}

/// The pages of a dump, from [`DumpReader::pages`]. The `<siteinfo>` block,
/// which comes before the first page, is kept for [`Pages::site_info`].
pub struct Pages<R: BufRead> {
    events: DumpReader<R>,
    site: Option<SiteInfo>,
}

impl<R: BufRead> Pages<R> {
    /// The dump's `<siteinfo>`, once it has been read (it comes before the
    /// first page, so after the first call to `next`).
    #[must_use]
    pub fn site_info(&self) -> Option<&SiteInfo> {
        self.site.as_ref()
    }
}

impl<R: BufRead> DumpReader<R> {
    /// The dump a page at a time, each page with all its revisions.
    #[must_use]
    pub fn pages(self) -> Pages<R> {
        Pages {
            events: self,
            site: None,
        }
    }
}

impl<R: BufRead> Iterator for Pages<R> {
    type Item = Result<Page, DumpError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut page: Option<Page> = None;
        loop {
            match self.events.next()? {
                Err(e) => return Some(Err(e)),
                Ok(Event::SiteInfo(si)) => self.site = Some(si),
                Ok(Event::PageStart(header)) => {
                    page = Some(Page {
                        header,
                        revisions: Vec::new(),
                    });
                }
                Ok(Event::Revision(r)) => {
                    if let Some(p) = page.as_mut() {
                        p.revisions.push(r);
                    }
                }
                Ok(Event::PageEnd { .. }) => {
                    if let Some(p) = page.take() {
                        return Some(Ok(p));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMP: &str = r#"<mediawiki xml:lang="en">
  <siteinfo><sitename>Example</sitename></siteinfo>
  <page><title>A</title><ns>0</ns><id>1</id>
    <revision><id>10</id><timestamp>2026-01-01T00:00:00Z</timestamp><text>one</text></revision>
    <revision><id>12</id><parentid>10</parentid><timestamp>2026-01-02T00:00:00Z</timestamp><text>two</text></revision>
  </page>
  <page><title>B</title><ns>4</ns><id>2</id><redirect title="A" /></page>
</mediawiki>
"#;

    #[test]
    fn pages_hold_their_revisions_and_the_site_info_is_kept() {
        let mut pages = DumpReader::new(DUMP.as_bytes()).pages();
        let a = pages.next().unwrap().unwrap();
        assert_eq!(
            pages.site_info().unwrap().sitename.as_deref(),
            Some("Example")
        );
        assert_eq!((a.header.id, a.header.title.as_str()), (1, "A"));
        assert_eq!(
            a.revisions.iter().map(|r| r.id).collect::<Vec<_>>(),
            [10, 12]
        );
        assert_eq!(a.latest().unwrap().text, "two");
        let b = pages.next().unwrap().unwrap();
        assert_eq!(b.header.redirect.as_deref(), Some("A"));
        assert!(
            b.revisions.is_empty(),
            "a page with no revisions is still a page"
        );
        assert!(pages.next().is_none());
    }

    #[test]
    fn a_cut_dump_ends_in_an_error() {
        let cut = &DUMP[..DUMP.find("</page>").unwrap() + 7];
        let mut pages = DumpReader::new(cut.as_bytes()).pages();
        assert_eq!(pages.next().unwrap().unwrap().header.title, "A");
        assert!(pages.next().unwrap().is_err());
    }

    #[test]
    fn a_namespace_filter_applies() {
        let pages: Vec<Page> = DumpReader::new(DUMP.as_bytes())
            .with_namespaces(&[4])
            .pages()
            .map(Result::unwrap)
            .collect();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].header.title, "B");
    }
}
