#[derive(Debug, Clone)]
pub enum DbMessage {
    SiteInfo {
        domain: String,
    },
    Page {
        id: u64,
        ns: i32,
        title: String,
    },
    Revision {
        rev_id: u64,
        parent_rev_id: Option<u64>,
        page_id: u64,
        file_path: String,
        offset_begin: u64,
        offset_end: u64,
        timestamp: String,
    },
    Finalize,
}
