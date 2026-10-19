-- The subset of the Wikipedia Citations Database schema the Postgres sink
-- writes to (docs/design/proposed_data_model.md in the WCD monorepo). The
-- authoritative DDL is wcd_schema.postgres; this copy exists so the sink can
-- be tested against a scratch database without the Python workspace.
CREATE TABLE domains (
    id                  SERIAL PRIMARY KEY,
    value               VARCHAR UNIQUE NOT NULL,
    top_level_domain    VARCHAR,
    parent_domain       INTEGER REFERENCES domains(id),
    for_container       INTEGER,
    internet_domains_id INTEGER UNIQUE
);
CREATE TABLE revision_bundles (
    id        SERIAL PRIMARY KEY,
    file_path VARCHAR UNIQUE NOT NULL
);
CREATE TABLE revisions (
    revision_id        BIGINT PRIMARY KEY,
    domain_id          INTEGER NOT NULL REFERENCES domains(id),
    page_id            INTEGER NOT NULL,
    parent_revision_id BIGINT,
    revision_timestamp TIMESTAMP NOT NULL,
    found_in_bundle    INTEGER REFERENCES revision_bundles(id),
    offset_begin       BIGINT,
    length             BIGINT
);
CREATE INDEX idx_revisions_page_ts    ON revisions (domain_id, page_id, revision_timestamp);
CREATE INDEX idx_revisions_page_revid ON revisions (domain_id, page_id, revision_id);
CREATE TABLE page_metadata (
    domain_id      INTEGER NOT NULL REFERENCES domains(id),
    page_id        INTEGER NOT NULL,
    page_title     VARCHAR,
    page_namespace INTEGER,
    PRIMARY KEY (domain_id, page_id)
);
CREATE TABLE web_resources (
    id                   SERIAL PRIMARY KEY,
    url                  VARCHAR,
    url_hash             CHAR(32) NOT NULL UNIQUE,
    instance_of_document INTEGER,
    availability_status  INTEGER,
    is_archive_of        INTEGER REFERENCES web_resources(id),
    domain_id            INTEGER REFERENCES domains(id),
    numeric_page_id      INTEGER,
    numeric_namespace_id INTEGER
);
