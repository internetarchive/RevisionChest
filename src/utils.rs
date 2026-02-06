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
