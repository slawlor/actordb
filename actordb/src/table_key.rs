//! Internal logical-table key and storage-partition encoding.

const TABLE_PREFIX_MARKER: &str = "\u{001e}actordb.table.v1:";
const TABLE_STORAGE_MARKER: &str = "\u{001d}actordb.table.store.v1:";

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

fn decode_hex(encoded: &str) -> Option<Vec<u8>> {
    let bytes = encoded.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Some((high << 4) | low)
        })
        .collect()
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn table_prefix(name: &str) -> String {
    format!("{TABLE_PREFIX_MARKER}{}:", hex(name.as_bytes()))
}

pub(crate) fn table_storage_prefix(name: &str) -> Vec<u8> {
    format!("{TABLE_STORAGE_MARKER}{}:", hex(name.as_bytes())).into_bytes()
}

pub(crate) fn encode_table_key(name: &str, key: &str) -> String {
    format!("{}{}", table_prefix(name), hex(key.as_bytes()))
}

pub(crate) fn encode_table_prefix(name: &str, prefix: &[u8]) -> Vec<u8> {
    format!("{}{}", table_prefix(name), hex(prefix)).into_bytes()
}

pub(crate) fn decode_table_key(name: &str, key: &str) -> Option<String> {
    let encoded = key.strip_prefix(&table_prefix(name))?;
    String::from_utf8(decode_hex(encoded)?).ok()
}

/// Return the table identity embedded in a logical key.
pub(crate) fn table_name(key: &str) -> Option<String> {
    let encoded = key.strip_prefix(TABLE_PREFIX_MARKER)?;
    let (table_hex, _) = encoded.split_once(':')?;
    String::from_utf8(decode_hex(table_hex)?).ok()
}

/// Strip the physical table-store partition prefix from a durable key.
pub(crate) fn strip_storage_prefix(key: &[u8]) -> Option<Vec<u8>> {
    if !key.starts_with(TABLE_STORAGE_MARKER.as_bytes()) {
        return None;
    }
    let remainder = &key[TABLE_STORAGE_MARKER.len()..];
    let colon = remainder.iter().position(|byte| *byte == b':')?;
    let table_hex = std::str::from_utf8(&remainder[..colon]).ok()?;
    decode_hex(table_hex)?;
    Some(remainder[colon + 1..].to_vec())
}

/// Hash table records by the unqualified user key so equal join keys
/// co-partition even though their physical storage namespaces differ.
pub(crate) fn routing_hash(key: &str) -> u64 {
    let bytes = if let Some(name) = table_name(key) {
        let prefix = table_prefix(&name);
        key.strip_prefix(&prefix)
            .and_then(decode_hex)
            .unwrap_or_else(|| key.as_bytes().to_vec())
    } else {
        key.as_bytes().to_vec()
    };
    seahash::hash(&bytes)
}
