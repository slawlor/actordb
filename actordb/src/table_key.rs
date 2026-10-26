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

pub(crate) fn decode_table_prefix(name: &str, prefix: &[u8]) -> Option<Vec<u8>> {
    let namespace = table_prefix(name);
    let encoded = std::str::from_utf8(prefix).ok()?.strip_prefix(&namespace)?;
    decode_hex(encoded)
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

/// Validate a reserved physical table namespace and borrow the exact logical
/// replicated key. Durable namespace bytes are not part of the record's wire
/// budget. A mismatched/invalid namespace must never be stripped permissively.
/// Plain fragment-local UTF-8 keys retain the low-level store contract.
pub(crate) fn logical_record_key(physical: &str) -> Result<&str, &'static str> {
    let Some(remainder) = physical.strip_prefix(TABLE_STORAGE_MARKER) else {
        return Ok(physical);
    };
    let (stored_table, logical) = remainder
        .split_once(':')
        .ok_or("invalid physical table namespace")?;
    let remainder = logical
        .strip_prefix(TABLE_PREFIX_MARKER)
        .ok_or("physical table record lacks logical namespace")?;
    let (logical_table, user_key) = remainder
        .split_once(':')
        .ok_or("invalid logical table namespace")?;
    if stored_table.is_empty()
        || stored_table != logical_table
        || !valid_hex_utf8(stored_table, true)
        || !valid_hex_utf8(user_key, false)
    {
        return Err("physical/logical table identities or encoded key disagree");
    }
    Ok(logical)
}

// Validate hex-encoded UTF-8 without allocating the entire decoded user key.
// Table identifiers use the canonical lower-case representation; user-key hex
// keeps the current case-insensitive decoder contract.
fn valid_hex_utf8(encoded: &str, canonical: bool) -> bool {
    let encoded = encoded.as_bytes();
    if !encoded.len().is_multiple_of(2) {
        return false;
    }
    let mut position = 0;
    while position < encoded.len() {
        let Some(first) = decoded_pair(&encoded[position..position + 2], canonical) else {
            return false;
        };
        let width = match first {
            0..=0x7f => 1,
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => return false,
        };
        let Some(end) = position
            .checked_add(width * 2)
            .filter(|end| *end <= encoded.len())
        else {
            return false;
        };
        let mut bytes = [0; 4];
        for (index, pair) in encoded[position..end].chunks_exact(2).enumerate() {
            let Some(byte) = decoded_pair(pair, canonical) else {
                return false;
            };
            bytes[index] = byte;
        }
        if std::str::from_utf8(&bytes[..width]).is_err() {
            return false;
        }
        position = end;
    }
    true
}
fn decoded_pair(pair: &[u8], canonical: bool) -> Option<u8> {
    if canonical
        && pair
            .iter()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(byte))
    {
        return None;
    }
    Some(hex_digit(pair[0])? << 4 | hex_digit(pair[1])?)
}

/// Hash table records by the unqualified user key so equal join keys
/// co-partition even though their physical storage namespaces differ.
pub(crate) fn routing_hash(key: &str) -> u64 {
    // Durable table stores add a binary-safe physical prefix after the
    // fragment prefix. RocksDB validates that physical key by asking for its
    // routing fragment, so hash the logical table key rather than the storage
    // partition marker.
    let key = key
        .strip_prefix(TABLE_STORAGE_MARKER)
        .and_then(|remainder| remainder.split_once(':').map(|(_, logical)| logical))
        .unwrap_or(key);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_name_round_trips_encoded_key() {
        let key = encode_table_key("KV", "ephemeral");
        assert_eq!(table_name(&key).as_deref(), Some("KV"));
    }

    #[test]
    fn durable_table_key_routes_like_its_logical_key() {
        let logical = encode_table_key("KV", "durable");
        let physical = format!(
            "{}{}",
            String::from_utf8(table_storage_prefix("KV")).unwrap(),
            logical
        );
        assert_eq!(routing_hash(&logical), routing_hash(&physical));
    }

    #[test]
    fn physical_namespace_validation_borrows_matching_logical_key_without_relaxing_identity() {
        for (table, key) in [("KV", ""), ("USERS", "snow:雪\0"), ("é", "😀")] {
            let logical = encode_table_key(table, key);
            let physical = format!(
                "{}{}",
                String::from_utf8(table_storage_prefix(table)).unwrap(),
                logical
            );
            assert_eq!(logical_record_key(&physical).unwrap(), logical);
        }
        assert_eq!(logical_record_key("plain key").unwrap(), "plain key");
        for physical in [
            format!(
                "{}{}",
                String::from_utf8(table_storage_prefix("A")).unwrap(),
                encode_table_key("B", "k")
            ),
            format!(
                "{}plain",
                String::from_utf8(table_storage_prefix("A")).unwrap()
            ),
            format!(
                "{}{}ff",
                String::from_utf8(table_storage_prefix("A")).unwrap(),
                table_prefix("A")
            ),
            format!(
                "{}{}f",
                String::from_utf8(table_storage_prefix("A")).unwrap(),
                table_prefix("A")
            ),
            format!("{TABLE_STORAGE_MARKER}4B56:{}", encode_table_key("KV", "k")),
        ] {
            assert!(logical_record_key(&physical).is_err(), "{physical:?}");
        }
        assert!(valid_hex_utf8("e99baa", true));
        assert!(valid_hex_utf8("E99BAA", false));
        for hex in ["c080", "eda080", "f4908080", "ff", "e9", "0g"] {
            assert!(!valid_hex_utf8(hex, false), "{hex}");
        }
    }
}
