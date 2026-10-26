use super::*;

#[test]
fn projection_value_contract_covers_metadata_ranges_and_nullable_aggregates() {
    let unsigned = [
        Projection::TimestampMs,
        Projection::LogicalCounter,
        Projection::OctetLength,
    ];
    for projection in unsigned {
        for value in [0, u64::MAX as i128] {
            assert!(projection.accepts_value(&QueryValue::Integer(value)));
        }
        for value in [-1, u64::MAX as i128 + 1] {
            assert!(!projection.accepts_value(&QueryValue::Integer(value)));
        }
        assert!(!projection.accepts_value(&QueryValue::Null));
        assert!(!projection.accepts_value(&QueryValue::Text("1".into())));
    }
    assert!(Projection::NodeId.accepts_value(&QueryValue::Text("writer".into())));
    assert!(!Projection::NodeId.accepts_value(&QueryValue::Bytes(b"writer".to_vec())));
    assert!(Projection::ExpiresAtMs.accepts_value(&QueryValue::Null));
    assert!(Projection::ExpiresAtMs.accepts_value(&QueryValue::Integer(u64::MAX as i128)));
    assert!(!Projection::ExpiresAtMs.accepts_value(&QueryValue::Integer(-1)));
    assert!(!Projection::ExpiresAtMs.accepts_value(&QueryValue::Integer(u64::MAX as i128 + 1)));
    for value in [0, 1] {
        assert!(Projection::Tombstone.accepts_value(&QueryValue::Integer(value)));
    }
    for value in [-1, 2] {
        assert!(!Projection::Tombstone.accepts_value(&QueryValue::Integer(value)));
    }
    for projection in [
        Projection::Aggregate {
            function: AggregateFunction::Sum,
            input: AggregateInput::OctetLength,
        },
        Projection::Aggregate {
            function: AggregateFunction::Min,
            input: AggregateInput::BigInt,
        },
        Projection::Aggregate {
            function: AggregateFunction::Max,
            input: AggregateInput::BigInt,
        },
        Projection::Aggregate {
            function: AggregateFunction::Sum,
            input: AggregateInput::BigInt,
        },
    ] {
        assert!(projection.accepts_value(&QueryValue::Null));
    }
    assert!(!Projection::Aggregate {
        function: AggregateFunction::Count,
        input: AggregateInput::All
    }
    .accepts_value(&QueryValue::Null));
    // Every output of the portable projector obeys the same contract used by
    // companion SDKs, including maximal metadata and nullable expiry.
    let plan = parse("SELECT key, value, frag, timestamp_ms, logical_counter, node_id, expires_at_ms, is_tombstone, PREFIX_BYTES(value, 2), OCTET_LENGTH(value) FROM kv").unwrap();
    for expiry in [None, Some(u64::MAX)] {
        let values = plan
            .project_with_metadata(
                "key",
                b"opaque",
                u32::MAX,
                &RecordMetadata {
                    timestamp_ms: u64::MAX,
                    logical_counter: u64::MAX,
                    node_id: "writer".into(),
                    expires_at_ms: expiry,
                    is_tombstone: true,
                },
            )
            .unwrap();
        assert!(plan
            .projections
            .iter()
            .zip(values)
            .all(|(projection, value)| projection.accepts_value(&value)));
    }
}

#[test]
fn parameters_are_typed_literals_and_fragment_scopes_intersect() {
    let plan = parse_with_params(
        "select key, value, frag, PREFIX_BYTES(value, ?), OCTET_LENGTH(value) from kv \
         where STARTS_WITH(key, ?) AND key >= ? AND frag IN (3, 1, 3) AND frag = ? \
         order by key asc limit ? offset ?;",
        &[
            QueryValue::Integer(2),
            QueryValue::Bytes(b"user:".to_vec()),
            QueryValue::Text("user:2".into()),
            QueryValue::Integer(3),
            QueryValue::Integer(5),
            QueryValue::Integer(1),
        ],
    )
    .unwrap();
    assert_eq!(plan.fragments, Some(vec![3]));
    assert_eq!(plan.limit, Some(5));
    assert_eq!(plan.offset, 1);
    assert!(plan.matches_key("user:3"));
    assert!(!plan.matches_key("user:1"));
    assert!(!plan.matches_key("other:3"));
    assert_eq!(
        plan.project("user:3", &[0, 255, 17], 3).unwrap(),
        vec![
            QueryValue::Text("user:3".into()),
            QueryValue::Bytes(vec![0, 255, 17]),
            QueryValue::Integer(3),
            QueryValue::Bytes(vec![0, 255]),
            QueryValue::Integer(3),
        ]
    );
    assert_eq!(parse(&plan.to_sql()).unwrap(), plan);
}

#[test]
fn durable_metadata_projects_without_decoding_application_bytes() {
    let plan =
        parse("SELECT timestamp_ms, logical_counter, node_id, expires_at_ms, is_tombstone FROM kv")
            .unwrap();
    let metadata = RecordMetadata {
        timestamp_ms: 42,
        logical_counter: 7,
        node_id: "writer-a".into(),
        expires_at_ms: Some(99),
        is_tombstone: false,
    };
    assert_eq!(
        plan.project_with_metadata("k", b"arbitrary bytes", 0, &metadata)
            .unwrap(),
        vec![
            QueryValue::Integer(42),
            QueryValue::Integer(7),
            QueryValue::Text("writer-a".into()),
            QueryValue::Integer(99),
            QueryValue::Integer(0),
        ]
    );
    assert_eq!(parse(&plan.to_sql()).unwrap(), plan);
}

#[test]
fn including_tombstones_and_metadata_predicates_round_trip() {
    let plan = parse(
        "SELECT key, is_tombstone FROM kv INCLUDING TOMBSTONES \
         WHERE timestamp_ms >= 10 AND node_id = 'writer-a' \
         AND expires_at_ms IS NULL AND is_tombstone = 1",
    )
    .unwrap();
    assert!(plan.include_tombstones);
    assert_eq!(plan.metadata_predicates.len(), 4);
    assert_eq!(parse(&plan.to_sql()).unwrap(), plan);
    assert!(plan.matches_metadata(&RecordMetadata {
        timestamp_ms: 10,
        node_id: "writer-a".into(),
        is_tombstone: true,
        ..Default::default()
    }));
    assert!(!plan.matches_metadata(&RecordMetadata {
        timestamp_ms: 10,
        node_id: "writer-a".into(),
        is_tombstone: false,
        ..Default::default()
    }));
}

#[test]
fn literal_escaping_preserves_unicode_empty_keys_and_injection_text() {
    let key = "雪'; DELETE FROM kv; --";
    let plan = parse_with_params(
        "SELECT key FROM kv WHERE key = ?",
        &[QueryValue::Text(key.into())],
    )
    .unwrap();
    assert_eq!(plan.exact_key(), Some(key));
    assert_eq!(parse(&plan.to_sql()).unwrap(), plan);
    let empty = parse("SELECT key FROM kv WHERE key = ''").unwrap();
    assert!(empty.matches_key(""));
    assert!(!empty.matches_key("x"));
    let unicode = parse("SELECT key FROM kv WHERE STARTS_WITH(key, '雪''人')").unwrap();
    assert!(unicode.matches_key("雪'人:1"));
    assert_eq!(parse(&unicode.to_sql()).unwrap(), unicode);
}

#[test]
fn binary_prefixes_round_trip_and_do_not_require_utf8() {
    let plan = parse("SELECT key FROM kv WHERE STARTS_WITH(key, X'e99b')").unwrap();
    assert!(plan.matches_key("雪"));
    assert_eq!(parse(&plan.to_sql()).unwrap(), plan);
    let impossible = parse("SELECT key FROM kv WHERE STARTS_WITH(key, X'ff')").unwrap();
    assert!(!impossible.matches_key("snow"));
    for sql in [
        "SELECT key FROM kv WHERE STARTS_WITH(key, X'0')",
        "SELECT key FROM kv WHERE STARTS_WITH(key, X'gg')",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn global_pagination_metadata_and_empty_scope_are_explicit() {
    let default = parse("SELECT key FROM kv").unwrap();
    assert_eq!(default.limit, None, "no hidden truncation default");
    let zero = parse("SELECT key FROM kv LIMIT 0 OFFSET 0").unwrap();
    assert_eq!(zero.limit, Some(0));
    let none = parse("SELECT COUNT(*) FROM kv WHERE frag = 1 AND frag = 2").unwrap();
    assert_eq!(none.fragments, Some(vec![]));
    assert_eq!(parse(&none.to_sql()).unwrap(), none);
    let mut accumulator = QueryAccumulator::new(&none);
    accumulator.push("key", b"value", 1).unwrap();
    assert_eq!(
        accumulator.finish().rows,
        vec![vec![QueryValue::Integer(0)]]
    );
}

#[test]
fn key_range_comparisons_are_conjunctive_and_lexicographic() {
    let plan =
        parse("SELECT key FROM kv WHERE key >= 'b' AND key < 'd' AND key > 'ba' AND key <= 'c'")
            .unwrap();
    for (key, matches) in [
        ("a", false),
        ("b", false),
        ("ba", false),
        ("bb", true),
        ("c", true),
        ("d", false),
    ] {
        assert_eq!(plan.matches_key(key), matches, "{key}");
    }
    assert_eq!(parse(&plan.to_sql()).unwrap(), plan);
}

#[test]
fn unsupported_sql_is_rejected_in_full() {
    for sql in [
        "",
        "SELECT * FROM kv",
        "SELECT key FROM kv; SELECT value FROM kv",
        "SELECT key FROM kv UNION SELECT key FROM kv",
        "SELECT key FROM kv JOIN other ON key = key",
        "SELECT key FROM (SELECT key FROM kv)",
        "SELECT COUNT(*) FROM kv GROUP BY key",
        "SELECT key FROM kv WHERE key = 'a' OR key = 'b'",
        "SELECT key FROM kv ORDER BY key DESC",
        "SELECT key FROM kv ORDER BY value",
        "SELECT SUM(value) FROM kv",
        "SELECT AVG(OCTET_LENGTH(value)) FROM kv",
        "SELECT key, COUNT(*) FROM kv",
        "SELECT COUNT(*) FROM kv LIMIT 0",
        "SELECT COUNT(*) FROM kv OFFSET 0",
        "SELECT COUNT(*) FROM kv ORDER BY key",
        "SELECT key AS name FROM kv",
        "SELECT key FROM kv -- comment",
        "SELECT key FROM kv WHERE key = 'unterminated",
        "SELECT key FROM kv WHERE frag IN (1,)",
        "SELECT key FROM kv LIMIT 1.2",
        "SELECT key FROM kv LIMIT -1",
        "SELECT key FROM kv WHERE value = 'x'",
        "SELECT key FROM kv WHERE STARTS_WITH(key, 'x') = true",
        "SELECT COUNT(value) FROM kv",
        "SELECT SUM(CAST(value AS INTEGER)) FROM kv",
        "SELECT key FROM kv WHERE (key = 'a')",
    ] {
        assert!(parse(sql).is_err(), "unexpectedly accepted {sql}");
    }
}

#[test]
fn parameters_reject_missing_extra_and_incompatible_types() {
    assert!(parse("SELECT key FROM kv WHERE key = ?").is_err());
    assert!(parse_with_params("SELECT key FROM kv", &[QueryValue::Integer(1)]).is_err());
    for value in [
        QueryValue::Null,
        QueryValue::Bytes(vec![1]),
        QueryValue::Integer(1),
    ] {
        assert!(parse_with_params("SELECT key FROM kv WHERE key = ?", &[value]).is_err());
    }
    assert!(parse_with_params(
        "SELECT key FROM kv LIMIT ?",
        &[QueryValue::Text("1".into())]
    )
    .is_err());
    assert!(parse_with_params(
        "SELECT key FROM kv WHERE frag = ?",
        &[QueryValue::Integer(i128::from(u32::MAX) + 1)]
    )
    .is_err());
}

#[test]
fn input_and_plan_resources_have_hard_bounds() {
    let oversized = " ".repeat(MAX_SQL_BYTES + 1);
    assert!(matches!(
        parse(&oversized),
        Err(QueryError::ResourceLimit {
            resource: "SQL bytes",
            ..
        })
    ));
    let tokens = format!("SELECT key FROM kv {}", "AND ".repeat(MAX_QUERY_TOKENS));
    assert!(matches!(
        parse(&tokens),
        Err(QueryError::ResourceLimit {
            resource: "tokens",
            ..
        })
    ));
    let projections = format!(
        "SELECT {} FROM kv",
        vec!["key"; MAX_QUERY_EXPRESSIONS + 1].join(",")
    );
    assert!(parse(&projections).is_err());
    let literal = QueryValue::Text("x".repeat(MAX_QUERY_LITERAL_BYTES + 1));
    assert!(matches!(
        parse_with_params("SELECT key FROM kv WHERE key = ?", &[literal]),
        Err(QueryError::ResourceLimit { .. })
    ));
    assert!(parse(&format!("SELECT key FROM kv LIMIT {}", MAX_QUERY_LIMIT + 1)).is_err());
    assert!(parse(&format!(
        "SELECT key FROM kv OFFSET {}",
        MAX_QUERY_OFFSET + 1
    ))
    .is_err());
    // The parser has no recursive expressions: deep nesting fails immediately.
    assert!(parse(&format!(
        "SELECT {}key{} FROM kv",
        "(".repeat(1000),
        ")".repeat(1000)
    ))
    .is_err());
}

#[test]
fn numeric_and_byte_aggregates_merge_exactly_across_pages() {
    let plan = parse("SELECT COUNT(*), SUM(OCTET_LENGTH(value)), MIN(CAST(value AS BIGINT)), MAX(CAST(value AS BIGINT)), SUM(CAST(value AS BIGINT)) FROM kv WHERE STARTS_WITH(key, 'n:') AND frag IN (1, 2)").unwrap();
    let mut first = QueryAccumulator::new(&plan);
    first.push("n:1", b"-3", 1).unwrap();
    first.push("ignored", b"bad", 1).unwrap();
    first.push("n:outside", b"bad", 3).unwrap();
    let mut second = QueryAccumulator::new(&plan);
    second.push("n:2", b"12", 2).unwrap();
    second.push("n:3", b"0", 2).unwrap();
    let mut merged = QueryAccumulator::new(&plan);
    merged.merge(&first.finish()).unwrap();
    merged.merge(&second.finish()).unwrap();
    assert_eq!(
        merged.finish().rows,
        vec![vec![
            QueryValue::Integer(3),
            QueryValue::Integer(5),
            QueryValue::Integer(-3),
            QueryValue::Integer(12),
            QueryValue::Integer(9),
        ]]
    );
}

#[test]
fn empty_aggregate_partials_preserve_sql_null_semantics() {
    let plan = parse("SELECT COUNT(*), SUM(OCTET_LENGTH(value)), MIN(OCTET_LENGTH(value)), MAX(OCTET_LENGTH(value)) FROM kv").unwrap();
    let empty = QueryAccumulator::new(&plan).finish();
    assert_eq!(
        empty.rows,
        vec![vec![
            QueryValue::Integer(0),
            QueryValue::Null,
            QueryValue::Null,
            QueryValue::Null
        ]]
    );
    let mut merged = QueryAccumulator::new(&plan);
    merged.merge(&empty).unwrap();
    merged.push("empty", b"", 0).unwrap();
    merged.merge(&empty).unwrap();
    assert_eq!(
        merged.finish().rows,
        vec![vec![
            QueryValue::Integer(1),
            QueryValue::Integer(0),
            QueryValue::Integer(0),
            QueryValue::Integer(0)
        ]]
    );
}

#[test]
fn bigint_conversion_is_strict_and_failed_pushes_are_atomic() {
    let plan = parse("SELECT COUNT(*), SUM(CAST(value AS BIGINT)) FROM kv").unwrap();
    let mut accumulator = QueryAccumulator::new(&plan);
    for value in [
        b"".as_slice(),
        b" 1",
        b"1 ",
        b"1.0",
        b"true",
        b"--1",
        b"+",
        b"9223372036854775808",
        b"-9223372036854775809",
        &[255],
    ] {
        assert!(
            matches!(
                accumulator.push("key", value, 0),
                Err(QueryError::InvalidValue(_))
            ),
            "{value:?}"
        );
    }
    accumulator.push("a", b"-9223372036854775808", 0).unwrap();
    accumulator.push("b", b"+9223372036854775807", 0).unwrap();
    assert_eq!(
        accumulator.finish().rows,
        vec![vec![QueryValue::Integer(2), QueryValue::Integer(-1)]]
    );
}

#[test]
fn malformed_or_overflowing_partials_are_rejected_atomically() {
    let plan = parse("SELECT COUNT(*), SUM(CAST(value AS BIGINT)) FROM kv").unwrap();
    let mut accumulator = QueryAccumulator::new(&plan);
    let partial = |count, sum| QueryResult {
        columns: plan.columns(),
        rows: vec![vec![count, sum]],
    };
    for bad in [
        partial(QueryValue::Null, QueryValue::Integer(0)),
        partial(QueryValue::Integer(-1), QueryValue::Integer(0)),
        partial(QueryValue::Integer(1), QueryValue::Text("1".into())),
        QueryResult {
            columns: vec!["wrong".into()],
            rows: vec![],
        },
    ] {
        assert!(accumulator.merge(&bad).is_err());
    }
    accumulator
        .merge(&partial(
            QueryValue::Integer(1),
            QueryValue::Integer(i128::MAX),
        ))
        .unwrap();
    assert_eq!(
        accumulator.merge(&partial(QueryValue::Integer(1), QueryValue::Integer(1))),
        Err(QueryError::Overflow)
    );
    assert_eq!(
        accumulator.finish().rows,
        vec![vec![QueryValue::Integer(1), QueryValue::Integer(i128::MAX)]]
    );
    let count = parse("SELECT COUNT(*) FROM kv").unwrap();
    let mut counter = QueryAccumulator::new(&count);
    counter
        .merge(&QueryResult {
            columns: count.columns(),
            rows: vec![vec![QueryValue::Integer(i128::from(u64::MAX))]],
        })
        .unwrap();
    assert_eq!(counter.push("key", b"", 0), Err(QueryError::Overflow));
}

#[test]
fn invalid_programmatic_aggregate_shapes_fail_validation() {
    let mut plan = parse("SELECT COUNT(*) FROM kv").unwrap();
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Sum,
        input: AggregateInput::All,
    }];
    assert!(plan.validate().is_err());
    assert!(QueryAccumulator::new(&plan).push("key", b"", 0).is_err());
}

#[test]
fn bound_plans_must_fit_the_portable_canonical_sql_limit() {
    let bound = vec![QueryValue::Text("'".repeat(MAX_QUERY_LITERAL_BYTES)); 3];
    assert!(matches!(
        parse_with_params(
            "SELECT key FROM kv WHERE key = ? AND key = ? AND key = ?",
            &bound
        ),
        Err(QueryError::ResourceLimit {
            resource: "canonical SQL bytes",
            ..
        })
    ));
}

#[test]
fn canonical_fragment_scopes_are_sets_for_structured_plans() {
    let mut plan = parse("SELECT COUNT(*) FROM kv").unwrap();
    plan.fragments = Some(vec![3, 1, 3]);
    plan.validate().unwrap();
    assert_eq!(
        plan.to_sql(),
        "SELECT COUNT(*) FROM kv WHERE frag IN (1, 3)"
    );
    assert_eq!(parse(&plan.to_sql()).unwrap().fragments, Some(vec![1, 3]));
}

#[test]
fn named_tables_are_preserved_in_the_portable_plan() {
    let plan = parse("SELECT key FROM users WHERE key = 'id'").unwrap();
    assert_eq!(plan.table, "USERS");
    assert_eq!(parse(&plan.to_sql()).unwrap().table, "USERS");
}
