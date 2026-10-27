//! Exact incremental unary reply sizing; no accumulated-response rescans.
use super::*;

pub(crate) struct BatchReply<T> {
    slots: Vec<Option<T>>,
    bytes: usize,
    tag: u32,
}

impl<T: Message> BatchReply<T> {
    pub(crate) fn new(count: usize, tag: u32) -> Self {
        Self {
            slots: (0..count).map(|_| None).collect(),
            bytes: 0,
            tag,
        }
    }
    pub(crate) fn insert(&mut self, index: usize, result: T) -> Result<(), wire::Error> {
        let next = self.bytes.checked_add(crate::wire::message_field_len(
            self.tag,
            result.encoded_len(),
        ));
        if next.is_none_or(|bytes| bytes > MAX_RPC_MESSAGE_BYTES) {
            return Err(error(
                wire::ErrorCode::ResourceLimit,
                "complete batch reply exceeds RPC byte limit",
            ));
        }
        assert!(
            self.slots[index].is_none(),
            "batch position completed twice"
        );
        self.slots[index] = Some(result);
        self.bytes = next.unwrap();
        Ok(())
    }
    pub(crate) fn finish(self) -> Vec<T> {
        self.slots
            .into_iter()
            .map(|slot| slot.expect("batch position omitted"))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountedRow {
        row: wire::ReadResult,
        calls: Arc<AtomicUsize>,
    }
    impl Message for CountedRow {
        fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
            self.row.encode_raw(buf);
        }
        fn merge_field(
            &mut self,
            tag: u32,
            wire: prost::encoding::WireType,
            buf: &mut impl bytes::Buf,
            ctx: prost::encoding::DecodeContext,
        ) -> Result<(), prost::DecodeError> {
            self.row.merge_field(tag, wire, buf, ctx)
        }
        fn encoded_len(&self) -> usize {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.row.encoded_len()
        }
        fn clear(&mut self) {
            self.row.clear();
        }
    }

    #[test]
    fn sizing_visits_each_result_once_and_orders_sparse_completion_positions() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut builder = BatchReply::new(4096, 2);
        for turn in 0..4096 {
            let index = (turn * 4093) % 4096;
            let row = wire::ReadResult {
                key: format!("key-{index}"),
                value: Some(wire::Value {
                    bytes: vec![7; [0, 1, 127, 128, 255][index % 5]],
                    ..Default::default()
                }),
                error: None,
            };
            builder
                .insert(
                    index,
                    CountedRow {
                        row,
                        calls: calls.clone(),
                    },
                )
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 4096);
        let bytes = builder.bytes;
        let reads: Vec<_> = builder.finish().into_iter().map(|row| row.row).collect();
        assert_eq!(reads[1024].key, "key-1024");
        let response = wire::Response {
            reads,
            ..Default::default()
        };
        assert_eq!(bytes, response.encoded_len());
        assert_eq!(bytes, response.encode_to_vec().len());
        assert_eq!(calls.load(Ordering::Relaxed), 4096);
    }

    #[test]
    fn exact_read_limit_accepts_the_boundary_and_rejects_before_storing_an_extra_row() {
        let encoded = |payload| {
            crate::wire::message_field_len(
                2,
                crate::wire::bytes_field_len(1, 1)
                    + crate::wire::message_field_len(2, crate::wire::bytes_field_len(1, payload)),
            )
        };
        let mut low = 0;
        let mut high = MAX_RPC_MESSAGE_BYTES;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if encoded(middle) <= MAX_RPC_MESSAGE_BYTES {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let mut builder = BatchReply::new(2, 2);
        builder
            .insert(
                0,
                wire::ReadResult {
                    key: "k".into(),
                    value: Some(wire::Value {
                        bytes: vec![0; low],
                        ..Default::default()
                    }),
                    error: None,
                },
            )
            .unwrap();
        assert_eq!(builder.bytes, MAX_RPC_MESSAGE_BYTES);
        let failure = builder.insert(1, wire::ReadResult::default()).unwrap_err();
        assert_eq!(failure.code, wire::ErrorCode::ResourceLimit as i32);
        assert!(builder.slots[1].is_none());
        assert_eq!(builder.bytes, MAX_RPC_MESSAGE_BYTES);
    }

    #[test]
    fn mutation_sizing_matches_the_actual_response_with_error_metadata() {
        let mut builder = BatchReply::new(2, 3);
        builder
            .insert(
                1,
                mutation_result(
                    "dup".into(),
                    Err(NodeError::Superseded {
                        timestamp_ms: u64::MAX,
                        logical_counter: 7,
                        node_id: "winner".into(),
                        is_tombstone: true,
                    }),
                ),
            )
            .unwrap();
        builder
            .insert(0, mutation_result("dup".into(), Ok(())))
            .unwrap();
        let bytes = builder.bytes;
        assert_eq!(
            bytes,
            wire::Response {
                mutations: builder.finish(),
                ..Default::default()
            }
            .encoded_len()
        );
    }
}
