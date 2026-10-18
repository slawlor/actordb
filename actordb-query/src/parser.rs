use crate::{
    check_len, AggregateFunction, AggregateInput, KeyPredicate, Projection, QueryError, QueryPlan,
    QueryValue, MAX_QUERY_EXPRESSIONS, MAX_QUERY_FRAGMENTS, MAX_QUERY_LITERAL_BYTES,
    MAX_QUERY_TOKENS, MAX_SQL_BYTES,
};

/// Parse one literal SQL statement. An optional terminal semicolon is accepted;
/// unsupported syntax and trailing statements are rejected in full.
pub fn parse(sql: &str) -> Result<QueryPlan, QueryError> {
    parse_with_params(sql, &[])
}

/// Parse and bind positional `?` parameters. Keys require text; prefixes accept
/// text or bytes; fragment IDs, lengths, LIMIT, and OFFSET require integers.
/// Every parameter must be consumed exactly once. Bound values never become SQL
/// syntax and canonical serialization escapes them as literals.
pub fn parse_with_params(sql: &str, params: &[QueryValue]) -> Result<QueryPlan, QueryError> {
    check_len("SQL bytes", sql.len(), MAX_SQL_BYTES)?;
    check_len("parameters", params.len(), MAX_QUERY_TOKENS)?;
    let tokens = tokenize(sql)?;
    Parser {
        tokens,
        cursor: 0,
        sql_len: sql.len(),
        params,
        param_cursor: 0,
    }
    .plan()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    Word(String),
    Number(String),
    Text(String),
    Bytes(Vec<u8>),
    Left,
    Right,
    Comma,
    Equal,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Star,
    Parameter,
    Semicolon,
}

#[derive(Debug)]
struct Token {
    kind: Kind,
    offset: usize,
}

fn tokenize(sql: &str) -> Result<Vec<Token>, QueryError> {
    let bytes = sql.as_bytes();
    let mut offset = 0;
    let mut tokens = Vec::new();
    while offset < bytes.len() {
        if bytes[offset].is_ascii_whitespace() {
            offset += 1;
            continue;
        }
        if tokens.len() == MAX_QUERY_TOKENS {
            return Err(QueryError::ResourceLimit {
                resource: "tokens",
                limit: MAX_QUERY_TOKENS,
            });
        }
        let start = offset;
        let kind = match bytes[offset] {
            b'(' => {
                offset += 1;
                Kind::Left
            }
            b')' => {
                offset += 1;
                Kind::Right
            }
            b',' => {
                offset += 1;
                Kind::Comma
            }
            b'=' => {
                offset += 1;
                Kind::Equal
            }
            b'*' => {
                offset += 1;
                Kind::Star
            }
            b'?' => {
                offset += 1;
                Kind::Parameter
            }
            b';' => {
                offset += 1;
                Kind::Semicolon
            }
            b'<' | b'>' => {
                let less = bytes[offset] == b'<';
                offset += 1;
                let equal = bytes.get(offset) == Some(&b'=');
                if equal {
                    offset += 1;
                }
                match (less, equal) {
                    (true, true) => Kind::LessEqual,
                    (true, false) => Kind::Less,
                    (false, true) => Kind::GreaterEqual,
                    (false, false) => Kind::Greater,
                }
            }
            b'\'' => Kind::Text(quoted(sql, &mut offset, MAX_QUERY_LITERAL_BYTES)?),
            b'x' | b'X' if bytes.get(offset + 1) == Some(&b'\'') => {
                offset += 1;
                let hex = quoted(sql, &mut offset, MAX_QUERY_LITERAL_BYTES * 2)?;
                if hex.len() % 2 != 0 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(parse_error(
                        start,
                        "hex byte literals require an even number of hexadecimal digits",
                    ));
                }
                let mut decoded = Vec::with_capacity(hex.len() / 2);
                for pair in hex.as_bytes().chunks_exact(2) {
                    decoded.push((hex_digit(pair[0]) << 4) | hex_digit(pair[1]));
                }
                Kind::Bytes(decoded)
            }
            b'0'..=b'9' | b'+' | b'-' => {
                if matches!(bytes[offset], b'+' | b'-') {
                    offset += 1;
                }
                let digit_start = offset;
                while bytes.get(offset).is_some_and(u8::is_ascii_digit) {
                    offset += 1;
                }
                if digit_start == offset {
                    return Err(parse_error(start, "expected decimal integer"));
                }
                Kind::Number(sql[start..offset].into())
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                offset += 1;
                while bytes
                    .get(offset)
                    .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                {
                    offset += 1;
                }
                Kind::Word(sql[start..offset].to_ascii_uppercase())
            }
            _ => return Err(parse_error(start, "unsupported character or SQL syntax")),
        };
        tokens.push(Token {
            kind,
            offset: start,
        });
    }
    Ok(tokens)
}

fn quoted(sql: &str, offset: &mut usize, limit: usize) -> Result<String, QueryError> {
    let start = *offset;
    *offset += 1;
    let mut segment = *offset;
    let mut text = String::new();
    let bytes = sql.as_bytes();
    while *offset < bytes.len() {
        if bytes[*offset] == b'\'' {
            let len = text.len() + *offset - segment;
            check_len("literal bytes", len, limit)?;
            text.push_str(&sql[segment..*offset]);
            *offset += 1;
            if bytes.get(*offset) == Some(&b'\'') {
                text.push('\'');
                *offset += 1;
                segment = *offset;
                check_len("literal bytes", text.len(), limit)?;
            } else {
                return Ok(text);
            }
        } else {
            *offset += 1;
            check_len("literal bytes", text.len() + *offset - segment, limit)?;
        }
    }
    Err(parse_error(start, "unterminated string literal"))
}

fn hex_digit(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => unreachable!(),
    }
}

fn parse_error(offset: usize, message: impl Into<String>) -> QueryError {
    QueryError::Parse {
        offset,
        message: message.into(),
    }
}

struct Parser<'a> {
    tokens: Vec<Token>,
    cursor: usize,
    sql_len: usize,
    params: &'a [QueryValue],
    param_cursor: usize,
}

impl Parser<'_> {
    fn plan(mut self) -> Result<QueryPlan, QueryError> {
        self.expect_word("SELECT")?;
        let mut projections = Vec::new();
        loop {
            if projections.len() == MAX_QUERY_EXPRESSIONS {
                return Err(QueryError::ResourceLimit {
                    resource: "projections",
                    limit: MAX_QUERY_EXPRESSIONS,
                });
            }
            projections.push(self.projection()?);
            if !self.eat(&Kind::Comma) {
                break;
            }
        }
        self.expect_word("FROM")?;
        self.expect_word("KV")?;
        let mut plan = QueryPlan {
            projections,
            predicates: Vec::new(),
            fragments: None,
            limit: None,
            offset: 0,
        };
        if self.eat_word("WHERE") {
            let mut expressions = 0;
            loop {
                expressions += 1;
                check_len("predicates", expressions, MAX_QUERY_EXPRESSIONS)?;
                self.predicate(&mut plan)?;
                if !self.eat_word("AND") {
                    break;
                }
            }
        }
        let ordered = self.eat_word("ORDER");
        if ordered {
            self.expect_word("BY")?;
            self.expect_word("KEY")?;
            self.eat_word("ASC");
        }
        if self.eat_word("LIMIT") {
            plan.limit = Some(self.unsigned("LIMIT")?);
        }
        let offset = self.eat_word("OFFSET");
        if offset {
            plan.offset = self.unsigned("OFFSET")?;
        }
        self.eat(&Kind::Semicolon);
        if self.cursor != self.tokens.len() {
            return Err(self.error("unsupported syntax or trailing statement"));
        }
        if self.param_cursor != self.params.len() {
            return Err(self.error("unused bound parameters"));
        }
        if plan.is_aggregate() && (ordered || offset) {
            return Err(QueryError::InvalidPlan(
                "aggregates cannot have ORDER BY or OFFSET".into(),
            ));
        }
        plan.validate()?;
        Ok(plan)
    }

    fn projection(&mut self) -> Result<Projection, QueryError> {
        if self.eat_word("KEY") {
            return Ok(Projection::Key);
        }
        if self.eat_word("VALUE") {
            return Ok(Projection::Value);
        }
        if self.eat_word("FRAG") {
            return Ok(Projection::Fragment);
        }
        if self.eat_word("PREFIX_BYTES") {
            self.expect(&Kind::Left)?;
            self.expect_word("VALUE")?;
            self.expect(&Kind::Comma)?;
            let count = self.unsigned("PREFIX_BYTES length")?;
            let count = u32::try_from(count)
                .map_err(|_| self.error("PREFIX_BYTES length must fit unsigned 32-bit integer"))?;
            self.expect(&Kind::Right)?;
            return Ok(Projection::PrefixBytes(count));
        }
        if self.peek_word("OCTET_LENGTH") {
            self.numeric_input()?;
            return Ok(Projection::OctetLength);
        }
        let function = if self.eat_word("COUNT") {
            AggregateFunction::Count
        } else if self.eat_word("SUM") {
            AggregateFunction::Sum
        } else if self.eat_word("MIN") {
            AggregateFunction::Min
        } else if self.eat_word("MAX") {
            AggregateFunction::Max
        } else {
            return Err(self.error("expected a supported projection"));
        };
        self.expect(&Kind::Left)?;
        let input = if function == AggregateFunction::Count {
            self.expect(&Kind::Star)?;
            AggregateInput::All
        } else {
            self.numeric_input()?
        };
        self.expect(&Kind::Right)?;
        Ok(Projection::Aggregate { function, input })
    }

    fn numeric_input(&mut self) -> Result<AggregateInput, QueryError> {
        if self.eat_word("OCTET_LENGTH") {
            self.expect(&Kind::Left)?;
            self.expect_word("VALUE")?;
            self.expect(&Kind::Right)?;
            Ok(AggregateInput::OctetLength)
        } else if self.eat_word("CAST") {
            self.expect(&Kind::Left)?;
            self.expect_word("VALUE")?;
            self.expect_word("AS")?;
            self.expect_word("BIGINT")?;
            self.expect(&Kind::Right)?;
            Ok(AggregateInput::BigInt)
        } else {
            Err(self.error("expected OCTET_LENGTH(value) or CAST(value AS BIGINT)"))
        }
    }

    fn predicate(&mut self, plan: &mut QueryPlan) -> Result<(), QueryError> {
        if self.eat_word("STARTS_WITH") {
            self.expect(&Kind::Left)?;
            self.expect_word("KEY")?;
            self.expect(&Kind::Comma)?;
            let bytes = match self.literal()? {
                QueryValue::Text(text) => text.into_bytes(),
                QueryValue::Bytes(bytes) => bytes,
                _ => return Err(self.error("key prefixes require a text or bytes literal")),
            };
            check_len("literal bytes", bytes.len(), MAX_QUERY_LITERAL_BYTES)?;
            self.expect(&Kind::Right)?;
            plan.predicates.push(KeyPredicate::Prefix(bytes));
            return Ok(());
        }
        if self.eat_word("KEY") {
            let comparison = self
                .next_kind()
                .ok_or_else(|| self.error("expected key comparison"))?;
            if !matches!(
                comparison,
                Kind::Equal | Kind::Less | Kind::LessEqual | Kind::Greater | Kind::GreaterEqual
            ) {
                return Err(self.error("key comparisons support =, <, <=, >, >="));
            }
            let text = match self.literal()? {
                QueryValue::Text(text) => text,
                _ => return Err(self.error("key comparisons require text literals")),
            };
            check_len("literal bytes", text.len(), MAX_QUERY_LITERAL_BYTES)?;
            let predicate = match comparison {
                Kind::Equal => KeyPredicate::Equal(text),
                Kind::Less => KeyPredicate::LessThan(text),
                Kind::LessEqual => KeyPredicate::LessOrEqual(text),
                Kind::Greater => KeyPredicate::GreaterThan(text),
                Kind::GreaterEqual => KeyPredicate::GreaterOrEqual(text),
                _ => unreachable!(),
            };
            plan.predicates.push(predicate);
            return Ok(());
        }
        if self.eat_word("FRAG") {
            let mut scope = Vec::new();
            if self.eat(&Kind::Equal) {
                scope.push(self.fragment()?);
            } else {
                self.expect_word("IN")?;
                self.expect(&Kind::Left)?;
                if !self.eat(&Kind::Right) {
                    loop {
                        if scope.len() == MAX_QUERY_FRAGMENTS {
                            return Err(QueryError::ResourceLimit {
                                resource: "fragments",
                                limit: MAX_QUERY_FRAGMENTS,
                            });
                        }
                        scope.push(self.fragment()?);
                        if !self.eat(&Kind::Comma) {
                            break;
                        }
                    }
                    self.expect(&Kind::Right)?;
                }
            }
            scope.sort_unstable();
            scope.dedup();
            plan.fragments = Some(match plan.fragments.take() {
                Some(previous) => previous
                    .into_iter()
                    .filter(|fragment| scope.binary_search(fragment).is_ok())
                    .collect(),
                None => scope,
            });
            return Ok(());
        }
        Err(self.error("expected key comparison, STARTS_WITH, or fragment scope"))
    }

    fn fragment(&mut self) -> Result<u32, QueryError> {
        let number = self.unsigned("fragment ID")?;
        u32::try_from(number)
            .map_err(|_| self.error("fragment ID must fit unsigned 32-bit integer"))
    }

    fn unsigned(&mut self, name: &str) -> Result<u64, QueryError> {
        match self.literal()? {
            QueryValue::Integer(number) => u64::try_from(number)
                .map_err(|_| self.error(format!("{name} requires an unsigned 64-bit integer"))),
            _ => Err(self.error(format!("{name} requires an integer literal"))),
        }
    }

    fn literal(&mut self) -> Result<QueryValue, QueryError> {
        match self.next_kind() {
            Some(Kind::Text(text)) => Ok(QueryValue::Text(text)),
            Some(Kind::Bytes(bytes)) => Ok(QueryValue::Bytes(bytes)),
            Some(Kind::Number(text)) => text
                .parse::<i128>()
                .map(QueryValue::Integer)
                .map_err(|_| self.error("integer literal is outside the signed 128-bit range")),
            Some(Kind::Parameter) => {
                let value = self
                    .params
                    .get(self.param_cursor)
                    .ok_or_else(|| self.error("missing bound parameter"))?;
                self.param_cursor += 1;
                // Check before cloning application-provided values.
                match value {
                    QueryValue::Text(text) => {
                        check_len("literal bytes", text.len(), MAX_QUERY_LITERAL_BYTES)?
                    }
                    QueryValue::Bytes(bytes) => {
                        check_len("literal bytes", bytes.len(), MAX_QUERY_LITERAL_BYTES)?
                    }
                    _ => {}
                }
                Ok(value.clone())
            }
            _ => Err(self.error("expected a literal or bound parameter")),
        }
    }

    fn peek_word(&self, word: &str) -> bool {
        self.tokens
            .get(self.cursor)
            .is_some_and(|token| matches!(&token.kind, Kind::Word(value) if value == word))
    }

    fn eat_word(&mut self, word: &str) -> bool {
        if self.peek_word(word) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn expect_word(&mut self, word: &str) -> Result<(), QueryError> {
        if self.eat_word(word) {
            Ok(())
        } else {
            Err(self.error(format!("expected {word}")))
        }
    }

    fn eat(&mut self, kind: &Kind) -> bool {
        if self
            .tokens
            .get(self.cursor)
            .is_some_and(|token| &token.kind == kind)
        {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, kind: &Kind) -> Result<(), QueryError> {
        if self.eat(kind) {
            Ok(())
        } else {
            Err(self.error(format!("expected {kind:?}")))
        }
    }

    fn next_kind(&mut self) -> Option<Kind> {
        let kind = self.tokens.get(self.cursor)?.kind.clone();
        self.cursor += 1;
        Some(kind)
    }

    fn error(&self, message: impl Into<String>) -> QueryError {
        parse_error(
            self.tokens
                .get(self.cursor)
                .map_or(self.sql_len, |token| token.offset),
            message,
        )
    }
}
