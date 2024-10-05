use std::fmt;
use std::marker::PhantomData;
use std::ops::Range;

use itertools::Itertools;
use serde::de;
use serde::de::DeserializeSeed;
use serde::Deserialize;
use serde::Deserializer;
use serde_json::Map;
use serde_json::Value;
use tokio_postgres::types::Kind;
use tokio_postgres::types::Type;
use tokio_postgres::Row;

use super::sql_over_http::BatchQueryData;
use super::sql_over_http::Payload;
use super::sql_over_http::QueryData;

#[derive(Clone, Copy)]
pub struct Slice {
    pub start: u32,
    pub len: u32,
}

impl Slice {
    pub fn into_range(self) -> Range<usize> {
        let start = self.start as usize;
        let end = start + self.len as usize;
        start..end
    }
}

#[derive(Default)]
pub struct Arena {
    pub str_arena: String,
    pub params_arena: Vec<Option<Slice>>,
}

impl Arena {
    fn alloc_str(&mut self, s: &str) -> Slice {
        let start = self.str_arena.len() as u32;
        let len = s.len() as u32;
        self.str_arena.push_str(s);
        Slice { start, len }
    }
}

pub struct SerdeArena<'a, T> {
    pub arena: &'a mut Arena,
    pub _t: PhantomData<T>,
}

impl<'a, T> SerdeArena<'a, T> {
    fn alloc_str(&mut self, s: &str) -> Slice {
        self.arena.alloc_str(s)
    }
}

impl<'a, 'de> DeserializeSeed<'de> for SerdeArena<'a, Vec<QueryData>> {
    type Value = Vec<QueryData>;
    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VecVisitor<'a>(SerdeArena<'a, Vec<QueryData>>);

        impl<'a, 'de> de::Visitor<'de> for VecVisitor<'a> {
            type Value = Vec<QueryData>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut values = Vec::new();

                while let Some(value) = seq.next_element_seed(SerdeArena {
                    arena: &mut *self.0.arena,
                    _t: PhantomData::<QueryData>,
                })? {
                    values.push(value);
                }

                Ok(values)
            }
        }

        d.deserialize_seq(VecVisitor(self))
    }
}

impl<'a, 'de> DeserializeSeed<'de> for SerdeArena<'a, Slice> {
    type Value = Slice;
    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor<'a>(SerdeArena<'a, Slice>);

        impl<'a, 'de> de::Visitor<'de> for Visitor<'a> {
            type Value = Slice;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(mut self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(self.0.alloc_str(v))
            }
        }

        d.deserialize_str(Visitor(self))
    }
}

enum States {
    Empty,
    HasQueries(Vec<QueryData>),
    HasPartialQueryData {
        query: Option<Slice>,
        params: Option<Slice>,
        #[allow(clippy::option_option)]
        array_mode: Option<Option<bool>>,
    },
}

enum Field {
    Queries,
    Query,
    Params,
    ArrayMode,
    Ignore,
}

struct FieldVisitor;

impl<'de> de::Visitor<'de> for FieldVisitor {
    type Value = Field;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(
            r#"a JSON object string of either "query", "params", "arrayMode", or "queries"."#,
        )
    }
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_bytes(v.as_bytes())
    }
    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match v {
            b"queries" => Ok(Field::Queries),
            b"query" => Ok(Field::Query),
            b"params" => Ok(Field::Params),
            b"arrayMode" => Ok(Field::ArrayMode),
            _ => Ok(Field::Ignore),
        }
    }
}

impl<'de> Deserialize<'de> for Field {
    #[inline]
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        d.deserialize_identifier(FieldVisitor)
    }
}

impl<'a, 'de> DeserializeSeed<'de> for SerdeArena<'a, QueryData> {
    type Value = QueryData;
    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor<'a>(SerdeArena<'a, QueryData>);
        impl<'a, 'de> de::Visitor<'de> for Visitor<'a> {
            type Value = QueryData;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str(
                    "a json object containing either a query object, or a list of query objects",
                )
            }
            #[inline]
            fn visit_map<A>(self, mut m: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut state = States::Empty;

                while let Some(key) = m.next_key()? {
                    match key {
                        Field::Query => {
                            let (params, array_mode) = match state {
                                States::HasQueries(_) => unreachable!(),
                                States::HasPartialQueryData { query: Some(_), .. } => {
                                    return Err(<A::Error as de::Error>::duplicate_field("query"))
                                }
                                States::Empty => (None, None),
                                States::HasPartialQueryData {
                                    query: None,
                                    params,
                                    array_mode,
                                } => (params, array_mode),
                            };
                            state = States::HasPartialQueryData {
                                query: Some(m.next_value_seed(SerdeArena {
                                    arena: &mut *self.0.arena,
                                    _t: PhantomData::<Slice>,
                                })?),
                                params,
                                array_mode,
                            };
                        }
                        Field::Params => {
                            let (query, array_mode) = match state {
                                States::HasQueries(_) => unreachable!(),
                                States::HasPartialQueryData {
                                    params: Some(_), ..
                                } => {
                                    return Err(<A::Error as de::Error>::duplicate_field("params"))
                                }
                                States::Empty => (None, None),
                                States::HasPartialQueryData {
                                    query,
                                    params: None,
                                    array_mode,
                                } => (query, array_mode),
                            };

                            let params = m.next_value::<PgText>()?.value;
                            let start = self.0.arena.params_arena.len() as u32;
                            let len = params.len() as u32;
                            for param in params {
                                match param {
                                    Some(s) => {
                                        let s = self.0.arena.alloc_str(&s);
                                        self.0.arena.params_arena.push(Some(s));
                                    }
                                    None => self.0.arena.params_arena.push(None),
                                }
                            }

                            state = States::HasPartialQueryData {
                                query,
                                params: Some(Slice { start, len }),
                                array_mode,
                            };
                        }
                        Field::ArrayMode => {
                            let (query, params) = match state {
                                States::HasQueries(_) => unreachable!(),
                                States::HasPartialQueryData {
                                    array_mode: Some(_),
                                    ..
                                } => {
                                    return Err(<A::Error as de::Error>::duplicate_field(
                                        "arrayMode",
                                    ))
                                }
                                States::Empty => (None, None),
                                States::HasPartialQueryData {
                                    query,
                                    params,
                                    array_mode: None,
                                } => (query, params),
                            };
                            state = States::HasPartialQueryData {
                                query,
                                params,
                                array_mode: Some(m.next_value()?),
                            };
                        }
                        Field::Queries | Field::Ignore => {
                            let _ = m.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                match state {
                    States::HasQueries(_) => unreachable!(),
                    States::HasPartialQueryData {
                        query: Some(query),
                        params: Some(params),
                        array_mode,
                    } => Ok(QueryData {
                        query,
                        params,
                        array_mode: array_mode.unwrap_or_default(),
                    }),
                    States::Empty | States::HasPartialQueryData { query: None, .. } => {
                        Err(<A::Error as de::Error>::missing_field("query"))
                    }
                    States::HasPartialQueryData { params: None, .. } => {
                        Err(<A::Error as de::Error>::missing_field("params"))
                    }
                }
            }
        }

        Deserializer::deserialize_struct(
            d,
            "QueryData",
            &["query", "params", "arrayMode"],
            Visitor(self),
        )
    }
}

impl<'a, 'de> DeserializeSeed<'de> for SerdeArena<'a, Payload> {
    type Value = Payload;
    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor<'a>(SerdeArena<'a, Payload>);
        impl<'a, 'de> de::Visitor<'de> for Visitor<'a> {
            type Value = Payload;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str(
                    "a json object containing either a query object, or a list of query objects",
                )
            }
            #[inline]
            fn visit_map<A>(self, mut m: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut state = States::Empty;

                while let Some(key) = m.next_key()? {
                    match key {
                        Field::Queries => match state {
                            States::Empty => {
                                state = States::HasQueries(m.next_value_seed(SerdeArena {
                                    arena: &mut *self.0.arena,
                                    _t: PhantomData::<Vec<QueryData>>,
                                })?);
                            }
                            States::HasQueries(_) => {
                                return Err(<A::Error as de::Error>::duplicate_field("queries"))
                            }
                            States::HasPartialQueryData { .. } => {
                                return Err(<A::Error as de::Error>::unknown_field(
                                    "queries",
                                    &["query", "params", "arrayMode"],
                                ))
                            }
                        },
                        Field::Query => {
                            let (params, array_mode) = match state {
                                States::HasQueries(_) => {
                                    return Err(<A::Error as de::Error>::unknown_field(
                                        "query",
                                        &["queries"],
                                    ))
                                }
                                States::HasPartialQueryData { query: Some(_), .. } => {
                                    return Err(<A::Error as de::Error>::duplicate_field("query"))
                                }
                                States::Empty => (None, None),
                                States::HasPartialQueryData {
                                    query: None,
                                    params,
                                    array_mode,
                                } => (params, array_mode),
                            };
                            state = States::HasPartialQueryData {
                                query: Some(m.next_value_seed(SerdeArena {
                                    arena: &mut *self.0.arena,
                                    _t: PhantomData::<Slice>,
                                })?),
                                params,
                                array_mode,
                            };
                        }
                        Field::Params => {
                            let (query, array_mode) = match state {
                                States::HasQueries(_) => {
                                    return Err(<A::Error as de::Error>::unknown_field(
                                        "params",
                                        &["queries"],
                                    ))
                                }
                                States::HasPartialQueryData {
                                    params: Some(_), ..
                                } => {
                                    return Err(<A::Error as de::Error>::duplicate_field("params"))
                                }
                                States::Empty => (None, None),
                                States::HasPartialQueryData {
                                    query,
                                    params: None,
                                    array_mode,
                                } => (query, array_mode),
                            };

                            let params = m.next_value::<PgText>()?.value;
                            let start = self.0.arena.params_arena.len() as u32;
                            let len = params.len() as u32;
                            for param in params {
                                match param {
                                    Some(s) => {
                                        let s = self.0.arena.alloc_str(&s);
                                        self.0.arena.params_arena.push(Some(s));
                                    }
                                    None => self.0.arena.params_arena.push(None),
                                }
                            }

                            state = States::HasPartialQueryData {
                                query,
                                params: Some(Slice { start, len }),
                                array_mode,
                            };
                        }
                        Field::ArrayMode => {
                            let (query, params) = match state {
                                States::HasQueries(_) => {
                                    return Err(<A::Error as de::Error>::unknown_field(
                                        "arrayMode",
                                        &["queries"],
                                    ))
                                }
                                States::HasPartialQueryData {
                                    array_mode: Some(_),
                                    ..
                                } => {
                                    return Err(<A::Error as de::Error>::duplicate_field(
                                        "arrayMode",
                                    ))
                                }
                                States::Empty => (None, None),
                                States::HasPartialQueryData {
                                    query,
                                    params,
                                    array_mode: None,
                                } => (query, params),
                            };
                            state = States::HasPartialQueryData {
                                query,
                                params,
                                array_mode: Some(m.next_value()?),
                            };
                        }
                        Field::Ignore => {
                            let _ = m.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                match state {
                    States::HasQueries(queries) => Ok(Payload::Batch(BatchQueryData { queries })),
                    States::HasPartialQueryData {
                        query: Some(query),
                        params: Some(params),
                        array_mode,
                    } => Ok(Payload::Single(QueryData {
                        query,
                        params,
                        array_mode: array_mode.unwrap_or_default(),
                    })),
                    States::Empty | States::HasPartialQueryData { query: None, .. } => {
                        Err(<A::Error as de::Error>::missing_field("query"))
                    }
                    States::HasPartialQueryData { params: None, .. } => {
                        Err(<A::Error as de::Error>::missing_field("params"))
                    }
                }
            }
        }

        Deserializer::deserialize_struct(
            d,
            "Payload",
            &["queries", "query", "params", "arrayMode"],
            Visitor(self),
        )
    }
}

struct PgText {
    value: Vec<Option<String>>,
}

impl<'de> Deserialize<'de> for PgText {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VecVisitor;

        impl<'de> de::Visitor<'de> for VecVisitor {
            type Value = Vec<Option<String>>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sequence of postgres parameters")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Vec<Option<String>>, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut values = Vec::new();

                // TODO: consider avoiding the allocations for json::Value here.
                while let Some(value) = seq.next_element()? {
                    values.push(json_value_to_pg_text(value));
                }

                Ok(values)
            }
        }

        let value = d.deserialize_seq(VecVisitor)?;

        Ok(PgText { value })
    }
}

fn json_value_to_pg_text(value: Value) -> Option<String> {
    match value {
        // special care for nulls
        Value::Null => None,

        // convert to text with escaping
        v @ (Value::Bool(_) | Value::Number(_) | Value::Object(_)) => Some(v.to_string()),

        // avoid escaping here, as we pass this as a parameter
        Value::String(s) => Some(s),

        // special care for arrays
        Value::Array(arr) => Some(json_array_to_pg_array(arr)),
    }
}

//
// Serialize a JSON array to a Postgres array. Contrary to the strings in the params
// in the array we need to escape the strings. Postgres is okay with arrays of form
// '{1,"2",3}'::int[], so we don't check that array holds values of the same type, leaving
// it for Postgres to check.
//
// Example of the same escaping in node-postgres: packages/pg/lib/utils.js
//
fn json_array_to_pg_array(arr: Vec<Value>) -> String {
    let vals = arr
        .into_iter()
        .map(json_array_value_to_pg_array)
        .map(|v| v.unwrap_or_else(|| "NULL".to_string()))
        .join(",");

    format!("{{{vals}}}")
}

fn json_array_value_to_pg_array(value: Value) -> Option<String> {
    match value {
        // special care for nulls
        Value::Null => None,

        // convert to text with escaping
        // here string needs to be escaped, as it is part of the array
        v @ (Value::Bool(_) | Value::Number(_) | Value::String(_)) => Some(v.to_string()),
        v @ Value::Object(_) => json_array_value_to_pg_array(Value::String(v.to_string())),

        // recurse into array
        Value::Array(arr) => Some(json_array_to_pg_array(arr)),
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum JsonConversionError {
    #[error("internal error compute returned invalid data: {0}")]
    AsTextError(tokio_postgres::Error),
    #[error("parse int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error("parse json error: {0}")]
    ParseJsonError(#[from] serde_json::Error),
    #[error("unbalanced array")]
    UnbalancedArray,
}

//
// Convert postgres row with text-encoded values to JSON object
//
pub(crate) fn pg_text_row_to_json(
    row: &Row,
    columns: &[Type],
    raw_output: bool,
    array_mode: bool,
) -> Result<Value, JsonConversionError> {
    let iter = row
        .columns()
        .iter()
        .zip(columns)
        .enumerate()
        .map(|(i, (column, typ))| {
            let name = column.name();
            let pg_value = row.as_text(i).map_err(JsonConversionError::AsTextError)?;
            let json_value = if raw_output {
                match pg_value {
                    Some(v) => Value::String(v.to_string()),
                    None => Value::Null,
                }
            } else {
                pg_text_to_json(pg_value, typ)?
            };
            Ok((name.to_string(), json_value))
        });

    if array_mode {
        // drop keys and aggregate into array
        let arr = iter
            .map(|r| r.map(|(_key, val)| val))
            .collect::<Result<Vec<Value>, JsonConversionError>>()?;
        Ok(Value::Array(arr))
    } else {
        let obj = iter.collect::<Result<Map<String, Value>, JsonConversionError>>()?;
        Ok(Value::Object(obj))
    }
}

//
// Convert postgres text-encoded value to JSON value
//
fn pg_text_to_json(pg_value: Option<&str>, pg_type: &Type) -> Result<Value, JsonConversionError> {
    if let Some(val) = pg_value {
        if let Kind::Array(elem_type) = pg_type.kind() {
            return pg_array_parse(val, elem_type);
        }

        match *pg_type {
            Type::BOOL => Ok(Value::Bool(val == "t")),
            Type::INT2 | Type::INT4 => {
                let val = val.parse::<i32>()?;
                Ok(Value::Number(serde_json::Number::from(val)))
            }
            Type::FLOAT4 | Type::FLOAT8 => {
                let fval = val.parse::<f64>()?;
                let num = serde_json::Number::from_f64(fval);
                if let Some(num) = num {
                    Ok(Value::Number(num))
                } else {
                    // Pass Nan, Inf, -Inf as strings
                    // JS JSON.stringify() does converts them to null, but we
                    // want to preserve them, so we pass them as strings
                    Ok(Value::String(val.to_string()))
                }
            }
            Type::JSON | Type::JSONB => Ok(serde_json::from_str(val)?),
            _ => Ok(Value::String(val.to_string())),
        }
    } else {
        Ok(Value::Null)
    }
}

//
// Parse postgres array into JSON array.
//
// This is a bit involved because we need to handle nested arrays and quoted
// values. Unlike postgres we don't check that all nested arrays have the same
// dimensions, we just return them as is.
//
fn pg_array_parse(pg_array: &str, elem_type: &Type) -> Result<Value, JsonConversionError> {
    _pg_array_parse(pg_array, elem_type, false).map(|(v, _)| v)
}

fn _pg_array_parse(
    pg_array: &str,
    elem_type: &Type,
    nested: bool,
) -> Result<(Value, usize), JsonConversionError> {
    let mut pg_array_chr = pg_array.char_indices();
    let mut level = 0;
    let mut quote = false;
    let mut entries: Vec<Value> = Vec::new();
    let mut entry = String::new();

    // skip bounds decoration
    if let Some('[') = pg_array.chars().next() {
        for (_, c) in pg_array_chr.by_ref() {
            if c == '=' {
                break;
            }
        }
    }

    fn push_checked(
        entry: &mut String,
        entries: &mut Vec<Value>,
        elem_type: &Type,
    ) -> Result<(), JsonConversionError> {
        if !entry.is_empty() {
            // While in usual postgres response we get nulls as None and everything else
            // as Some(&str), in arrays we get NULL as unquoted 'NULL' string (while
            // string with value 'NULL' will be represented by '"NULL"'). So catch NULLs
            // here while we have quotation info and convert them to None.
            if entry == "NULL" {
                entries.push(pg_text_to_json(None, elem_type)?);
            } else {
                entries.push(pg_text_to_json(Some(entry), elem_type)?);
            }
            entry.clear();
        }

        Ok(())
    }

    while let Some((mut i, mut c)) = pg_array_chr.next() {
        let mut escaped = false;

        if c == '\\' {
            escaped = true;
            (i, c) = pg_array_chr.next().unwrap();
        }

        match c {
            '{' if !quote => {
                level += 1;
                if level > 1 {
                    let (res, off) = _pg_array_parse(&pg_array[i..], elem_type, true)?;
                    entries.push(res);
                    for _ in 0..off - 1 {
                        pg_array_chr.next();
                    }
                }
            }
            '}' if !quote => {
                level -= 1;
                if level == 0 {
                    push_checked(&mut entry, &mut entries, elem_type)?;
                    if nested {
                        return Ok((Value::Array(entries), i));
                    }
                }
            }
            '"' if !escaped => {
                if quote {
                    // end of quoted string, so push it manually without any checks
                    // for emptiness or nulls
                    entries.push(pg_text_to_json(Some(&entry), elem_type)?);
                    entry.clear();
                }
                quote = !quote;
            }
            ',' if !quote => {
                push_checked(&mut entry, &mut entries, elem_type)?;
            }
            _ => {
                entry.push(c);
            }
        }
    }

    if level != 0 {
        return Err(JsonConversionError::UnbalancedArray);
    }

    Ok((Value::Array(entries), 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_atomic_types_to_pg_params() {
        let pg_params = json_value_to_pg_text(Value::Bool(true));
        assert_eq!(pg_params, Some("true".to_owned()));
        let pg_params = json_value_to_pg_text(Value::Bool(false));
        assert_eq!(pg_params, Some("false".to_owned()));

        let json = Value::Number(serde_json::Number::from(42));
        let pg_params = json_value_to_pg_text(json);
        assert_eq!(pg_params, Some("42".to_owned()));

        let json = Value::String("foo\"".to_string());
        let pg_params = json_value_to_pg_text(json);
        assert_eq!(pg_params, Some("foo\"".to_owned()));

        let json = Value::Null;
        let pg_params = json_value_to_pg_text(json);
        assert_eq!(pg_params, None);
    }

    #[test]
    fn test_json_array_to_pg_array() {
        // atoms and escaping
        let json = "[true, false, null, \"NULL\", 42, \"foo\", \"bar\\\"-\\\\\"]";
        let json: Value = serde_json::from_str(json).unwrap();
        let pg_params = json_value_to_pg_text(json);
        assert_eq!(
            pg_params,
            Some("{true,false,NULL,\"NULL\",42,\"foo\",\"bar\\\"-\\\\\"}".to_owned())
        );

        // nested arrays
        let json = "[[true, false], [null, 42], [\"foo\", \"bar\\\"-\\\\\"]]";
        let json: Value = serde_json::from_str(json).unwrap();
        let pg_params = json_value_to_pg_text(json);
        assert_eq!(
            pg_params,
            Some("{{true,false},{NULL,42},{\"foo\",\"bar\\\"-\\\\\"}}".to_owned())
        );
        // array of objects
        let json = r#"[{"foo": 1},{"bar": 2}]"#;
        let json: Value = serde_json::from_str(json).unwrap();
        let pg_params = json_value_to_pg_text(json);
        assert_eq!(
            pg_params,
            Some(r#"{"{\"foo\":1}","{\"bar\":2}"}"#.to_owned())
        );
    }

    #[test]
    fn test_atomic_types_parse() {
        assert_eq!(
            pg_text_to_json(Some("foo"), &Type::TEXT).unwrap(),
            json!("foo")
        );
        assert_eq!(pg_text_to_json(None, &Type::TEXT).unwrap(), json!(null));
        assert_eq!(pg_text_to_json(Some("42"), &Type::INT4).unwrap(), json!(42));
        assert_eq!(pg_text_to_json(Some("42"), &Type::INT2).unwrap(), json!(42));
        assert_eq!(
            pg_text_to_json(Some("42"), &Type::INT8).unwrap(),
            json!("42")
        );
        assert_eq!(
            pg_text_to_json(Some("42.42"), &Type::FLOAT8).unwrap(),
            json!(42.42)
        );
        assert_eq!(
            pg_text_to_json(Some("42.42"), &Type::FLOAT4).unwrap(),
            json!(42.42)
        );
        assert_eq!(
            pg_text_to_json(Some("NaN"), &Type::FLOAT4).unwrap(),
            json!("NaN")
        );
        assert_eq!(
            pg_text_to_json(Some("Infinity"), &Type::FLOAT4).unwrap(),
            json!("Infinity")
        );
        assert_eq!(
            pg_text_to_json(Some("-Infinity"), &Type::FLOAT4).unwrap(),
            json!("-Infinity")
        );

        let json: Value =
            serde_json::from_str("{\"s\":\"str\",\"n\":42,\"f\":4.2,\"a\":[null,3,\"a\"]}")
                .unwrap();
        assert_eq!(
            pg_text_to_json(
                Some(r#"{"s":"str","n":42,"f":4.2,"a":[null,3,"a"]}"#),
                &Type::JSONB
            )
            .unwrap(),
            json
        );
    }

    #[test]
    fn test_pg_array_parse_text() {
        fn pt(pg_arr: &str) -> Value {
            pg_array_parse(pg_arr, &Type::TEXT).unwrap()
        }
        assert_eq!(
            pt(r#"{"aa\"\\\,a",cha,"bbbb"}"#),
            json!(["aa\"\\,a", "cha", "bbbb"])
        );
        assert_eq!(
            pt(r#"{{"foo","bar"},{"bee","bop"}}"#),
            json!([["foo", "bar"], ["bee", "bop"]])
        );
        assert_eq!(
            pt(r#"{{{{"foo",NULL,"bop",bup}}}}"#),
            json!([[[["foo", null, "bop", "bup"]]]])
        );
        assert_eq!(
            pt(r#"{{"1",2,3},{4,NULL,6},{NULL,NULL,NULL}}"#),
            json!([["1", "2", "3"], ["4", null, "6"], [null, null, null]])
        );
    }

    #[test]
    fn test_pg_array_parse_bool() {
        fn pb(pg_arr: &str) -> Value {
            pg_array_parse(pg_arr, &Type::BOOL).unwrap()
        }
        assert_eq!(pb(r#"{t,f,t}"#), json!([true, false, true]));
        assert_eq!(pb(r#"{{t,f,t}}"#), json!([[true, false, true]]));
        assert_eq!(
            pb(r#"{{t,f},{f,t}}"#),
            json!([[true, false], [false, true]])
        );
        assert_eq!(
            pb(r#"{{t,NULL},{NULL,f}}"#),
            json!([[true, null], [null, false]])
        );
    }

    #[test]
    fn test_pg_array_parse_numbers() {
        fn pn(pg_arr: &str, ty: &Type) -> Value {
            pg_array_parse(pg_arr, ty).unwrap()
        }
        assert_eq!(pn(r#"{1,2,3}"#, &Type::INT4), json!([1, 2, 3]));
        assert_eq!(pn(r#"{1,2,3}"#, &Type::INT2), json!([1, 2, 3]));
        assert_eq!(pn(r#"{1,2,3}"#, &Type::INT8), json!(["1", "2", "3"]));
        assert_eq!(pn(r#"{1,2,3}"#, &Type::FLOAT4), json!([1.0, 2.0, 3.0]));
        assert_eq!(pn(r#"{1,2,3}"#, &Type::FLOAT8), json!([1.0, 2.0, 3.0]));
        assert_eq!(
            pn(r#"{1.1,2.2,3.3}"#, &Type::FLOAT4),
            json!([1.1, 2.2, 3.3])
        );
        assert_eq!(
            pn(r#"{1.1,2.2,3.3}"#, &Type::FLOAT8),
            json!([1.1, 2.2, 3.3])
        );
        assert_eq!(
            pn(r#"{NaN,Infinity,-Infinity}"#, &Type::FLOAT4),
            json!(["NaN", "Infinity", "-Infinity"])
        );
        assert_eq!(
            pn(r#"{NaN,Infinity,-Infinity}"#, &Type::FLOAT8),
            json!(["NaN", "Infinity", "-Infinity"])
        );
    }

    #[test]
    fn test_pg_array_with_decoration() {
        fn p(pg_arr: &str) -> Value {
            pg_array_parse(pg_arr, &Type::INT2).unwrap()
        }
        assert_eq!(
            p(r#"[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}"#),
            json!([[[1, 2, 3], [4, 5, 6]]])
        );
    }

    #[test]
    fn test_pg_array_parse_json() {
        fn pt(pg_arr: &str) -> Value {
            pg_array_parse(pg_arr, &Type::JSONB).unwrap()
        }
        assert_eq!(pt(r#"{"{}"}"#), json!([{}]));
        assert_eq!(
            pt(r#"{"{\"foo\": 1, \"bar\": 2}"}"#),
            json!([{"foo": 1, "bar": 2}])
        );
        assert_eq!(
            pt(r#"{"{\"foo\": 1}", "{\"bar\": 2}"}"#),
            json!([{"foo": 1}, {"bar": 2}])
        );
        assert_eq!(
            pt(r#"{{"{\"foo\": 1}", "{\"bar\": 2}"}}"#),
            json!([[{"foo": 1}, {"bar": 2}]])
        );
    }
}
