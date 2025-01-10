use postgres_client::types::{Kind, Type};
use postgres_client::Row;
use serde_json::{Map, Value};

//
// Convert json non-string types to strings, so that they can be passed to Postgres
// as parameters.
//
pub(crate) fn json_to_pg_text(json: Vec<Value>) -> Vec<Option<String>> {
    json.iter().map(json_value_to_pg_text).collect()
}

fn json_value_to_pg_text(value: &Value) -> Option<String> {
    match value {
        // special care for nulls
        Value::Null => None,

        // convert to text with escaping
        v @ (Value::Bool(_) | Value::Number(_) | Value::Object(_)) => Some(v.to_string()),

        // avoid escaping here, as we pass this as a parameter
        Value::String(s) => Some(s.to_string()),

        // special care for arrays
        Value::Array(_) => json_array_to_pg_array(value),
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
fn json_array_to_pg_array(value: &Value) -> Option<String> {
    match value {
        // special care for nulls
        Value::Null => None,

        // convert to text with escaping
        // here string needs to be escaped, as it is part of the array
        v @ (Value::Bool(_) | Value::Number(_) | Value::String(_)) => Some(v.to_string()),
        v @ Value::Object(_) => json_array_to_pg_array(&Value::String(v.to_string())),

        // recurse into array
        Value::Array(arr) => {
            let vals = arr
                .iter()
                .map(json_array_to_pg_array)
                .map(|v| v.unwrap_or_else(|| "NULL".to_string()))
                .collect::<Vec<_>>()
                .join(",");

            Some(format!("{{{vals}}}"))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum JsonConversionError {
    #[error("internal error compute returned invalid data: {0}")]
    AsTextError(postgres_client::Error),
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
    pg_array_parse_inner(pg_array, elem_type, false).map(|(v, _)| v)
}

fn pg_array_parse_inner(
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
            let Some(x) = pg_array_chr.next() else {
                return Err(JsonConversionError::UnbalancedArray);
            };
            (i, c) = x;
        }

        match c {
            '{' if !quote => {
                level += 1;
                if level > 1 {
                    let (res, off) = pg_array_parse_inner(&pg_array[i..], elem_type, true)?;
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
#[expect(clippy::unwrap_used)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_atomic_types_to_pg_params() {
        let json = vec![Value::Bool(true), Value::Bool(false)];
        let pg_params = json_to_pg_text(json);
        assert_eq!(
            pg_params,
            vec![Some("true".to_owned()), Some("false".to_owned())]
        );

        let json = vec![Value::Number(serde_json::Number::from(42))];
        let pg_params = json_to_pg_text(json);
        assert_eq!(pg_params, vec![Some("42".to_owned())]);

        let json = vec![Value::String("foo\"".to_string())];
        let pg_params = json_to_pg_text(json);
        assert_eq!(pg_params, vec![Some("foo\"".to_owned())]);

        let json = vec![Value::Null];
        let pg_params = json_to_pg_text(json);
        assert_eq!(pg_params, vec![None]);
    }

    #[test]
    fn test_json_array_to_pg_array() {
        // atoms and escaping
        let json = "[true, false, null, \"NULL\", 42, \"foo\", \"bar\\\"-\\\\\"]";
        let json: Value = serde_json::from_str(json).unwrap();
        let pg_params = json_to_pg_text(vec![json]);
        assert_eq!(
            pg_params,
            vec![Some(
                "{true,false,NULL,\"NULL\",42,\"foo\",\"bar\\\"-\\\\\"}".to_owned()
            )]
        );

        // nested arrays
        let json = "[[true, false], [null, 42], [\"foo\", \"bar\\\"-\\\\\"]]";
        let json: Value = serde_json::from_str(json).unwrap();
        let pg_params = json_to_pg_text(vec![json]);
        assert_eq!(
            pg_params,
            vec![Some(
                "{{true,false},{NULL,42},{\"foo\",\"bar\\\"-\\\\\"}}".to_owned()
            )]
        );
        // array of objects
        let json = r#"[{"foo": 1},{"bar": 2}]"#;
        let json: Value = serde_json::from_str(json).unwrap();
        let pg_params = json_to_pg_text(vec![json]);
        assert_eq!(
            pg_params,
            vec![Some(r#"{"{\"foo\":1}","{\"bar\":2}"}"#.to_owned())]
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
