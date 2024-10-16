//! [`serde_json::Value`] but uses RawValue internally
//!
//! This code forks from the serde_json code, but replaces internal Value with RawValue where possible.
//!
//! Taken from <https://github.com/serde-rs/json/blob/faab2e8d2fcf781a3f77f329df836ffb3aaacfba/src/value/de.rs>
//! Licensed from serde-rs under MIT or APACHE-2.0, with modifications by Conrad Ludgate

use core::fmt;
use std::borrow::Cow;

use serde::{
    de::{IgnoredAny, MapAccess, SeqAccess, Visitor},
    Deserialize,
};
use serde_json::value::RawValue;

pub enum LazyValue<'de> {
    Null,
    Bool,
    Number,
    String(Cow<'de, str>),
    Array(Vec<&'de RawValue>),
    Object,
}

impl<'de> Deserialize<'de> for LazyValue<'de> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<LazyValue<'de>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = LazyValue<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any valid JSON value")
            }

            #[inline]
            fn visit_bool<E>(self, _value: bool) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::Bool)
            }

            #[inline]
            fn visit_i64<E>(self, _value: i64) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::Number)
            }

            #[inline]
            fn visit_u64<E>(self, _value: u64) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::Number)
            }

            #[inline]
            fn visit_f64<E>(self, _value: f64) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::Number)
            }

            #[inline]
            fn visit_str<E>(self, value: &str) -> Result<LazyValue<'de>, E>
            where
                E: serde::de::Error,
            {
                self.visit_string(String::from(value))
            }

            #[inline]
            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<LazyValue<'de>, E>
            where
                E: serde::de::Error,
            {
                Ok(LazyValue::String(Cow::Borrowed(value)))
            }

            #[inline]
            fn visit_string<E>(self, value: String) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::String(Cow::Owned(value)))
            }

            #[inline]
            fn visit_none<E>(self) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::Null)
            }

            #[inline]
            fn visit_some<D>(self, deserializer: D) -> Result<LazyValue<'de>, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Deserialize::deserialize(deserializer)
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::Null)
            }

            #[inline]
            fn visit_seq<V>(self, mut visitor: V) -> Result<LazyValue<'de>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let mut vec = Vec::new();

                while let Some(elem) = visitor.next_element()? {
                    vec.push(elem);
                }

                Ok(LazyValue::Array(vec))
            }

            fn visit_map<V>(self, mut visitor: V) -> Result<LazyValue<'de>, V::Error>
            where
                V: MapAccess<'de>,
            {
                while visitor.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(LazyValue::Object)
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use typed_json::json;

    use super::LazyValue;

    #[test]
    fn object() {
        let json = json! {{
            "foo": {
                "bar": 1
            },
            "baz": [2, 3],
        }}
        .to_string();

        let lazy: LazyValue = serde_json::from_str(&json).unwrap();

        let LazyValue::Object = lazy else {
            panic!("expected object")
        };
    }

    #[test]
    fn array() {
        let json = json! {[
            {
                "bar": 1
            },
            [2, 3],
        ]}
        .to_string();

        let lazy: LazyValue = serde_json::from_str(&json).unwrap();

        let LazyValue::Array(array) = lazy else {
            panic!("expected array")
        };
        assert_eq!(array.len(), 2);

        assert_eq!(array[0].get(), r#"{"bar":1}"#);
        assert_eq!(array[1].get(), r#"[2,3]"#);
    }

    #[test]
    fn string() {
        let json = json! { "hello world" }.to_string();

        let lazy: LazyValue = serde_json::from_str(&json).unwrap();

        let LazyValue::String(Cow::Borrowed(string)) = lazy else {
            panic!("expected borrowed string")
        };
        assert_eq!(string, "hello world");

        let json = json! { "hello \n world" }.to_string();

        let lazy: LazyValue = serde_json::from_str(&json).unwrap();

        let LazyValue::String(Cow::Owned(string)) = lazy else {
            panic!("expected owned string")
        };
        assert_eq!(string, "hello \n world");
    }
}
