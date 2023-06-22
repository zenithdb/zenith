//!
//! Helper functions for dealing with filenames of the image and delta layer files.
//!
use crate::repository::Key;
use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;
use std::str::FromStr;

use utils::lsn::Lsn;

use super::PersistentLayerDesc;

// Note: Timeline::load_layer_map() relies on this sort order
#[derive(PartialEq, Eq, Clone, Hash)]
pub struct DeltaFileName {
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,
}

impl std::fmt::Debug for DeltaFileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use super::RangeDisplayDebug;

        f.debug_struct("DeltaFileName")
            .field("key_range", &RangeDisplayDebug(&self.key_range))
            .field("lsn_range", &self.lsn_range)
            .finish()
    }
}

impl PartialOrd for DeltaFileName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeltaFileName {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut cmp = self.key_range.start.cmp(&other.key_range.start);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.key_range.end.cmp(&other.key_range.end);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.lsn_range.start.cmp(&other.lsn_range.start);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.lsn_range.end.cmp(&other.lsn_range.end);

        cmp
    }
}

/// Represents the filename of a DeltaLayer
///
///    <key start>-<key end>__<LSN start>-<LSN end>
///
impl DeltaFileName {
    ///
    /// Parse a string as a delta file name. Returns None if the filename does not
    /// match the expected pattern.
    ///
    pub fn parse_str(fname: &str) -> Option<Self> {
        let mut parts = fname.split("__");
        let mut key_parts = parts.next()?.split('-');
        let mut lsn_parts = parts.next()?.split('-');

        let key_start_str = key_parts.next()?;
        let key_end_str = key_parts.next()?;
        let lsn_start_str = lsn_parts.next()?;
        let lsn_end_str = lsn_parts.next()?;
        if parts.next().is_some() || key_parts.next().is_some() || key_parts.next().is_some() {
            return None;
        }

        let key_start = Key::from_hex(key_start_str).ok()?;
        let key_end = Key::from_hex(key_end_str).ok()?;

        let start_lsn = Lsn::from_hex(lsn_start_str).ok()?;
        let end_lsn = Lsn::from_hex(lsn_end_str).ok()?;

        if start_lsn >= end_lsn {
            return None;
            // or panic?
        }

        if key_start >= key_end {
            return None;
            // or panic?
        }

        Some(DeltaFileName {
            key_range: key_start..key_end,
            lsn_range: start_lsn..end_lsn,
        })
    }
}

impl fmt::Display for DeltaFileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}__{:016X}-{:016X}",
            self.key_range.start,
            self.key_range.end,
            u64::from(self.lsn_range.start),
            u64::from(self.lsn_range.end),
        )
    }
}

#[derive(PartialEq, Eq, Clone, Hash)]
pub struct ImageFileName {
    pub key_range: Range<Key>,
    pub lsn: Lsn,
}

impl std::fmt::Debug for ImageFileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use super::RangeDisplayDebug;

        f.debug_struct("ImageFileName")
            .field("key_range", &RangeDisplayDebug(&self.key_range))
            .field("lsn", &self.lsn)
            .finish()
    }
}

impl PartialOrd for ImageFileName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ImageFileName {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut cmp = self.key_range.start.cmp(&other.key_range.start);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.key_range.end.cmp(&other.key_range.end);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.lsn.cmp(&other.lsn);

        cmp
    }
}

impl ImageFileName {
    pub fn lsn_as_range(&self) -> Range<Lsn> {
        // Saves from having to copypaste this all over
        PersistentLayerDesc::image_layer_lsn_range(self.lsn)
    }
}

///
/// Represents the filename of an ImageLayer
///
///    <key start>-<key end>__<LSN>
impl ImageFileName {
    ///
    /// Parse a string as an image file name. Returns None if the filename does not
    /// match the expected pattern.
    ///
    pub fn parse_str(fname: &str) -> Option<Self> {
        let mut parts = fname.split("__");
        let mut key_parts = parts.next()?.split('-');

        let key_start_str = key_parts.next()?;
        let key_end_str = key_parts.next()?;
        let lsn_str = parts.next()?;
        if parts.next().is_some() || key_parts.next().is_some() {
            return None;
        }

        let key_start = Key::from_hex(key_start_str).ok()?;
        let key_end = Key::from_hex(key_end_str).ok()?;

        let lsn = Lsn::from_hex(lsn_str).ok()?;

        Some(ImageFileName {
            key_range: key_start..key_end,
            lsn,
        })
    }
}

impl fmt::Display for ImageFileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}__{:016X}",
            self.key_range.start,
            self.key_range.end,
            u64::from(self.lsn),
        )
    }
}
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum LayerFileName {
    Image(ImageFileName),
    Delta(DeltaFileName),
}

impl LayerFileName {
    pub fn file_name(&self) -> String {
        match self {
            Self::Image(fname) => fname.to_string(),
            Self::Delta(fname) => fname.to_string(),
        }
    }
    pub fn get_lsn_range(&self) -> Range<Lsn> {
        match self {
            Self::Image(fname) => fname.lsn..fname.lsn + 1,
            Self::Delta(fname) => fname.lsn_range.clone(),
        }
    }
    pub fn get_key_range(&self) -> Range<Key> {
        match self {
            Self::Image(fname) => fname.key_range.clone(),
            Self::Delta(fname) => fname.key_range.clone(),
        }
    }
}

impl From<ImageFileName> for LayerFileName {
    fn from(fname: ImageFileName) -> Self {
        Self::Image(fname)
    }
}
impl From<DeltaFileName> for LayerFileName {
    fn from(fname: DeltaFileName) -> Self {
        Self::Delta(fname)
    }
}

impl FromStr for LayerFileName {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let delta = DeltaFileName::parse_str(value);
        let image = ImageFileName::parse_str(value);
        let ok = match (delta, image) {
            (None, None) => {
                return Err(format!(
                    "neither delta nor image layer file name: {value:?}"
                ))
            }
            (Some(delta), None) => Self::Delta(delta),
            (None, Some(image)) => Self::Image(image),
            (Some(_), Some(_)) => unreachable!(),
        };
        Ok(ok)
    }
}

impl serde::Serialize for LayerFileName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Image(fname) => serializer.serialize_str(&fname.to_string()),
            Self::Delta(fname) => serializer.serialize_str(&fname.to_string()),
        }
    }
}

impl<'de> serde::Deserialize<'de> for LayerFileName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_string(LayerFileNameVisitor)
    }
}

struct LayerFileNameVisitor;

impl<'de> serde::de::Visitor<'de> for LayerFileNameVisitor {
    type Value = LayerFileName;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "a string that is a valid image or delta layer file name"
        )
    }
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        v.parse().map_err(|e| E::custom(e))
    }
}
