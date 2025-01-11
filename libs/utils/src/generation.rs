use std::fmt::Debug;

use serde::{Deserialize, Serialize};

/// Tenant generations are used to provide split-brain safety and allow
/// multiple pageservers to attach the same tenant concurrently.
///
/// See docs/rfcs/025-generation-numbers.md for detail on how generation
/// numbers are used.
#[derive(Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub enum Generation {
    // The None Generation is used in the metadata of layers written before generations were
    // introduced.  A running Tenant always has a valid generation, but the layer metadata may
    // include None generations.
    None,

    Valid(u32),
}

/// The Generation type represents a number associated with a Tenant, which
/// increments every time the tenant is attached to a new pageserver, or
/// an attached pageserver restarts.
///
/// It is included as a suffix in S3 keys, as a protection against split-brain
/// scenarios where pageservers might otherwise issue conflicting writes to
/// remote storage
impl Generation {
    pub const MAX: Self = Self::Valid(u32::MAX);

    /// Create a new Generation that represents a legacy key format with
    /// no generation suffix
    pub fn none() -> Self {
        Self::None
    }

    pub const fn new(v: u32) -> Self {
        Self::Valid(v)
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    #[track_caller]
    pub fn get_suffix(&self) -> impl std::fmt::Display {
        match self {
            Self::Valid(v) => GenerationFileSuffix(Some(*v)),
            Self::None => GenerationFileSuffix(None),
        }
    }

    /// `suffix` is the part after "-" in a key
    ///
    /// Returns None if parsing was unsuccessful
    pub fn parse_suffix(suffix: &str) -> Option<Generation> {
        u32::from_str_radix(suffix, 16).map(Generation::new).ok()
    }

    #[track_caller]
    pub fn previous(&self) -> Generation {
        match self {
            Self::Valid(n) => {
                if *n == 0 {
                    // Since a tenant may be upgraded from a pre-generations state, interpret the "previous" generation
                    // to 0 as being "no generation".
                    Self::None
                } else {
                    Self::Valid(n - 1)
                }
            }
            Self::None => Self::None,
        }
    }

    #[track_caller]
    pub fn next(&self) -> Generation {
        match self {
            Self::Valid(n) => Self::Valid(*n + 1),
            Self::None => Self::Valid(1),
        }
    }

    pub fn into(self) -> Option<u32> {
        if let Self::Valid(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

struct GenerationFileSuffix(Option<u32>);

impl std::fmt::Display for GenerationFileSuffix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(g) = self.0 {
            write!(f, "-{g:08x}")
        } else {
            Ok(())
        }
    }
}

impl Serialize for Generation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Self::Valid(v) = self {
            v.serialize(serializer)
        } else {
            // We should never be asked to serialize a None. Structures
            // that include an optional generation should convert None to an
            // Option<Generation>::None
            Err(serde::ser::Error::custom(format!(
                "Tried to serialize invalid generation ({self:?})"
            )))
        }
    }
}

impl<'de> Deserialize<'de> for Generation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::Valid(u32::deserialize(deserializer)?))
    }
}

// We intentionally do not implement Display for Generation, to reduce the
// risk of a bug where the generation is used in a format!() string directly
// instead of using get_suffix().
impl Debug for Generation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Valid(v) => {
                write!(f, "{:08x}", v)
            }
            Self::None => {
                write!(f, "<none>")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn generation_gt() {
        // Important that a None generation compares less than a valid one, during upgrades from
        // pre-generation systems.
        assert!(Generation::none() < Generation::new(0));
        assert!(Generation::none() < Generation::new(1));
    }

    #[test]
    fn suffix_is_stable() {
        use std::fmt::Write as _;

        // the suffix must remain stable through-out the pageserver remote storage evolution and
        // not be changed accidentially without thinking about migration
        let examples = [
            (line!(), Generation::None, ""),
            (line!(), Generation::Valid(0), "-00000000"),
            (line!(), Generation::Valid(u32::MAX), "-ffffffff"),
        ];

        let mut s = String::new();
        for (line, gen, expected) in examples {
            s.clear();
            write!(s, "{}", &gen.get_suffix()).expect("string grows");
            assert_eq!(s, expected, "example on {line}");
        }
    }
}
