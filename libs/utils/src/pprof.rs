use anyhow::bail;
use flate2::write::{GzDecoder, GzEncoder};
use flate2::Compression;
use itertools::Itertools as _;
use once_cell::sync::Lazy;
use pprof::protos::{Function, Line, Location, Message as _, Profile};
use regex::Regex;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::io::Write as _;

/// Decodes a gzip-compressed Protobuf-encoded pprof profile.
pub fn decode(bytes: &[u8]) -> anyhow::Result<Profile> {
    let mut gz = GzDecoder::new(Vec::new());
    gz.write_all(bytes)?;
    Ok(Profile::parse_from_bytes(&gz.finish()?)?)
}

/// Encodes a pprof profile as gzip-compressed Protobuf.
pub fn encode(profile: &Profile) -> anyhow::Result<Vec<u8>> {
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    profile.write_to_writer(&mut gz)?;
    Ok(gz.finish()?)
}

/// Symbolizes a pprof profile using the current binary.
pub fn symbolize(mut profile: Profile) -> anyhow::Result<Profile> {
    if !profile.function.is_empty() {
        return Ok(profile); // already symbolized
    }

    // Collect function names.
    let mut functions: HashMap<String, Function> = HashMap::new();
    let mut strings: HashMap<String, i64> = profile
        .string_table
        .into_iter()
        .enumerate()
        .map(|(i, s)| (s, i as i64))
        .collect();

    // Helper to look up or register a string.
    let mut string_id = |s: &str| -> i64 {
        // Don't use .entry() to avoid unnecessary allocations.
        if let Some(id) = strings.get(s) {
            return *id;
        }
        let id = strings.len() as i64;
        strings.insert(s.to_string(), id);
        id
    };

    for loc in &mut profile.location {
        if !loc.line.is_empty() {
            continue;
        }

        // Resolve the line and function for each location.
        backtrace::resolve(loc.address as *mut c_void, |symbol| {
            let Some(symname) = symbol.name() else {
                return;
            };
            let mut name = symname.to_string();

            // Strip the Rust monomorphization suffix from the symbol name.
            static SUFFIX_REGEX: Lazy<Regex> =
                Lazy::new(|| Regex::new("::h[0-9a-f]{16}$").expect("invalid regex"));
            if let Some(m) = SUFFIX_REGEX.find(&name) {
                name.truncate(m.start());
            }

            let function_id = match functions.get(&name) {
                Some(function) => function.id,
                None => {
                    let id = functions.len() as u64 + 1;
                    let system_name = String::from_utf8_lossy(symname.as_bytes());
                    let filename = symbol
                        .filename()
                        .map(|path| path.to_string_lossy())
                        .unwrap_or(Cow::Borrowed(""));
                    let function = Function {
                        id,
                        name: string_id(&name),
                        system_name: string_id(&system_name),
                        filename: string_id(&filename),
                        ..Default::default()
                    };
                    functions.insert(name, function);
                    id
                }
            };
            loc.line.push(Line {
                function_id,
                line: symbol.lineno().unwrap_or(0) as i64,
                ..Default::default()
            });
        });
    }

    // Store the resolved functions, and mark the mapping as resolved.
    profile.function = functions.into_values().sorted_by_key(|f| f.id).collect();
    profile.string_table = strings
        .into_iter()
        .sorted_by_key(|(_, i)| *i)
        .map(|(s, _)| s)
        .collect();

    for mapping in &mut profile.mapping {
        mapping.has_functions = true;
        mapping.has_filenames = true;
    }

    Ok(profile)
}

/// Strips locations (stack frames) matching the given mappings (substring) or function names
/// (regex). The function bool specifies whether child frames should be stripped as well.
///
/// The string definitions are left behind in the profile for simplicity, to avoid rewriting all
/// string references.
pub fn strip_locations(
    mut profile: Profile,
    mappings: &[&str],
    functions: &[(Regex, bool)],
) -> Profile {
    // Strip mappings.
    let mut strip_mappings: HashSet<u64> = HashSet::new();

    profile.mapping.retain(|mapping| {
        let Some(name) = profile.string_table.get(mapping.filename as usize) else {
            return true;
        };
        if mappings.iter().any(|substr| name.contains(substr)) {
            strip_mappings.insert(mapping.id);
            return false;
        }
        true
    });

    // Strip functions.
    let mut strip_functions: HashMap<u64, bool> = HashMap::new();

    profile.function.retain(|function| {
        let Some(name) = profile.string_table.get(function.name as usize) else {
            return true;
        };
        for (regex, strip_children) in functions {
            if regex.is_match(name) {
                strip_functions.insert(function.id, *strip_children);
                return false;
            }
        }
        true
    });

    // Strip locations. The bool specifies whether child frames should be stripped too.
    let mut strip_locations: HashMap<u64, bool> = HashMap::new();

    profile.location.retain(|location| {
        for line in &location.line {
            if let Some(strip_children) = strip_functions.get(&line.function_id) {
                strip_locations.insert(location.id, *strip_children);
                return false;
            }
        }
        if strip_mappings.contains(&location.mapping_id) {
            strip_locations.insert(location.id, false);
            return false;
        }
        true
    });

    // Strip sample locations.
    for sample in &mut profile.sample {
        // First, find the uppermost function with child removal and truncate the stack.
        if let Some(truncate) = sample
            .location_id
            .iter()
            .rposition(|id| strip_locations.get(id) == Some(&true))
        {
            sample.location_id.drain(..=truncate);
        }
        // Next, strip any individual frames without child removal.
        sample
            .location_id
            .retain(|id| !strip_locations.contains_key(id));
    }

    profile
}

/// Generates an SVG flamegraph from a symbolized pprof profile.
pub fn flamegraph(
    profile: Profile,
    opts: &mut inferno::flamegraph::Options,
) -> anyhow::Result<Vec<u8>> {
    if profile.mapping.iter().any(|m| !m.has_functions) {
        bail!("profile not symbolized");
    }

    // Index locations, functions, and strings.
    let locations: HashMap<u64, Location> =
        profile.location.into_iter().map(|l| (l.id, l)).collect();
    let functions: HashMap<u64, Function> =
        profile.function.into_iter().map(|f| (f.id, f)).collect();
    let strings = profile.string_table;

    // Resolve stacks as function names, and sum sample values per stack. Also reverse the stack,
    // since inferno expects it bottom-up.
    let mut stacks: HashMap<Vec<&str>, i64> = HashMap::new();
    for sample in profile.sample {
        let mut stack = Vec::with_capacity(sample.location_id.len());
        for location in sample.location_id.into_iter().rev() {
            let Some(location) = locations.get(&location) else {
                bail!("missing location {location}");
            };
            for line in location.line.iter().rev() {
                let Some(function) = functions.get(&line.function_id) else {
                    bail!("missing function {}", line.function_id);
                };
                let Some(name) = strings.get(function.name as usize) else {
                    bail!("missing string {}", function.name);
                };
                stack.push(name.as_str());
            }
        }
        let Some(&value) = sample.value.first() else {
            bail!("missing value");
        };
        *stacks.entry(stack).or_default() += value;
    }

    // Construct stack lines for inferno.
    let lines = stacks
        .into_iter()
        .map(|(stack, value)| (stack.into_iter().join(";"), value))
        .map(|(stack, value)| format!("{stack} {value}"))
        .sorted()
        .collect_vec();

    // Construct the flamegraph.
    let mut bytes = Vec::new();
    let lines = lines.iter().map(|line| line.as_str());
    inferno::flamegraph::from_lines(opts, lines, &mut bytes)?;
    Ok(bytes)
}
