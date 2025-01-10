//! User credentials used in authentication.

use std::collections::HashSet;
use std::net::IpAddr;
use std::str::FromStr;

use itertools::Itertools;
use pq_proto::StartupMessageParams;
use thiserror::Error;
use tracing::{debug, warn};

use crate::auth::password_hack::parse_endpoint_param;
use crate::context::RequestContext;
use crate::error::{ReportableError, UserFacingError};
use crate::metrics::{Metrics, SniKind};
use crate::proxy::NeonOptions;
use crate::serverless::SERVERLESS_DRIVER_SNI;
use crate::types::{EndpointId, RoleName};

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub(crate) enum ComputeUserInfoParseError {
    #[error("Parameter '{0}' is missing in startup packet.")]
    MissingKey(&'static str),

    #[error(
        "Inconsistent project name inferred from \
         SNI ('{}') and project option ('{}').",
        .domain, .option,
    )]
    InconsistentProjectNames {
        domain: EndpointId,
        option: EndpointId,
    },

    #[error(
        "Common name inferred from SNI ('{}') is not known",
        .cn,
    )]
    UnknownCommonName { cn: String },

    #[error("Project name ('{0}') must contain only alphanumeric characters and hyphen.")]
    MalformedProjectName(EndpointId),
}

impl UserFacingError for ComputeUserInfoParseError {}

impl ReportableError for ComputeUserInfoParseError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        crate::error::ErrorKind::User
    }
}

/// Various client credentials which we use for authentication.
/// Note that we don't store any kind of client key or password here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ComputeUserInfoMaybeEndpoint {
    pub(crate) user: RoleName,
    pub(crate) endpoint_id: Option<EndpointId>,
    pub(crate) options: NeonOptions,
}

impl ComputeUserInfoMaybeEndpoint {
    #[inline]
    pub(crate) fn endpoint(&self) -> Option<&str> {
        self.endpoint_id.as_deref()
    }
}

pub(crate) fn endpoint_sni(
    sni: &str,
    common_names: &HashSet<String>,
) -> Result<Option<EndpointId>, ComputeUserInfoParseError> {
    let Some((subdomain, common_name)) = sni.split_once('.') else {
        return Err(ComputeUserInfoParseError::UnknownCommonName { cn: sni.into() });
    };
    if !common_names.contains(common_name) {
        return Err(ComputeUserInfoParseError::UnknownCommonName {
            cn: common_name.into(),
        });
    }
    if subdomain == SERVERLESS_DRIVER_SNI {
        return Ok(None);
    }
    Ok(Some(EndpointId::from(subdomain)))
}

impl ComputeUserInfoMaybeEndpoint {
    pub(crate) fn parse(
        ctx: &RequestContext,
        params: &StartupMessageParams,
        sni: Option<&str>,
        common_names: Option<&HashSet<String>>,
    ) -> Result<Self, ComputeUserInfoParseError> {
        // Some parameters are stored in the startup message.
        let get_param = |key| {
            params
                .get(key)
                .ok_or(ComputeUserInfoParseError::MissingKey(key))
        };
        let user: RoleName = get_param("user")?.into();

        // Project name might be passed via PG's command-line options.
        let endpoint_option = params
            .options_raw()
            .and_then(|options| {
                // We support both `project` (deprecated) and `endpoint` options for backward compatibility.
                // However, if both are present, we don't exactly know which one to use.
                // Therefore we require that only one of them is present.
                options
                    .filter_map(parse_endpoint_param)
                    .at_most_one()
                    .ok()?
            })
            .map(|name| name.into());

        let endpoint_from_domain = if let Some(sni_str) = sni {
            if let Some(cn) = common_names {
                endpoint_sni(sni_str, cn)?
            } else {
                None
            }
        } else {
            None
        };

        let endpoint = match (endpoint_option, endpoint_from_domain) {
            // Invariant: if we have both project name variants, they should match.
            (Some(option), Some(domain)) if option != domain => {
                Some(Err(ComputeUserInfoParseError::InconsistentProjectNames {
                    domain,
                    option,
                }))
            }
            // Invariant: project name may not contain certain characters.
            (a, b) => a.or(b).map(|name| {
                if project_name_valid(name.as_ref()) {
                    Ok(name)
                } else {
                    Err(ComputeUserInfoParseError::MalformedProjectName(name))
                }
            }),
        }
        .transpose()?;

        if let Some(ep) = &endpoint {
            ctx.set_endpoint_id(ep.clone());
        }

        let metrics = Metrics::get();
        debug!(%user, "credentials");
        if sni.is_some() {
            debug!("Connection with sni");
            metrics.proxy.accepted_connections_by_sni.inc(SniKind::Sni);
        } else if endpoint.is_some() {
            metrics
                .proxy
                .accepted_connections_by_sni
                .inc(SniKind::NoSni);
            debug!("Connection without sni");
        } else {
            metrics
                .proxy
                .accepted_connections_by_sni
                .inc(SniKind::PasswordHack);
            debug!("Connection with password hack");
        }

        let options = NeonOptions::parse_params(params);

        Ok(Self {
            user,
            endpoint_id: endpoint,
            options,
        })
    }
}

pub(crate) fn check_peer_addr_is_in_list(peer_addr: &IpAddr, ip_list: &[IpPattern]) -> bool {
    ip_list.is_empty() || ip_list.iter().any(|pattern| check_ip(peer_addr, pattern))
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum IpPattern {
    Subnet(ipnet::IpNet),
    Range(IpAddr, IpAddr),
    Single(IpAddr),
    None,
}

impl<'de> serde::de::Deserialize<'de> for IpPattern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StrVisitor;
        impl serde::de::Visitor<'_> for StrVisitor {
            type Value = IpPattern;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "comma separated list with ip address, ip address range, or ip address subnet mask")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(parse_ip_pattern(v).unwrap_or_else(|e| {
                    warn!("Cannot parse ip pattern {v}: {e}");
                    IpPattern::None
                }))
            }
        }
        deserializer.deserialize_str(StrVisitor)
    }
}

impl FromStr for IpPattern {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_ip_pattern(s)
    }
}

fn parse_ip_pattern(pattern: &str) -> anyhow::Result<IpPattern> {
    if pattern.contains('/') {
        let subnet: ipnet::IpNet = pattern.parse()?;
        return Ok(IpPattern::Subnet(subnet));
    }
    if let Some((start, end)) = pattern.split_once('-') {
        let start: IpAddr = start.parse()?;
        let end: IpAddr = end.parse()?;
        return Ok(IpPattern::Range(start, end));
    }
    let addr: IpAddr = pattern.parse()?;
    Ok(IpPattern::Single(addr))
}

fn check_ip(ip: &IpAddr, pattern: &IpPattern) -> bool {
    match pattern {
        IpPattern::Subnet(subnet) => subnet.contains(ip),
        IpPattern::Range(start, end) => start <= ip && ip <= end,
        IpPattern::Single(addr) => addr == ip,
        IpPattern::None => false,
    }
}

fn project_name_valid(name: &str) -> bool {
    name.chars().all(|c| c.is_alphanumeric() || c == '-')
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use serde_json::json;
    use ComputeUserInfoParseError::*;

    use super::*;

    #[test]
    fn parse_bare_minimum() -> anyhow::Result<()> {
        // According to postgresql, only `user` should be required.
        let options = StartupMessageParams::new([("user", "john_doe")]);
        let ctx = RequestContext::test();
        let user_info = ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, None, None)?;
        assert_eq!(user_info.user, "john_doe");
        assert_eq!(user_info.endpoint_id, None);

        Ok(())
    }

    #[test]
    fn parse_excessive() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("database", "world"), // should be ignored
            ("foo", "bar"),        // should be ignored
        ]);
        let ctx = RequestContext::test();
        let user_info = ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, None, None)?;
        assert_eq!(user_info.user, "john_doe");
        assert_eq!(user_info.endpoint_id, None);

        Ok(())
    }

    #[test]
    fn parse_project_from_sni() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([("user", "john_doe")]);

        let sni = Some("foo.localhost");
        let common_names = Some(["localhost".into()].into());

        let ctx = RequestContext::test();
        let user_info =
            ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, sni, common_names.as_ref())?;
        assert_eq!(user_info.user, "john_doe");
        assert_eq!(user_info.endpoint_id.as_deref(), Some("foo"));
        assert_eq!(user_info.options.get_cache_key("foo"), "foo");

        Ok(())
    }

    #[test]
    fn parse_project_from_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("options", "-ckey=1 project=bar -c geqo=off"),
        ]);

        let ctx = RequestContext::test();
        let user_info = ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, None, None)?;
        assert_eq!(user_info.user, "john_doe");
        assert_eq!(user_info.endpoint_id.as_deref(), Some("bar"));

        Ok(())
    }

    #[test]
    fn parse_endpoint_from_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("options", "-ckey=1 endpoint=bar -c geqo=off"),
        ]);

        let ctx = RequestContext::test();
        let user_info = ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, None, None)?;
        assert_eq!(user_info.user, "john_doe");
        assert_eq!(user_info.endpoint_id.as_deref(), Some("bar"));

        Ok(())
    }

    #[test]
    fn parse_three_endpoints_from_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            (
                "options",
                "-ckey=1 endpoint=one endpoint=two endpoint=three -c geqo=off",
            ),
        ]);

        let ctx = RequestContext::test();
        let user_info = ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, None, None)?;
        assert_eq!(user_info.user, "john_doe");
        assert!(user_info.endpoint_id.is_none());

        Ok(())
    }

    #[test]
    fn parse_when_endpoint_and_project_are_in_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("options", "-ckey=1 endpoint=bar project=foo -c geqo=off"),
        ]);

        let ctx = RequestContext::test();
        let user_info = ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, None, None)?;
        assert_eq!(user_info.user, "john_doe");
        assert!(user_info.endpoint_id.is_none());

        Ok(())
    }

    #[test]
    fn parse_projects_identical() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([("user", "john_doe"), ("options", "project=baz")]);

        let sni = Some("baz.localhost");
        let common_names = Some(["localhost".into()].into());

        let ctx = RequestContext::test();
        let user_info =
            ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, sni, common_names.as_ref())?;
        assert_eq!(user_info.user, "john_doe");
        assert_eq!(user_info.endpoint_id.as_deref(), Some("baz"));

        Ok(())
    }

    #[test]
    fn parse_multi_common_names() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([("user", "john_doe")]);

        let common_names = Some(["a.com".into(), "b.com".into()].into());
        let sni = Some("p1.a.com");
        let ctx = RequestContext::test();
        let user_info =
            ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, sni, common_names.as_ref())?;
        assert_eq!(user_info.endpoint_id.as_deref(), Some("p1"));

        let common_names = Some(["a.com".into(), "b.com".into()].into());
        let sni = Some("p1.b.com");
        let ctx = RequestContext::test();
        let user_info =
            ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, sni, common_names.as_ref())?;
        assert_eq!(user_info.endpoint_id.as_deref(), Some("p1"));

        Ok(())
    }

    #[test]
    fn parse_projects_different() {
        let options =
            StartupMessageParams::new([("user", "john_doe"), ("options", "project=first")]);

        let sni = Some("second.localhost");
        let common_names = Some(["localhost".into()].into());

        let ctx = RequestContext::test();
        let err = ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, sni, common_names.as_ref())
            .expect_err("should fail");
        match err {
            InconsistentProjectNames { domain, option } => {
                assert_eq!(option, "first");
                assert_eq!(domain, "second");
            }
            _ => panic!("bad error: {err:?}"),
        }
    }

    #[test]
    fn parse_inconsistent_sni() {
        let options = StartupMessageParams::new([("user", "john_doe")]);

        let sni = Some("project.localhost");
        let common_names = Some(["example.com".into()].into());

        let ctx = RequestContext::test();
        let err = ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, sni, common_names.as_ref())
            .expect_err("should fail");
        match err {
            UnknownCommonName { cn } => {
                assert_eq!(cn, "localhost");
            }
            _ => panic!("bad error: {err:?}"),
        }
    }

    #[test]
    fn parse_neon_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("options", "neon_lsn:0/2 neon_endpoint_type:read_write"),
        ]);

        let sni = Some("project.localhost");
        let common_names = Some(["localhost".into()].into());
        let ctx = RequestContext::test();
        let user_info =
            ComputeUserInfoMaybeEndpoint::parse(&ctx, &options, sni, common_names.as_ref())?;
        assert_eq!(user_info.endpoint_id.as_deref(), Some("project"));
        assert_eq!(
            user_info.options.get_cache_key("project"),
            "project endpoint_type:read_write lsn:0/2"
        );

        Ok(())
    }

    #[test]
    fn test_check_peer_addr_is_in_list() {
        fn check(v: serde_json::Value) -> bool {
            let peer_addr = IpAddr::from([127, 0, 0, 1]);
            let ip_list: Vec<IpPattern> = serde_json::from_value(v).unwrap();
            check_peer_addr_is_in_list(&peer_addr, &ip_list)
        }

        assert!(check(json!([])));
        assert!(check(json!(["127.0.0.1"])));
        assert!(!check(json!(["8.8.8.8"])));
        // If there is an incorrect address, it will be skipped.
        assert!(check(json!(["88.8.8", "127.0.0.1"])));
    }
    #[test]
    fn test_parse_ip_v4() -> anyhow::Result<()> {
        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        // Ok
        assert_eq!(parse_ip_pattern("127.0.0.1")?, IpPattern::Single(peer_addr));
        assert_eq!(
            parse_ip_pattern("127.0.0.1/31")?,
            IpPattern::Subnet(ipnet::IpNet::new(peer_addr, 31)?)
        );
        assert_eq!(
            parse_ip_pattern("0.0.0.0-200.0.1.2")?,
            IpPattern::Range(IpAddr::from([0, 0, 0, 0]), IpAddr::from([200, 0, 1, 2]))
        );

        // Error
        assert!(parse_ip_pattern("300.0.1.2").is_err());
        assert!(parse_ip_pattern("30.1.2").is_err());
        assert!(parse_ip_pattern("127.0.0.1/33").is_err());
        assert!(parse_ip_pattern("127.0.0.1-127.0.3").is_err());
        assert!(parse_ip_pattern("1234.0.0.1-127.0.3.0").is_err());
        Ok(())
    }

    #[test]
    fn test_check_ipv4() -> anyhow::Result<()> {
        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let peer_addr_next = IpAddr::from([127, 0, 0, 2]);
        let peer_addr_prev = IpAddr::from([127, 0, 0, 0]);
        // Success
        assert!(check_ip(&peer_addr, &IpPattern::Single(peer_addr)));
        assert!(check_ip(
            &peer_addr,
            &IpPattern::Subnet(ipnet::IpNet::new(peer_addr_prev, 31)?)
        ));
        assert!(check_ip(
            &peer_addr,
            &IpPattern::Subnet(ipnet::IpNet::new(peer_addr_next, 30)?)
        ));
        assert!(check_ip(
            &peer_addr,
            &IpPattern::Range(IpAddr::from([0, 0, 0, 0]), IpAddr::from([200, 0, 1, 2]))
        ));
        assert!(check_ip(
            &peer_addr,
            &IpPattern::Range(peer_addr, peer_addr)
        ));

        // Not success
        assert!(!check_ip(&peer_addr, &IpPattern::Single(peer_addr_prev)));
        assert!(!check_ip(
            &peer_addr,
            &IpPattern::Subnet(ipnet::IpNet::new(peer_addr_next, 31)?)
        ));
        assert!(!check_ip(
            &peer_addr,
            &IpPattern::Range(IpAddr::from([0, 0, 0, 0]), peer_addr_prev)
        ));
        assert!(!check_ip(
            &peer_addr,
            &IpPattern::Range(peer_addr_next, IpAddr::from([128, 0, 0, 0]))
        ));
        // There is no check that for range start <= end. But it's fine as long as for all this cases the result is false.
        assert!(!check_ip(
            &peer_addr,
            &IpPattern::Range(peer_addr, peer_addr_prev)
        ));
        Ok(())
    }

    #[test]
    fn test_connection_blocker() {
        fn check(v: serde_json::Value) -> bool {
            let peer_addr = IpAddr::from([127, 0, 0, 1]);
            let ip_list: Vec<IpPattern> = serde_json::from_value(v).unwrap();
            check_peer_addr_is_in_list(&peer_addr, &ip_list)
        }

        assert!(check(json!([])));
        assert!(check(json!(["127.0.0.1"])));
        assert!(!check(json!(["255.255.255.255"])));
    }
}
