//! Client authentication mechanisms.

pub mod backend;
pub use backend::ServerlessBackend;

mod credentials;
pub(crate) use credentials::{
    check_peer_addr_is_in_list, endpoint_sni, ComputeUserInfoMaybeEndpoint,
    ComputeUserInfoParseError, IpPattern,
};

mod password_hack;
pub(crate) use password_hack::parse_endpoint_param;
use password_hack::PasswordHackPayload;

mod flow;
use std::io;
use std::net::IpAddr;

pub(crate) use flow::*;
use thiserror::Error;
use tokio::time::error::Elapsed;

use crate::auth::backend::jwt::JwtError;
use crate::control_plane;
use crate::error::{ReportableError, UserFacingError};

/// Convenience wrapper for the authentication error.
pub(crate) type Result<T> = std::result::Result<T, AuthError>;

/// Common authentication error.
#[derive(Debug, Error)]
pub(crate) enum AuthError {
    #[error(transparent)]
    Web(#[from] backend::WebAuthError),

    #[error(transparent)]
    GetAuthInfo(#[from] control_plane::errors::GetAuthInfoError),

    /// SASL protocol errors (includes [SCRAM](crate::scram)).
    #[error(transparent)]
    Sasl(#[from] crate::sasl::Error),

    #[error("Unsupported authentication method: {0}")]
    BadAuthMethod(Box<str>),

    #[error("Malformed password message: {0}")]
    MalformedPassword(&'static str),

    #[error(
        "Endpoint ID is not specified. \
        Either please upgrade the postgres client library (libpq) for SNI support \
        or pass the endpoint ID (first part of the domain name) as a parameter: '?options=endpoint%3D<endpoint-id>'. \
        See more at https://neon.tech/sni"
    )]
    MissingEndpointName,

    #[error("password authentication failed for user '{0}'")]
    PasswordFailed(Box<str>),

    /// Errors produced by e.g. [`crate::stream::PqStream`].
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(
        "This IP address {0} is not allowed to connect to this endpoint. \
        Please add it to the allowed list in the Neon console. \
        Make sure to check for IPv4 or IPv6 addresses."
    )]
    IpAddressNotAllowed(IpAddr),

    #[error("Too many connections to this endpoint. Please try again later.")]
    TooManyConnections,

    #[error("Authentication timed out")]
    UserTimeout(Elapsed),

    #[error("Disconnected due to inactivity after {0}.")]
    ConfirmationTimeout(humantime::Duration),

    #[error(transparent)]
    Jwt(#[from] JwtError),
}

impl AuthError {
    pub(crate) fn bad_auth_method(name: impl Into<Box<str>>) -> Self {
        AuthError::BadAuthMethod(name.into())
    }

    pub(crate) fn password_failed(user: impl Into<Box<str>>) -> Self {
        AuthError::PasswordFailed(user.into())
    }

    pub(crate) fn ip_address_not_allowed(ip: IpAddr) -> Self {
        AuthError::IpAddressNotAllowed(ip)
    }

    pub(crate) fn too_many_connections() -> Self {
        AuthError::TooManyConnections
    }

    pub(crate) fn is_password_failed(&self) -> bool {
        matches!(self, AuthError::PasswordFailed(_))
    }

    pub(crate) fn user_timeout(elapsed: Elapsed) -> Self {
        AuthError::UserTimeout(elapsed)
    }

    pub(crate) fn confirmation_timeout(timeout: humantime::Duration) -> Self {
        AuthError::ConfirmationTimeout(timeout)
    }
}

impl UserFacingError for AuthError {
    fn to_string_client(&self) -> String {
        match self {
            Self::Web(e) => e.to_string_client(),
            Self::GetAuthInfo(e) => e.to_string_client(),
            Self::Sasl(e) => e.to_string_client(),
            Self::PasswordFailed(_) => self.to_string(),
            Self::BadAuthMethod(_) => self.to_string(),
            Self::MalformedPassword(_) => self.to_string(),
            Self::MissingEndpointName => self.to_string(),
            Self::Io(_) => "Internal error".to_string(),
            Self::IpAddressNotAllowed(_) => self.to_string(),
            Self::TooManyConnections => self.to_string(),
            Self::UserTimeout(_) => self.to_string(),
            Self::ConfirmationTimeout(_) => self.to_string(),
            Self::Jwt(_) => self.to_string(),
        }
    }
}

impl ReportableError for AuthError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            Self::Web(e) => e.get_error_kind(),
            Self::GetAuthInfo(e) => e.get_error_kind(),
            Self::Sasl(e) => e.get_error_kind(),
            Self::PasswordFailed(_) => crate::error::ErrorKind::User,
            Self::BadAuthMethod(_) => crate::error::ErrorKind::User,
            Self::MalformedPassword(_) => crate::error::ErrorKind::User,
            Self::MissingEndpointName => crate::error::ErrorKind::User,
            Self::Io(_) => crate::error::ErrorKind::ClientDisconnect,
            Self::IpAddressNotAllowed(_) => crate::error::ErrorKind::User,
            Self::TooManyConnections => crate::error::ErrorKind::RateLimit,
            Self::UserTimeout(_) => crate::error::ErrorKind::User,
            Self::ConfirmationTimeout(_) => crate::error::ErrorKind::User,
            Self::Jwt(_) => crate::error::ErrorKind::User,
        }
    }
}
