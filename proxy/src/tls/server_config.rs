use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context};
use itertools::Itertools;
use rustls::crypto::ring::{self, sign};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use super::{TlsServerEndPoint, PG_ALPN_PROTOCOL};

pub struct TlsConfig {
    pub config: Arc<rustls::ServerConfig>,
    pub common_names: HashSet<String>,
    pub cert_resolver: Arc<CertResolver>,
}

impl TlsConfig {
    pub fn to_server_config(&self) -> Arc<rustls::ServerConfig> {
        self.config.clone()
    }
}

/// Configure TLS for the main endpoint.
pub fn configure_tls(
    key_path: &str,
    cert_path: &str,
    certs_dir: Option<&String>,
    allow_tls_keylogfile: bool,
) -> anyhow::Result<TlsConfig> {
    let mut cert_resolver = CertResolver::new();

    // add default certificate
    cert_resolver.add_cert_path(key_path, cert_path, true)?;

    // add extra certificates
    if let Some(certs_dir) = certs_dir {
        for entry in std::fs::read_dir(certs_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                // file names aligned with default cert-manager names
                let key_path = path.join("tls.key");
                let cert_path = path.join("tls.crt");
                if key_path.exists() && cert_path.exists() {
                    cert_resolver.add_cert_path(
                        &key_path.to_string_lossy(),
                        &cert_path.to_string_lossy(),
                        false,
                    )?;
                }
            }
        }
    }

    let common_names = cert_resolver.get_common_names();

    let cert_resolver = Arc::new(cert_resolver);

    // allow TLS 1.2 to be compatible with older client libraries
    let mut config =
        rustls::ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .context("ring should support TLS1.2 and TLS1.3")?
            .with_no_client_auth()
            .with_cert_resolver(cert_resolver.clone());

    config.alpn_protocols = vec![PG_ALPN_PROTOCOL.to_vec()];

    if allow_tls_keylogfile {
        // KeyLogFile will check for the SSLKEYLOGFILE environment variable.
        config.key_log = Arc::new(rustls::KeyLogFile::new());
    }

    Ok(TlsConfig {
        config: Arc::new(config),
        common_names,
        cert_resolver,
    })
}

#[derive(Default, Debug)]
pub struct CertResolver {
    certs: HashMap<String, (Arc<rustls::sign::CertifiedKey>, TlsServerEndPoint)>,
    default: Option<(Arc<rustls::sign::CertifiedKey>, TlsServerEndPoint)>,
}

impl CertResolver {
    pub fn new() -> Self {
        Self::default()
    }

    fn add_cert_path(
        &mut self,
        key_path: &str,
        cert_path: &str,
        is_default: bool,
    ) -> anyhow::Result<()> {
        let priv_key = {
            let key_bytes = std::fs::read(key_path)
                .with_context(|| format!("Failed to read TLS keys at '{key_path}'"))?;
            rustls_pemfile::private_key(&mut &key_bytes[..])
                .with_context(|| format!("Failed to parse TLS keys at '{key_path}'"))?
                .with_context(|| format!("Failed to parse TLS keys at '{key_path}'"))?
        };

        let cert_chain_bytes = std::fs::read(cert_path)
            .context(format!("Failed to read TLS cert file at '{cert_path}.'"))?;

        let cert_chain = {
            rustls_pemfile::certs(&mut &cert_chain_bytes[..])
                .try_collect()
                .with_context(|| {
                    format!("Failed to read TLS certificate chain from bytes from file at '{cert_path}'.")
                })?
        };

        self.add_cert(priv_key, cert_chain, is_default)
    }

    pub fn add_cert(
        &mut self,
        priv_key: PrivateKeyDer<'static>,
        cert_chain: Vec<CertificateDer<'static>>,
        is_default: bool,
    ) -> anyhow::Result<()> {
        let key = sign::any_supported_type(&priv_key).context("invalid private key")?;

        let first_cert = &cert_chain[0];
        let tls_server_end_point = TlsServerEndPoint::new(first_cert)?;
        let pem = x509_parser::parse_x509_certificate(first_cert)
            .context("Failed to parse PEM object from cerficiate")?
            .1;

        let common_name = pem.subject().to_string();

        // We need to get the canonical name for this certificate so we can match them against any domain names
        // seen within the proxy codebase.
        //
        // In scram-proxy we use wildcard certificates only, with the database endpoint as the wildcard subdomain, taken from SNI.
        // We need to remove the wildcard prefix for the purposes of certificate selection.
        //
        // auth-broker does not use SNI and instead uses the Neon-Connection-String header.
        // Auth broker has the subdomain `apiauth` we need to remove for the purposes of validating the Neon-Connection-String.
        //
        // Console Redirect proxy does not use any wildcard domains and does not need any certificate selection or conn string
        // validation, so let's we can continue with any common-name
        let common_name = if let Some(s) = common_name.strip_prefix("CN=*.") {
            s.to_string()
        } else if let Some(s) = common_name.strip_prefix("CN=apiauth.") {
            s.to_string()
        } else if let Some(s) = common_name.strip_prefix("CN=") {
            s.to_string()
        } else {
            bail!("Failed to parse common name from certificate")
        };

        let cert = Arc::new(rustls::sign::CertifiedKey::new(cert_chain, key));

        if is_default {
            self.default = Some((cert.clone(), tls_server_end_point));
        }

        self.certs.insert(common_name, (cert, tls_server_end_point));

        Ok(())
    }

    pub fn get_common_names(&self) -> HashSet<String> {
        self.certs.keys().map(|s| s.to_string()).collect()
    }
}

impl rustls::server::ResolvesServerCert for CertResolver {
    fn resolve(
        &self,
        client_hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        self.resolve(client_hello.server_name()).map(|x| x.0)
    }
}

impl CertResolver {
    pub fn resolve(
        &self,
        server_name: Option<&str>,
    ) -> Option<(Arc<rustls::sign::CertifiedKey>, TlsServerEndPoint)> {
        // loop here and cut off more and more subdomains until we find
        // a match to get a proper wildcard support. OTOH, we now do not
        // use nested domains, so keep this simple for now.
        //
        // With the current coding foo.com will match *.foo.com and that
        // repeats behavior of the old code.
        if let Some(mut sni_name) = server_name {
            loop {
                if let Some(cert) = self.certs.get(sni_name) {
                    return Some(cert.clone());
                }
                if let Some((_, rest)) = sni_name.split_once('.') {
                    sni_name = rest;
                } else {
                    return None;
                }
            }
        } else {
            // No SNI, use the default certificate, otherwise we can't get to
            // options parameter which can be used to set endpoint name too.
            // That means that non-SNI flow will not work for CNAME domains in
            // verify-full mode.
            //
            // If that will be a problem we can:
            //
            // a) Instead of multi-cert approach use single cert with extra
            //    domains listed in Subject Alternative Name (SAN).
            // b) Deploy separate proxy instances for extra domains.
            self.default.clone()
        }
    }
}
