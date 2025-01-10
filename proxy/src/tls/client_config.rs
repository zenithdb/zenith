use std::sync::Arc;

use anyhow::bail;
use rustls::crypto::ring;

pub(crate) fn load_certs() -> anyhow::Result<Arc<rustls::RootCertStore>> {
    let der_certs = rustls_native_certs::load_native_certs();

    if !der_certs.errors.is_empty() {
        bail!("could not parse certificates: {:?}", der_certs.errors);
    }

    let mut store = rustls::RootCertStore::empty();
    store.add_parsable_certificates(der_certs.certs);
    Ok(Arc::new(store))
}

/// Loads the root certificates and constructs a client config suitable for connecting to the neon compute.
/// This function is blocking.
pub fn compute_client_config_with_root_certs() -> anyhow::Result<rustls::ClientConfig> {
    Ok(
        rustls::ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("ring should support the default protocol versions")
            .with_root_certificates(load_certs()?)
            .with_no_client_auth(),
    )
}

#[cfg(test)]
pub fn compute_client_config_with_certs(
    certs: impl IntoIterator<Item = rustls::pki_types::CertificateDer<'static>>,
) -> rustls::ClientConfig {
    let mut store = rustls::RootCertStore::empty();
    store.add_parsable_certificates(certs);

    rustls::ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("ring should support the default protocol versions")
        .with_root_certificates(store)
        .with_no_client_auth()
}
