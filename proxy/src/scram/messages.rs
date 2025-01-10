//! Definitions for SCRAM messages.

use std::fmt;
use std::ops::Range;

use super::base64_decode_array;
use super::key::{ScramKey, SCRAM_KEY_LEN};
use super::signature::SignatureBuilder;
use crate::sasl::ChannelBinding;

/// Faithfully taken from PostgreSQL.
pub(crate) const SCRAM_RAW_NONCE_LEN: usize = 18;

/// Although we ignore all extensions, we still have to validate the message.
fn validate_sasl_extensions<'a>(parts: impl Iterator<Item = &'a str>) -> Option<()> {
    for mut chars in parts.map(|s| s.chars()) {
        let attr = chars.next()?;
        if !attr.is_ascii_alphabetic() {
            return None;
        }
        let eq = chars.next()?;
        if eq != '=' {
            return None;
        }
    }

    Some(())
}

#[derive(Debug)]
pub(crate) struct ClientFirstMessage<'a> {
    /// `client-first-message-bare`.
    pub(crate) bare: &'a str,
    /// Channel binding mode.
    pub(crate) cbind_flag: ChannelBinding<&'a str>,
    /// Client nonce.
    pub(crate) nonce: &'a str,
}

impl<'a> ClientFirstMessage<'a> {
    // NB: FromStr doesn't work with lifetimes
    pub(crate) fn parse(input: &'a str) -> Option<Self> {
        let mut parts = input.split(',');

        let cbind_flag = ChannelBinding::parse(parts.next()?)?;

        // PG doesn't support authorization identity,
        // so we don't bother defining GS2 header type
        let authzid = parts.next()?;
        if !authzid.is_empty() {
            return None;
        }

        // Unfortunately, `parts.as_str()` is unstable
        let pos = authzid.as_ptr() as usize - input.as_ptr() as usize + 1;
        let (_, bare) = input.split_at(pos);

        // In theory, these might be preceded by "reserved-mext" (i.e. "m=")
        let username = parts.next()?.strip_prefix("n=")?;

        // https://github.com/postgres/postgres/blob/f83908798f78c4cafda217ca875602c88ea2ae28/src/backend/libpq/auth-scram.c#L13-L14
        if !username.is_empty() {
            tracing::warn!(username, "scram username provided, but is not expected");
            // TODO(conrad):
            // return None;
        }

        let nonce = parts.next()?.strip_prefix("r=")?;

        // Validate but ignore auth extensions
        validate_sasl_extensions(parts)?;

        Some(Self {
            bare,
            cbind_flag,
            nonce,
        })
    }

    /// Build a response to [`ClientFirstMessage`].
    pub(crate) fn build_server_first_message(
        &self,
        nonce: &[u8; SCRAM_RAW_NONCE_LEN],
        salt_base64: &str,
        iterations: u32,
    ) -> OwnedServerFirstMessage {
        use std::fmt::Write;

        let mut message = String::new();
        write!(&mut message, "r={}", self.nonce).unwrap();
        base64::encode_config_buf(nonce, base64::STANDARD, &mut message);
        let combined_nonce = 2..message.len();
        write!(&mut message, ",s={salt_base64},i={iterations}").unwrap();

        // This design guarantees that it's impossible to create a
        // server-first-message without receiving a client-first-message
        OwnedServerFirstMessage {
            message,
            nonce: combined_nonce,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ClientFinalMessage<'a> {
    /// `client-final-message-without-proof`.
    pub(crate) without_proof: &'a str,
    /// Channel binding data (base64).
    pub(crate) channel_binding: &'a str,
    /// Combined client & server nonce.
    pub(crate) nonce: &'a str,
    /// Client auth proof.
    pub(crate) proof: [u8; SCRAM_KEY_LEN],
}

impl<'a> ClientFinalMessage<'a> {
    // NB: FromStr doesn't work with lifetimes
    pub(crate) fn parse(input: &'a str) -> Option<Self> {
        let (without_proof, proof) = input.rsplit_once(',')?;

        let mut parts = without_proof.split(',');
        let channel_binding = parts.next()?.strip_prefix("c=")?;
        let nonce = parts.next()?.strip_prefix("r=")?;

        // Validate but ignore auth extensions
        validate_sasl_extensions(parts)?;

        let proof = base64_decode_array(proof.strip_prefix("p=")?)?;

        Some(Self {
            without_proof,
            channel_binding,
            nonce,
            proof,
        })
    }

    /// Build a response to [`ClientFinalMessage`].
    pub(crate) fn build_server_final_message(
        &self,
        signature_builder: SignatureBuilder<'_>,
        server_key: &ScramKey,
    ) -> String {
        let mut buf = String::from("v=");
        base64::encode_config_buf(
            signature_builder.build(server_key),
            base64::STANDARD,
            &mut buf,
        );

        buf
    }
}

/// We need to keep a convenient representation of this
/// message for the next authentication step.
pub(crate) struct OwnedServerFirstMessage {
    /// Owned `server-first-message`.
    message: String,
    /// Slice into `message`.
    nonce: Range<usize>,
}

impl OwnedServerFirstMessage {
    /// Extract combined nonce from the message.
    #[inline(always)]
    pub(crate) fn nonce(&self) -> &str {
        &self.message[self.nonce.clone()]
    }

    /// Get reference to a text representation of the message.
    #[inline(always)]
    pub(crate) fn as_str(&self) -> &str {
        &self.message
    }
}

impl fmt::Debug for OwnedServerFirstMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerFirstMessage")
            .field("message", &self.as_str())
            .field("nonce", &self.nonce())
            .finish()
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_client_first_message() {
        use ChannelBinding::*;

        // (Almost) real strings captured during debug sessions
        let cases = [
            (NotSupportedClient, "n,,n=,r=t8JwklwKecDLwSsA72rHmVju"),
            (NotSupportedServer, "y,,n=,r=t8JwklwKecDLwSsA72rHmVju"),
            (
                Required("tls-server-end-point"),
                "p=tls-server-end-point,,n=,r=t8JwklwKecDLwSsA72rHmVju",
            ),
        ];

        for (cb, input) in cases {
            let msg = ClientFirstMessage::parse(input).unwrap();

            assert_eq!(msg.bare, "n=,r=t8JwklwKecDLwSsA72rHmVju");
            assert_eq!(msg.nonce, "t8JwklwKecDLwSsA72rHmVju");
            assert_eq!(msg.cbind_flag, cb);
        }
    }

    #[test]
    fn parse_client_first_message_with_invalid_gs2_authz() {
        assert!(ClientFirstMessage::parse("n,authzid,n=,r=nonce").is_none());
    }

    #[test]
    fn parse_client_first_message_with_extra_params() {
        let msg = ClientFirstMessage::parse("n,,n=,r=nonce,a=foo,b=bar,c=baz").unwrap();
        assert_eq!(msg.bare, "n=,r=nonce,a=foo,b=bar,c=baz");
        assert_eq!(msg.nonce, "nonce");
        assert_eq!(msg.cbind_flag, ChannelBinding::NotSupportedClient);
    }

    #[test]
    fn parse_client_first_message_with_extra_params_invalid() {
        // must be of the form `<ascii letter>=<...>`
        assert!(ClientFirstMessage::parse("n,,n=,r=nonce,abc=foo").is_none());
        assert!(ClientFirstMessage::parse("n,,n=,r=nonce,1=foo").is_none());
        assert!(ClientFirstMessage::parse("n,,n=,r=nonce,a").is_none());
    }

    #[test]
    fn parse_client_final_message() {
        let input = [
            "c=eSws",
            "r=iiYEfS3rOgn8S3rtpSdrOsHtPLWvIkdgmHxA0hf3JNOAG4dU",
            "p=SRpfsIVS4Gk11w1LqQ4QvCUBZYQmqXNSDEcHqbQ3CHI=",
        ]
        .join(",");

        let msg = ClientFinalMessage::parse(&input).unwrap();
        assert_eq!(
            msg.without_proof,
            "c=eSws,r=iiYEfS3rOgn8S3rtpSdrOsHtPLWvIkdgmHxA0hf3JNOAG4dU"
        );
        assert_eq!(
            msg.nonce,
            "iiYEfS3rOgn8S3rtpSdrOsHtPLWvIkdgmHxA0hf3JNOAG4dU"
        );
        assert_eq!(
            base64::encode(msg.proof),
            "SRpfsIVS4Gk11w1LqQ4QvCUBZYQmqXNSDEcHqbQ3CHI="
        );
    }
}
