use anyhow::bail;

/// A [url](url::Url) type with additional guarantees.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiUrl(url::Url);

impl ApiUrl {
    /// Consume the wrapper and return inner [url](url::Url).
    pub(crate) fn into_inner(self) -> url::Url {
        self.0
    }

    /// See [`url::Url::path_segments_mut`].
    pub(crate) fn path_segments_mut(&mut self) -> url::PathSegmentsMut<'_> {
        // We've already verified that it works during construction.
        self.0.path_segments_mut().expect("bad API url")
    }
}

/// This instance imposes additional requirements on the url.
impl std::str::FromStr for ApiUrl {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let mut url: url::Url = s.parse()?;

        // Make sure that we can build upon this URL.
        if url.path_segments_mut().is_err() {
            bail!("bad API url provided");
        }

        Ok(Self(url))
    }
}

/// This instance is safe because it doesn't allow us to modify the object.
impl std::ops::Deref for ApiUrl {
    type Target = url::Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for ApiUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn bad_url() {
        let url = "test:foobar";
        url.parse::<url::Url>().expect("unexpected parsing failure");
        url.parse::<ApiUrl>().expect_err("should not parse");
    }

    #[test]
    fn good_url() {
        let url = "test://foobar";
        let mut a = url.parse::<url::Url>().expect("unexpected parsing failure");
        let mut b = url.parse::<ApiUrl>().expect("unexpected parsing failure");

        a.path_segments_mut().unwrap().push("method");
        b.path_segments_mut().push("method");

        assert_eq!(a, b.into_inner());
    }
}
