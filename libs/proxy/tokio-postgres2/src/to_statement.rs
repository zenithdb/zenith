use crate::to_statement::private::{Sealed, ToStatementType};
use crate::Statement;

mod private {
    use crate::{Client, Error, Statement};

    pub trait Sealed {}

    pub enum ToStatementType<'a> {
        Statement(&'a Statement),
        Query(&'a str),
    }

    impl ToStatementType<'_> {
        pub async fn into_statement(self, client: &Client) -> Result<Statement, Error> {
            match self {
                ToStatementType::Statement(s) => Ok(s.clone()),
                ToStatementType::Query(s) => client.prepare(s).await,
            }
        }
    }
}

/// A trait abstracting over prepared and unprepared statements.
///
/// Many methods are generic over this bound, so that they support both a raw query string as well as a statement which
/// was prepared previously.
///
/// This trait is "sealed" and cannot be implemented by anything outside this crate.
pub trait ToStatement: Sealed {
    #[doc(hidden)]
    fn __convert(&self) -> ToStatementType<'_>;
}

impl ToStatement for Statement {
    fn __convert(&self) -> ToStatementType<'_> {
        ToStatementType::Statement(self)
    }
}

impl Sealed for Statement {}

impl ToStatement for str {
    fn __convert(&self) -> ToStatementType<'_> {
        ToStatementType::Query(self)
    }
}

impl Sealed for str {}

impl ToStatement for String {
    fn __convert(&self) -> ToStatementType<'_> {
        ToStatementType::Query(self)
    }
}

impl Sealed for String {}
