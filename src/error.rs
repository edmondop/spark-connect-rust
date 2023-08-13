use std::error;
use std::fmt;
use tonic::Status;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkSessionCreationError(String);

impl SparkSessionCreationError {
    #[inline]
    fn message(&self) -> String {
        format!("Error creating SparkSession {}", self.0)
    }
}

impl fmt::Display for SparkSessionCreationError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_str())
    }
}

impl error::Error for SparkSessionCreationError {}

impl From<tonic::transport::Error> for SparkSessionCreationError {
    fn from(err: tonic::transport::Error) -> Self {
        SparkSessionCreationError(err.to_string())
    }
}
/// Sum type of all errors possibly returned from Spark operations.
#[derive(Debug, Clone)]
pub enum SparkError {
    DeserializationFailed(DeserializationError),
    EmptyResponse,
    Generic(GenericError),
    HiveCatalogNotEnabled(HiveCatalogNotEnabledError),
    InvalidSyntax(ParseSyntaxError),
    NotImplementedYet(NotImplementedYetError),
    Unexpected(UnexpectedError),
    TableOrViewNotFound(TableOrViewNotFoundError),
    UnresolvedColumnWithSuggestion(UnresolvedColumnWithSuggestionError),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeserializationError(pub String);

impl From<DeserializationError> for SparkError {
    #[inline]
    fn from(err: DeserializationError) -> Self {
        Self::DeserializationFailed(err)
    }
}

impl fmt::Display for DeserializationError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}
impl error::Error for DeserializationError {}

impl DeserializationError {
    #[inline]
    fn message(&self) -> &'static str {
        "Table or view not found"
    }
}

#[derive(Debug, Clone)]
pub struct GenericError(Status);

impl From<GenericError> for SparkError {
    #[inline]
    fn from(err: GenericError) -> Self {
        Self::Generic(err)
    }
}

impl fmt::Display for GenericError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_str())
    }
}
impl error::Error for GenericError {}

impl GenericError {
    #[inline]
    fn message(&self) -> String {
        String::from(self.0.message())
    }
}

#[derive(Debug, Clone)]
pub struct HiveCatalogNotEnabledError(Status);

impl From<HiveCatalogNotEnabledError> for SparkError {
    #[inline]
    fn from(err: HiveCatalogNotEnabledError) -> Self {
        Self::HiveCatalogNotEnabled(err)
    }
}

impl HiveCatalogNotEnabledError {
    #[inline]
    fn message(&self) -> &'static str {
        "Hive Catalog not enabled"
    }
}

impl fmt::Display for HiveCatalogNotEnabledError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl error::Error for HiveCatalogNotEnabledError {}

#[derive(Debug, Clone)]
pub struct NotImplementedYetError(pub(crate) String);

impl From<NotImplementedYetError> for SparkError {
    #[inline]
    fn from(err: NotImplementedYetError) -> Self {
        Self::NotImplementedYet(err)
    }
}

impl NotImplementedYetError {
    #[inline]
    fn message(&self) -> String {
        self.0.clone()
    }
}

impl fmt::Display for NotImplementedYetError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_str())
    }
}

impl error::Error for NotImplementedYetError {}

#[derive(Debug, Clone)]
pub struct ParseSyntaxError(Status);

impl From<ParseSyntaxError> for SparkError {
    #[inline]
    fn from(err: ParseSyntaxError) -> Self {
        Self::InvalidSyntax(err)
    }
}

impl fmt::Display for ParseSyntaxError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_str())
    }
}
impl error::Error for ParseSyntaxError {}

impl ParseSyntaxError {
    #[inline]
    fn message(&self) -> String {
        String::from(self.0.message())
    }
}

#[derive(Debug, Clone)]
pub struct TableOrViewNotFoundError(Status);

impl From<TableOrViewNotFoundError> for SparkError {
    #[inline]
    fn from(err: TableOrViewNotFoundError) -> Self {
        Self::TableOrViewNotFound(err)
    }
}

impl fmt::Display for TableOrViewNotFoundError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}
impl error::Error for TableOrViewNotFoundError {}

impl TableOrViewNotFoundError {
    #[inline]
    fn message(&self) -> &'static str {
        "Table or view not found"
    }
}

#[derive(Debug, Clone)]
pub struct UnexpectedError(pub(crate) String);

impl From<UnexpectedError> for SparkError {
    #[inline]
    fn from(err: UnexpectedError) -> Self {
        Self::Unexpected(err)
    }
}

impl fmt::Display for UnexpectedError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_str())
    }
}
impl error::Error for UnexpectedError {}

impl UnexpectedError {
    #[inline]
    fn message(&self) -> String {
        self.0.clone()
    }
}

#[derive(Debug, Clone)]
pub struct UnresolvedColumnWithSuggestionError(Status);

impl From<UnresolvedColumnWithSuggestionError> for SparkError {
    #[inline]
    fn from(err: UnresolvedColumnWithSuggestionError) -> Self {
        Self::UnresolvedColumnWithSuggestion(err)
    }
}

impl fmt::Display for UnresolvedColumnWithSuggestionError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_str())
    }
}
impl error::Error for UnresolvedColumnWithSuggestionError {}

impl UnresolvedColumnWithSuggestionError {
    #[inline]
    fn message(&self) -> String {
        String::from(self.0.message())
    }
}

impl error::Error for SparkError {
    #[inline]
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::DeserializationFailed(ref err) => Some(err),
            Self::EmptyResponse => None,
            Self::Generic(ref err) => Some(err),
            Self::HiveCatalogNotEnabled(ref err) => Some(err),
            Self::InvalidSyntax(ref err) => Some(err),
            Self::NotImplementedYet(ref err) => Some(err),
            Self::TableOrViewNotFound(ref err) => Some(err),
            Self::Unexpected(_) => None,
            Self::UnresolvedColumnWithSuggestion(ref err) => Some(err),
        }
    }
}

impl fmt::Display for SparkError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeserializationFailed(err) => err.fmt(f),
            Self::EmptyResponse => f.write_str("Empty response"),
            Self::Generic(err) => err.fmt(f),
            Self::HiveCatalogNotEnabled(err) => err.fmt(f),
            Self::InvalidSyntax(ref err) => err.fmt(f),
            Self::NotImplementedYet(ref err) => err.fmt(f),
            Self::TableOrViewNotFound(err) => err.fmt(f),
            Self::Unexpected(err) => f.write_str(err.0.as_str()),
            Self::UnresolvedColumnWithSuggestion(ref err) => err.fmt(f),
        }
    }
}

type ErrorFactory = fn(err: Status) -> SparkError;

static ERROR_MAPPINGS: &[(&str, ErrorFactory)] = &[
    ("[TABLE_OR_VIEW_NOT_FOUND]", |err| {
        SparkError::TableOrViewNotFound(TableOrViewNotFoundError(err))
    }),
    ("[HIVE_CATALOG_NOT_ENABLED]", |err| {
        SparkError::HiveCatalogNotEnabled(HiveCatalogNotEnabledError(err))
    }),
    ("[PARSE_SYNTAX_ERROR]", |err| {
        SparkError::InvalidSyntax(ParseSyntaxError(err))
    }),
    ("[UNRESOLVED_COLUMN.WITH_SUGGESTION]", |err| {
        SparkError::UnresolvedColumnWithSuggestion(UnresolvedColumnWithSuggestionError(err))
    }),
];

impl From<Status> for SparkError {
    #[inline]
    fn from(err: Status) -> Self {
        for (tag, error_constructor) in ERROR_MAPPINGS {
            if err.message().contains(tag) {
                return error_constructor(err);
            }
        }
        Self::Generic(GenericError(err))
    }
}
