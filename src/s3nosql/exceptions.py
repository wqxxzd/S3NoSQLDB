class S3NoSQLError(Exception):
    """Base exception for S3NoSQL."""
    pass

class CollectionError(S3NoSQLError):
    """Raised when there are issues with collections."""
    pass

class DocumentError(S3NoSQLError):
    """Raised when there are issues with documents."""
    pass
