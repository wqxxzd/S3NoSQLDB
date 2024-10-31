from .core import S3NoSQLDB
from .exceptions import CollectionError, DocumentError

__version__ = "0.1.0"
__all__ = ["S3NoSQLDB", "CollectionError", "DocumentError"]
