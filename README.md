# S3NoSQL

A NoSQL database implementation using Amazon S3 as storage.

## Features

- Document-based storage using S3
- Collection management with indexing
- Batch operations support
- Compression support
- Thread-safe operations
- Streaming query results

## Installation

```bash
pip install s3nosql
```

## Quick Start

```python
from s3nosql import S3NoSQLDB

# Initialize the database
db = S3NoSQLDB(
    bucket_name="my-database",
    base_path="production",
    region_name="us-east-1"
)

# Create a collection with an index
db.create_collection("users", indexes=["email"])

# Insert a document
user = {
    "email": "dan.zhang0224@gmail.com",
    "name": "Dan Zhang"
}
doc_id = db.insert("users", user)

# Find a document
result = db.find_one("users", {"email": "dan.zhang0224@gmail.com"})
print(f"Found user: {result}")
```

## Documentation

For full documentation, visit [docs/](docs/).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
