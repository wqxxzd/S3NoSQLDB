# S3NoSQLDB

A NoSQL Database Implementation using Amazon S3 for storage.

## Features

- Create and manage collections in S3 buckets
- Batch document insertion with automatic ID generation
- Concurrent processing using ThreadPoolExecutor
- Query documents using custom filter functions
- Parquet file format with gzip compression

## Installation

```bash
pip install s3nosqldb
```

## Quick Start

```python
from s3nosqldb import S3NoSQLDB

# Initialize the database
db = S3NoSQLDB(
    bucket_name="your-bucket-name",
    base_path="optional/path",
    batch_size=50000,
    max_workers=4
)

# Create a collection
db.create_collection("users", id_field="user_id", auto_generate_id=True)

# Insert documents
users = [
    {"name": "John Doe", "age": 30},
    {"name": "Jane Smith", "age": 25}
]
db.insert_batch("users", users)

# Query documents
def age_filter(doc):
    return doc["age"] > 25

results = db.query("users", filter_fn=age_filter)
```

## Development

1. Clone the repository:
```bash
git clone https://github.com/wqxxzd/s3nosqldb.git
cd s3nosqldb
```

2. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .[dev]
```

3. Run tests:
```bash
pytest
```

## License

This project is licensed under the MIT License.

## Badges

[![codecov](https://codecov.io/github/wqxxzd/S3NoSQLDB/graph/badge.svg?token=l8wT7p6YxQ)](https://codecov.io/github/wqxxzd/S3NoSQLDB)
