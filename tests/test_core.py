import json
import pytest
from moto import mock_aws
import boto3
from s3nosqldb import S3NoSQLDB, CollectionError, DocumentError

@pytest.fixture
def s3():
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="test-bucket")
        yield s3

@pytest.fixture
def db(s3):
    return S3NoSQLDB(
        bucket_name="test-bucket",
        base_path="test",
        batch_size=2,
        max_workers=2
    )

def test_create_collection(db):
    db.create_collection("test_collection")
    metadata = json.loads(
        db.s3.get_object(
            Bucket="test-bucket",
            Key="test/metadata.json"
        )["Body"].read()
    )
    assert "test_collection" in metadata["collections"]

def test_create_duplicate_collection(db):
    db.create_collection("test_collection")
    with pytest.raises(CollectionError):
        db.create_collection("test_collection")

def test_insert_batch(db):
    db.create_collection("test_collection")
    data = [
        {"id": "1", "name": "Test 1"},
        {"id": "2", "name": "Test 2"}
    ]
    db.insert_batch("test_collection", data)
    results = db.query("test_collection")
    assert len(results) == 2
    assert results[0]["name"] == "Test 1"
    assert results[1]["name"] == "Test 2"

def test_insert_duplicate_id(db):
    db.create_collection("test_collection")
    data = [
        {"id": "1", "name": "Test 1"},
        {"id": "1", "name": "Test 2"}
    ]
    with pytest.raises(DocumentError):
        db.insert_batch("test_collection", data)

def test_query_with_filter(db):
    db.create_collection("test_collection")
    data = [
        {"id": "1", "value": 10},
        {"id": "2", "value": 20},
        {"id": "3", "value": 30}
    ]
    db.insert_batch("test_collection", data)
    
    def filter_fn(doc):
        return doc["value"] > 15
    
    results = db.query("test_collection", filter_fn=filter_fn)
    assert len(results) == 2
    assert all(r["value"] > 15 for r in results)

def test_delete_collection(db):
    db.create_collection("test_collection")
    data = [{"id": "1", "name": "Test"}]
    db.insert_batch("test_collection", data)
    
    db.delete_collection("test_collection")
    metadata = json.loads(
        db.s3.get_object(
            Bucket="test-bucket",
            Key="test/metadata.json"
        )["Body"].read()
    )
    assert "test_collection" not in metadata["collections"]
