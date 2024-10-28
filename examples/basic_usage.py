from s3nosql import S3NoSQLDB

def main():
    """Example usage of S3NoSQL database."""
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
        "email": "user@example.com",
        "name": "John Doe",
        "age": 30
    }
    doc_id = db.insert("users", user)
    print(f"Inserted document with ID: {doc_id}")

    # Find a document
    result = db.find_one("users", {"email": "user@example.com"})
    print(f"Found user: {result}")

    # Batch insert
    users = [
        {"email": f"user{i}@example.com", "name": f"User {i}", "age": 20 + i}
        for i in range(5)
    ]
    doc_ids = db.batch_insert("users", users)
    print(f"Inserted {len(doc_ids)} documents")

    # Stream find
    for batch in db.stream_find("users", {"age": 25}, batch_size=2):
        for user in batch:
            print(f"Found user: {user}")

if __name__ == "__main__":
    main()
