import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Any, Callable, Dict, List, Optional

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from .exceptions import CollectionError, DocumentError


class S3NoSQLDB:
    def __init__(
        self,
        bucket_name: str,
        base_path: str = "",
        metadata_key: str = "metadata.json",
        batch_size: int = 50000,
        max_workers: int = 4,
    ) -> None:
        self.s3 = boto3.client("s3")
        self.bucket_name = bucket_name
        self.base_path = base_path.strip("/")
        self.metadata_key = os.path.join(self.base_path, metadata_key) if self.base_path else metadata_key
        self.batch_size = batch_size
        self.max_workers = max_workers

    def _get_full_path(self, key: str) -> str:
        return os.path.join(self.base_path, key) if self.base_path else key

    def _load_metadata(self) -> Dict[str, Any]:
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=self.metadata_key)
            return json.loads(response["Body"].read().decode())
        except self.s3.exceptions.NoSuchKey:
            return {"collections": {}}
        except Exception as e:
            msg = f"Failed to load metadata: {str(e)}"
            raise CollectionError(msg) from e

    def _save_metadata(self, metadata: Dict[str, Any]) -> None:
        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=self.metadata_key,
                Body=json.dumps(metadata).encode(),
            )
        except Exception as e:
            msg = f"Failed to save metadata: {str(e)}"
            raise CollectionError(msg) from e

    def create_collection(
        self, name: str, id_field: str = "id", auto_generate_id: bool = True
    ) -> None:
        if not name:
            msg = "Collection name cannot be empty"
            raise CollectionError(msg)
            
        metadata = self._load_metadata()
        if name in metadata["collections"]:
            msg = f"Collection '{name}' already exists"
            raise CollectionError(msg)
            
        metadata["collections"][name] = {
            "count": 0,
            "files": [],
            "used_ids": [],
            "id_field": id_field,
            "auto_generate_id": auto_generate_id,
        }
        self._save_metadata(metadata)

    def _validate_and_prepare_data(self, collection: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        metadata = self._load_metadata()
        if collection not in metadata["collections"]:
            msg = f"Collection '{collection}' does not exist"
            raise CollectionError(msg)
            
        used_ids = set(metadata["collections"][collection]["used_ids"])
        id_field = metadata["collections"][collection]["id_field"]
        auto_generate = metadata["collections"][collection]["auto_generate_id"]
        prepared_data = []
        new_ids = set()

        for item in data:
            if id_field not in item:
                if not auto_generate:
                    msg = f"Missing required ID field '{id_field}'"
                    raise DocumentError(msg)
                item[id_field] = str(uuid.uuid4())

            if item[id_field] in used_ids or item[id_field] in new_ids:
                msg = f"Duplicate ID found: {item[id_field]}"
                raise DocumentError(msg)

            new_ids.add(item[id_field])
            prepared_data.append(item)

        return prepared_data

    def _upload_batch(self, collection: str, batch_data: List[Dict[str, Any]], file_id: int) -> str:
        try:
            key = self._get_full_path(f"{collection}/{file_id}.parquet.gz")
            table = pa.Table.from_pylist(batch_data)
            buffer = BytesIO()
            pq.write_table(table, buffer, compression="gzip")
            buffer.seek(0)
            self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=buffer.getvalue())
            return key
        except Exception as e:
            msg = f"Failed to upload batch: {str(e)}"
            raise DocumentError(msg) from e

    def insert_batch(self, collection: str, data: List[Dict[str, Any]]) -> None:
        if not data:
            return

        prepared_data = self._validate_and_prepare_data(collection, data)
        metadata = self._load_metadata()
        id_field = metadata["collections"][collection]["id_field"]

        start_file_id = metadata["collections"][collection]["count"]
        batches = [
            (collection, prepared_data[i : i + self.batch_size], start_file_id + i // self.batch_size)
            for i in range(0, len(prepared_data), self.batch_size)
        ]

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                new_files = [
                    future.result()
                    for future in [executor.submit(self._upload_batch, *batch) for batch in batches]
                ]

            metadata["collections"][collection]["files"].extend(new_files)
            metadata["collections"][collection]["count"] += len(batches)
            metadata["collections"][collection]["used_ids"].extend(
                [item[id_field] for item in prepared_data]
            )
            self._save_metadata(metadata)
        except Exception as e:
            msg = f"Failed to insert batch: {str(e)}"
            raise DocumentError(msg) from e

    def _process_file(
        self, file_key: str, filter_fn: Optional[Callable[[Dict[str, Any]], bool]]
    ) -> List[Dict[str, Any]]:
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
            table = pq.read_table(BytesIO(response["Body"].read()))
            chunk_data = table.to_pylist()
            return [item for item in chunk_data if filter_fn(item)] if filter_fn else chunk_data
        except Exception as e:
            msg = f"Failed to process file {file_key}: {str(e)}"
            raise DocumentError(msg) from e

    def query(
        self, collection: str, filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None
    ) -> List[Dict[str, Any]]:
        metadata = self._load_metadata()
        if collection not in metadata["collections"]:
            msg = f"Collection '{collection}' does not exist"
            raise CollectionError(msg)
            
        files = metadata["collections"][collection]["files"]

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = []
                for chunk in [
                    future.result()
                    for future in [
                        executor.submit(self._process_file, file_key, filter_fn) for file_key in files
                    ]
                ]:
                    results.extend(chunk)
            return results
        except Exception as e:
            msg = f"Failed to query collection: {str(e)}"
            raise DocumentError(msg) from e

    def delete_collection(self, collection: str) -> None:
        metadata = self._load_metadata()
        if collection not in metadata["collections"]:
            msg = f"Collection '{collection}' does not exist"
            raise CollectionError(msg)

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                [
                    future.result()
                    for future in [
                        executor.submit(self.s3.delete_object, Bucket=self.bucket_name, Key=file_key)
                        for file_key in metadata["collections"][collection]["files"]
                    ]
                ]

            del metadata["collections"][collection]
            self._save_metadata(metadata)
        except Exception as e:
            msg = f"Failed to delete collection: {str(e)}"
            raise CollectionError(msg) from e
