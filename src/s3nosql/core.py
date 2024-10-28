from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Generator, List, Optional, Tuple, Set
from datetime import datetime
import gzip
import json
import threading
from cachetools import TTLCache, LRUCache
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from exceptions import CollectionError, DocumentError, S3NoSQLError
from utils import generate_document_id, get_partition_id

class S3NoSQLDB:
    """Optimized NoSQL database using Amazon S3 as storage."""

    def __init__(
        self,
        bucket_name: str,
        base_path: str = "",
        region_name: str = "us-east-1",
        partition_size: int = 1000,
        docs_per_object: int = 10000,  # Increased for better S3 utilization
        compression: bool = True,  # Default to True for cost savings
        max_pool_connections: int = 100,
        cache_ttl: int = 300,  # 5 minutes cache TTL
        cache_size: int = 1000,
    ) -> None:
        """Initialize with optimized configuration."""
        try:
            # Optimized S3 client configuration
            config = Config(
                retries=dict(max_attempts=3),
                connect_timeout=5,
                read_timeout=60,
                max_pool_connections=max_pool_connections,
            )
            self.s3 = boto3.client("s3", region_name=region_name, config=config)
            self.bucket = bucket_name
            self.base_path = base_path.strip("/")
            self.partition_size = partition_size
            self.docs_per_object = docs_per_object
            self.compression = compression
            
            # Initialize caches
            self.metadata_cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
            self.object_cache = LRUCache(maxsize=cache_size)
            self.cache_lock = threading.Lock()
            
            # Initialize collection metadata
            self.collections = self._load_metadata()
            
            # Threadpool for parallel operations
            self.executor = ThreadPoolExecutor(max_workers=max_pool_connections)
            
        except Exception as e:
            raise S3NoSQLError(f"Failed to initialize database: {str(e)}") from e

    def _get_full_path(self, key: str) -> str:
        return f"{self.base_path}/{key}" if self.base_path else key

    def _compress_data(self, data: str) -> bytes:
        return gzip.compress(data.encode()) if self.compression else data.encode()

    def _decompress_data(self, data: bytes) -> str:
        return gzip.decompress(data).decode() if self.compression else data.decode()

    def _get_cached_object(self, key: str) -> Optional[Dict]:
        """Get object from cache or S3."""
        cached_data = self.object_cache.get(key)
        if cached_data is not None:
            return cached_data

        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(self._decompress_data(response["Body"].read()))
            self.object_cache[key] = data
            return data
        except Exception:
            return None

    def _batch_get_objects(
        self, 
        keys: List[str], 
        parallel: bool = True
    ) -> Dict[str, Dict]:
        """Optimized batch get with caching and parallel processing."""
        results = {}
        missing_keys = []

        # Check cache first
        for key in keys:
            cached_data = self.object_cache.get(key)
            if cached_data is not None:
                results[key] = cached_data
            else:
                missing_keys.append(key)

        if not missing_keys:
            return results

        # Fetch missing keys in parallel
        if parallel:
            futures = []
            for i in range(0, len(missing_keys), 100):
                batch = missing_keys[i:i + 100]
                futures.extend([
                    self.executor.submit(self._get_cached_object, key)
                    for key in batch
                ])
            
            for key, future in zip(missing_keys, futures):
                try:
                    data = future.result()
                    if data is not None:
                        results[key] = data
                except Exception:
                    continue
        else:
            for key in missing_keys:
                data = self._get_cached_object(key)
                if data is not None:
                    results[key] = data

        return results

    def _batch_put_objects(self, updates: Dict[str, Dict]) -> None:
        """Optimized batch put with caching."""
        futures = []
        
        for key, data in updates.items():
            # Update cache immediately
            self.object_cache[key] = data
            
            # Prepare for S3 upload
            compressed_data = self._compress_data(json.dumps(data))
            futures.append(
                self.executor.submit(
                    self.s3.put_object,
                    Bucket=self.bucket,
                    Key=key,
                    Body=compressed_data
                )
            )

        # Wait for all uploads to complete
        for future in futures:
            future.result()

    def _update_partition_metadata(
        self, 
        collection: str,
        partition_updates: Dict[int, int]
    ) -> None:
        """Batch update partition metadata."""
        metadata_updates = {}
        
        for partition_id, count_change in partition_updates.items():
            metadata_key = self._get_full_path(
                f"{collection}/partitions/p{partition_id:08d}/metadata.json"
            )
            
            try:
                # Get existing metadata
                metadata = self.metadata_cache.get(metadata_key)
                if metadata is None:
                    try:
                        response = self.s3.get_object(Bucket=self.bucket, Key=metadata_key)
                        metadata = json.loads(self._decompress_data(response["Body"].read()))
                    except ClientError:
                        metadata = {"doc_count": 0}

                # Update document count
                metadata["doc_count"] = max(0, metadata.get("doc_count", 0) + count_change)
                metadata["updated_at"] = datetime.now().isoformat()
                
                # Cache updated metadata
                self.metadata_cache[metadata_key] = metadata
                metadata_updates[metadata_key] = metadata
                
            except Exception:
                continue

        # Batch write metadata updates
        if metadata_updates:
            futures = [
                self.executor.submit(
                    self.s3.put_object,
                    Bucket=self.bucket,
                    Key=key,
                    Body=self._compress_data(json.dumps(data))
                )
                for key, data in metadata_updates.items()
            ]
            [future.result() for future in futures]

    def batch_insert(
        self, 
        collection: str, 
        documents: List[Dict[str, Any]]
    ) -> List[str]:
        """Optimized batch insert with improved batching strategy."""
        if collection not in self.collections:
            raise CollectionError(f"Collection {collection} does not exist")

        try:
            # Pre-process all documents
            doc_batches: Dict[str, Dict[str, Dict]] = defaultdict(dict)
            partition_counts: Dict[int, int] = defaultdict(int)
            doc_ids = []
            index_updates: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))

            for doc in documents:
                doc_id = generate_document_id(doc)
                partition_id = get_partition_id(doc_id, self.partition_size)
                
                # Track partition document counts
                partition_counts[partition_id] += 1
                
                # Calculate object key
                doc_index = partition_counts[partition_id] - 1
                object_id = doc_index // self.docs_per_object
                object_key = self._get_object_key(collection, partition_id, object_id)
                
                # Group documents by object
                doc_batches[object_key][doc_id] = doc
                doc_ids.append(doc_id)
                
                # Collect index updates
                for index_field in self.collections[collection]["indexes"]:
                    if index_field in doc:
                        field_value = str(doc[index_field])
                        index_updates[index_field][field_value].add(doc_id)

            # Batch insert documents
            self._batch_insert_objects(collection, doc_batches)

            # Update indexes in parallel
            if index_updates:
                self._batch_update_indexes(collection, index_updates)

            # Update partition metadata
            self._update_partition_metadata(collection, partition_counts)

            # Update collection count
            with self.cache_lock:
                self.collections[collection]["count"] += len(documents)
                self._save_metadata()

            return doc_ids

        except Exception as e:
            raise DocumentError(f"Failed to insert documents: {str(e)}") from e

    def _batch_insert_objects(
        self, 
        collection: str,
        doc_batches: Dict[str, Dict[str, Dict]]
    ) -> None:
        """Optimized batch insert implementation."""
        # Get existing objects
        existing_objects = self._batch_get_objects(list(doc_batches.keys()))

        # Prepare updates
        updates = {}
        for key, new_docs in doc_batches.items():
            current_data = existing_objects.get(key, {})
            current_data.update(new_docs)
            updates[key] = current_data

        # Batch upload updates
        self._batch_put_objects(updates)

    def _batch_update_indexes(
        self,
        collection: str,
        index_updates: Dict[str, Dict[str, Set[str]]]
    ) -> None:
        """Optimized index updates with batching."""
        index_keys = [
            self._get_full_path(f"{collection}/indexes/{field}.json")
            for field in index_updates.keys()
        ]
        
        # Get current index data
        current_indexes = self._batch_get_objects(index_keys, parallel=False)
        
        # Prepare updates
        updates = {}
        for field, field_updates in index_updates.items():
            key = self._get_full_path(f"{collection}/indexes/{field}.json")
            current_data = current_indexes.get(key, {})
            
            # Update index entries
            for value, doc_ids in field_updates.items():
                if value not in current_data:
                    current_data[value] = []
                current_data[value].extend(doc_ids)
            
            updates[key] = current_data

        # Batch upload index updates
        self._batch_put_objects(updates)

    def _get_object_key(
        self, 
        collection: str, 
        partition_id: int, 
        object_id: int
    ) -> str:
        """Generate object key with efficient structure."""
        return self._get_full_path(
            f"{collection}/partitions/p{partition_id:08d}/o{object_id:08d}.json"
        )

    def stream_find(
        self, 
        collection: str, 
        query: Dict[str, Any], 
        batch_size: int = 100
    ) -> Generator[List[Dict], None, None]:
        """Optimized document streaming with smart batching."""
        if collection not in self.collections:
            raise CollectionError(f"Collection {collection} does not exist")

        # Check for indexed field
        indexed_field = next(
            (field for field in query.keys()
             if field in self.collections[collection]["indexes"]),
            None
        )

        if indexed_field:
            yield from self._stream_by_index(
                collection, indexed_field, query[indexed_field], query, batch_size
            )
        else:
            yield from self._stream_full_scan(collection, query, batch_size)

    def _stream_by_index(
        self,
        collection: str,
        index_field: str,
        index_value: Any,
        query: Dict[str, Any],
        batch_size: int,
    ) -> Generator[List[Dict], None, None]:
        """Optimized index-based streaming."""
        try:
            # Get index data
            key = self._get_full_path(f"{collection}/indexes/{index_field}.json")
            index_data = self._get_cached_object(key)
            if not index_data:
                return
            
            doc_ids = index_data.get(str(index_value), [])
            if not doc_ids:
                return

            # Group document IDs by partition for efficient retrieval
            partition_docs: Dict[int, List[str]] = defaultdict(list)
            for doc_id in doc_ids:
                partition_id = get_partition_id(doc_id, self.partition_size)
                partition_docs[partition_id].append(doc_id)

            # Process each partition
            batch = []
            for partition_id, partition_doc_ids in partition_docs.items():
                docs = self._get_partition_documents(collection, partition_id, partition_doc_ids)
                matching_docs = [
                    doc for doc in docs
                    if all(doc.get(k) == v for k, v in query.items())
                ]
                
                batch.extend(matching_docs)
                while len(batch) >= batch_size:
                    yield batch[:batch_size]
                    batch = batch[batch_size:]

            if batch:
                yield batch

        except Exception as e:
            raise S3NoSQLError(f"Failed to stream by index: {str(e)}") from e

    def _get_partition_documents(
        self, 
        collection: str,
        partition_id: int,
        doc_ids: List[str]
    ) -> List[Dict]:
        """Efficiently retrieve multiple documents from a partition."""
        try:
            # Get partition metadata
            metadata_key = self._get_full_path(
                f"{collection}/partitions/p{partition_id:08d}/metadata.json"
            )
            metadata = self.metadata_cache.get(metadata_key)
            
            if metadata is None:
                try:
                    response = self.s3.get_object(Bucket=self.bucket, Key=metadata_key)
                    metadata = json.loads(self._decompress_data(response["Body"].read()))
                    self.metadata_cache[metadata_key] = metadata
                except ClientError:
                    return []

            doc_count = metadata.get("doc_count", 0)
            if doc_count == 0:
                return []

            # Calculate object range
            max_object_id = (doc_count - 1) // self.docs_per_object

            # Get all objects in parallel
            object_keys = [
                self._get_object_key(collection, partition_id, object_id)
                for object_id in range(max_object_id + 1)
            ]
            
            objects_data = self._batch_get_objects(object_keys)

            # Collect matching documents
            docs = []
            for object_data in objects_data.values():
                for doc_id in doc_ids:
                    if doc_id in object_data:
                        docs.append(object_data[doc_id])

            return docs

        except Exception:
            return []

    def _stream_full_scan(
        self, 
        collection: str, 
        query: Dict[str, Any], 
        batch_size: int
    ) -> Generator[List[Dict], None, None]:
        """Optimized full collection scan."""
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            prefix = self._get_full_path(f"{collection}/partitions/")
            
            # Track processed documents to avoid duplicates
            processed_docs = set()
            batch = []

            # Process each partition directory
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue

                # Filter for document storage objects and group by partition
                partition_objects: Dict[int, List[str]] = defaultdict(list)
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if key.endswith(".json") and "metadata" not in key:
                        # Extract partition ID from key
                        parts = key.split("/")
                        if len(parts) >= 2:
                            partition_dir = parts[-2]  # Format: pXXXXXXXX
                            if partition_dir.startswith("p"):
                                partition_id = int(partition_dir[1:])
                                partition_objects[partition_id].append(key)

                # Process each partition's objects in parallel
                for partition_id, object_keys in partition_objects.items():
                    # Get objects in batches to optimize memory usage
                    for i in range(0, len(object_keys), 10):
                        batch_keys = object_keys[i:i + 10]
                        objects_data = self._batch_get_objects(batch_keys)

                        # Process objects
                        for object_data in objects_data.values():
                            matching_docs = [
                                doc for doc_id, doc in object_data.items()
                                if doc_id not in processed_docs and
                                all(doc.get(k) == v for k, v in query.items())
                            ]
                            
                            # Track processed documents
                            processed_docs.update(
                                generate_document_id(doc) for doc in matching_docs
                            )
                            
                            # Add to batch and yield when full
                            batch.extend(matching_docs)
                            while len(batch) >= batch_size:
                                yield batch[:batch_size]
                                batch = batch[batch_size:]

            # Yield remaining documents
            if batch:
                yield batch

        except Exception as e:
            raise S3NoSQLError(f"Failed to stream full scan: {str(e)}") from e

    def insert(self, collection: str, document: Dict[str, Any]) -> str:
        """Optimized single document insert."""
        return self.batch_insert(collection, [document])[0]

    def find_one(self, collection: str, query: Dict[str, Any]) -> Optional[Dict]:
        """Optimized single document retrieval."""
        for batch in self.stream_find(collection, query, batch_size=1):
            if batch:
                return batch[0]
        return None

    def exists(
        self, 
        collection: str, 
        query: Dict[str, Any]
    ) -> bool:
        """Optimized existence check."""
        if collection not in self.collections:
            raise CollectionError(f"Collection {collection} does not exist")

        try:
            # Check if we can use an index
            indexed_field = next(
                (field for field in query.keys()
                 if field in self.collections[collection]["indexes"]),
                None
            )

            if indexed_field and len(query) == 1:
                # Fast path: check index directly
                key = self._get_full_path(f"{collection}/indexes/{indexed_field}.json")
                index_data = self._get_cached_object(key)
                return bool(index_data and str(query[indexed_field]) in index_data)

            # Fall back to finding first matching document
            return bool(self.find_one(collection, query))

        except Exception as e:
            raise S3NoSQLError(f"Failed to check existence: {str(e)}") from e

    def count(
        self, 
        collection: str, 
        query: Dict[str, Any]
    ) -> int:
        """Optimized document counting."""
        if collection not in self.collections:
            raise CollectionError(f"Collection {collection} does not exist")

        try:
            # Fast path: empty query returns total collection count
            if not query:
                return self.collections[collection]["count"]

            # Check if we can use an index for exact count
            indexed_field = next(
                (field for field in query.keys()
                 if field in self.collections[collection]["indexes"]),
                None
            )

            if indexed_field and len(query) == 1:
                # Use index for fast counting
                key = self._get_full_path(f"{collection}/indexes/{indexed_field}.json")
                index_data = self._get_cached_object(key)
                return len(index_data.get(str(query[indexed_field]), [])) if index_data else 0

            # Fall back to counting all matching documents
            total = 0
            for batch in self.stream_find(collection, query, batch_size=1000):
                total += len(batch)
            return total

        except Exception as e:
            raise S3NoSQLError(f"Failed to count documents: {str(e)}") from e

    def delete(
        self, 
        collection: str, 
        query: Dict[str, Any]
    ) -> int:
        """Optimized document deletion."""
        if collection not in self.collections:
            raise CollectionError(f"Collection {collection} does not exist")

        try:
            deleted_count = 0
            partition_updates: Dict[int, int] = defaultdict(int)
            index_removals: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))

            # Collect documents to delete with batching
            for batch in self.stream_find(collection, query, batch_size=1000):
                # Group documents by partition
                partition_docs: Dict[int, List[Tuple[str, Dict]]] = defaultdict(list)
                for doc in batch:
                    doc_id = generate_document_id(doc)
                    partition_id = get_partition_id(doc_id, self.partition_size)
                    partition_docs[partition_id].append((doc_id, doc))
                    
                    # Track index removals
                    for index_field in self.collections[collection]["indexes"]:
                        if index_field in doc:
                            field_value = str(doc[index_field])
                            index_removals[index_field][field_value].add(doc_id)

                # Process each partition
                for partition_id, docs in partition_docs.items():
                    deleted = self._delete_partition_documents(collection, partition_id, docs)
                    if deleted:
                        deleted_count += len(docs)
                        partition_updates[partition_id] -= len(docs)

            if deleted_count > 0:
                # Update partition metadata
                self._update_partition_metadata(collection, partition_updates)
                
                # Update indexes
                if index_removals:
                    self._update_indexes_after_delete(collection, index_removals)
                
                # Update collection count
                with self.cache_lock:
                    self.collections[collection]["count"] -= deleted_count
                    self._save_metadata()

            return deleted_count

        except Exception as e:
            raise S3NoSQLError(f"Failed to delete documents: {str(e)}") from e

    def _delete_partition_documents(
        self,
        collection: str,
        partition_id: int,
        docs: List[Tuple[str, Dict]]
    ) -> bool:
        """Delete documents from a partition."""
        try:
            # Get partition metadata
            metadata_key = self._get_full_path(
                f"{collection}/partitions/p{partition_id:08d}/metadata.json"
            )
            metadata = self._get_cached_object(metadata_key)
            if not metadata:
                return False

            doc_count = metadata.get("doc_count", 0)
            max_object_id = (doc_count - 1) // self.docs_per_object

            # Get all relevant objects
            object_keys = [
                self._get_object_key(collection, partition_id, object_id)
                for object_id in range(max_object_id + 1)
            ]
            
            objects_data = self._batch_get_objects(object_keys)
            if not objects_data:
                return False

            # Remove documents and track modified objects
            modified_objects = {}
            doc_ids = {doc_id for doc_id, _ in docs}
            
            for key, object_data in objects_data.items():
                modified = False
                for doc_id in doc_ids:
                    if doc_id in object_data:
                        del object_data[doc_id]
                        modified = True
                
                if modified:
                    modified_objects[key] = object_data

            # Update modified objects
            if modified_objects:
                self._batch_put_objects(modified_objects)
                return True

            return False

        except Exception:
            return False

    def _update_indexes_after_delete(
        self,
        collection: str,
        index_removals: Dict[str, Dict[str, Set[str]]]
    ) -> None:
        """Update indexes after document deletion."""
        try:
            # Get current index data
            index_keys = [
                self._get_full_path(f"{collection}/indexes/{field}.json")
                for field in index_removals.keys()
            ]
            current_indexes = self._batch_get_objects(index_keys)

            # Prepare updates
            updates = {}
            for field, field_removals in index_removals.items():
                key = self._get_full_path(f"{collection}/indexes/{field}.json")
                current_data = current_indexes.get(key, {})
                
                for value, doc_ids in field_removals.items():
                    if value in current_data:
                        current_data[value] = [
                            doc_id for doc_id in current_data[value]
                            if doc_id not in doc_ids
                        ]
                        if not current_data[value]:
                            del current_data[value]

                updates[key] = current_data

            # Batch update indexes
            if updates:
                self._batch_put_objects(updates)

        except Exception as e:
            raise S3NoSQLError(f"Failed to update indexes after delete: {str(e)}") from e
