#Data Type: Pipeline
from google.cloud import storage
from google.oauth2 import service_account
import gzip
import json 
from utils.logger import setup_logger


class BaseExtractor:
    """ This class is base class for all extractor """

    def __init__(self, bucket_name: str):
        """ Initialize extractor from GCS bucket """
        SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

        credentials = service_account.Credentials.from_service_account_file( r"C:\Users\Admin\Desktop\Final Project K41\config\minpyws-e52b3983be71.json", scopes = SCOPES)
        self.client = storage.Client(credentials = credentials)
        self.bucket = self.client.bucket(bucket_name)
        self.bucket_name = bucket_name
        self.logger = setup_logger(self.__class__.__name__)

    def extract_json_gzip(self, prefix_path):
        """ Extract file json.gz from prefix path in GCS"""
        self.logger.info(f"Extracting: gs://{self.bucket_name}/{prefix_path}")

        blob = self.bucket.blob(prefix_path)

        # Giải pháp fix MemoryError triệt để: sử dụng thư viện ijson
        # ijson sẽ parse luồng byte trực tiếp mà không cần nạp vào chuỗi ký tự khổng lồ
        import tempfile
        import ijson
        with tempfile.TemporaryFile() as temp_file:
            blob.download_to_file(temp_file)
            temp_file.seek(0)
            
            # Đọc theo byte ('rb') để ijson stream nhanh và tiết kiệm RAM nhất
            with gzip.open(temp_file, 'rb') as f:
                # Kiểm tra byte đầu tiên xem file là đối tượng Dict hay Array
                first_byte = f.read(1)
                while first_byte.isspace() and first_byte != b'':
                    first_byte = f.read(1)
                f.seek(0)
                
                if first_byte == b'[':
                    # Mảng JSON: Parse từ từ từng phần tử con và nhét vào một danh sách (list)
                    data = []
                    for item in ijson.items(f, 'item'):
                        data.append(item)
                else:
                    # Nếu là JSON Dict đơn lẻ, đọc đối tượng gốc
                    data = next(ijson.items(f, ''), None)
                
        self.logger.info(f"Extracted {len(data) if isinstance(data, list) else 1} records from {prefix_path}")
        return data

    def extract_json_gzip_chunked(self, prefix_path, chunk_size=50000):
        """
        Extract file json.gz from GCS as a generator of chunks.
        Each yielded chunk is a list of up to `chunk_size` records.
        This keeps peak memory low for very large files.

        Args:
            prefix_path: GCS path to the json.gz file.
            chunk_size: Number of records per chunk.

        Yields:
            List of dicts (each list has at most chunk_size items).
        """
        self.logger.info(f"Extracting (chunked): gs://{self.bucket_name}/{prefix_path}")

        blob = self.bucket.blob(prefix_path)

        import tempfile
        import ijson
        total = 0
        with tempfile.TemporaryFile() as temp_file:
            blob.download_to_file(temp_file)
            temp_file.seek(0)

            with gzip.open(temp_file, 'rb') as f:
                first_byte = f.read(1)
                while first_byte.isspace() and first_byte != b'':
                    first_byte = f.read(1)
                f.seek(0)

                if first_byte == b'[':
                    chunk = []
                    for item in ijson.items(f, 'item'):
                        chunk.append(item)
                        if len(chunk) >= chunk_size:
                            total += len(chunk)
                            self.logger.info(f"  Yielding chunk: {len(chunk)} records (total so far: {total})")
                            yield chunk
                            chunk = []
                    if chunk:
                        total += len(chunk)
                        yield chunk
                else:
                    data = next(ijson.items(f, ''), None)
                    if data:
                        total = 1
                        yield [data]

        self.logger.info(f"Chunked extraction complete: {total} total records from {prefix_path}")

    def extract_json_gzip_batches(self, prefix: str, pattern: str, batch_count: int):
        """
        Extract multiple batch files and combine results.

        Args:
            prefix: GCS prefix path (e.g., 'shopify/')
            pattern: Filename pattern with {} placeholder (e.g., 'orders_batch_{}.json.gz')
            batch_count: Number of batch files (0-indexed)

        Returns:
            Combined list of all records from all batches.
        """
        all_data = []
        for i in range(batch_count):
            file_path = f"{prefix}{pattern.format(i)}"
            self.logger.info(f"Extracting batch {i+1}/{batch_count}: {file_path}")
            batch_data = self.extract_json_gzip(file_path)
            if isinstance(batch_data, list):
                all_data.extend(batch_data)
            else:
                all_data.append(batch_data)
        self.logger.info(f"Total records from {batch_count} batches: {len(all_data)}")
        return all_data

    def list_blobs(self, prefix: str):
        """
        List all blobs under a given prefix in the bucket.

        Args:
            prefix: GCS prefix path to list.

        Returns:
            List of blob names.
        """
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        blob_names = [blob.name for blob in blobs]
        self.logger.info(f"Found {len(blob_names)} blobs under gs://{self.bucket_name}/{prefix}")
        return blob_names



