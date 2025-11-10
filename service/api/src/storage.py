import hashlib, os, time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

_BUCKET = os.getenv("MINIO_BUCKET_RAW", "astro-raw")
_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
_SECRET = os.getenv("MINIO_SECRET_KEY", "minio123")
_REGION = os.getenv("MINIO_REGION", "us-east-1")  # MinIO ignores but boto3 wants one

_session = boto3.session.Session()
_s3 = _session.client(
    "s3",
    endpoint_url=_ENDPOINT,
    aws_access_key_id=_KEY,
    aws_secret_access_key=_SECRET,
    region_name=_REGION,
    config=Config(connect_timeout=2, read_timeout=10, retries={"max_attempts": 3}),
)

def ensure_bucket():
    try:
        _s3.head_bucket(Bucket=_BUCKET)
    except ClientError:
        _s3.create_bucket(Bucket=_BUCKET)

def put_bytes(namespace: str, content: bytes, content_type="application/xml") -> str:
    """
    Idempotent keying: hash of content + timestamp suffix in a namespaced folder.
    Returns s3://bucket/key
    """
    h = hashlib.sha256(content).hexdigest()[:16]
    ts = int(time.time() * 1000)
    key = f"{namespace}/{h}-{ts}.xml"
    _s3.put_object(Bucket=_BUCKET, Key=key, Body=content, ContentType=content_type)
    return f"s3://{_BUCKET}/{key}"
