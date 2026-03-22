#!/usr/bin/env python3
"""
Upload the compressed directory to a Cloudflare R2 bucket.
Preserves directory structure as object keys.
"""

import os
import sys
import time
import mimetypes
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config

# --- Configuration from environment ---
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")
# Optional: prefix all keys with this path (e.g. "uploads/")
R2_KEY_PREFIX = os.environ.get("R2_KEY_PREFIX", "")

WORKERS = int(os.environ.get("UPLOAD_WORKERS", "10"))
SOURCE_DIR = Path("/data/compressed")


def validate_env():
    missing = []
    for var in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"):
        if not os.environ.get(var):
            missing.append(var)
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
        region_name="auto",
    )


def get_content_type(filepath: Path) -> str:
    content_type, _ = mimetypes.guess_type(str(filepath))
    return content_type or "application/octet-stream"


def format_size(size_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def upload_file(client, filepath: Path, key: str) -> dict:
    """Upload a single file to R2."""
    result = {"file": filepath, "key": key, "error": None}
    try:
        content_type = get_content_type(filepath)
        client.upload_file(
            str(filepath),
            R2_BUCKET_NAME,
            key,
            ExtraArgs={"ContentType": content_type},
        )
        result["size"] = filepath.stat().st_size
    except Exception as e:
        result["error"] = str(e)
    return result


def main():
    validate_env()

    if not SOURCE_DIR.exists():
        print(f"ERROR: Source directory {SOURCE_DIR} does not exist.")
        print("Run the compressor first, or check your volume mount.")
        sys.exit(1)

    client = get_s3_client()

    # Verify bucket access
    try:
        client.head_bucket(Bucket=R2_BUCKET_NAME)
        print(f"Connected to R2 bucket: {R2_BUCKET_NAME}")
    except Exception as e:
        print(f"ERROR: Cannot access bucket '{R2_BUCKET_NAME}': {e}")
        sys.exit(1)

    # Collect files
    files = []
    total_size = 0
    for root, _dirs, filenames in os.walk(SOURCE_DIR):
        for fname in filenames:
            filepath = Path(root) / fname
            rel_path = filepath.relative_to(SOURCE_DIR)
            key = R2_KEY_PREFIX + rel_path.as_posix()
            files.append((filepath, key))
            total_size += filepath.stat().st_size

    total = len(files)
    print(f"Found {total} files ({format_size(total_size)}) to upload using {WORKERS} workers\n")

    start_time = time.time()
    uploaded = 0
    uploaded_bytes = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(upload_file, client, fp, key): (fp, key)
            for fp, key in files
        }

        for future in as_completed(futures):
            result = future.result()
            uploaded += 1

            if result["error"]:
                errors += 1
                print(f"[{uploaded}/{total}] FAILED {result['key']}: {result['error']}")
            else:
                uploaded_bytes += result["size"]
                if uploaded % 100 == 0 or uploaded == total:
                    print(f"[{uploaded}/{total}] Uploaded {format_size(uploaded_bytes)}")

    elapsed = time.time() - start_time

    print("\n" + "=" * 60)
    print("UPLOAD SUMMARY")
    print("=" * 60)
    print(f"Total files:   {total}")
    print(f"Uploaded:      {uploaded - errors}")
    print(f"Failed:        {errors}")
    print(f"Total size:    {format_size(uploaded_bytes)}")
    print(f"Time:          {elapsed:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
