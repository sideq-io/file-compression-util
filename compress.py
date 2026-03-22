#!/usr/bin/env python3
"""
Image compression utility.
Walks a source directory, compresses images (JPEG, PNG, WebP),
and copies everything (including non-images) to a destination directory
preserving the directory structure and original filenames.
"""

import os
import sys
import shutil
import time
from pathlib import Path
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration (overridable via environment variables) ---
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "78"))
PNG_QUALITY_MIN = int(os.environ.get("PNG_QUALITY_MIN", "55"))
PNG_QUALITY_MAX = int(os.environ.get("PNG_QUALITY_MAX", "78"))
WEBP_QUALITY = int(os.environ.get("WEBP_QUALITY", "78"))
MAX_WIDTH = int(os.environ.get("MAX_WIDTH", "2400"))
SKIP_UNDER_KB = int(os.environ.get("SKIP_UNDER_KB", "50"))
WORKERS = int(os.environ.get("WORKERS", str(os.cpu_count() or 4)))

# Extensions we attempt to compress
JPEG_EXTS = {".jpg", ".jpeg"}
PNG_EXTS = {".png"}
WEBP_EXTS = {".webp"}
IMAGE_EXTS = JPEG_EXTS | PNG_EXTS | WEBP_EXTS

# Stats
stats = {
    "compressed": 0,
    "copied": 0,
    "skipped_small": 0,
    "errors": 0,
    "original_bytes": 0,
    "final_bytes": 0,
}


def ensure_dir(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


def resize_if_needed(img: Image.Image) -> Image.Image:
    """Resize image if wider than MAX_WIDTH, preserving aspect ratio."""
    if img.width > MAX_WIDTH:
        ratio = MAX_WIDTH / img.width
        new_height = int(img.height * ratio)
        img = img.resize((MAX_WIDTH, new_height), Image.LANCZOS)
    return img


def compress_jpeg(src: Path, dst: Path) -> int:
    """Compress JPEG: resize, strip metadata, save with Pillow then optimize with mozjpeg."""
    img = Image.open(src)
    img = resize_if_needed(img)
    # Convert RGBA/P to RGB for JPEG
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")
    img.save(dst, format="JPEG", quality=JPEG_QUALITY, optimize=True)

    # Then try mozjpeg for further reduction
    try:
        import subprocess
        tmp = dst.with_suffix(".moz.jpg")
        result = subprocess.run(
            [
                "cjpeg",
                "-quality", str(JPEG_QUALITY),
                "-optimize",
                "-outfile", str(tmp),
                str(dst),
            ],
            capture_output=True,
            timeout=30,
        )
        if result.returncode == 0 and tmp.stat().st_size < dst.stat().st_size:
            tmp.replace(dst)
        else:
            tmp.unlink(missing_ok=True)
    except FileNotFoundError:
        pass  # mozjpeg not installed, keep Pillow version

    return dst.stat().st_size


def compress_png(src: Path, dst: Path) -> int:
    """Compress PNG using Pillow optimization + pngquant if available."""
    img = Image.open(src)
    img = resize_if_needed(img)
    # First save optimized with Pillow
    img.save(dst, format="PNG", optimize=True)

    # Then try pngquant for further reduction
    try:
        import subprocess
        result = subprocess.run(
            [
                "pngquant",
                "--force",
                f"--quality={PNG_QUALITY_MIN}-{PNG_QUALITY_MAX}",
                "--speed=1",
                "--output", str(dst),
                str(dst),
            ],
            capture_output=True,
            timeout=30,
        )
        # pngquant returns 99 if conversion results in larger file (skip)
        if result.returncode not in (0, 99):
            pass  # keep Pillow-optimized version
    except FileNotFoundError:
        pass  # pngquant not installed, keep Pillow version

    return dst.stat().st_size


def compress_webp(src: Path, dst: Path) -> int:
    """Compress WebP: resize and re-encode."""
    img = Image.open(src)
    img = resize_if_needed(img)
    img.save(dst, format="WEBP", quality=WEBP_QUALITY, method=6)
    return dst.stat().st_size


def process_file(src: Path, dst: Path) -> dict:
    """Process a single file: compress if image, copy otherwise."""
    result = {"src": src, "dst": dst, "action": "copy", "error": None}
    original_size = src.stat().st_size
    result["original_size"] = original_size

    ensure_dir(dst)
    ext = src.suffix.lower()

    # Non-image: just copy
    if ext not in IMAGE_EXTS:
        shutil.copy2(src, dst)
        result["final_size"] = original_size
        result["action"] = "copied"
        return result

    # Small image: just copy
    if original_size < SKIP_UNDER_KB * 1024:
        shutil.copy2(src, dst)
        result["final_size"] = original_size
        result["action"] = "skipped_small"
        return result

    # Compress
    try:
        if ext in JPEG_EXTS:
            final_size = compress_jpeg(src, dst)
        elif ext in PNG_EXTS:
            final_size = compress_png(src, dst)
        elif ext in WEBP_EXTS:
            final_size = compress_webp(src, dst)
        else:
            shutil.copy2(src, dst)
            result["final_size"] = original_size
            return result

        # If compressed version is larger, keep the original
        if final_size >= original_size:
            shutil.copy2(src, dst)
            result["final_size"] = original_size
            result["action"] = "copied"
        else:
            result["final_size"] = final_size
            result["action"] = "compressed"
    except Exception as e:
        # On error, copy the original
        shutil.copy2(src, dst)
        result["final_size"] = original_size
        result["action"] = "error"
        result["error"] = str(e)

    return result


def format_size(size_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def main():
    src_dir = Path("/data/current")
    dst_dir = Path("/data/compressed")

    print("=" * 60)
    print("CONFIGURATION")
    print("=" * 60)
    print(f"  JPEG quality:    {JPEG_QUALITY}")
    print(f"  PNG quality:     {PNG_QUALITY_MIN}-{PNG_QUALITY_MAX}")
    print(f"  WebP quality:    {WEBP_QUALITY}")
    print(f"  Max width:       {MAX_WIDTH}px")
    print(f"  Skip under:      {SKIP_UNDER_KB}KB")
    print(f"  Workers:         {WORKERS}")
    print("=" * 60 + "\n")

    if not src_dir.exists():
        print(f"ERROR: Source directory {src_dir} does not exist.")
        print("Mount your files directory to /data/current")
        sys.exit(1)

    dst_dir.mkdir(parents=True, exist_ok=True)

    # Collect all files
    files = []
    for root, _dirs, filenames in os.walk(src_dir):
        for fname in filenames:
            src_path = Path(root) / fname
            rel_path = src_path.relative_to(src_dir)
            dst_path = dst_dir / rel_path
            files.append((src_path, dst_path))

    total = len(files)
    print(f"Found {total} files to process using {WORKERS} workers\n")

    start_time = time.time()
    processed = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(process_file, src, dst): (src, dst)
            for src, dst in files
        }

        for future in as_completed(futures):
            result = future.result()
            processed += 1
            original = result["original_size"]
            final = result["final_size"]
            action = result["action"]

            stats["original_bytes"] += original
            stats["final_bytes"] += final

            if action == "compressed":
                stats["compressed"] += 1
                saved_pct = (1 - final / original) * 100
                print(
                    f"[{processed}/{total}] COMPRESSED {result['src'].name}: "
                    f"{format_size(original)} -> {format_size(final)} "
                    f"(-{saved_pct:.0f}%)"
                )
            elif action == "skipped_small":
                stats["skipped_small"] += 1
            elif action == "error":
                stats["errors"] += 1
                print(
                    f"[{processed}/{total}] ERROR {result['src'].name}: "
                    f"{result['error']} (copied original)"
                )
            else:
                stats["copied"] += 1

    elapsed = time.time() - start_time
    total_saved = stats["original_bytes"] - stats["final_bytes"]
    total_pct = (
        (1 - stats["final_bytes"] / stats["original_bytes"]) * 100
        if stats["original_bytes"] > 0
        else 0
    )

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total files:      {total}")
    print(f"Compressed:       {stats['compressed']}")
    print(f"Copied as-is:     {stats['copied']}")
    print(f"Skipped (small):  {stats['skipped_small']}")
    print(f"Errors:           {stats['errors']}")
    print(f"Original size:    {format_size(stats['original_bytes'])}")
    print(f"Final size:       {format_size(stats['final_bytes'])}")
    print(f"Saved:            {format_size(total_saved)} ({total_pct:.1f}%)")
    print(f"Time:             {elapsed:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
