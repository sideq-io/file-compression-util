#!/usr/bin/env python3
"""
Image compression utility with checkpoint/resume support.
Walks a source directory, compresses images (JPEG, PNG, WebP),
and copies everything (including non-images) to a destination directory
preserving the directory structure and original filenames.

On crash or OOM, re-run and it resumes from where it left off.
"""

import json
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
WORKERS = int(os.environ.get("WORKERS", "2"))

CHECKPOINT_FILE = Path("/data/checkpoint.json")

# Extensions we attempt to compress
JPEG_EXTS = {".jpg", ".jpeg"}
PNG_EXTS = {".png"}
WEBP_EXTS = {".webp"}
IMAGE_EXTS = JPEG_EXTS | PNG_EXTS | WEBP_EXTS


def load_checkpoint() -> dict:
    """Load checkpoint: set of already-processed relative paths + accumulated stats."""
    if CHECKPOINT_FILE.exists():
        try:
            data = json.loads(CHECKPOINT_FILE.read_text())
            return {
                "done": set(data.get("done", [])),
                "stats": data.get("stats", {}),
            }
        except (json.JSONDecodeError, KeyError):
            pass
    return {"done": set(), "stats": {}}


def save_checkpoint(done: set, stats: dict):
    """Persist checkpoint to disk."""
    CHECKPOINT_FILE.write_text(json.dumps({
        "done": list(done),
        "stats": stats,
    }))


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
    """Compress JPEG: resize, strip metadata, save with Pillow."""
    with Image.open(src) as img:
        img = resize_if_needed(img)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        img.save(dst, format="JPEG", quality=JPEG_QUALITY, optimize=True)
    return dst.stat().st_size


def compress_png(src: Path, dst: Path) -> int:
    """Compress PNG using Pillow optimization + pngquant if available."""
    with Image.open(src) as img:
        img = resize_if_needed(img)
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
    with Image.open(src) as img:
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
        # On error, copy the original via chunked read (safe even under low memory)
        try:
            with open(src, "rb") as fsrc, open(dst, "wb") as fdst:
                while chunk := fsrc.read(1024 * 1024):
                    fdst.write(chunk)
        except Exception:
            pass
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

    # Load checkpoint
    checkpoint = load_checkpoint()
    already_done = checkpoint["done"]
    stats = {
        "compressed": checkpoint["stats"].get("compressed", 0),
        "copied": checkpoint["stats"].get("copied", 0),
        "skipped_small": checkpoint["stats"].get("skipped_small", 0),
        "skipped_checkpoint": 0,
        "errors": checkpoint["stats"].get("errors", 0),
        "original_bytes": checkpoint["stats"].get("original_bytes", 0),
        "final_bytes": checkpoint["stats"].get("final_bytes", 0),
    }

    print("=" * 60)
    print("CONFIGURATION")
    print("=" * 60)
    print(f"  JPEG quality:    {JPEG_QUALITY}")
    print(f"  PNG quality:     {PNG_QUALITY_MIN}-{PNG_QUALITY_MAX}")
    print(f"  WebP quality:    {WEBP_QUALITY}")
    print(f"  Max width:       {MAX_WIDTH}px")
    print(f"  Skip under:      {SKIP_UNDER_KB}KB")
    print(f"  Workers:         {WORKERS}")
    print(f"  Checkpoint:      {len(already_done)} files already processed")
    print("=" * 60 + "\n")

    if not src_dir.exists():
        print(f"ERROR: Source directory {src_dir} does not exist.")
        print("Mount your files directory to /data/current")
        sys.exit(1)

    dst_dir.mkdir(parents=True, exist_ok=True)

    # Collect all files, skip already-done ones
    files = []
    total_all = 0
    for root, _dirs, filenames in os.walk(src_dir):
        for fname in filenames:
            src_path = Path(root) / fname
            rel_path = str(src_path.relative_to(src_dir))
            total_all += 1
            if rel_path in already_done:
                continue
            dst_path = dst_dir / rel_path
            files.append((src_path, dst_path, rel_path))

    total_remaining = len(files)
    print(f"Total files: {total_all} | Already done: {len(already_done)} | Remaining: {total_remaining}\n")

    if total_remaining == 0:
        print("Nothing to do — all files already processed.")
        return

    start_time = time.time()
    processed = 0
    checkpoint_interval = 50  # save checkpoint every N files

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(process_file, src, dst): (src, dst, rel)
            for src, dst, rel in files
        }

        for future in as_completed(futures):
            src, dst, rel = futures[future]
            try:
                result = future.result()
            except Exception as e:
                # Worker crashed entirely — record as error, move on
                stats["errors"] += 1
                already_done.add(rel)
                processed += 1
                print(f"[{processed}/{total_remaining}] CRASHED {rel}: {e}")
                continue

            processed += 1
            original = result["original_size"]
            final = result.get("final_size", original)
            action = result["action"]

            stats["original_bytes"] += original
            stats["final_bytes"] += final

            if action == "compressed":
                stats["compressed"] += 1
                saved_pct = (1 - final / original) * 100
                print(
                    f"[{processed}/{total_remaining}] COMPRESSED {result['src'].name}: "
                    f"{format_size(original)} -> {format_size(final)} "
                    f"(-{saved_pct:.0f}%)"
                )
            elif action == "skipped_small":
                stats["skipped_small"] += 1
            elif action == "error":
                stats["errors"] += 1
                print(
                    f"[{processed}/{total_remaining}] ERROR {result['src'].name}: "
                    f"{result['error']} (copied original)"
                )
            else:
                stats["copied"] += 1

            # Mark as done and periodically save checkpoint
            already_done.add(rel)
            if processed % checkpoint_interval == 0:
                save_checkpoint(already_done, stats)
                print(f"  [checkpoint saved: {len(already_done)} files done]")

    # Final checkpoint
    save_checkpoint(already_done, stats)

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
    print(f"Total files:      {total_all}")
    print(f"This run:         {total_remaining}")
    print(f"Compressed:       {stats['compressed']}")
    print(f"Copied as-is:     {stats['copied']}")
    print(f"Skipped (small):  {stats['skipped_small']}")
    print(f"Errors:           {stats['errors']}")
    print(f"Original size:    {format_size(stats['original_bytes'])}")
    print(f"Final size:       {format_size(stats['final_bytes'])}")
    print(f"Saved:            {format_size(total_saved)} ({total_pct:.1f}%)")
    print(f"Time (this run):  {elapsed:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
