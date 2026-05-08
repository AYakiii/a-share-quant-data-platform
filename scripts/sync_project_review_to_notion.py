from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

import requests

NOTION_VERSION = "2022-06-28"
DOCS_ROOT = Path("docs/notion_project_review")
MAX_RETRIES = 5


def rich_text(text: str) -> list[dict]:
    return [{"type": "text", "text": {"content": text[:2000]}}]


def md_to_blocks(lines: list[str]) -> list[dict]:
    blocks: list[dict] = []
    for raw in lines:
        line = raw.rstrip("\n")
        if not line.strip():
            continue
        if line.startswith("### "):
            blocks.append({"object": "block", "type": "heading_3", "heading_3": {"rich_text": rich_text(line[4:])}})
        elif line.startswith("## "):
            blocks.append({"object": "block", "type": "heading_2", "heading_2": {"rich_text": rich_text(line[3:])}})
        elif line.startswith("# "):
            blocks.append({"object": "block", "type": "heading_1", "heading_1": {"rich_text": rich_text(line[2:])}})
        elif line.lstrip().startswith(("- ", "* ")):
            blocks.append({"object": "block", "type": "bulleted_list_item", "bulleted_list_item": {"rich_text": rich_text(line.lstrip()[2:])}})
        else:
            blocks.append({"object": "block", "type": "paragraph", "paragraph": {"rich_text": rich_text(line)}})
    return blocks


def _request_with_retry(method: str, url: str, *, headers: dict, json_body: dict, timeout: int = 30) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.request(method, url, headers=headers, json=json_body, timeout=timeout)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                wait_s = min(30.0, 2 ** (attempt - 1))
                print(f"[WARN] HTTP {r.status_code} on {method} {url}; retry {attempt}/{MAX_RETRIES} after {wait_s:.1f}s")
                if attempt < MAX_RETRIES:
                    time.sleep(wait_s)
                    continue
            r.raise_for_status()
            return r
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            wait_s = min(30.0, 2 ** (attempt - 1))
            print(f"[WARN] Network error on {method} {url}: {exc}; retry {attempt}/{MAX_RETRIES} after {wait_s:.1f}s")
            if attempt < MAX_RETRIES:
                time.sleep(wait_s)
                continue
            raise
        except requests.HTTPError as exc:
            last_error = exc
            if attempt >= MAX_RETRIES:
                raise
    if last_error is not None:
        raise RuntimeError(f"request failed after retries: {method} {url}") from last_error
    raise RuntimeError(f"request failed: {method} {url}")


def create_child_page(headers: dict, parent_page_id: str, title: str) -> str:
    r = _request_with_retry(
        "POST",
        "https://api.notion.com/v1/pages",
        headers=headers,
        json_body={
            "parent": {"page_id": parent_page_id},
            "properties": {"title": {"title": [{"type": "text", "text": {"content": title}}]}},
        },
    )
    return r.json()["id"]


def append_blocks(headers: dict, block_id: str, blocks: list[dict], *, batch_size: int, sleep_s: float, progress_prefix: str) -> None:
    url = f"https://api.notion.com/v1/blocks/{block_id}/children"
    total = max(1, (len(blocks) + batch_size - 1) // batch_size)
    for i in range(0, len(blocks), batch_size):
        batch_idx = i // batch_size + 1
        chunk = blocks[i : i + batch_size]
        print(f"[INFO] {progress_prefix} batch {batch_idx}/{total}")
        _request_with_retry("PATCH", url, headers=headers, json_body={"children": chunk})
        if sleep_s > 0:
            time.sleep(sleep_s)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync notion project review markdown docs")
    p.add_argument("--mode", choices=["child-pages", "inline"], default="child-pages")
    p.add_argument("--batch-size", type=int, default=50, help="Notion append batch size")
    p.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between append batches")
    p.add_argument("--start-from", default=None, help="Resume from specific markdown filename, e.g. 03_panel_and_feature_store.md")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    token = os.getenv("NOTION_TOKEN")
    parent = os.getenv("NOTION_PARENT_PAGE_ID")
    if not token or not parent:
        print("[ERROR] Missing NOTION_TOKEN or NOTION_PARENT_PAGE_ID")
        return 1
    if args.batch_size <= 0:
        print("[ERROR] --batch-size must be > 0")
        return 1
    if not DOCS_ROOT.exists():
        print(f"[ERROR] Docs root not found: {DOCS_ROOT}")
        return 1

    headers = {"Authorization": f"Bearer {token}", "Notion-Version": NOTION_VERSION, "Content-Type": "application/json"}
    md_files = sorted([p for p in DOCS_ROOT.glob("*.md") if p.name != "README_SYNC.md"])
    if not md_files:
        print("[WARN] No markdown files found to sync.")
        return 0

    if args.start_from:
        names = [p.name for p in md_files]
        if args.start_from not in names:
            print(f"[ERROR] --start-from file not found: {args.start_from}")
            return 1
        start_idx = names.index(args.start_from)
        md_files = md_files[start_idx:]
        print(f"[INFO] Resuming from {args.start_from}, files to sync: {len(md_files)}")

    if args.mode == "child-pages":
        for fp in md_files:
            try:
                page_id = create_child_page(headers, parent, fp.stem)
                append_blocks(
                    headers,
                    page_id,
                    md_to_blocks(fp.read_text(encoding="utf-8").splitlines()),
                    batch_size=args.batch_size,
                    sleep_s=args.sleep,
                    progress_prefix=f"Child-page sync file {fp.name}",
                )
                print(f"[OK] child-page synced: {fp.name} -> {page_id}")
            except Exception as exc:
                print(f"[ERROR] Failed syncing {fp.name}: {exc}")
                return 1
        return 0

    print("[WARN] inline mode appends into parent page and may duplicate content on re-run.")
    print("[WARN] Recommended: use a fresh parent page or manually clear previous synced blocks before retry.")
    for fp in md_files:
        try:
            blocks: list[dict] = [
                {"object": "block", "type": "heading_1", "heading_1": {"rich_text": rich_text(f"{fp.stem} | {fp.name}")}}
            ]
            blocks.extend(md_to_blocks(fp.read_text(encoding="utf-8").splitlines()))
            blocks.append({"object": "block", "type": "divider", "divider": {}})
            append_blocks(
                headers,
                parent,
                blocks,
                batch_size=args.batch_size,
                sleep_s=args.sleep,
                progress_prefix=f"Inline sync file {fp.name}",
            )
            print(f"[OK] inline synced file: {fp.name}")
        except Exception as exc:
            print(f"[ERROR] Failed inline syncing {fp.name}: {exc}")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
