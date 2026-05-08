from __future__ import annotations

import argparse
import os
from pathlib import Path

import requests

NOTION_VERSION = "2022-06-28"
DOCS_ROOT = Path("docs/notion_project_review")


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


def create_child_page(headers: dict, parent_page_id: str, title: str) -> str:
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=headers,
        json={"parent": {"page_id": parent_page_id}, "properties": {"title": {"title": [{"type": "text", "text": {"content": title}}]}}},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["id"]


def append_blocks(headers: dict, block_id: str, blocks: list[dict]) -> None:
    url = f"https://api.notion.com/v1/blocks/{block_id}/children"
    for i in range(0, len(blocks), 100):
        r = requests.patch(url, headers=headers, json={"children": blocks[i : i + 100]}, timeout=30)
        r.raise_for_status()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync notion project review markdown docs")
    p.add_argument("--mode", choices=["child-pages", "inline"], default="child-pages")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    token = os.getenv("NOTION_TOKEN")
    parent = os.getenv("NOTION_PARENT_PAGE_ID")
    if not token or not parent:
        print("[ERROR] Missing NOTION_TOKEN or NOTION_PARENT_PAGE_ID")
        return 1
    if not DOCS_ROOT.exists():
        print(f"[ERROR] Docs root not found: {DOCS_ROOT}")
        return 1

    headers = {"Authorization": f"Bearer {token}", "Notion-Version": NOTION_VERSION, "Content-Type": "application/json"}
    md_files = sorted([p for p in DOCS_ROOT.glob("*.md") if p.name != "README_SYNC.md"])
    if not md_files:
        print("[WARN] No markdown files found to sync.")
        return 0

    if args.mode == "child-pages":
        for fp in md_files:
            try:
                page_id = create_child_page(headers, parent, fp.stem)
                append_blocks(headers, page_id, md_to_blocks(fp.read_text(encoding="utf-8").splitlines()))
                print(f"[OK] child-page synced: {fp.name} -> {page_id}")
            except Exception as exc:
                print(f"[ERROR] Failed syncing {fp.name}: {exc}")
        return 0

    # inline mode
    all_blocks: list[dict] = []
    for idx, fp in enumerate(md_files):
        all_blocks.append({"object": "block", "type": "heading_1", "heading_1": {"rich_text": rich_text(f"{fp.stem} | {fp.name}")}})
        all_blocks.extend(md_to_blocks(fp.read_text(encoding="utf-8").splitlines()))
        if idx != len(md_files) - 1:
            all_blocks.append({"object": "block", "type": "divider", "divider": {}})

    try:
        append_blocks(headers, parent, all_blocks)
        print(f"[OK] inline synced {len(md_files)} files into parent page {parent}")
    except Exception as exc:
        print(f"[ERROR] Failed inline sync: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
