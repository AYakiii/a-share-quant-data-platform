#!/usr/bin/env python3
from __future__ import annotations
import os
from pathlib import Path
from typing import List, Dict
import requests
NOTION_VERSION="2022-06-28"
DOC_ROOT=Path("docs/notion_project_review")

def env_or_fail(name:str)->str:
    v=os.getenv(name)
    if not v: raise SystemExit(f"[ERROR] Missing environment variable: {name}")
    return v

def parse_markdown_to_blocks(text:str)->List[Dict]:
    blocks=[]
    for raw in text.splitlines():
        line=raw.rstrip()
        if not line.strip(): continue
        if line.startswith("### "):
            t,content="heading_3",line[4:]
        elif line.startswith("## "):
            t,content="heading_2",line[3:]
        elif line.startswith("# "):
            t,content="heading_1",line[2:]
        elif line.startswith("- ") or line.startswith("* "):
            blocks.append({"object":"block","type":"bulleted_list_item","bulleted_list_item":{"rich_text":[{"type":"text","text":{"content":line[2:].strip()}}]}}); continue
        else:
            blocks.append({"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":line}}]}}); continue
        blocks.append({"object":"block","type":t,t:{"rich_text":[{"type":"text","text":{"content":content}}]}})
    return blocks

def chunked(items,size=100):
    for i in range(0,len(items),size): yield items[i:i+size]

def create_child_page(headers,parent_page_id,title):
    resp=requests.post("https://api.notion.com/v1/pages",headers=headers,json={"parent":{"page_id":parent_page_id},"properties":{"title":{"title":[{"type":"text","text":{"content":title}}]}}},timeout=30)
    if resp.status_code>=300: raise RuntimeError(f"Create page failed ({resp.status_code}): {resp.text}")
    return resp.json()["id"]

def append_blocks(headers,block_id,blocks):
    url=f"https://api.notion.com/v1/blocks/{block_id}/children"
    for batch in chunked(blocks,100):
        resp=requests.patch(url,headers=headers,json={"children":batch},timeout=30)
        if resp.status_code>=300: raise RuntimeError(f"Append blocks failed ({resp.status_code}): {resp.text}")

def main():
    token=env_or_fail("NOTION_TOKEN"); parent=env_or_fail("NOTION_PARENT_PAGE_ID")
    if not DOC_ROOT.exists(): raise SystemExit(f"[ERROR] Missing docs directory: {DOC_ROOT}")
    md_files=sorted(p for p in DOC_ROOT.glob("*.md") if p.name!="README_SYNC.md")
    if not md_files: raise SystemExit("[ERROR] No markdown files found to sync.")
    headers={"Authorization":f"Bearer {token}","Notion-Version":NOTION_VERSION,"Content-Type":"application/json"}
    print(f"[INFO] Start syncing {len(md_files)} files from {DOC_ROOT} ...")
    for md in md_files:
        try:
            page_id=create_child_page(headers,parent,md.stem)
            blocks=parse_markdown_to_blocks(md.read_text(encoding='utf-8'))
            if blocks: append_blocks(headers,page_id,blocks)
            print(f"[OK] Synced {md.name} -> page_id={page_id}")
        except Exception as e:
            print(f"[ERROR] Failed syncing {md.name}: {e}")
    print("[INFO] Sync finished.")
if __name__=="__main__": main()
