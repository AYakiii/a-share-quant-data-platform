# README_SYNC｜Notion 同步说明（V2）

## 同步模式

### 1) child-pages 模式（目录型知识库）
- 每个 Markdown 文件会创建一个新的 Notion 子页面。
- 适合把 `00~12` 作为目录化知识库管理。
- 命令：

```bash
PYTHONPATH=src python scripts/sync_project_review_to_notion.py --mode child-pages
```

### 2) inline 模式（阅读型复盘页面）
- 所有 Markdown 文档会直接 append 到 parent page。
- 每个文件开头会加标题，文件之间插入 divider。
- 适合首次复盘时快速阅读全量内容。
- 命令：

```bash
PYTHONPATH=src python scripts/sync_project_review_to_notion.py --mode inline
```

## 重要警告（避免重复）
- `child-pages` 每次运行都会创建新子页面，可能产生重复页面。
- `inline` 每次运行都会向 parent page 继续追加，可能产生重复内容。
- 当前脚本**不会自动覆盖/清空旧内容**。
- 建议：重跑前手动清理旧同步内容，或使用全新的 parent page。

## 环境准备
1. 创建 Notion integration，拿到 token。  
2. 将目标 parent page share 给 integration。  
3. 设置环境变量：

```bash
export NOTION_TOKEN="secret_xxx"
export NOTION_PARENT_PAGE_ID="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
```

## 故障排查
- token 是否有效
- parent page 是否授权给 integration
- page id 是否正确
- 网络/API 限流问题
