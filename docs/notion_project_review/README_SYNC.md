# Notion 同步工具说明

## 1) 创建 Notion integration
在 Notion integrations 页面创建 internal integration，获得 token。

## 2) 分享 parent page
把目标父页面 Share 给该 integration（否则 API 无权限创建子页）。

## 3) 环境变量
```bash
export NOTION_TOKEN="secret_xxx"
export NOTION_PARENT_PAGE_ID="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
```

## 4) 运行
```bash
PYTHONPATH=src python scripts/sync_project_review_to_notion.py
```

## 5) 同步失败排查
- token 是否有效
- parent page 是否已 share
- page id 格式是否正确
- 网络是否可访问 `api.notion.com`

## 6) 当前限制
- 仅支持简单标题/段落/无序列表。
- 表格、代码块、复杂嵌套会被简化。
