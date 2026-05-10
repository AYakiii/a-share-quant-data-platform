# Phase15e Real Feature-Store Builder Debug Note

## Problem
在 Colab 传入 CSI 成分 symbol 列表（`--symbols`）时，`build_real_feature_store` 报错：

`ValueError: No data fetched from AkShare for requested symbols/date range`

尽管 notebook 中单 symbol AkShare 调用可返回数据。

## Root-cause hypotheses verified in code
1. 原实现 `_normalize_daily_frame` 对中文列支持不足（仅映射 `日期`/`股票代码`，未映射 `开盘/最高/最低/收盘/成交量/成交额/换手率/代码`）。
2. 原实现 `_safe_fetch_daily` 只调用 `stock_zh_a_daily`，无 `stock_zh_a_hist` fallback。
3. 日期过滤在字符串层进行（`trade_date >= start_date`），在部分格式/类型场景容易导致全部被过滤。
4. 无 `--skip-failed-symbols` 和逐 symbol 诊断可见性，难定位“全部被丢弃”的具体原因。

## Minimal fix applied
仅改数据构建脚本与测试，不改 signal/backtest/research 逻辑。

### 1) Fetch fallback
- `_safe_fetch_daily`：先试 `ak.stock_zh_a_daily(symbol="sh600000", adjust="")`；空时 fallback 到 `ak.stock_zh_a_hist(symbol="600000", period="daily", adjust="")`。
- 新增 `_to_hist_symbol` 从 `sh/sz/bj` 前缀提取 6 位代码。

### 2) Column normalization hardening
`_normalize_daily_frame` 现在同时支持英文字段与中文字段：
- `date/日期 -> trade_date`
- `open/开盘 -> open`
- `high/最高 -> high`
- `low/最低 -> low`
- `close/收盘 -> close`
- `volume/成交量 -> volume`
- `amount/成交额 -> amount`
- `turnover/换手率 -> turnover`
- `code/股票代码/代码 -> ts_code`

### 3) Robust date filtering
- 在 `build_real_feature_store` 内先将 `trade_date` 转为 datetime，再执行 `start_date/end_date` 过滤，最后统一转回 `YYYY-MM-DD` 字符串。

### 4) Diagnostics + skip mode
新增参数：
- `--skip-failed-symbols`
- `--verbose`

`--verbose` 输出包括：
- symbol
- raw shape / columns
- raw date min/max（若可解析）
- normalized shape（过滤前后）
- skip reason（`raw_empty` / `empty_after_date_filter` / `fetch_or_normalize_error:*`）

## Tests added/updated
`tests/utils/test_build_real_feature_store.py` 新增覆盖：
1. 英文列 daily 响应可写分区。
2. 中文列 hist 响应可标准化且输出 `REQUIRED_COLUMNS`。
3. `start_date=2025-01-01` 时保留 2025 行。
4. `skip_failed_symbols=True` 时坏 symbol 被跳过，其他 symbol 继续产出。

## Targeted test command
```bash
PYTHONPATH=src pytest -q tests/utils/test_build_real_feature_store.py
```

## Non-goals (explicit)
- 未新增 alpha 候选
- 未改 signal 逻辑
- 未改 baseline candidate suite 逻辑
- 未改风险控制/ML
- `REQUIRED_COLUMNS` 保持不变
