# Phase15e Real Feature-Store Status Audit

## Audit scope
本次审计目标：判断当前仓库是否仅完成 CSI universe/index-member ingestion，还是已完成可用于 baseline candidate suite evaluation 的真实 AkShare feature-store 构建。

审计范围（按要求）：
- `src/qsys/universe/`
- `src/qsys/utils/`
- `docs/`
- `README.md`
- `tests/`
- 本地未追踪数据目录与输出目录（若存在）

---

## 1) CSI universe / index-member ingestion status

### 结论
**当前仓库内证据不足以认定“CSI300/CSI500 成分股（index constituents/member） ingestion 完成”。**

### 证据
1. `src/qsys/universe/eligibility.py` 只实现了基于特征列的可交易/阈值过滤（`build_eligibility_mask`, `apply_eligibility_mask`），未见“指数成分拉取/标准化/落盘”逻辑。  
2. `src/qsys/rebalance/index_benchmarks.py` 实现的是指数价格曲线/收益曲线加载（`load_akshare_index_benchmark_curve`, `build_index_return_curve`），用于 benchmark return，不是 constituent/member ingestion。  
3. `README.md` 虽提到 benchmark comparison，但未声明 CSI constituents 数据管线。  
4. tests 中关于 CSI 主要是 benchmark 价格曲线与报告引用（如 `tests/rebalance/test_index_benchmarks.py`、`tests/rebalance/test_report_rebalance_policy_comparison.py`），并非成分股 ingestion 测试。

### 审计判断
- **CSI index benchmark return ingestion**：部分实现（价格曲线层）。
- **CSI index member/constituent ingestion**：**未在本仓库主路径中确认**。

---

## 2) Real AkShare feature-store construction status

### 结论
**代码能力层面：已实现 real feature-store builder。**  
**运行产物层面：本仓库当前工作区未发现 `data/processed/feature_store/v1` 实际分区数据，无法直接确认“已稳定完成真实构建”。**

### 证据（代码能力）
1. `src/qsys/utils/build_real_feature_store.py` 明确实现 AkShare 日线抓取、重试、标准化、分区写盘。  
2. `REQUIRED_COLUMNS` 包含 `ret_1d/ret_5d/ret_20d/fwd_ret_5d/fwd_ret_20d` 等关键列。  
3. `README.md` 提供了 real feature-store 构建命令，目标路径是 `data/processed/feature_store/v1`。  
4. `tests/utils/test_build_real_feature_store.py` 验证了分区写入与列完整性（基于 monkeypatch mock 数据）。

### 证据（产物存在性）
- 本地文件系统检查未发现 `data/`、`outputs/` 下对应 real run 产物（见“执行命令”章节）。
- `.gitignore` 明确忽略 `data/`、`*.parquet`、`*.db`，说明数据产物通常不入库，**不能据此判定失败**。

### 审计判断
- **功能实现**：是（Implemented）。
- **稳定实跑完成并可复核**：当前仓库中**未确认**（Requires verification，主要因数据产物未随仓库提供）。

---

## 3) Baseline candidate suite real run status

### 结论
**未在当前仓库中发现 `outputs/baseline_candidate_suite_real/` 相关产物，无法确认 real baseline candidate suite run 完成。**

### 目标文件检查结果
未发现以下文件：
- `outputs/baseline_candidate_suite_real/run_manifest.json`
- `outputs/baseline_candidate_suite_real/signal_quality_report.csv`
- `outputs/baseline_candidate_suite_real/warnings.md`

### 审计判断
- **real baseline candidate suite run complete**：当前证据下不能确认。
- 更接近状态标签：`blocked by data artifacts unavailable in repo`。

---

## 4) Provenance checks (synthetic vs real)

### 结论
- 当前工作区未发现 `_feature_store_provenance.json`。
- 未发现 `run_manifest.json` 标记 `data_source_type=real` 的证据文件。
- 未发现 `data/sample/feature_store/v1` 与 `data/processed/feature_store/v1` 实体目录可供比对。

### 风险提示
仓库中存在 `src/qsys/utils/generate_synthetic_feature_store.py`（synthetic 生成脚本），因此在没有 provenance 文件时，不能把任意本地 feature-store 自动视为 real。

---

## 5) Should old status be updated?

旧状态："real AkShare feature store has not been stably built yet"。

### 建议更新方式（分层）
建议改成更精确的双层状态：
1. **代码实现状态**：`real feature-store build pipeline implemented`。  
2. **运行与可复核状态**：`real feature-store stable build not yet confirmed in repo artifacts`。

也就是：**可以更新“未实现”为“已实现但未在仓库内完成稳定产物验收证明”。**

---

## 6) Suggested final status labels

基于当前证据，建议使用：
- ✅ `CSI universe ingestion complete, real feature store not confirmed`（谨慎：其中 CSI universe 更准确应写“benchmark index ingestion 部分完成；constituent ingestion 未确认”）
- ✅ `blocked by data artifacts unavailable in repo`
- ⛔ `real feature store small-sample build complete`（当前仓库无实物证据，不建议直接打）
- ⛔ `real baseline candidate suite run complete`（未发现 run 产物）
- ⚠️ `blocked by AkShare/network/schema issue`（当前审计无法直接证明该阻塞，需实跑日志）

---

## 7) Exact local verification commands (when data is available locally)

> 下列命令用于你本地（有数据和网络时）进行最终确认。

### A. 检查 real feature-store 分区是否存在
```bash
find data/processed/feature_store/v1 -maxdepth 2 -type f -name 'data.parquet' | wc -l
```

### B. 检查日期范围 / 资产数 / 必需列
```bash
python - <<'PY'
from pathlib import Path
import pandas as pd

root = Path('data/processed/feature_store/v1')
files = sorted(root.glob('trade_date=*/data.parquet'))
print('partition_count=', len(files))
if not files:
    raise SystemExit('No partitions found')

first = pd.read_parquet(files[0])
print('sample_columns=', list(first.columns))

req = {'ret_1d','ret_5d','ret_20d','fwd_ret_5d','fwd_ret_20d'}
print('required_columns_present=', req.issubset(set(first.columns)))

all_dates = [p.parent.name.split('=',1)[1] for p in files]
print('date_min=', min(all_dates), 'date_max=', max(all_dates))

assets = set()
for p in files[:20]:
    df = pd.read_parquet(p, columns=['ts_code'])
    assets.update(df['ts_code'].dropna().astype(str).unique().tolist())
print('unique_assets_from_first_20_partitions=', len(assets))
PY
```

### C. 检查 provenance（是否 synthetic）
```bash
find data -maxdepth 5 -type f -name '*provenance*.json' -o -name 'run_manifest.json'
```

### D. 检查 baseline real suite 输出
```bash
ls -lah outputs/baseline_candidate_suite_real
cat outputs/baseline_candidate_suite_real/run_manifest.json
head -n 20 outputs/baseline_candidate_suite_real/signal_quality_report.csv
cat outputs/baseline_candidate_suite_real/warnings.md
```

---

## 8) Commands executed in this audit

```bash
find src/qsys/universe -type f | sort
find src/qsys/utils -type f | sort
rg -n "CSI|index member|constituent|...|run_manifest|data_source_type|synthetic|sample" README.md src/qsys docs tests .gitignore
find data -maxdepth 5 -type d 2>/dev/null
find outputs -maxdepth 4 -type f 2>/dev/null
find . -maxdepth 4 -type f \( -name "*provenance*.json" -o -name "run_manifest.json" -o -name "warnings.md" -o -name "signal_quality_report.csv" \)
```

---

## Final answer (short)

- **Is CSI universe ingestion complete?**  
  本仓库内仅能确认指数 benchmark 价格曲线 ingestion（CSI300/CSI500），不能确认 constituent/member ingestion 完成。

- **Is real AkShare feature store construction complete?**  
  代码实现已完成；但本仓库当前未提供可复核 real 产物，稳定完成状态未确认。

- **Is baseline candidate suite real run complete?**  
  未发现 `outputs/baseline_candidate_suite_real` 证据，不能确认完成。

- **Should old status be updated?**  
  应更新为“构建能力已实现，但 real 稳定产物与基线 real run 仍需基于本地产物/manifest 复核确认”。
