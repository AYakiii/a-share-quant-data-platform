# 09_tests_and_reliability

## 测试体系现状
当前仓库已覆盖 `data/features/signals/backtest/rebalance/research/risk/universe/utils` 多层测试，偏向行为正确性（behavior test）与模块回归保护。代表文件：`tests/signals/test_engine.py`、`tests/backtest/test_portfolio_constraints.py`、`tests/research/test_ic.py`、`tests/rebalance/test_buffered_top_n_backtest.py`。

## 本次已记录的定向测试证据
- 运行命令：

```bash
PYTHONPATH=src pytest -q tests/signals/test_engine.py tests/backtest/test_portfolio.py tests/data/test_daily_panel.py
```

- 结果摘要：
  - `tests/backtest/test_portfolio.py`：通过（pass）。
  - `tests/data/test_daily_panel.py`：通过（pass）。
  - `tests/signals/test_engine.py`：存在 1 个失败。
- 失败用例：
  - `tests/signals/test_engine.py::test_demo_alpha_combination_behavior`

## 为什么这个失败重要
该失败直接关联 `demo_alpha_signal` 的行为预期与实现一致性：
- 若是测试预期错误，会误导后续信号设计判断。
- 若是实现偏离预期，会影响示例 alpha 在研究链路中的解释基线。
- 虽然是 demo 级信号，但它在学习/演示路径中是“默认参考行为”。

## 与本次文档/同步改动的关系判断
本次改动仅涉及：
- `docs/notion_project_review/*.md`
- `docs/notion_project_review/project_map.json`
- `README_SYNC.md`
- Notion 同步脚本（无 signal 逻辑变更）

因此，现有证据更支持该失败是**既有行为一致性问题**，而非本次文档改动引入。

## 建议下一步排查（不直接改逻辑）
1. 复核 `test_demo_alpha_combination_behavior` 的业务意图：确认测试期望是否合理。  
2. 对照 `qsys.signals.engine.demo_alpha_signal` 与 `qsys.signals.transforms.zscore_cross_section/rank_cross_section` 的数值路径做逐步打印。  
3. 明确是“应改测试期望”还是“应改实现逻辑”，在意图确认前不修改 signal 生产逻辑。  
4. 将结论写入 phase log，作为后续 signal baseline 决策依据。

## 覆盖与盲区
- 已覆盖：索引对齐、组合约束、回测成本与指标、部分脚本入口。
- 未充分覆盖：真实网络数据波动、性能与规模回归、统一端到端基线快照。
