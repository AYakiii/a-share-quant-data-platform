# 09 Tests and Reliability

## 测试分布
- data/features/signals/backtest/rebalance/risk/research/utils 均有测试。
- 代表文件：`tests/features/test_feature_compute.py`, `tests/signals/test_engine.py`, `tests/backtest/test_simulator_metrics.py`, `tests/rebalance/test_buffered_top_n_backtest.py` 等。

## 覆盖情况
- 行为测试：信号变换、组合约束、回测指标。
- smoke/流程测试：部分 utils 脚本。

## 未覆盖风险
- 实际 AkShare 网络异常与数据漂移。
- 大规模全市场性能/内存压力。
- notebook 与脚本路径一致性。
