# 08 Model Review（核心）

## 当前模型形态
- 更接近“规则打分模型（rule-based scoring）+ 约束化组合构建”，而非统计学习或优化器主导模型。

## 当前因子假设
- momentum: `ret_20d` 排名为主信号。
- volatility treatment: `-0.5*zscore(vol_20d)` 线性惩罚。
- liquidity/size: 主要在组合约束层处理。
- reversal: 当前主流程未见独立 reversal 因子默认接入。

## 为什么线性 volatility penalty 可能不足
- 波动与未来收益关系可能非线性、状态依赖；固定系数在不同 regime 不稳定。

## base model 方向
1. 先做统一评估协议：walk-forward + bootstrap CI。
2. 将 signal recipe 参数化并网格化（结合 `run_signal_sanity_grid.py`）。
3. 引入条件化模型（按波动/流动性 regime 分层）。
4. 再考虑非线性模型（树模型/简单 NN）并与线性基线做同口径比较。

## 增复杂度前先验证
- 标签泄漏、调仓延迟、交易成本鲁棒性、稳定性区间。
