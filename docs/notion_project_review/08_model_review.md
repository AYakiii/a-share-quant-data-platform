# 08_model_review
## 当前模型本质
当前更接近**规则打分模型（rule-based scoring model）**：
- 动量：`ret_20d` rank
- 波动惩罚：`vol_20d` zscore 线性负权
- 组合：top_n + 规则约束

## 已实现假设
- momentum 在横截面上有效（`demo_alpha_signal`）。
- volatility 线性惩罚可改进风险收益。
- 流动性/规模处理在组合层，而非信号层（部分中性化函数未主流程默认使用）。

## 风险
- 线性 volatility penalty 可能在高波动 regime 下失效。
- 无显式 reversal 因子默认接入。
- 无状态切换（regime-dependent）与非线性组合。

## base model 方向
1) 先做“可解释 baseline”：momentum + volatility + liquidity + size（固定权重与回归权重并行）。
2) 加入稳定性验证：分市场状态、分市值桶、分流动性桶。
3) 再考虑非线性：分段函数/树模型/状态机，不直接跳复杂模型。

## 增复杂度前必测
- 标签泄露检查、窗口敏感性、交易成本弹性、调仓频率鲁棒性。
