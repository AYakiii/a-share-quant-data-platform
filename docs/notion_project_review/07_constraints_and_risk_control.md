# 07 Constraints and Risk Control

## 当前已实现约束（portfolio constraints）
- liquidity filter (`min_liquidity`)
- single-name cap (`max_single_weight`)
- size-aware scaling (`size_aware_scaling`)
- group cap (`group_cap`, long-only)

## 与 full risk model 区别
- 当前是组合构建层规则约束，不是协方差矩阵驱动的风险预算模型。
- 缺少系统化因子暴露约束、tracking error 预算、场景压力测试。

## 风险控制假设
- 流动性与集中度可通过简单阈值/上限控制。
- 该假设在极端市场环境需进一步验证。
