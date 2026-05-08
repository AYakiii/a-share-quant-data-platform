# 07_constraints_and_risk_control
## 已实现约束
在 `build_top_n_portfolio` 中：
- liquidity filter（`liquidity` + `min_liquidity`）
- `max_single_weight`
- `size_aware_scaling`
- `group_cap`（long-only）

## 与完整 risk model 的差异
- 当前主要是“组合构建时约束”（portfolio constraints）。
- 尚未实现协方差风险预算、行业/风格目标暴露控制、tracking error 优化。

## 假设与技术债
- same-date 可用性假设成立（anti-lookahead 基础）。
- group label 质量与稳定性未见完整校验链路。
- 约束效果评估脚本存在（`constraint_impact.py`），但未形成长期监控。
