# 10_next_development_roadmap

## 短期（下一阶段）
1. **可靠性任务（新增优先项）**  
   - 调查 `tests/signals/test_engine.py::test_demo_alpha_combination_behavior` 失败。  
   - 明确是“测试期望需更新”还是“实现行为需修正”。  
   - 在意图明确前，**不修改 signal logic**（避免误修造成研究基线漂移）。
2. 固化 base model 评估协议：统一输入特征、标签窗口、交易成本口径、诊断输出字段。
3. 固化 strict_top_n / buffered_top_n / benchmark 对比模板，确保可重复复盘。

## 中期（2~4 个 phase）
1. 将 constraints 从“组合构建规则”升级到更系统的 risk-control module v1（目标暴露/预算约束）。
2. 建立 diagnostics 标准报告结构（IC/quantile/turnover/decay/exposure 一体化输出）。
3. 建立数据质量与增量更新审计：schema 变更记录、缺失/异常告警、分区更新日志。

## 长期（架构演进）
1. 实验管理层：统一 experiment config、结果注册与可追溯对比。
2. 风险模型层：从约束规则走向因子风险/协方差驱动的组合优化。
3. 报告层：形成稳定的 research report / tearsheet 流程并版本化。

## 与 sqlite metadata / incremental update 相关路线
- 当前判定：README 提及 sqlite metadata 与 incremental update，但在 `src/qsys` 主路径中仍需进一步验证完整接线程度。  
- 路线建议：先补“现状核查清单”，再决定是升级现有机制还是新建 metadata registry。
