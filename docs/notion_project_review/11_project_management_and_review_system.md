# 11_project_management_and_review_system
## 维护原则
- 每个开发 Phase 必须同步更新：00/01/对应模块文档 + tracking log。
- 每次变更都记录：目的、假设、影响范围、回滚条件。

## MVP vs 长期架构
- 在文档中单列“临时 MVP 实现”与“长期架构方向”。
- 如 `execution` fallback、线性成本模型，应标记为 MVP 假设。

## technical debt 追踪
- 每个债务项写明：来源文件、风险、触发条件、优先级、偿还建议。

## 假设追踪
- 模型假设：统一记录在 `08_model_review.md` 与 phase log。
- 风控假设：统一记录在 `07_constraints_and_risk_control.md`。

## 防止过时
- 约定：代码改动 PR 合并前必须检查对应文档段落。
- 每月做一次“文档-代码一致性审查”。
