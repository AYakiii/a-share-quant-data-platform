# PROJECT_SYSTEM_MANUAL_DRAFT（V3 草案）

> 中文优先长文草案。目标是把仓库解释为“运行中的研究系统”，不是文件清单。

## 1. 这个项目当前是什么
本项目是一个面向 A-share 日频/低频研究的量化系统框架。核心价值不是单一策略收益，而是**研究流程可重复**：数据接入、特征构建、信号形成、组合构建、回测评估、诊断反馈、约束影响分析。`README.md` 中明确了该定位，并区分了已实现 V1 与未实现生产能力。

**状态判断**
- 已实现：Feature Store v1、Signal Engine v1、Backtest MVP、Diagnostics v1。
- 部分实现：Benchmark/Report 工作流、risk-control 系统化能力。
- 尚未实现：实盘 OMS/EMS、完整风险模型与优化器。

## 2. 总体运行流程（系统视角）
系统主链路可写为：

`Data → Panel → Feature → Signal → Portfolio → Backtest → Diagnostics → Constraints → Report`

每个箭头都对应一个明确的代码层：
- Data：`qsys.utils.build_real_feature_store`
- Panel：`qsys.data.panel.daily_panel`
- Feature：`qsys.features.compute/store/registry`
- Signal：`qsys.signals.engine/transforms/combine`
- Portfolio+Backtest：`qsys.backtest.*`
- Diagnostics：`qsys.research.*`
- Constraints/Risk：`qsys.backtest.portfolio`, `qsys.risk.exposure`, `qsys.research.constraint_impact`
- Report/Rebalance：`qsys.rebalance.*`, `qsys.utils/report_*`

## 3. Data → Panel
数据入口 `build_real_feature_store` 负责抓取、标准化、分区写入。设计重点在字段规范与可回放性：写出 `trade_date=.../data.parquet`，并统一列集合。`daily_panel.py` 读取后转为 `MultiIndex [date, asset]`，形成研究层统一数据接口。

### 实现状态
- 已实现：抓取、标准化、分区、读取。
- 需验证：README 提到 sqlite metadata/incremental update 与主路径接线程度。

### 风险点
- 外部 API 波动
- schema 演进没有完整治理工具链

## 4. Panel → Feature Store
`default_feature_registry` 统一定义回报、波动、流动性、市值和 forward returns。`compute_features` 按 feature 名列表计算并对齐索引；`write_feature_store` 负责落盘。这里的核心是“特征生成可重放”，而不是复杂特征数量。

### 设计选择
- 函数式特征定义（`FunctionFeature`）可快速扩展。
- 强依赖 `MultiIndex [date, asset]` 规范。

### MVP 与技术债
- MVP：版本治理轻量，速度优先。
- 债务：缺 feature availability 时间协议和 registry 版本锁定。

## 5. Feature Store → Signal
Signal engine 使用 recipe 机制构建信号：选择列、做横截面变换、线性组合。默认 demo 是 `rank(ret_20d) - 0.5*zscore(vol_20d)`，体现“动量 + 波动惩罚”的规则模型思路。

### 关键问题
- demo 信号主要用于研究样例，不等于最终生产模型。
- 当前存在 `test_demo_alpha_combination_behavior` 的行为一致性问题，必须先确认“预期与实现谁应调整”。

## 6. Signal → Portfolio
`build_top_n_portfolio` 把 signal 映射成权重，支持流动性过滤、单票上限、规模感知缩放、group cap。其定位是“研究期组合构建器”，用规则保证可解释。

### 关键假设
- 同日可得信息可用于约束（anti-lookahead 语义依赖输入时点正确性）。
- 规则约束足以做第一阶段风险控制。

## 7. Portfolio → Backtest
`run_backtest_from_signal`/`run_backtest_from_weights` 完成执行对齐、收益计算、成本扣减、绩效汇总。配置由 `BacktestConfig` 集中管理。

### MVP 简化
- 执行模型简化（`next_open` fallback 说明了现实差异）。
- 交易成本线性化，不含冲击与容量。

## 8. Backtest → Diagnostics
research 模块给出 IC、quantile、decay、turnover、exposure、correlation 等指标。这些指标不是“结论本身”，而是帮助回答：
- 信号是否有预测性？
- 收益是否来自稳定结构？
- 是否被高换手/特定暴露驱动？

### 现实局限
- 指标虽多，但统一报告 schema 仍较弱。
- 显著性与稳健性验证需补强。

## 9. Constraints vs Full Risk Control
当前项目已具备较完整**组合约束**能力，但这与 full risk model 不同：
- 已有：liquidity、single-name cap、group cap、size-aware scaling。
- 未有：协方差驱动风险预算、TE 约束优化、全局风险目标函数。

因此应把当前能力定义为“risk-control 前置层（constraints-first）”，而非完整 risk engine。

## 10. 当前模型假设
系统当前更像“可解释规则模型基线”：
- 动量排序 + 波动惩罚
- 约束式组合构建
- 指标驱动迭代

这有利于研究透明，但在 regime shift、非线性结构、容量压力下可能不足。

## 11. MVP 简化与 technical debt
1) Data 层：缺 metadata registry 与系统化增量调度。  
2) Feature 层：缺版本治理与可用时点协议。  
3) Signal 层：参数治理与行为一致性问题待清理。  
4) Backtest 层：执行与成本模型简化。  
5) Diagnostics 层：缺统一报告与显著性框架。  
6) Risk 层：约束存在但 full risk model 未实现。

## 12. 如何查询并验证细节
建议使用 `PROJECT_QUERY_GUIDE.md` 做入口，再回到对应源码与测试。  
每次得到结论时，至少绑定三类证据：
- 代码函数/类
- 测试文件
- 运行命令或脚本输出

## 13. 下一步开发优先级（建议）
1. **可靠性优先**：先解决 signal demo 的测试一致性判定。  
2. **协议优先**：统一 feature/signal/backtest 的实验协议（输入、标签、口径）。  
3. **风险优先**：从 constraints 过渡到 risk model v1（先目标暴露，再优化器）。  
4. **报告优先**：把 diagnostics 与 backtest 结果标准化输出，支持长期复盘。

---

## 附：Implemented / Partially implemented / Not implemented / Requires verification
- Implemented：panel、feature v1、signal v1、backtest MVP、基础 diagnostics。
- Partially implemented：benchmark/report workflow、constraints 到 risk-control 过渡。
- Not implemented：full risk model + optimizer、production OMS/EMS。
- Requires verification：sqlite metadata 与 incremental update 在主路径的完整接线。
