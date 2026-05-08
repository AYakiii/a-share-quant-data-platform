# 00_index｜Notion 复盘系统入口（V2）

## 为什么需要这套系统
该仓库的目标不只是“能跑回测”，而是形成长期可复用的研究工程体系。`README.md` 明确了 Data → Panel → Feature → Signal → Backtest → Diagnostics → Constraints 的研究链路，因此需要一套跨 phase 的项目记忆系统，避免认知断层。

## 本系统解决的问题
- 把“代码结构”转化为“可持续复盘结构”。
- 将 MVP 实现与长期架构目标分层记录。
- 把测试证据、模型假设、技术债放到同一知识面板。

## 推荐阅读顺序
1. `01_architecture_map.md`：全局模块与依赖边界。  
2. `02_data_pipeline.md` + `03_panel_and_feature_store.md`：数据到特征。  
3. `04_signal_engine.md` + `05_backtest_engine.md`：信号与策略执行层。  
4. `06_diagnostics.md` + `07_constraints_and_risk_control.md`：评估与风险控制。  
5. `08_model_review.md`：模型假设与 base model 设计方向。  
6. `09~12`：可靠性、路线图、项目管理机制、phase 模板。

## 当前状态（代码依据）
- 已实现：feature store v1、signal engine v1、backtest MVP、diagnostics v1（见 `src/qsys/features`、`src/qsys/signals`、`src/qsys/backtest`、`src/qsys/research`）。
- 部分实现：benchmark/report workflow、risk-control 更偏 constraints 而非 full risk model（见 `src/qsys/rebalance`、`src/qsys/backtest/portfolio.py`）。
- 需验证：README 中 sqlite metadata/incremental update 与 `src/qsys` 主路径的接线完整度。
