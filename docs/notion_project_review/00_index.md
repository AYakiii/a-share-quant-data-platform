# 00 Index｜项目复盘入口

## 项目整体定位
本项目是 A-share 日频量化研究框架，定位在研究与验证，不是生产交易系统。依据 `README.md` 的流程定义为 Data→Panel→Feature→Signal→Backtest→Diagnostics→Constraints。

## 知识库用途
- 建立长期项目记忆（架构、假设、技术债）。
- 支持 phase 迭代时的变更追踪。

## 使用方式
1. 先看 `01_architecture_map.md`。
2. 按研究链路阅读 02-08。
3. 在每次迭代后更新 `12_project_tracking_log_template.md`。

## 当前实现状态总览
- 已实现：feature store v1、signal engine v1、backtest MVP、diagnostics v1、portfolio constraints v1。
- 部分实现：benchmark 与 buffered rebalance（在 `qsys/rebalance`）。
- 尚未实现：完整 risk model、优化器、OMS/EMS。

## 开放问题
- `build_real_feature_store` 与 `features/store.py` 双路径并存，长期应统一。
- 约束目前主要在组合层，不等于完整 risk-control module。
