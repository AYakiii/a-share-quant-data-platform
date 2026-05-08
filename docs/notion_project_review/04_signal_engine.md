# 04_signal_engine（V2）

## 模块为什么存在
Signal engine 把 feature 变成可交易排序依据。它是 Feature 与 Portfolio/Backtest 之间的关键桥梁，决定 alpha 如何被表达。

## 在主流程中的位置
Feature → Signal：输入特征列，经过横截面变换和组合，输出 `MultiIndex [date, asset]` 信号序列。

## 核心协作关系
- `load_feature_store_frame`：加载分区特征并转换索引。
- `SignalEngine.build_transformed_signals`：按 recipe 执行列提取+变换。
- 变换函数：`rank_cross_section`、`zscore_cross_section`、`winsorize_cross_section`。
- `linear_combine`：线性加权合成。
- `demo_alpha_signal`：默认示例信号。

## 输入/输出
- 输入：feature frame、recipe、weights。
- 输出：signal series（按 date/asset 对齐）。

## 关键设计选择
- recipe 风格易扩展、可解释性强。
- 先变换后线性组合，保证每一步可检查。

## MVP / technical debt / 待验证
- MVP：`demo_alpha_signal` 采用简单规则模型（动量 rank + 波动惩罚）。
- 技术债：参数管理/版本管理尚轻量；未形成统一 signal registry。
- 待验证：`tests/signals/test_engine.py::test_demo_alpha_combination_behavior` 失败提示“预期-实现一致性”问题，需明确应改测试还是改实现。

## 相关测试
- `tests/signals/test_engine.py`
- `tests/signals/test_transforms.py`
