# 04 Signal Engine

## 当前实现
- `SignalEngine` 支持 recipe 方式拼装信号列并应用 transform。
- transform 已实现：winsorize/zscore/rank。
- 组合：`linear_combine`。
- demo 预置信号：`demo_alpha_signal = rank(ret_20d) - 0.5*zscore(vol_20d)`。

## 假设与问题
- 假设：横截面标准化可提升稳健性。
- 问题：线性权重固定，缺少 regime/self-adaptive 机制。

## liquidity/size/reversal
- signal 层未直接实现 liquidity/size/reversal 中性化流程（size/group neutralization 方法存在于 `signals/transforms.py`，但默认 demo 未接入）。

## 技术债
- recipe 缺少配置化持久层（YAML/JSON）。
- 缺少信号版本追踪与回放。
