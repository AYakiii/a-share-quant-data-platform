# 06 Diagnostics

## 已实现
- IC/Rank IC：`research/ic.py`。
- quantile return/top-minus-bottom：`research/quantiles.py` 与 `research/signal_quality/quantile.py`。
- turnover/decay：`research/turnover.py`, `research/decay.py`。
- exposure：`research/exposure.py`, `risk/exposure.py`, `research/portfolio_exposure.py`。

## 解释框架
- Rank IC 看排序预测能力；quantile spread 看分层可交易性；turnover 看可实施性；decay 看持有期适配。

## 弱点与误读风险
- 小样本时期 IC 波动易误读。
- 若未统一 universe/可交易过滤，诊断可能偏乐观。
