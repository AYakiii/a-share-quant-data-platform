# 13_strategy_contract_mfs_v04｜MFS-v0.4 策略研究记录

## 0. 记录定位

本记录用于保存 2026-06-18 讨论后形成的最新策略设计结论。它不是实盘交易说明，也不是已经验证有效的 alpha 声明，而是一份可执行、可审计、可继续工程化的 Strategy Contract 草案。

本记录继承项目主线：

```text
Data → Panel → Feature → Signal/Model → Portfolio → Backtest → Diagnostics → Constraints → Report
```

并明确：项目当前仍是 A 股中低频量化研究平台，不是 production trading system。

---

## 1. 总体结论

原先的“固定权重多因子 + Top-N 等权组合”不足以作为正式策略系统。新的主线应升级为：

```text
MFS-v0.4 = Fundamental-Gated Learned Multi-Factor Strategy with Turnover-Aware Portfolio Construction
```

核心思想：

```text
PIT 基本面软筛
→ 日频因子族压缩
→ 学习型 Alpha 模型
→ score-to-expected-return calibration
→ Mini-Barra-style 风险约束
→ turnover-aware / buffered portfolio construction
→ A 股可执行回测
→ manifest / warnings / artifacts 记录
```

该路线的目标不是马上追求复杂模型，而是先把数学对象、信息集、数据可用性、组合约束和执行假设全部写清楚。

---

## 2. 审计后的关键修正

### 2.1 执行约束必须进入回测

`t+1 open` 只能作为简化成交价格假设，不等于真实可执行性。A 股环境下必须至少记录：

```text
停牌
涨停买不进
跌停卖不出
无有效开盘价
ST / 退市风险
交易成本
滑点压力测试
```

目标仓位和实际成交仓位必须区分：

```text
p_target → p_exec → net_return
```

真实收益使用 `p_exec`，而不是理想目标权重。

### 2.2 派生变量必须克制

不能把大量同源派生变量直接输入模型。应先做因子族压缩：

```text
price family
liquidity family
friction family
margin family
risk / exposure family
fundamental quality family
```

每个 family 内部先去重、聚合或选代表，再进入 alpha model。

### 2.3 固定人为权重不可作为正式策略

类似：

```text
score = 0.6 * technical + 0.4 * liquidity
```

只能作为 smoke test 或 sanity check，不能作为正式模型。正式策略必须使用过去数据学习权重或学习排序函数。

候选模型线：

```text
1. Rolling ElasticNet / Huber Ridge expected-return model
2. Direct portfolio utility learning
3. LightGBM Ranker / learning-to-rank challenger
4. IC-optimal weights only as diagnostic baseline
```

### 2.4 RankIC 保留，但不能作为唯一目标

RankIC 是 alpha diagnostic，不是最终策略目标。必须同时记录：

```text
RankIC
TailIC
Top-minus-benchmark spread
Precision@N
score calibration by bins
net return
turnover
cost / gross return
failed execution rate
exposure summary
```

核心目标应更接近：

```text
maximize expected net return under risk, turnover, execution and exposure constraints
```

### 2.5 市值暴露不应简单视为污染

市值可能代理流动性、成熟度、融资约束、信息扩散速度、风险偏好等信息。处理方式应分三版本：

```text
Raw version
Size-neutral version
Size-constrained version
```

如果 Raw 有效但 Neutral 失效，应诚实命名为 size/liquidity style strategy，而不是包装成纯 alpha。

### 2.6 Top-N 需要升级为持仓稳定机制

单期截面 Top-N 会导致过高换手。应引入：

```text
N_buy / N_sell buffer
m_in / m_out 连续入围或跌出确认
rank smoothing
turnover-aware objective
transaction cost stress test
```

Buffered Top-N 可作为连续优化前的离散近似。

### 2.7 组合权重必须允许小于满仓

正式组合约束应为：

```text
0 <= p_i <= u_i
sum_i p_i <= e_t
0 <= e_t <= 1
```

现金仓位：

```text
cash_t = 1 - sum_i p_i
```

仓位强度 `e_t` 应由信号强度、模型置信度、市场风险、流动性风险和成本覆盖情况决定，而不是默认满仓。

### 2.8 风控不能简化为波动率择时

风险应拆分为：

```text
market risk
liquidity / execution risk
micro / fundamental risk
model confidence risk
future macro risk extension
```

v0.4 暂不假装完整解决宏观风险，但要预留 `exposure engine` 接口。

### 2.9 基本面粗筛采用 soft gate，不做永久硬排除

基本面数据不是日频 alpha，而是慢变量候选池质量控制。重点关注：

```text
盈利质量
现金流质量
资本效率 / capital productivity proxy
```

杠杆跨行业差异大，第一版只作为 risk warning 或行业内比较，不作为强筛。成长高度依赖产品和行业属性，暂不作为核心粗筛变量。

基本面 gate 应避免永久排除健康但特殊的企业。

---

## 3. 数学合同

### 3.1 信息集

在交易日 `t`，所有特征必须属于：

```text
F_t
```

未来收益 label 只能用于训练和评估，不能作为 feature。

主 label 可定义为：

```text
y_i,t^(h) = log(open_i,t+h / open_i,t+1)
```

但这只是训练标签；真实组合收益由 execution layer 计算。

### 3.2 PIT 基本面软筛

财报使用必须满足：

```text
available_time <= t
```

如果只有 update_date，则保守使用 next_trading_day(update_date)。

基本面 soft gate：

```text
g_fund_i,t = G(B_i,t) in [0, 1]
```

其中 `B_i,t` 是截至 `t` 最新可用财报信息。

建议基本面变量：

```text
ProfitQuality
CashFlowQuality
CapitalProductivityProxy
LeverageRisk as warning / industry-relative control
```

### 3.3 日频因子族表示

将原始日频特征先转为稳健横截面表示，再聚合为 family representation：

```text
g_daily_i,t = [g_price, g_liquidity, g_friction, g_margin, g_risk]
```

其中 risk family 首先作为 exposure / constraint / diagnostic，不默认作为 alpha。

### 3.4 学习型 Alpha

正式 score：

```text
s_i,t = f_theta_t(g_daily_i,t, g_fund_i,t)
```

其中：

```text
theta_t = Learn({g_i,tau, y_i,tau}_{tau < t})
```

只允许使用过去窗口。

第一版主模型建议：

```text
Rolling ElasticNet / Huber Ridge expected-return model
```

机器学习 challenger：

```text
LightGBM Ranker, with each trade date as one ranking group
```

### 3.5 Score calibration

score 不能直接等同于 expected return。需要历史窗口校准：

```text
score → expected_return_hat
```

通过 score bins 估计：

```text
E[y | score in bin_m]
```

用于区分胜率和期望收益。

### 3.6 Mini-Barra-style 风险层

不直接上完整 Barra，但需要风险暴露层：

```text
B_t = [size, liquidity, volatility, momentum, optional industry]
Sigma_t = B_t Omega_t B_t' + D_t
```

用途：

```text
风险解释
暴露约束
size/liquidity/volatility 归因
检测伪 alpha
```

### 3.7 Portfolio construction

正式目标：

```text
maximize mu_hat_t' p_t - lambda/2 * p_t' Sigma_t p_t - C(p_t, p_t_minus)
```

约束：

```text
0 <= p_i,t <= u_i
sum_i p_i,t <= e_t
p_i,t = 0 if g_fund_i,t = 0
exposure deviations <= delta
```

交易成本项：

```text
C = commission + stamp_duty + transfer_fee + slippage + impact_proxy + turnover_penalty
```

v0.4 中 impact_proxy 可先置 0，但必须保留成交额容量约束。

### 3.8 Buffered Top-N 离散近似

第一版若不直接解连续优化，可使用：

```text
buy if smoothed_rank <= N_buy for m_in periods
hold while smoothed_rank <= N_sell
sell if smoothed_rank > N_sell for m_out periods
```

其中：

```text
N_buy < N_sell
```

这对应交易成本下的 no-trade region 直觉。

### 3.9 Exposure strength

不默认满仓：

```text
sum_i p_i,t <= e_t
```

可用近似公式：

```text
e_t = clip((mu_hat_p,t - TC_t) / (lambda * sigma_hat_p,t^2), 0, 1)
```

含义：预测收益覆盖不了成本时降低仓位；风险上升时降低仓位；信号更强且置信度更高时提高仓位。

---

## 4. 数据可实现性

v0.4 所需数据不应超出现阶段可获取范围。

### 4.1 必需数据

```text
trading_calendar
stock_basic_table
daily_bar_panel
index_bar_panel
financial_statement_pit_panel
margin_panel
industry / board / ST status if available
```

### 4.2 必需字段

```text
open / high / low / close / pre_close
volume / amount / turnover
market_cap / float_market_cap
is_tradable / suspension / ST / board
index close / index return
financial report_date
financial update_date or announcement_date
operating_profit / gross_profit / net_profit
operating_cash_flow
fixed_assets / working_capital / invested capital proxy
total_assets / total_liabilities
financing_balance / financing_buy / financing_repayment
securities lending fields if available
```

### 4.3 数据质量 warning

必须显式记录：

```text
financial_pit_proxy_if_update_date_only
industry_neutralization_incomplete
execution_constraints_approximated
impact_model_not_implemented
macro_risk_not_implemented
full_barra_model_not_implemented
```

---

## 5. 实验路线

### Line 0：Sanity benchmarks

```text
Equal-weight universe benchmark
Naive Top-N signal benchmark
No-gate baseline
```

### Line 1：Soft fundamental gate + simple daily score

用于验证基本面 gate 是否误杀样本，不能作为最终 alpha。

### Line 2：Soft gate + Rolling ElasticNet / Huber Ridge

解释型主 baseline。

### Line 3：Soft gate + direct portfolio utility learning

长期主线，直接优化净效用。

### Line 4：Soft gate + LightGBM Ranker

机器学习经验线，必须 walk-forward 验证。

### Line 5：Mini risk model / exposure constrained portfolio

用于验证收益来源和控制暴露，不直接产生 alpha。

---

## 6. 验收标准

v0.4 不能只看净值曲线。第一轮必须检查：

```text
1. 所有 feature 有 PIT 可用性说明
2. financial data 使用 available_time 规则
3. fwd_ret labels 没有进入 feature
4. 因子族相关矩阵不过度冗余
5. 基本面 gate 不永久误杀特殊公司
6. alpha 权重从过去数据学习
7. score calibration 显示高分组有更高期望收益
8. buffered / optimizer 后 turnover 低于 naive Top-N
9. 成本和滑点压力测试后结果不过度坍塌
10. failed execution rate 单独统计
11. Raw / SizeNeutral / SizeConstrained 三版本解释 size/liquidity 暴露
12. 每个正式实验有 manifest / warnings / artifacts
```

---

## 7. 与现有路线的关系

本记录不是推翻 Stage 1-4，而是把 Stage 3 的 Signal / Model Workflow 和 Stage 4 的 Experiment Registry 具体化为一条策略主线。

工程推进仍应遵守：

```text
AkShare / API
→ Source Adapter Registry
→ Raw Factor Lake
→ Normalized Panels
→ Feature Store / Factor Store
→ Data Handler / Dataset
→ Signal or Model
→ Backtest / Diagnostics
→ Report Manifest / Warnings / Artifacts
```

短期应采用 Thin Vertical Slice，而不是无限扩 DWH：只为 MFS-v0.4 所需数据打通最窄完整链路。

---

## 8. 下一步建议

新增 Strategy Contract 文件：

```text
strategy_contract_mfs_v04.yaml
```

并拆成：

```text
data_contract.yaml
model_contract.yaml
portfolio_contract.yaml
execution_contract.yaml
evaluation_contract.yaml
```

然后进入最小实现：

```text
S0: contract freeze
S1: minimal data panel / PIT financial proxy
S2: factor family builder
S3: model baseline and challenger
S4: buffered portfolio / turnover-aware optimizer
S5: walk-forward evaluation and registry artifacts
```

---

## 9. 重要禁止项

```text
禁止固定 0.5 / 0.6 / 0.4 等人为权重作为正式策略
禁止把 fwd_ret label 当 feature
禁止全样本选择最佳 N 或最佳权重
禁止把财报报告期当作可用时间
禁止把基本面 gate 设计成永久硬排除
禁止只用 RankIC 宣称策略有效
禁止忽略交易成本、滑点和不可成交约束
禁止没有 manifest / warnings 的实验进入正式结论
```
