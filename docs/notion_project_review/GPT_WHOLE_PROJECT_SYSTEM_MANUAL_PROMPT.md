# GPT_WHOLE_PROJECT_SYSTEM_MANUAL_PROMPT

你将收到一个 quant research platform 项目材料包。

请你不要把任务理解为：
- 总结 README；
- 总结最新改动；
- 扩写 Codex 报告；
- 生成面试材料；
- 生成普通项目介绍。

你的任务是：

生成或更新一份中文长文版全项目系统手册：

docs/notion_project_review/PROJECT_SYSTEM_MANUAL.md

这份手册的目标是帮助项目 owner 长期理解、查询、复盘和维护整个 quant research platform。

它应该回答：

1. 这个项目当前到底是什么；
2. 整个系统如何从 Data 运作到 Report；
3. 每一层为什么存在；
4. 每一层的输入、输出、关键文件、关键函数、关键测试是什么；
5. 当前哪些部分已经实现；
6. 哪些只是 MVP；
7. 哪些尚未实现；
8. 哪些需要进一步验证；
9. 哪些是 technical debt；
10. 新增模块如何影响整个系统；
11. 哪些原有假设仍然成立；
12. 哪些假设需要更新；
13. 下一阶段最重要的开发优先级是什么；
14. 项目 owner 以后查询某个系统细节时应该从哪里入手。

---

## 1. 输入材料读取规则

请动态读取项目材料包中的所有相关内容，不要假设项目只到某个固定 phase，也不要只读取固定编号文档。

请优先综合以下材料：

1. README.md
2. docs/notion_project_review/ 下所有当前存在的 Markdown 文档
3. docs/notion_project_review/project_map.json
4. docs/notion_project_review/GPT_REVIEW_BUNDLE.md，如果存在
5. docs/notion_project_review/deep_review_evidence_pack.md，如果存在
6. docs/notion_project_review/PROJECT_QUERY_GUIDE.md，如果存在
7. docs/notion_project_review/PROJECT_SYSTEM_MANUAL_DRAFT.md，如果存在
8. docs/notion_project_review/phases/ 下所有 phase log，如果存在
9. docs/notion_project_review/deep_reviews/ 下所有 deep review 文档，如果存在
10. src/qsys/**
11. tests/**
12. scripts/**

注意：

- 不要只依赖 Codex 的总结。
- Codex 报告可以作为索引和证据包，但你需要结合源码、测试、README 和 project_map 做全项目判断。
- 如果 Codex 报告与源码证据冲突，以源码和测试为准。
- 如果 README、文档和源码不一致，请明确指出“不一致”或“需进一步验证”。

---

## 2. 核心分析主线

请围绕以下主链路展开：

Data → Panel → Feature → Signal → Portfolio → Backtest → Diagnostics → Constraints → Report

对每一层说明：

- 这一层为什么存在；
- 它解决什么研究问题或工程问题；
- 它的输入是什么；
- 它的输出是什么；
- 它依赖上游哪些模块；
- 它服务下游哪些模块；
- 关键文件路径；
- 关键函数 / 类 / 脚本；
- 相关测试；
- 当前实现状态；
- MVP 假设；
- technical debt；
- 后续演化方向。

不要只列 bullet。请写成能帮助项目 owner 理解系统运作方式的长文。

---

## 3. 状态标签要求

对所有重要判断，请显式标注状态：

- 已实现
- 部分实现
- 尚未实现
- 需进一步验证
- MVP 假设
- technical debt
- 已过时文档
- 需人工决策

不要把“README 提到”直接等同于“代码已实现”。

例如：

- 如果 README 提到 sqlite metadata，但 src/qsys 主路径中证据不足，请标记为“需进一步验证”。
- 如果存在工具脚本但未形成统一主流程，请标记为“部分实现”。
- 如果某功能只是通过简单规则近似，请标记为“MVP 假设”。
- 如果某模块存在测试失败或行为不一致，请标记为“technical debt / 需人工决策”。

---

## 4. 必须重点分析的问题

请特别关注以下问题。

### 4.1 当前项目阶段判断

请判断当前项目处于什么阶段。

不要简单写“V1 complete”。

请区分：

- research infrastructure；
- MVP strategy layer；
- diagnostics layer；
- risk-control prototype；
- report/review workflow；
- 尚未达到的 production trading system / full risk model / experiment registry。

### 4.2 当前系统里的 model 到底是什么

请解释当前项目中的 “model” 分成哪几层：

- signal scoring model；
- portfolio construction rule；
- backtest execution assumption；
- diagnostics feedback loop；
- constraints / risk-control layer。

请说明：

- 当前是否是完整 predictive model；
- 当前是否是完整 ML model；
- 当前是否是完整 risk model；
- 当前 demo alpha 的本质是什么；
- base model 下一步应该怎么定义。

### 4.3 Signal / Portfolio / Risk Control 的边界

请重点解释：

- feature 如何变成 signal；
- signal 如何变成 portfolio weights；
- portfolio constraints 和 full risk-control module 的区别；
- volatility treatment 当前属于 signal 层、portfolio 层，还是 risk-control 层；
- 为什么固定 linear volatility penalty 只是 MVP 假设；
- 后续 volatility 应该作为 penalty、condition、risk exposure，还是 regime state。

### 4.4 Diagnostics 能证明什么，不能证明什么

请解释这些指标的意义和局限：

- IC / Rank IC；
- ICIR；
- quantile return；
- top-minus-bottom；
- turnover；
- decay；
- exposure；
- conditioned IC；
- benchmark comparison。

请明确说明：

- diagnostics 不能直接证明策略可实盘赚钱；
- Rank IC 高不等于净收益稳定；
- quantile spread 可能受样本、分组边界、交易成本影响；
- exposure 需要区分 signal exposure 和 portfolio exposure；
- turnover 必须和 cost 一起看。

### 4.5 测试与可靠性

请检查 tests/**，总结当前测试体系。

特别关注：

- 当前是否存在测试收集问题；
- 是否存在 signal 行为预期冲突；
- 是否存在 portfolio constraints / index alignment 问题；
- 是否存在环境依赖导致的测试失败；
- 哪些失败属于代码行为问题；
- 哪些失败属于测试预期问题；
- 哪些失败属于本地环境问题；
- 哪些必须在标准本地环境复现后才能作为正式 bug 判断。

如果发现测试失败，请不要直接建议改代码。  
请先判断它影响哪个系统假设、哪个模块边界、哪个后续开发决策。

### 4.6 文档过时检查

请检查 README 和 docs/notion_project_review 下现有文档是否有过时描述。

请指出：

- 哪些描述仍然成立；
- 哪些描述需要加限定；
- 哪些 Future Work 已经部分实现；
- 哪些模块已经新增但旧文档未覆盖；
- 哪些文档应该被 PROJECT_SYSTEM_MANUAL.md 取代或引用。

---

## 5. 必须包含的章节结构

请输出完整 Markdown 文档，建议包含以下章节。

# PROJECT_SYSTEM_MANUAL｜Quant Research Platform 全项目系统手册

## 0. 阅读方式与证据边界

说明本手册读取了哪些材料，哪些材料不存在，哪些判断只是当前仓库快照下的判断。

## 1. 项目当前阶段判断

说明项目当前到底是什么，不是什么。

需要包括：

- 一句话定位；
- 当前工程阶段；
- 已实现能力；
- 部分实现能力；
- 尚未实现能力；
- 与完整 production system 的差距。

## 2. 全链路系统视角

围绕：

Data → Panel → Feature → Signal → Portfolio → Backtest → Diagnostics → Constraints → Report

逐层解释整个系统如何运作。

## 3. 端到端运行示例

请给出一个具体的运行叙事：

- 数据如何进入；
- panel 如何构造；
- feature store 如何生成；
- signal 如何计算；
- portfolio weights 如何生成；
- backtest 如何对齐收益和成本；
- diagnostics 如何评估；
- report / review 如何输出。

请尽量引用具体脚本、函数和文件路径。

## 4. 模块级系统手册

对核心模块分别说明：

- Data ingestion；
- Daily panel；
- Feature store；
- Signal engine；
- Portfolio construction；
- Backtest engine；
- Rebalance policy；
- Diagnostics / signal quality；
- Constraints / risk control；
- Report / Notion review；
- Tests / reliability。

每个模块都要说明：

- 作用；
- 输入；
- 输出；
- 关键文件；
- 关键函数 / 类；
- 相关测试；
- 当前状态；
- MVP 假设；
- technical debt；
- 后续演化方向。

## 5. 当前模型本质与模型假设

解释当前系统里的 model 是什么。

必须区分：

- signal scoring model；
- portfolio construction model；
- risk-control layer；
- diagnostics feedback loop；
- predictive model；
- base model。

请说明当前为什么不应该直接跳到复杂 ML 模型。

## 6. Constraints vs Risk Control

说明当前已经实现的 constraints / eligibility / exposure diagnostics。

同时说明为什么这还不是 full risk model。

请说明未来 risk-control module 可以如何演化。

## 7. Diagnostics 解释框架

说明每个 diagnostics 指标如何解读、如何误读、应该如何组合使用。

## 8. 技术债与可靠性问题

列出高优先级、中优先级、低优先级 technical debt。

请特别关注：

- 测试稳定性；
- index alignment；
- feature / label schema；
- rebalance semantics；
- signal baseline behavior；
- report schema；
- experiment config；
- data quality；
- Notion sync duplication。

## 9. 已过时或需要修订的文档

指出 README 和 review docs 中哪些内容需要更新。

## 10. 需人工决策的问题

列出不能由代码自动决定的问题。

例如：

- volatility 是 penalty、condition 还是 risk exposure；
- demo alpha 应改测试还是改实现；
- weekly rebalance 用周初还是周末；
- constraints 是否拆成独立 layer；
- feature store 是否允许 label columns；
- 下一阶段先做 base model、risk control 还是 diagnostics report schema。

## 11. 如何查询系统细节

这一章非常重要。

请用“问题 → 查询路径”的形式组织。

至少包括：

### 我想查 feature 如何生成

请给出：

- 推荐阅读章节；
- 源码文件；
- 关键函数；
- 相关测试；
- 应该继续问 GPT/Codex 的问题。

### 我想查 fwd_ret_5d / fwd_ret_20d 是如何构造的

同上。

### 我想查 signal 如何生成

同上。

### 我想查 signal 如何进入 portfolio weights

同上。

### 我想查 backtest execution assumption

同上。

### 我想查 volatility penalty 的实现和含义

同上。

### 我想查 constraints 和 full risk model 的区别

同上。

### 我想查某个测试失败的影响

同上。

### 我想查 report / benchmark workflow

同上。

## 12. 下一阶段开发优先级

从全项目角度给出优先级。

请不要只列愿望清单。

请分：

- P0：必须先处理，否则影响后续判断；
- P1：研究协议和实验标准化；
- P2：report / diagnostics schema；
- P3：risk-control module；
- P4：data governance；
- P5：更复杂模型或 nonlinear extension。

每个优先级都要说明为什么。

## 13. 最终结论

总结当前项目的真实状态、最重要的优势、最危险的技术债、下一阶段最合理方向。

请避免使用“面试导向”措辞。  
不要写“可面试”。  
请使用：

- 可解释；
- 可复盘；
- 可继续演进；
- 可长期维护。

---

## 6. 写作要求

请遵守以下写作要求：

1. 中文为主。
2. 不要写成短摘要。
3. 不要只写 bullet list。
4. 应该是长文系统手册，允许使用表格和小标题。
5. 保留所有文件路径、函数名、类名、测试文件名。
6. 不要发明未实现功能。
7. 不要把 Codex 报告当作唯一事实来源。
8. 所有重要判断必须能回到代码文件、测试、README、project_map 或 evidence pack。
9. 如果证据不足，请明确写“需进一步验证”。
10. 如果发现文档过时，请明确写“文档需更新”。
11. 如果测试结果来自当前审查环境，请明确写“需在本地标准环境复现确认”。
12. 不要把 Notion 作为 source of truth；GitHub docs 才是正式记录，Notion 是同步阅读层。
13. 不要把报告写成面试材料。
14. 不要过度美化项目，要保留问题、风险和技术债。
15. 结论要服务于项目 owner 的后续开发决策。

---

## 7. 输出要求

请直接输出 Markdown 正文。

开头使用：

# PROJECT_SYSTEM_MANUAL｜Quant Research Platform 全项目系统手册

不要输出额外解释。

不要说“以下是报告”。

不要把报告拆成多个回答，除非内容过长必须分段。

---

## 8. 质量验收标准

生成后，这份手册应该能帮助项目 owner 回答：

1. 我这个项目现在到底是什么阶段？
2. 一个 feature 从生成到进入 backtest 的路径是什么？
3. signal model、portfolio construction、risk control 的边界在哪里？
4. 当前 volatility treatment 的实际含义是什么？
5. diagnostics 结果应该怎么理解？
6. 哪些结果还不能相信？
7. 当前最危险的技术债是什么？
8. 哪些 README / review docs 已经过时？
9. 下一阶段为什么不应该直接上复杂模型？
10. 我以后想查某个系统细节，应该从哪里开始？