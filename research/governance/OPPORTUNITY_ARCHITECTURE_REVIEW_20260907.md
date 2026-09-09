# BTC HUNTER 筛选目标与下一轮采集衔接核对

CHANGE_ID: OPPORTUNITY_ARCHITECTURE_REVIEW_20260907

状态：DOCUMENTED_PROPOSAL；作者 Codex；尚无独立审核，不认证实现或盈利能力。

范围：恢复用户真实设计意图；核对架构内部歧义和下一轮数据能力边界；将方案索引追加到 MASTER / CHANGELOG。不是 NEXT_DATASET_COLLECTOR_READINESS 的实现或验收，也不修改其范围。

退出条件：给出原文位置、问题分类、具体修订方案、验收方法及所处阶段；保留旧文与冻结研究；不新增交易规则，不运行采集、Discovery或测试。

预算：新文件2（本报告、文档追加脚本），新目录0，全仓扫描0，全量测试0，reconciliation 0；MASTER追加不超过2000 UTF-8字节，CHANGELOG一条。

阅读边界：本轮建立两TXT章节/变更导航，重点读取第一部分核心目标、第四部分评分/完整计划/三层Gate/验证/拒绝候选审计，以及第七部分2-B/C/D/K/L/P/R、下一阶段冲突清单，并核对CHANGELOG对应记录、当前状态及OF2 prestart/readiness合同。不是对MASTER全部63667行和CHANGELOG全部3613行的逐行完整认证，也不是全代码库审计。先前说“通读架构”不应扩大成“所有历史正文都读完并认证”。下列行号为本轮追加前定位，章节名为长期定位依据。

## 1. 需要首先纠正的是助手对项目的概括

用户已经设计了：只让经验证的正期望机会消耗风险预算；允许零交易；预先冻结风险；错误受控、正确保留右尾。MASTER第一部分170–194、438–556、662–828明确记录，不能再次把它们当作新建议。

真正的筛选对象是完整候选交易计划：Entry、Initial Stop、Target/Runner、Time Horizon、Risk，而非一笔主动买卖或某一指标。第四部分四-K（15546起）明确要求作为联合对象冻结比较；核心定位（254–276）要求考察相似可观察状态、Regime和完整Lifecycle下的Net Realized R排序。最终目标是风险约束几何增长，不是追求最高胜率。

研究路径包含五个Canonical Feature Family：CONTEXT / ATTACK / LIQUIDITY / RESPONSE / CONTROL_TRANSFER。参与者激励属于机制解释与约束背景，不要求先识别对手身份，更不需要证明其隐藏收益才能研究可观察关系。

系统寻找的候选结构包括：攻击力度与价格响应是否匹配；盘口消耗、撤退候选、补充及恢复过程；位移后的流动性重建；局部价格位置和更长周期状态下的条件差异。见2-B/C/D/P。后续才将有证据的关系用于机会排序和完整计划验证。

下一轮先保留不可补回的原始事实，再离线派生和检验，这与当前处于可测数据采集准备阶段一致。现阶段不应要求用户拿尚未产生的数据证明完整筛选器盈利，也不能把尚待实现的远期模块算成当前采集器缺陷。

EFFORT_RESULT_DIVERGENCE_V1的既有否定性发现只约束那份冻结问题及数据，不代表整个体系无效，也不自动证明下一层交互有价值。其结论与权限不变。

## 2. 值得修正的路线歧义：完成低阶研究，不等于低阶必须成功

分类：有原文依据的表述冲突风险，尚不证明程序执行了错误门禁。

依据：2-B（31988–32007、32049–32082）和2-P九/十六（35057–35067、35130–35135）保护低阶弱但交互有价值的研究路径；2-C（32179–32183）、2-D（32312–32318）要求LEVEL 1完成后按新假设流程推进。可是2-P二十四（35220–35226）又简写为“H1→若有结构：H2”。后一写法容易被AI误读为H1没有稳健结构就禁止H2。

建议在未来路线解释中统一为：

> H1完成并登记结果后，具有明确机制依据、所需数据和限定研究预算的H2可以作为新假设提出。H1成功不是H2准入的必要条件。H2不继承H1证据等级，不修改H1结论，仍须新版本、冻结、试验登记及冻结后未来验证。无机制依据的组合搜索不得借此放行。

验收：以“H1无稳健证据但H2有预注册机制与新增观测”为例，只允许进入研究申请/队列，不产生Confirmation或交易许可；另一例“仅为救回H1反复找子集”不得通过。下一主假设仍需另行冻结，此报告不替用户选定或启动它。

## 3. 实际数据契约缺口：可以补采，不代表可以无限延期

分类：OF2 prestart具体条目缺少回收时限；属于下一Dataset准备的有限补充。

依据：OF2_PRESTART_DESIGN_V1.json第287–296行将5分钟OI历史列为can_backfill_later，注释not urgent for Day 1；但没有该条目的历史可查询期限和最迟获取时间。Binance官方openInterestHist文档本轮直接核实：只提供最近一个月数据。官方页面：https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data （Open Interest Statistics）。这不是说OI一定要新增实时高频流，也不声称所有其他来源均无法补回。

建议：在既有raw-data sufficiency条目增加source_history_window、latest_safe_fetch_deadline、fetch_cadence、last_successful_coverage_end、fallback_source及失败处置。期限按实际源保留窗减去重试余量设定，实施时冻结；不能等几个月后才第一次尝试该REST源。

验收：模拟补采到期前、重试中、已超过官方源保留窗三种状态。覆盖仍可取得、即将丢失、该来源已无法补回必须分开；有其他来源时须另验语义与时间证据。历史记录timestamp不能直接冒充当时本机可知时间。

当前影响：不重开已关闭Collector Implementation，不把OI变成当前Readiness G1–G8的新阻断项。在正式Dataset启动/保留计划中明确“及时保存”或“本轮不承诺OI条件研究覆盖”；不能继续无限期写“以后补”。本报告不创建定时任务。

## 4. 已有待办的具体收口：评分对象与Lifecycle选择接口

分类：文档自己已列出的未冻结接口，不是本轮新发现的哲学缺陷。

依据：四-K要求完整计划联合评价；四-N（15789–15904）将Opportunity、Lifecycle、Survival分层；第八部分“仍未解决”第1项（54719–54725）已经指出Candidate包含Entry/Stop/TP，但另一流程又写Score后生成Trade Plan。整个FROZEN POLICY STACK的OOS验证也早已存在（18181–18212）。

建议接口：Setup生成后，只枚举事先获准的有限计划/Policy组合，每项绑定candidate_id、plan_version、lifecycle_policy_version、execution_model_version及当时信息快照。涉及Net R的Score必须绑定所评价的Policy；不依赖Policy的纯市场状态描述仍可在Setup层保留。之后按冻结选择规则选计划，账户Survival Gate再给可执行数量或NO_TRADE。

验证Lifecycle时用预先冻结的研究风险设定，而非后来才知道的账户路径；最终评估整个选择流程。不得先用某一退出方式评分，再用另一退出方式的历史赢家替换而沿用原分数。

验收：同一Setup配不同Stop/Runner应有不同计划身份及Outcome；变更Policy后旧Score不可静默复用；所选计划失败或未成交时不得事后换成另一计划的盈利结果。

实施时点：研究评分/计划模块之前冻结，当前不往Collector中加入评分或生命周期选择逻辑。

## 5. 已有时间原则的具体收口：Response Residual什么时候能用

分类：2-R实现接口的歧义防护；没有证据表明现有代码已发生泄漏。

依据：2-R三（35575–35583）定义实际响应减条件预期响应，同时描述冻结Horizon；2-K及2-R七早已要求known_time约束，但这一公式没有直接区分过去窗口残差和未来响应标签。

建议冻结两种用途：

- 已完成窗口的残差：只在全部输入、实际响应和计算结果已经可知之后，成为后续Candidate的Feature。
- 从t开始的未来窗口残差：在t时只能是待成熟Outcome，不能用于t时筛选；若在t+h成熟后使用，则必须建立新的decision_time和之后的Outcome窗口。

残差基准模型的训练截止也必须早于其验证用途；同窗事后拟合的残差可以做Discovery诊断，不能作为冻结后前瞻证据。

验收：截断未来数据时，t时已生成的Candidate/Feature必须不变；只有Outcome成熟状态随后变化。复用既有Temporal Provenance，不新增一套时间系统。

## 6. 当前只需补一张能力对应表，避免把长期目标压到第一批数据上

复用既有raw-data sufficiency review的future_question / research_families_enabled，不另建平行注册表。正式Dataset manifest应引用实际stream、schema与延期项，并为每个拟研究问题记录：所需原始字段、可重建时间精度、连续性覆盖、已知时间限制、当前可测/部分可测/不可测及理由。

例如：

|既有研究方向|下一轮数据定位|承诺边界|
|ATTACK×RESPONSE×LIQUIDITY、盘口恢复|aggTrade、diff-depth、快照、时间/质量记录提供基础|须通过回放、派生定义和连续性验收后才可测试；采集成功不是效应成立|
|Session / 价格位置|冻结日历、历史成交或K线可后算|补足lookback；Session不是参与者身份|
|OI条件、Funding / Basis|区分有限历史源与实时源|OI按期限回收；已延期的markPriceUpdate不能宣称有逐tick实时覆盖；已结算Funding不代替当时预期Funding|
|完整Trade Plan / Runner收益|还需要Entry/Fill、成本、退出规则及成熟Outcome|不是只采了盘口就自动拥有可实现净R标签|

这张表负责明确本轮能回答哪些问题；不要求把所有远期外部数据和交易模块都做好才允许开始核心采集。

## 7. 建议顺序与范围

1. 当前继续独立完成Collector Readiness G1–G8；报告不重新审核其运行状态。
2. 正式Dataset启动前收口实际数据能力、OI等有限补采期限与存储计划，沿用新物理路径和版本规则。
3. 先积累Raw Truth。未来研究先选一个主要问题及至多一个诊断旁支，按既有2-P资源纪律冻结；不必等所有远期Feature公式完成才开始原始采集。
4. 进入对应研究模块时收口计划/Policy接口和残差时间接口；之后依既有Discovery→Freeze→Future Confirmation流程推进。

本轮不新增“完美机会分”，不要求多加一批Gate，不调整止损、不复活旧假设。已存在的NO_TRADE、受控试错、右尾、样本相关性、功效、拒绝候选审计和整栈OOS规则继续有效，不冒充本轮创新。

BUILD_HANDOFF：文档方案已给出；尚未实现、尚未独立审核；无业务代码变更，无正式数据/研究/交易运行。TXT追加校验由配套脚本执行，是否成功以该脚本实际输出为准。
