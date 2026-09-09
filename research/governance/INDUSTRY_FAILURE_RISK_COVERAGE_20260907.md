# BTC行业参与者、失败案例与BTC HUNTER风险覆盖核对

CHANGE_ID：INDUSTRY_FAILURE_RISK_COVERAGE_20260907
日期：2026-09-07。作者：Codex；独立审核：未进行。本次为用户授权的文档分析与追加记录，不是Collector代码changeset。
BASE：formal HEAD 4d5446a2477f488afbe47a9729c5e139397141ad；工作区另有READINESS相关代码，本次不修改、不认证其完成状态。
源材料：C:/Users/Kina/Downloads/可以。把我们现在做的 BTC 量化研究放到整个行业里看，会发现一个很重.txt
源材料SHA256：431c132c019d11b01fd345bff512f78bd3ba9dbce5f22be664f5bab2043315a6。

SCOPE：涵盖源TXT全部11部分的主题，核对已有概念/风险设计、有限源码证据、缺口与验证建议；更新MASTER及CHANGELOG索引。附件内的“应该收集/可以写进”等建议属于待评估资料，不作为自动改代码或授权交易的指令。
EXIT_CRITERIA：完成主题映射、事实订正、风险对策与验收建议；MASTER增量<=2000 UTF-8字节、CHANGELOG一条；保留旧文；不启动研究/采集/交易；不扫描全仓库、不重跑全量测试。
范围路径：本报告、一次性文档追加脚本、指定MASTER/CHANGELOG；不改PROJECT_CURRENT_STATE或在进行中的Collector工作。
新文件2、新目录0、全仓扫描0、全量测试0、project reconciliation 0。文档与提案不能升级为VERIFIED、ACTIVE或Trade Permission。

## 结论

现有体系在理念和研究架构上已覆盖相当多问题，尤其MASTER第七部分2-H至2-P。真正缺少的主要是：把跨层风险写成可验收的失败场景、记录风险控制的实际实现证据、明确公开数据的不可识别边界。不能将“已在TXT里写过”解释为“资金已经受到程序保护”。

当前采集器建设能改善数据和运行可靠性；它不能防止交易所挪用资产、保证提现、识别所有隐性杠杆，也不能证明任何交易策略有净收益。历史案例用于提出压力场景，不作为BTCUSDT上的因果证明或新增交易信号。

本文状态含义：已有设计=找到了明确MASTER条款；局部实现证据=直接读到有限相关源码；待验收=未取得足够当前行为证据；补充提案=本次建议，尚未实施。没有把未检索到的内容断言为全项目绝对不存在。

## 一、参与者与利润来源：保留现有分类，避免虚构身份识别

原TXT的参与者表可纳入已有2-H FLOW_MOTIVE/PARTICIPANT_CONSTRAINT_CONTEXT及2-I PNL_SOURCE，而不新增第六类Feature Family。

|参与者|经济来源与约束|本系统允许怎样使用|
|---|---|---|
|交易所、经纪与Prime|交易/融资/托管/执行服务收入；同时有运营、信用和资金可用性风险|区分场所、风险引擎和服务提供者，不把正常API当作偿付能力证明|
|做市/HFT/Prop|spread、符合资格的rebate，减去逆向选择、库存和对冲成本；不是保证利润|研究报价撤退、补充和执行成本；不能看到补单就确认某家做市商|
|Basis/Funding套利|现货—期货/永续的相对定价与资金转移，受融资、分腿、保证金与结算约束|区分期货到期basis收敛与永续浮动funding；两者不等价，更不无风险|
|期权/波动率台|波动率风险定价、库存及gamma/vega对冲|需要实际期权价格/合约与时间戳证据；不能从单场所aggTrade推断全市场净gamma|
|ETF Sponsor/AP/LP|管理费与申赎/执行/套利角色不同，具体流程取决于基金和生效规则|外部flow按发布时间和修订版本入库；不能把每日流量倒填为当时已知的分钟买盘|
|借贷/信用台|借贷利差、融资服务，受抵押品、期限错配与对手方约束|透明度不足时记UNKNOWN，不把承诺利息当Edge或可用资本|
|矿工|补贴与交易费收入，减去能源、设备、融资等成本；可能有套保和其他业务|属低频外部背景；链上转账不等于已在Binance卖出|
|方向基金/CTA/散户|方向风险暴露及策略收益，风险期限不同|交易主动方方向不等于动机、身份、开仓/平仓或预测能力|
|Treasury/长期持有人|资产升值、融资及再平衡|按可观察约束提出候选解释，不把大额流量自动叫“机构吸筹”|

价格驱动因素与参与者利润驱动因素必须分开。保留Micro/Meso/Macro三层，不合成未经验证的Mega Score。交易所手续费、借贷收益等商业收入也不是本项目能够直接复制的收益源。

## 二、重大问题 → 已有对策 → 缺口 → 验证方法

### R1 交易所、托管与资金无法取回（Mt. Gox / FTX）

已有：2-L SETTLEMENT/COLLECTIBILITY/VENUE REALIZABILITY明确列出提现、账户访问、托管与结算风险；2-N Capital Mobility、2-O集中度与Liquidity Reserve已覆盖概念。当前TRADE_PERMISSION=false提供当前不执行交易的边界，不能解释成交易所余额无风险。
缺口/补充：未来有真实资金暴露之前，需把API健康、可交易、可结算、可提现四种状态分开；建立Venue/托管/结算币种暴露及外部负债未知项，留出场外储备；不能仅依据PoR或盘口数据推断负债、资产隔离或偿付能力。
验收提案：模拟交易API正常但提现冻结、账户查询超时、单一Venue不可用；系统不得把冻结余额算作可调拨资金，也不得在未确认资金到账时跨场所重复分配。资金限额由独立资金政策预先确定，不从最佳回测倒推。本条是未来实盘准入要求，不扩展当前公开市场数据Collector。

### R2 强平—流动性—价格的反馈（2020年3月）

已有：2-H强制流候选、2-O Shock Absorption及Secondary Forced Flow；最新Collector范围有aggTrade、diff-depth、forceOrder。源码ForceOrderCollector保存windowed-snapshot语义。
缺口/补充：forceOrder是场所范围的窗口快照，不是全市场逐笔强平全量；无消息不能当零强平。OI/funding与价格变化不能唯一识别开/平仓、杠杆率或被迫卖方身份。markPriceUpdate按当前范围仍延期；不能因此宣称已有完整清算风险观测。
验收提案：三种情况分别测试“无强平消息但连接活跃”“连接中断”“窗口内多笔强平只收到部分快照”；数据质量状态必须分开，不凭缺失补零，不把重连后的行情连续性当历史深度已恢复。稀疏forceOrder流的健康判断应使用连接/心跳证据，不单靠业务消息频率。
未来假设可研究ATTACK×LIQUIDITY×FORCED_FLOW的条件增量，但须新hypothesis_id、已知时点、冻结周期/基线及未来样本；这不是对原EFFORT_RESULT_DIVERGENCE_V1的救活或确认。

### R3 流动性消失、排队与容量

已有：2-C/2-D动态流动性重建、2-M Execution Reality、2-O分别定义spread/depth/imbalance恢复时间，而非单一recovery_time；已有成本与执行可行性设计。
缺口/补充：L2聚合数据只观察价格档位净变化，无法精确分离同档位同时发生的撤单、补单及成交，也不能证明隐藏订单或意图。消费/quote-pull/replenishment应保留候选与未解释残差；成交价穿越不是我方订单一定成交。
验收提案：用可知真值的合成订单簿回放，加入相同价格成交与补单、乱序、丢帧、跨重连；不确定量不能写成确切撤单。研究模拟加入spread扩大、可用深度骤降、退出冲击、排队、部分成交和拒单；输出容量与成本敏感性，不只输出mid-price收益。执行模型独立校准后冻结，再用于未来OOS。

### R4 低方向暴露仍可能破产（Carry / LTCM）

已有：2-I净收益来源、2-J Waiting Capacity/Carry Burn、2-L生存到兑现、2-N Deployable Capital，明确研究正确不等于现在值得交易。
缺口/补充：净delta小不能抵销两个账户分别需要的保证金和资金；联合场景至少包含basis先扩大、资金费率反转、融资成本升高、保证金上调、一条腿不可成交、对冲场所不可达。可获maker rebate必须按实际账户资格，不能借用机构优惠。
验收提案：按路径逐时点计算两腿毛敞口、现金需求、可调拨资金、保证金缺口及强制退出损失；“最终收敛但途中强平”必须判失败。优先报告最长可等待时间及联合冲击下的资金缺口；仅在计划部署这类策略之前推进实现，不要求当前采集器变成套利系统。

### R5 稳定币脱锚与反身性抵押品（Terra）

已有：2-O明确区分USDT总供给、流通/赎回、交易所余额、做市库存、账户抵押与可部署资本；2-N已有资金跨链/跨场所可达性与结算约束。
缺口/补充：压力测试单列报价币、保证金币、结算币和最终记账币；同一资产同时支撑策略收益和抵押价值时记录wrong-way risk。不同稳定币机制分别建模，不能把UST案例直接判为USDT同机制或必然失效。
验收提案：同时施加BTC亏损、稳定币折价、抵押haircut提高、赎回延迟和价格源中断；不把名义USDT收益直接当可兑现美元收益；缺可靠兑换价时标UNKNOWN。此类压力保护不能根据模拟的“死亡循环”生成方向信号。

### R6 高收益、隐藏杠杆与传染（3AC / Celsius / Genesis）

已有：2-I收益来源候选、2-N融资/调拨约束、2-O损失分配与集中度。
缺口/补充：外部融资、再抵押、关联实体和负债网络并非公开盘口可推断。未来资金政策需单列借款人/托管方/抵押品/到期与可提取性；无法穿透时记UNKNOWN并限制依赖，不编造完整对手方图。
验收提案：场所、借贷方和稳定币同时冲击、提现排队和资产负债期限错配；以可实现现金流检验生存，而非显示利息或未实现利润。研究资金不自动投入Earn或借贷产品。宏观收益故事不能替代真实净EV/OOS。

### R7 软件失控与运维失效（Knight Capital）

已有：冻结写保护、独立审核、版本哈希、有限核对、Supervisor/重连、raw payload与Gap Ledger；READINESS现有范围G1–G8覆盖线程异常、provenance、60分钟soak、资源、进程重启、环境冻结及启动Gate。
源码现状：已读到run_component_lifecycle异常包装、RawPayloadWriter有界队列及连续性checkpoint；这些说明有实现代码，不代表本轮已独立验收。PROJECT_CURRENT_STATE关于131项测试/live smoke为前轮记录，本轮不重跑、不外推readiness PASS。
缺口/补充：Collector健康只能保证采集相关属性，不能等同于交易层Kill Switch。未来真实订单入口另需幂等键、订单速率/金额限制、账户总风险限制、确认未知时先对账、最小权限、部署版本检查和可演练停止机制。
验收提案：当前复用G1–G8，覆盖未捕获异常、writer堵塞/磁盘满、checkpoint损坏、重启、断流、异常退出通知；预期状态/阻止行为/恢复条件事先列明。真实交易前再注入“订单已接受但响应丢失”“重复回调”“部分部署/旧进程仍活跃”；不得盲目重发，不得认为发出撤单即撤单成功。停止新增风险与保留有效保护单应分开，不能以kill switch误撤全部保护单。

### R8 杠杆、摊平亏损与生存路径（个人交易 / LTCM共同教训）

已有：2-J已禁止用Waiting Capacity包装LOSS_AVERAGING/STOP_LOOSENED/Martingale；配置中PAPER_ONLY=true、AUTO_TRADE=false及风险参数存在。数值配置存在不代表执行引擎已强制执行，本轮不把默认数值作为实盘建议。
补充：风险预算要在账户/策略/关联敞口层累计；收益历史不能自动提高风险上限，未实现利润不自动转成新的可提款资本。新阈值由后续风控政策与独立验收冻结，本轮不调阈值。
验收提案：跳空穿止损、多策略同向、连续亏损、波动骤增、手续费/冲击升高；统计回撤、尾部损失、资金缺口和强平路径。风险限制触发后阻止新增风险，并验证恢复需满足哪些条件。保留SURVIVAL > MODEL CONVICTION，但risk control不创造alpha。

### R9 ETF、矿工、宏观及跨场所不可观测性

已有：2-H不同约束候选、2-K Temporal Causality、Micro/Meso/Macro分层；OF2定义只是时钟参考标签，不识别实际开市、ETF执行或交易者身份。
缺口/补充：加入外部变量前标明来源/场所/合约/事件时点/发布时间/首次可知时间/获取时间/修订版本；区分单交易所样本与全市场。可后补的ETF日流量、矿工和宏观数据不必现在无限扩展Collector；不可恢复的原始接收时序/深度按已有采集合同保留。
验收提案：as-of join只允许known_time<=decision_time；用首次公布版本与最终修订版本对照以发现泄漏；跨场所补样本之前不宣称结论普适。禁止从一个地区时段标签推导“该地区机构正在买”。

## 三、源TXT的事实与措辞核对

历史引文只用于风险动机。以下网页于本轮实际打开；不把公开指控写成终审认定，也不把当时余额写成当前尚未偿付数额。

1. Knight：原文约440 million；SEC调查记录为2012-08-01前45分钟产生大量错误订单，损失超过460 million。本报告采用后者，原TXT不改。[SEC](https://www.sec.gov/newsroom/press-releases/2013-222)
2. FTX：2024-03-28 DOJ记录SBF获刑25年及客户资金挪用。只据此讨论职责和资金隔离，不把这句话扩展为所有量化团队或交易所的行为。[DOJ](https://www.justice.gov/archives/opa/pr/samuel-bankman-fried-sentenced-25-years-his-orchestration-multiple-fraudulent-schemes)
3. Mt. Gox：约647,000 BTC来自2023年DOJ公告中的指控叙述，需保留“据起诉材料/指控”限定。[DOJ](https://www.justice.gov/usao-sdny/pr/russian-nationals-charged-hacking-one-cryptocurrency-exchange-and-illicitly-operating)
4. Genesis/Gemini：340,000投资者、约9亿美元是SEC在2023年公告中对2022年11月提款停止情形的陈述，不是当前损失余额；Celsius资料也为执法指控口径。[Genesis SEC](https://www.sec.gov/newsroom/press-releases/2023-7)；[Celsius SEC](https://www.sec.gov/enforcement-litigation/litigation-releases/lr-25779)
5. Terra：BIS讨论算法兑换和信心崩溃的反馈，不能把此机制当所有稳定币的共同担保方式。[BIS](https://www.bis.org/publications/aer-2022/future-monetary-system)
6. Carry：BIS Working Paper 1087明确指出套利资本稀缺、保证金跳升和回撤中清算风险；作者研究不等于当前可获得回报或BTC HUNTER已验证edge。LTCM风险链有美联储2006年证词支持。[BIS](https://www.bis.org/publications/working-paper-1087-crypto-carry)；[Federal Reserve](https://www.federalreserve.gov/newsevents/testimony/parkinson20060516a.htm)
7. ETF：给出的具体链接是截至2024-12-31的10-K，不能单凭该链接证明“部分2026年文件列出某交易商”。本次不把2026年机构名单写入事实层。申赎方式和参与者需按具体基金/时期核对，不能强行等同于AP亲自完成底层现货交易。[所给10-K](https://www.sec.gov/Archives/edgar/data/2015034/000095017025029405/btc-20241231.htm)
8. CME页面支持BTIC按参考基准交易basis；矿企10-K支持2024减半后3.125 BTC区块补贴及交易费、能源/经营成本的重要性。它们不证明矿工卖压可由本系统盘口直接识别。[CME](https://www.cmegroup.com/articles/2024/btic-transactions-on-cryptocurrency-futures.html)；[矿企10-K](https://www.sec.gov/Archives/edgar/data/1591956/000159195626000004/any-20251231.htm)
9. 2020年3月：所给PDF为Thomas Chippas在CFTC TAC的2020-07-16演示材料，不能笼统当CFTC正式调查结论；价格点位和11亿美元强平统计本轮不作独立确认，也不把它们作为压力测试的唯一校准值。[原PDF](https://www.cftc.gov/media/4246/TAC071620_BTCStockVolatility/download)

## 四、调整次序与验证口径

近期只对齐现有NEXT_DATASET_COLLECTOR_READINESS的G1–G8，不新增一轮无边界工程：数据断流/持久化/重启/资源/启动门禁的验收仍由该changeset原builder和reviewer完成。本报告不覆盖他们的结论，不修改其源码或角色。

后续研究前：R2/R3/R9先形成可观察字段和测量误差说明，候选标签保持候选；由独立新假设预注册基线、horizon、成本、可知时点、依赖控制及多重试验记录，再进入未来样本。原10083条仍为Discovery only，NO_ROBUST_REPEATABLE_EVIDENCE保持。

真实资本/执行前：R1/R4/R5/R6/R7/R8作为既有Venue/Execution/Survival层的验收细化候选，优先于扩大风险或启用交易；不凭本报告直接写入数值阈值或开放自动交易。新增外部数据只在具体问题需要、成本及可知时点可交代时纳入。

每个场景用同一验收模板：风险事件→可用观测与UNKNOWN→故障/压力输入→期望状态→必须拒绝的行为→恢复条件→实际输出证据→版本→独立审核。数值冲击集合及容忍度在观察策略测试结果前固定；合成极端情景只测试脆弱性，不给它虚构发生概率。历史危机可做单独保留的情景测试集，不能既用来调策略又声称独立验证。

本轮补充属于DOCUMENTED/PROPOSED_VALIDATION，未自动变成全部已接纳的实现backlog。待后续owner明确纳入某changeset时，用现有requirement/implementation registry登记稳定ID、scope和exit criteria；不新增平行风险引擎。没有当前影响证据的历史细节只记NON_BLOCKING_BACKLOG，不递归深挖。

## BUILD_HANDOFF（文档变更）

FILES_CHANGED：本报告、一次性追加脚本、MASTER 2-BU/13CD与总览、CHANGELOG一条。原附件保留。
CLAIMS：11部分主题已映射；既有设计与局部源码证据分开；事实订正及不可识别边界明示；近期/研究前/实盘前对策和验收分层。
TEST_RESULT：文档结构/编号/预算/旧文保留检查；不运行pytest、live smoke、Collector、Discovery或实盘。
KNOWN_UNKNOWNS：本次不是全源码或全部历史文献审计；Readiness最终验收由其当前changeset负责；机构身份/实际隐性杠杆/对手方资产负债无法从本地市场数据确认；本报告独立审核未执行。
下一步：文档落盘后STOP；不自动扩展Collector changeset或交易权限。
