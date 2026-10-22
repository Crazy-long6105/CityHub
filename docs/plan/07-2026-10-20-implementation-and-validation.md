# 计划实施与验证记录

日期：2026-10-20。根据同目录 01 至 06 的设计实施。原设计文件保留讨论时状态，本文件记录代码现状。

## 实施范围

| 模块 | 主要实现 | 默认模块验证 |
| --- | --- | --- |
| 01 秒杀可靠性 | v2 控制键、Redis TIME 原子预扣、订单绑定资格、请求恢复索引、数据库创建/终止事务、签名查询凭据、有限发送与异常消息隔离 | SeckillModuleTest，3 项 |
| 02 支付关单 | 数据库时间支付截止、状态 CAS、归属校验、事务回补及补偿表、条件领取和租约恢复 | PaymentModuleTest，4 项 |
| 03 缓存读取 | 逻辑过期、受控冷加载、空值标记、锁 token 发布保护、失败退避、有界刷新 | LogicalCacheModuleTest，1 项默认验证 |
| 04 缓存一致性 | 商家与 Outbox 同事务、提交后失效、撤销在途重建、Kafka 投递预约、租约及持久退避、异常消息隔离 | CacheOutboxModuleTest，1 项 |
| 05 二级缓存 | Caffeine 单键合并、30 秒真实值/2 秒负值、对象复制、每实例广播删除及可靠发送依据 | TwoLevelCacheModuleTest，1 项 |
| 06 多维限流 | METHOD/IP/USER 原子判断、Redis TIME、UUID 成员、可信代理链、启动参数校验、高风险故障拒绝 | RateLimiterModuleTest，4 项默认验证 |

旧缓存消息只允许操作 `voucher:<数字ID>` 展示键，不能覆盖商家详情或秒杀控制键。旧监听器的失败路径改为抛出异常，由 Kafka 有界重试和确认后的 DLT 隔离处理，不再休眠后直接确认。

新增 API：

- `POST /voucher-order/seckill/{id}`：返回字符串订单号、queryToken、受理/创建状态。
- `GET /voucher-order/requests/{id}?token=...`：仅当前用户可查，正常轮询只读 Redis。
- `GET /voucher-order/{id}`：当前用户的数据库订单状态及 releasePending。
- `POST /voucher-order/{id}/pay`：模拟支付，不连接真实支付渠道。

## 本次验证

执行 Maven `test`，编译成功；共 18 项，14 项通过、0 失败、0 错误、4 项跳过。按模块组织聚合测试，只有失败测试桩修复后做了必要的定向复测及最终全量确认，没有重复运行压测。

跳过项为秒杀 Redis、逻辑缓存 Redis、限流 Redis、支付 MySQL 的真实依赖测试。默认通过项主要使用 Mock 和可控执行器，不能替代真实事务、Lua 并发、Kafka 故障及多实例验收。

环境检查：Maven 3.9.9、Java 17 可用，项目仍以 Java 8 为编译目标。Redis 6379 与 Kafka 9092 未运行，Docker 守护进程未启动；本机 MySQL 3306 可达，但项目 `.env` 中的凭据认证失败。本次没有修改业务数据库、启动基础服务、执行迁移或声称性能改善百分比。

## 迁移与启用

已有数据库先暂停旧秒杀生产者/消费者与商家相关写入，备份并依次执行一次：

1. `src/main/resources/db/migration/seckill.sql`。
2. `src/main/resources/db/payment.sql`。
3. `src/main/resources/db/cache-reliability.sql`。

不要重新导入含 DROP TABLE 的 `hmdp.sql`。Docker Compose 已挂载增量 SQL，仅新建空数据卷时会自动按顺序初始化；已有数据卷不会自动执行迁移，也不要通过删除数据卷升级。

启用前必须核对历史有效订单、重复购买及资格，补齐确认过的待支付订单截止时间和数据库购买资格。历史 `pay_expire_time` 默认为 NULL，未核对记录不会自动支付/关单。新创建失败记录使用独立状态 7。

秒杀控制键使用 `seckill:v2:`，不把旧 Set 当 Hash，不从数据库剩余库存覆盖已经接收请求的 Redis 库存。新活动仅在数据库提交后尝试初始化控制键，失败时活动拒绝受理并记录错误；恢复初始化只适用于确认从未开放的活动，不提供自动重置控制库存功能。Redis 控制数据丢失后必须停止受理并人工核对。

在受控切换窗口仅分批清理旧格式 `cache:shop:<数字ID>` 详情缓存，不使用 FLUSHDB，不清理登录、GEO 或秒杀数据。所有实例统一部署新的发布/失效协议。

配置见 `.env.example`：

- `HMDP_INSTANCE_ID`：每个实例稳定且唯一。默认 `local-1` 仅适用于单个本地实例；Compose 单实例默认 `hmdp-backend-1`，多实例必须分别设置，不能共享广播 group。
- `HMDP_SECKILL_QUERY_SECRET`：至少 32 字节随机密钥，各实例一致且妥善保存。变更会使旧查询凭据无法验证。
- 迁移及历史核对完成后，设置 `HMDP_SECKILL_ENABLED=true` 与 `HMDP_ORDER_LIFECYCLE_ENABLED=true`。默认关闭这两个入口/调度开关，防止在旧表结构和旧资格数据上开始交易。
- 只有明确配置 `rate-limiter.trusted-proxy-cidrs` 才信任转发头，默认直接使用连接地址。

初始调度保守使用单工作者、有限批次；秒杀恢复每次只领取 1 个请求，订单关单/补偿每批最多 100。缓存刷新为 10 线程/队列 100，消息发送为 2 线程/队列 100。容量参数与全局限流阈值需在实际部署规模下调整，不能把默认值视为已验证吞吐。

## 真实依赖测试入口

- 秒杀与限流：设置 `REDIS_TEST_HOST`、`REDIS_TEST_PORT`、可选 `REDIS_TEST_PASSWORD`，运行 `SeckillRedisModuleTest,RateLimiterModuleTest`。秒杀默认使用 DB 15（可用 `REDIS_TEST_DATABASE` 指定 1 至 15）；仅精确清理本次测试数据。
- 逻辑缓存：设置 `CACHE_REDIS_INTEGRATION=true` 及 `SPRING_REDIS_HOST/PORT/PASSWORD`，运行 `LogicalCacheModuleTest`。
- 支付数据库：设置 `HMDP_MYSQL_TEST_URL/USER/PASSWORD`，运行 `PaymentDatabaseModuleTest`。仅允许数据库名 `hmdp_test` 或 `hmdp_test_*`，测试创建随机隔离表并仅清理本次表。

可用 `mvn -Dtest=模块测试类 test` 单独执行一个模块。Kafka 确认丢失、应用重启、多实例广播、真实跨组件故障恢复、SQL 执行计划及容量压测仍待有服务的集成环境验收，本次不把这些项标为已通过。

未完成请求、补偿任务和 Outbox 不因重试次数上限删除；完成后释放标记与请求至少保留 24 小时，Outbox 完成记录至少保留 7 天。当前不自动清理这些数据库记录，运维清理前须确保广播发送已完成且满足消息重放保留期。
