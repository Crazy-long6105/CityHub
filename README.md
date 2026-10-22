# CityHub

CityHub 是一个本地生活服务平台的后端项目，为用户提供本地商户与优惠信息，并支持店铺发现、优惠券活动和生活内容分享。项目基于 Spring Boot 构建，使用 MySQL 保存业务数据，Redis 提供缓存、地理位置查询和签到能力，Kafka 用于秒杀下单及缓存事件处理。

> 当前仓库提供后端 REST API，不包含前端应用。默认 API 地址为 `http://localhost:8081`。

## 功能

- 用户验证码登录、用户信息、每日签到和连续签到统计
- 店铺分类、店铺详情、名称搜索，以及按类别和地理位置查找附近店铺
- 普通优惠券查询与秒杀活动、秒杀订单查询和支付接口
- 生活笔记发布、热门内容、点赞和关注动态
- 关注关系与共同关注查询
- Redis 缓存和 GEO 查询、Kafka 异步消息处理、秒杀库存与订单恢复、接口限流

秒杀与订单生命周期的后台处理默认关闭，可通过环境变量显式启用，具体配置见下文。

## 技术栈

| 类别 | 技术 |
| --- | --- |
| 语言与框架 | Java 8、Spring Boot 2.3.12、Spring MVC |
| 数据访问 | MyBatis-Plus 3.4.3、MySQL 5.7 |
| 缓存与并发 | Redis 6、Lettuce、Redisson、Lua |
| 消息处理 | Kafka、Spring for Apache Kafka |
| 其他 | Spring AOP、Validation、Lombok、Hutool |
| 本地部署 | Docker Compose |

## 快速开始

### 使用 Docker Compose

需要 Docker 和 Docker Compose。仓库提供 MySQL、Redis、Kafka 与后端服务的编排：

```bash
docker compose up -d --build
```

后端 API 默认监听 `http://localhost:8081`。可用店铺分类接口检查服务：

```bash
curl http://localhost:8081/shop-type/list
```

停止服务但保留数据卷：

```bash
docker compose down
```

数据库数据保存在 Docker volume 中。清除数据卷会删除本地数据库数据，请在确认后再执行：

```bash
docker compose down -v
```

### 本地运行后端

1. 安装 Java 8、Maven，并启动 MySQL 5.7、Redis 6 和 Kafka（Kafka 需要 Zookeeper）。也可以用 Docker Compose 启动依赖服务后，在本机运行后端。
2. 初始化数据库。Docker Compose 会在 MySQL 首次创建数据目录时自动载入 `src/main/resources/db/` 下的初始化脚本和迁移脚本；非 Docker 环境请按顺序执行 `hmdp.sql`、`migration/seckill.sql`、`payment.sql` 和 `cache-reliability.sql`。
3. 设置数据库、Redis 和 Kafka 连接环境变量。可参考 `.env.example`；应用配置也可在 `src/main/resources/application.yaml` 中查看。
4. 在项目根目录启动：

```bash
mvn spring-boot:run
```

默认服务端口是 `8081`。数据库密码需通过 `SPRING_DATASOURCE_PASSWORD` 提供；Redis 密码可通过 `SPRING_REDIS_PASSWORD` 提供。连接地址分别支持 `SPRING_DATASOURCE_URL`、`SPRING_REDIS_HOST`、`SPRING_REDIS_PORT` 和 `SPRING_KAFKA_BOOTSTRAP_SERVERS` 覆盖。

## 秒杀与订单配置

秒杀功能及订单生命周期调度默认关闭。启用时请配置足够强的查询签名密钥：

```bash
HMDP_SECKILL_ENABLED=true
HMDP_SECKILL_QUERY_SECRET=<至少32字节的随机密钥>
HMDP_ORDER_LIFECYCLE_ENABLED=true
```

Docker Compose 部署时可将这些变量放入本地 `.env` 文件。不要将实际密钥提交到版本库。

## API 概览

以下是主要接口，完整请求参数和响应结构以对应 Controller 实现为准。需要登录的接口通过登录后获得的令牌进行身份校验。

| 模块 | 方法 | 路径 | 说明 |
| --- | --- | --- | --- |
| 用户 | `POST` | `/user/code` | 发送登录验证码 |
| 用户 | `POST` | `/user/login` | 登录 |
| 用户 | `POST` | `/user/logout` | 登出 |
| 用户 | `GET` | `/user/me` | 当前用户 |
| 用户 | `POST` | `/user/sign` | 每日签到 |
| 用户 | `GET` | `/user/sign/count` | 连续签到统计 |
| 店铺 | `GET` | `/shop/{id}` | 店铺详情 |
| 店铺 | `GET` | `/shop/of/type` | 按类别查询，可传坐标进行附近搜索 |
| 店铺 | `GET` | `/shop/of/name` | 按名称搜索 |
| 店铺分类 | `GET` | `/shop-type/list` | 分类列表 |
| 优惠券 | `GET` | `/voucher/list/{shopId}` | 查询店铺优惠券 |
| 优惠券 | `POST` | `/voucher/seckill` | 创建秒杀券 |
| 秒杀订单 | `POST` | `/voucher-order/seckill/{id}` | 发起秒杀 |
| 秒杀订单 | `GET` | `/voucher-order/requests/{id}` | 查询秒杀请求结果 |
| 笔记 | `POST` | `/blog` | 发布笔记 |
| 笔记 | `GET` | `/blog/hot` | 热门笔记 |
| 笔记 | `GET` | `/blog/of/follow` | 关注动态 |
| 笔记 | `PUT` | `/blog/like/{id}` | 点赞或取消点赞 |
| 关注 | `PUT` | `/follow/{id}/{isFollow}` | 关注或取消关注 |
| 关注 | `GET` | `/follow/common/{id}` | 查询共同关注 |
## 项目结构

```text
src/main/java/com/hmdp/
  config/       Spring、Redis、Kafka、MyBatis 与 Web 配置
  controller/   REST API 控制器
  dto/          请求与响应数据对象
  entity/       业务实体
  mapper/       数据访问接口
  service/      业务逻辑及订单处理
  utils/        缓存、登录拦截、限流与消息监听等组件
src/main/resources/
  application.yaml
  db/           数据库初始化与迁移脚本
  mapper/       MyBatis SQL 映射
  *.lua         Redis 原子操作脚本
```

## 相关文档

- [Docker 部署指南](README.Docker.md)
- [数据库初始化脚本](src/main/resources/db/hmdp.sql)
