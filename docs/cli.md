# CLI Usage

`neocortex` 的 CLI 目前分成 7 个域：

- `db`
- `connector`
- `sync`
- `market-data-provider`
- `indicator`
- `agent`
- `feishu`

默认入口：

```bash
uv run python -m neocortex --help
```

CLI 的业务结果输出到标准输出；诊断信息走 logging。

命令结构约束：
- 新命令和已迁移到 command kernel 的命令，所有选项都必须挂在叶子命令上
- 不在中间节点定义选项，例如使用 `db query --db-path ...`，而不是 `db --db-path ... query ...`

带 `--start-date/--end-date` 的命令在不显式传 `--end-date` 时：
- `CN` 市场会先判断今天是否为交易日；如果不是，则取前一个交易日
- `CN` 市场如果今天是交易日，则在 BaoStock 数据通常于北京时间 `18:30` 后可用时取今天，否则取前一个交易日
- 其他市场暂时直接取当天
- `--start-date` 默认仍是 `--end-date` 往前 10 年

带 `--as-of-date` 的命令在不显式传值时，也会复用同一套 market-aware 默认日期规则。

## DB

`db` 用来直接查询 SQLite 数据库。

按表查看：

```bash
uv run python -m neocortex db query --table daily_price_bars --limit 20
```

执行原始 SQL：

```bash
uv run python -m neocortex db query \
  --sql "SELECT source, market, trade_date, is_trading_day FROM trading_dates LIMIT 10"
```

## Connector

`connector` 用来直连单个数据源做调试，不负责 DB-first 或 source 路由。

查看 A 股证券列表：

```bash
uv run python -m neocortex connector baostock securities --market CN
```

查看单个股票的公司概况：

```bash
uv run python -m neocortex connector efinance profile --name 中芯国际
```

查看原始日线：

```bash
uv run python -m neocortex connector baostock daily \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20
```

查看复权日线：

```bash
uv run python -m neocortex connector akshare adjusted-daily \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20 \
  --adjustment-type qfq
```


## Market Data Provider

`market-data-provider` 走运行时统一入口：先读 DB，缺失时按 source priority 回源，并在适合的资源上 write-through。

初始化数据库：

```bash
uv run python -m neocortex market-data-provider init-db
```

查看统一视图下的证券列表：

```bash
uv run python -m neocortex market-data-provider securities --market CN
```

查看统一视图下的公司概况：

```bash
uv run python -m neocortex market-data-provider profile --name 中芯国际
```

查看历史行情：

```bash
uv run python -m neocortex market-data-provider bars \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20
```

查看前复权行情：

```bash
uv run python -m neocortex market-data-provider bars \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20 \
  --adjust qfq
```

查询区间交易日历：

```bash
uv run python -m neocortex market-data-provider trading-dates \
  --market CN \
  --start-date 2026-03-19 \
  --end-date 2026-03-23
```

查询单日是否为交易日：

```bash
uv run python -m neocortex market-data-provider trading-dates \
  --market CN \
  --date 2026-03-21
```

## Sync

`sync bars` 必须且只能选择一种目标模式：

- `--symbol` / `--name`
- `--ticker`（可重复；单次也可跟多个值；既支持 `<symbol>.<exchange>`，也支持名称模糊搜索）
- `--all-securities`

同步证券列表：

```bash
uv run python -m neocortex sync securities --market CN
```

同步单只股票历史行情：

```bash
uv run python -m neocortex sync bars \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20
```

同步一组股票历史行情：

```bash
uv run python -m neocortex sync bars \
  --market CN \
  --ticker 600519.XSHG \
  --ticker 000001.XSHE \
  --start-date 2026-03-01 \
  --end-date 2026-03-20
```

也可以在一个 `--ticker` 后面直接跟多个值：

```bash
uv run python -m neocortex sync bars \
  --ticker 赣锋 天齐 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20
```

`--ticker` 也可以直接给名称：

```bash
uv run python -m neocortex sync bars \
  --ticker 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20
```

同步当前 market 下全部证券历史行情：

```bash
uv run python -m neocortex sync bars \
  --market CN \
  --all-securities \
  --start-date 2026-03-01 \
  --end-date 2026-03-20
```

同步 CN 全量交易日历：

```bash
uv run python -m neocortex sync trading-dates
```

这个命令固定同步：
- `market=CN`
- `start_date=1990-12-19`
- `end_date=today`

## Agent

`agent` 用来调试单个 agent 的 request 和 prompt。

`technical` 在构建技术指标时默认使用前复权（`qfq`）行情。

渲染 request 和 prompt 的 JSON：

```bash
uv run python -m neocortex agent render \
  --role technical \
  --name 中芯国际 \
  --as-of-date 2026-03-20 \
  --format json
```

直接输出最终 prompt 文本：

```bash
uv run python -m neocortex agent render \
  --role technical \
  --name 中芯国际 \
  --as-of-date 2026-03-20 \
  --format text
```

## Indicator

`indicator` 用来基于统一 market-data provider 取行情并计算技术指标。

`indicator <指标名>` 默认使用前复权（`qfq`）行情；如需其他口径可显式传 `--adjust`。

查看支持的指标：

```bash
uv run python -m neocortex indicator list
```

计算单个指标并输出表格：

```bash
uv run python -m neocortex indicator roc \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20 \
  --param period=5
```

计算带参数的指标并输出 JSON：

```bash
uv run python -m neocortex indicator macd \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20 \
  --param normalization=close \
  --format json
```

`--param` 也支持在一个 flag 后面直接跟多个参数：

```bash
uv run python -m neocortex indicator macd \
  --name 中芯国际 \
  --param fast_window=10 slow_window=20
```

## Feishu

启动长连接：

```bash
uv run python -m neocortex feishu longconn
```

清理过期的 bot event receipts 和已完成 job：

```bash
uv run python -m neocortex feishu cleanup --older-than-days 3
```

Bot 聊天入口：

- 群聊：必须先 `@bot`，然后发送 `help`、`job <job-id>` 或 `cli <full-cli-command>`
- 私聊：可直接发送 `help`、`job <job-id>` 或 `cli <full-cli-command>`
- 不再支持旧的 `/neo ...`、`profile ...`、`bars ...`、`db ...` 文本命令

示例：

```text
@bot help
@bot job 12
@bot cli db query --table company_profiles
```

## Shared Options

所有域都支持：

```bash
uv run python -m neocortex --log-level DEBUG ...
uv run python -m neocortex --env-file .env.local ...
```

其中：

- `--log-level` 控制日志级别
- 未传 `--env-file` 时，CLI 会先走 `python-dotenv` 的默认查找逻辑
- `--env-file` 会覆盖默认查找路径，并在构建完整 parser 之前先加载环境变量
- 对支持日期区间的命令，未传 `--start-date` 时默认取 `end-date` 往前 10 年
- `market-data-provider trading-dates` 支持 `--date` 单点查询，或 `--start-date/--end-date` 区间查询；不支持未来日期
- 需要证券标识的命令通常支持 `--symbol` 或 `--name` 二选一；`--name` 依赖本地 market-data DB 中已有 alias 数据
- `CN` 市场下使用 `--symbol` 时可以省略 `--exchange`，CLI 会根据 `symbol` 自动推断；其他市场仍建议显式传 `--exchange`
- `indicator <指标名>` 默认 `--format table`
- `indicator <指标名>` 默认 `--adjust qfq`
- `agent render` 默认 `--format json`

## Agent Skill Usage

这一节面向把 `neocortex` 当作本地 skill 调用的 agent。

### Entry Principles

- 优先走本地 CLI，不通过 Feishu bot 间接调用
- 标准入口固定为：

```bash
uv run python -m neocortex ...
```

- 顶层共享参数始终放在命令前：

```bash
uv run python -m neocortex --env-file .env.local --log-level DEBUG ...
```

### Domain Selection Guide

- `market-data-provider`
  - 统一运行时读数据入口，默认优先使用
  - 适合查证券列表、公司概况、历史行情、交易日历、基本面和披露内容
- `indicator`
  - 基于统一 provider 取行情并计算技术指标
  - 默认优先于手工拼 `market-data-provider bars` + 外部计算
- `connector`
  - 只在调试单个 source 行为、排查 source 差异、验证上游 payload 时使用
- `db`
  - 只在直接查底表、核对持久化结果、做 SQLite 排障时使用
- `sync`
  - 只在显式同步/回填数据时使用
- `agent`
  - 只在调试单个 agent 的 request 和 prompt 渲染时使用
- `feishu`
  - 只用于 bot transport 管理，不是通用市场数据入口

### Typical Agent Calling Patterns

- 查公司概况：优先 `market-data-provider profile`
- 查历史行情：优先 `market-data-provider bars`
- 算技术指标：优先 `indicator <name>`
- 调试单源异常：再切到 `connector`
- 直接看 SQLite 表或排障：才用 `db query`

### Output Conventions

- CLI 的业务结果输出到标准输出；诊断信息走 logging
- 默认输出通常是表格文本；skill 消费时优先选结构化输出
- 当前高频 JSON 路径：
  - `db query --format json`
  - `indicator <name> --format json`
  - `agent render --format json`
  - `market-data-provider profile`
  - `market-data-provider fundamentals`
  - `market-data-provider disclosures`
  - `market-data-provider macro`
  - `sync securities`
  - `sync bars`
  - `sync trading-dates`
- 不支持 JSON 的命令目前只能消费表格文本，例如：
  - `market-data-provider bars`
  - `market-data-provider securities`
  - 多数 `connector` 列表和行情命令

### Failure Semantics

- usage/help 错误会直接返回命令自己的帮助文本
- `db query` 只允许单条只读 SQL；写操作、DDL、多语句和非法表名会被拒绝
- command kernel 约束一个 `CommandSpec` 对应一个叶子命令；选项必须挂在叶子上
- `CN` 市场下，未显式传 `--end-date` / `--as-of-date` 时，会使用 market-aware 默认日期规则，而不是盲目取当天
- `sync bars` 必须且只能选择一种目标模式：
  - `--symbol` / `--name`
  - `--ticker`
  - `--all-securities`
- `sync bars --all-securities` 在 Feishu transport 中会走 async job；本地 CLI 仍同步执行

### Copyable Agent Examples

查统一视图下的公司概况：

```bash
uv run python -m neocortex market-data-provider profile --name 中芯国际
```

查统一视图下的历史行情：

```bash
uv run python -m neocortex market-data-provider bars \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20
```

用结构化输出计算指标：

```bash
uv run python -m neocortex indicator macd \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20 \
  --format json
```

调试单个 source 的公司概况：

```bash
uv run python -m neocortex connector efinance profile --name 中芯国际
```

直接查 SQLite 底表：

```bash
uv run python -m neocortex db query \
  --table daily_price_bars \
  --limit 20 \
  --format json
```

调试单个 agent 的 prompt 渲染：

```bash
uv run python -m neocortex agent render \
  --role technical \
  --name 中芯国际 \
  --as-of-date 2026-03-20 \
  --format json
```
