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
- 尚未迁移的旧命令会逐步收敛到这个形态

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
  --sql "SELECT source, market, trade_date, is_trading_day FROM trading_dates LIMIT 10" \
  --format table
```

## Connector

`connector` 用来直连单个数据源做调试，不负责 DB-first 或 source 路由。

查看 A 股证券列表：

```bash
uv run python -m neocortex connector akshare securities --market CN
```

查看单个股票的公司概况：

```bash
uv run python -m neocortex connector efinance profile \
  --market CN \
  --name 中芯国际
```

查看原始日线：

```bash
uv run python -m neocortex connector baostock daily \
  --market CN \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20
```

查看复权日线：

```bash
uv run python -m neocortex connector akshare adjusted-daily \
  --market CN \
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
uv run python -m neocortex market-data-provider profile \
  --market CN \
  --name 中芯国际
```

查看历史行情：

```bash
uv run python -m neocortex market-data-provider bars \
  --market CN \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20
```

查看前复权行情：

```bash
uv run python -m neocortex market-data-provider bars \
  --market CN \
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
- `--ticker`（可重复；既支持 `<symbol>.<exchange>`，也支持名称模糊搜索）
- `--all-securities`

同步证券列表：

```bash
uv run python -m neocortex sync securities --market CN
```

同步单只股票历史行情：

```bash
uv run python -m neocortex sync bars \
  --market CN \
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

`--ticker` 也可以直接给名称：

```bash
uv run python -m neocortex sync bars \
  --market CN \
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
  --market CN \
  --name 中芯国际 \
  --as-of-date 2026-03-20 \
  --format json
```

直接输出最终 prompt 文本：

```bash
uv run python -m neocortex agent render \
  --role technical \
  --market CN \
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
  --market CN \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20 \
  --param period=5
```

计算带参数的指标并输出 JSON：

```bash
uv run python -m neocortex indicator macd \
  --market CN \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20 \
  --param normalization=close \
  --format json
```

## Feishu

启动长连接：

```bash
uv run python -m neocortex feishu longconn
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
- 对支持日期区间的命令，未传 `--end-date` 时默认取当天；未传 `--start-date` 时默认取 `end-date` 往前 10 年
- `market-data-provider trading-dates` 支持 `--date` 单点查询，或 `--start-date/--end-date` 区间查询；不支持未来日期
- 需要证券标识的命令通常支持 `--symbol` 或 `--name` 二选一；`--name` 依赖本地 market-data DB 中已有 alias 数据
- `CN` 市场下使用 `--symbol` 时可以省略 `--exchange`，CLI 会根据 `symbol` 自动推断；其他市场仍建议显式传 `--exchange`
- `indicator <指标名>` 默认 `--format table`
- `indicator <指标名>` 默认 `--adjust qfq`
- `agent render` 默认 `--format json`
