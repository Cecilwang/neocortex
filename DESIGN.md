# Neocortex Design

## Goal

Build Neocortex as a prompt-driven multi-market equity research platform with four major capabilities:

1. A multi-agent equity analyzer driven by structured prompts.
2. A provider or connector layer that supplies the data required by prompts.
3. A rich indicator library with formulas, explanations, and interpretation guidance.
4. A frontend application that shows stock basics, candlesticks, custom indicators, and agent outputs.

The product must support multiple stock markets from the start:

- US equities
- Japan equities
- Hong Kong equities
- China A-shares

## Step 1 Scope

This step defines the stable architecture boundary before any provider or agent implementation work:

- Project layout
- Shared domain models
- Agent input and output protocol
- Design details maintained in this document

No external data access or LLM execution is implemented yet.

## Architecture

The system is split into four layers:

1. Data layer
   - Connectors fetch market, fundamentals, news, macro, and sector benchmark data.
   - Connectors normalize upstream schemas into stable internal models.
   - Market-aware adapters handle symbol format, exchange mapping, timezone, trading calendar, and benchmark selection.
2. Feature layer
   - Indicator engine computes technical and financial features.
   - Prompt builders convert normalized data and indicators into agent-ready payloads.
3. Agent layer
   - The LLM component manages endpoint configuration, authentication env vars, and request-level inference parameters.
   - Specialized agents consume structured prompt inputs and return validated JSON outputs.
   - The orchestration layer routes outputs through a fixed hierarchy.
4. Application layer
   - API exposes data, indicators, and agent traces.
   - Frontend renders charts, stock details, and agent decision flows.

## Initial Module Layout

```text
src/neocortex/
  models/
    core.py
    agent.py
```

Planned later modules:

```text
src/neocortex/
  llm/
  connectors/
  indicators/
  prompts/
  agents/
  services/
  api/
  frontend/
```

Current implementation status:

- `models/` defines the normalized market, company, price, fundamentals, macro, and agent contracts.
- `llm/` defines static endpoint config and request-level inference settings.
- `connectors/` now defines the normalized connector interface and provider ticker codecs.
- `connectors/` also includes an in-memory connector for tests, fixtures, and local development.

## Core Data Contracts

The data layer normalizes all sources into these stable objects:

- `SecurityId`: canonical stock identifier with `symbol + market + exchange`.
- `MarketContext`: market-level settings such as timezone, calendar, benchmark, and trading currency.
- `PriceBar`: OHLCV bar plus optional adjusted close.
- `FundamentalSnapshot`: raw and derived company fundamentals as of a date.
- `NewsItem`: title, summary, source, published time, sentiment tags.
- `MacroSeriesPoint`: named macro observation with frequency and rate-of-change fields.
- `SectorBenchmark`: sector-level aggregates used by higher-level agents.
- `CompanyProfile`: stock identity, sector, industry, and currency fields.

These models are intentionally provider-agnostic so AkShare, Yahoo Finance, EDINET-style sources, or later replacements do not leak into downstream code.

Multi-market implication:

- Connectors must normalize different symbol conventions such as `AAPL`, `7203.T`, `0700.HK`, and `600519.SH`.
- Different providers may disagree on the same security identifier, so each connector must implement its own `SecurityId <-> provider ticker` conversion logic.
- Agent prompts must receive explicit market context so the model can interpret trading sessions, accounting cadence, language, and benchmark references correctly.
- Sector benchmarks are market-scoped; a Japan auto stock must not be compared against a US auto sector average by default.
- Macro inputs are market-scoped first, with optional cross-market overlays later.

Provider symbol policy:

- `SecurityId` is the only canonical identity used inside the system.
- Each connector is responsible for converting between `SecurityId` and its own ticker format.
- Provider-specific symbol syntax must not leak into indicators, prompts, agent traces, or frontend state.
- If ticker conversion later requires shared state, introduce a dedicated resolver service in the connector layer instead of pushing provider types into core models.

Initial codec scope:

- Yahoo Finance uses `AAPL`, `7203.T`, `0700.HK`, `600519.SS`, and `000001.SZ`.
- AkShare currently uses lowercase CN-prefixed tickers such as `sh600519` and `sz000001`.
- `MANUAL` accepts the canonical `MARKET:SYMBOL` form for fixtures and local test data.
- Exchange inference is only automatic where the provider ticker format makes it unambiguous.

## Agent Protocol

Every agent uses the same envelope:

- `AgentRequest`: identifies the request, agent, security, analysis date, expected schema version, and structured input payload.
- `AgentResponse`: the current shared response schema with request identity, score, confidence, reasoning, and raw model output.
- `AgentExecutionTrace`: records request, response, prompt version, full inference configuration, timing, and validation status.

This protocol supports:

- Traceability for prompt debugging
- Replay and backtesting
- Frontend visualization of intermediate agent outputs
- Schema validation and controlled retries

Response schema policy:

- Keep the shared response contract minimal.
- Add agent-specific response schemas only when a concrete agent needs extra fields such as signals, risks, or citations.
- Do not widen `AgentResponse` preemptively for fields that only some agents may emit.
- `as_of_date` is the market data cutoff date, not the execution identifier; repeated runs on the same day must be distinguished by `request_id` and trace timestamps.

Inference configuration policy:

- Do not represent a model call with only `model_name`; the runtime must capture service family, base URL, auth environment variable, and request parameters.
- Separate static endpoint config from per-request sampling settings.
- Keep LLM service configuration in a dedicated component rather than embedding provider details inside agent protocol definitions.
- Trace records must be sufficient to reproduce which LLM service and invocation settings produced a response.

## Agent Hierarchy

The planned workflow follows the paper's hierarchical design:

1. `technical_agent`
2. `quant_fundamental_agent`
3. `qualitative_fundamental_agent`
4. `news_agent`
5. `sector_agent`
6. `macro_agent`
7. `pm_agent`

Execution graph:

```text
technical_agent -------------------|
quant_fundamental_agent ---------- |
qualitative_fundamental_agent ---- |--> sector_agent --|
news_agent ------------------------|                  |
macro_agent ------------------------------------------|--> pm_agent
```

## Step 1 Decisions

- Use Python 3.11 and `uv` project management.
- Keep shared contracts in standard-library dataclasses first.
- Prefer immutable, typed domain models to avoid schema drift.
- Keep all agent payloads JSON-serializable.
- Keep design details in `DESIGN.md` rather than duplicating them in code.
- Treat market as a first-class dimension in identifiers, prompts, and benchmarks.
- Keep normalized identifiers stable even when upstream providers use incompatible ticker formats.
- Treat provider ticker resolution as a dedicated mapping concern, not as ad hoc string conversion inside connectors.
- Remove speculative extension fields from shared models until real call sites require them.

## Next Steps

1. Implement the first external-data connector on top of the normalized connector interface.
2. Implement the indicator specification registry and calculation engine.
3. Implement prompt builders for the technical and quantitative agents first.
4. Implement agent runtime with schema validation, retries, and trace storage.
5. Add API and frontend workbench views.
