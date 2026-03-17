# AGENTS.md

## Project overview
This repository is an A-share research-oriented trading system.
Current foundation includes:
- AkShare data ingestion
- retry mechanism
- incremental update
- parquet storage
- sqlite metadata
- daily A-share market data infrastructure

Target architecture:
Data -> Feature -> Signal -> Backtest -> Portfolio -> Execution Simulation

## Current priorities
1. Feature Store
2. Signal Engine
3. Backtest Engine

## Coding rules
- Preserve existing data ingestion interfaces as much as possible
- Prefer pandas + parquet workflow
- Use sqlite only for metadata / registry
- Keep modules small and composable
- Add type hints and docstrings
- Add tests for new modules

## Expected directory direction
- src/qsys/data
- src/qsys/features
- src/qsys/signals
- src/qsys/backtest
- tests/

## Before finishing a task
- Run tests if available
- Update README when behavior changes
- Do not refactor unrelated modules