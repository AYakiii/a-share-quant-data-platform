# A-Share Quant Data Platform

A research-oriented A-share systematic trading framework built around a modular pipeline:

**Data → Panel → Feature → Signal → Backtest → Diagnostics → Constraints**

This project focuses on **daily-frequency / low-frequency research workflows**, not production live trading.

---

## Overview

This repository started from an A-share data engineering pipeline and was extended into a research-oriented trading system with the following layers:

### Data Layer
- AkShare-based ingestion
- parquet storage
- sqlite metadata
- legacy notebook pipeline

### Research Layer
- panel abstraction
- feature store
- signal engine
- diagnostics
- exposure analysis

### Portfolio / Strategy Layer
- portfolio construction
- execution simulation
- transaction cost modeling
- portfolio constraints
- constraint impact analysis

---

## Project Status

**Current status: V1 complete**

### Included in V1
- standardized research panel access
- feature store v1
- signal engine v1
- backtest MVP
- diagnostics v1
- exposure analysis v1
- portfolio constraints v1
- constraint impact analysis v1

### Not included in V1
- live trading / OMS / EMS
- high-frequency / order book research
- large-scale ML alpha platform
- full benchmark attribution / optimizer stack

---

## Repository Structure

```text
.
├─ A_share_Analytical_DWH.ipynb
├─ run_demo.py
├─ requirements.txt
├─ src/
│  └─ qsys/
│     ├─ data/
│     ├─ features/
│     ├─ signals/
│     ├─ backtest/
│     ├─ research/
│     └─ utils/
└─ tests/

---

## Key Modules

### src/qsys/data/
- panel access layer

### src/qsys/features/
- feature abstractions
- feature registry
- feature materialization / store

### src/qsys/signals/
- cross-sectional transforms
- signal combination
- demo alpha construction

### src/qsys/backtest/
- portfolio construction
- execution alignment
- transaction cost
- simulator
- summary metrics

### src/qsys/research/
- IC / Rank IC
- quantile analysis
- persistence / turnover
- decay analysis
- signal correlation
- exposure analysis
- constraint impact analysis

### src/qsys/utils/
- CLI / example entrypoints
- synthetic demo data generator

---

## Quickstart

### 1. Install dependencies

```bash
pip install -r requirements.txt

