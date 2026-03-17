# A-Share Quant Data Platform

A research-oriented A-share systematic trading framework built around a modular pipeline:

Data → Panel → Feature → Signal → Backtest → Diagnostics → Constraints

This project focuses on daily-frequency / low-frequency research workflows, not production live trading.

---

## Overview

This repository started from an A-share data engineering pipeline and was extended into a research-oriented trading system.

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

Current status: V1 complete

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

    pip install -r requirements.txt

### 2. Generate synthetic demo feature store

    PYTHONPATH=src python -m qsys.utils.generate_synthetic_feature_store

This creates:

    data/processed/feature_store/v1/

---

### 3. Run demos

Run all:

    python run_demo.py --mode all

Or:

    python run_demo.py --mode signal
    python run_demo.py --mode diagnostics
    python run_demo.py --mode backtest
    python run_demo.py --mode impact

---

## Example Workflow

1. Load feature-store data  
2. Construct signal  
   Example: rank(ret_20d) - 0.5 * zscore(vol_20d)  
3. Run diagnostics  
   - IC / Rank IC  
   - quantile spread  
   - exposure analysis  
4. Construct portfolio  
5. Run backtest  
6. Evaluate constraint impact  

---

## Synthetic Demo Note

This repository does not include full market datasets.

To make it runnable:

    PYTHONPATH=src python -m qsys.utils.generate_synthetic_feature_store

This is for:
- smoke testing
- architecture validation
- demo usage

It does NOT represent real trading performance.

---

## Legacy Notebook

A_share_Analytical_DWH.ipynb is kept as the original pipeline.

Current system:

    src/qsys/

---

## Testing

    PYTHONPATH=src pytest -q

---

## Future Work

- integrate real standardized panel outputs  
- improve execution assumptions  
- benchmark comparison  
- richer exposure controls  
- report / tearsheet generation  
- broader strategy research support  

---

## Summary

This project is a research-oriented A-share systematic trading framework focused on:

- modular architecture  
- reproducible research  
- signal diagnostics  
- portfolio behavior analysis  

It aims to answer:

- Does a signal have predictive power?  
- How stable is the alpha?  
- What exposures drive the signal?  
- How do constraints affect performance?  
