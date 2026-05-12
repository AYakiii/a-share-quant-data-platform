from __future__ import annotations
import argparse, json, time
from pathlib import Path
import pandas as pd
from qsys.data.sources.akshare_margin import fetch_stock_margin_detail_sse, fetch_stock_margin_detail_szse
from qsys.reporting.artifacts import write_warnings

def _dedupe_keep_order(values:list[str])->list[str]:
    seen=set(); out=[]
    for v in values:
        x=v.strip()
        if x and x not in seen: seen.add(x); out.append(x)
    return out

def _normalize_symbol(s:str)->str:
    x=str(s).strip().lower();
    if x.startswith(("sh","sz","bj")) and len(x)==8:return x
    d=''.join(ch for ch in x if ch.isdigit())
    if d.startswith(("60","68")):return f"sh{d}"
    if d.startswith(("00","30")):return f"sz{d}"
    return f"bj{d}"

def _load_symbols(symbols,symbols_file):
    merged=list(symbols or [])
    if symbols_file: merged += [x.strip() for x in Path(symbols_file).read_text(encoding='utf-8').splitlines() if x.strip()]
    out=_dedupe_keep_order(merged)
    if not out: raise ValueError("No symbols provided. Please pass --symbols and/or --symbols-file.")
    return [_normalize_symbol(s) for s in out]

def _pick_col(df, choices):
    for c in choices:
        if c in df.columns:return c
    return None

def _normalize_raw_margin(raw, date_str, symbols):
    if raw is None or raw.empty:return pd.DataFrame(columns=["date","asset","financing_balance","financing_buy_amount","margin_total_balance"])
    symbol_col=_pick_col(raw,["ts_code","证券代码","股票代码","标的证券代码","code"])
    date_col=_pick_col(raw,["trade_date","信用交易日期","交易日期","date","日期"])
    fb=_pick_col(raw,["financing_balance","融资余额","融资余额(元)"])
    buy=_pick_col(raw,["financing_buy_amount","融资买入额","融资买入额(元)"])
    total=_pick_col(raw,["margin_total_balance","融资融券余额","融资融券余额(元)"])
    if None in (symbol_col,fb,buy,total): return pd.DataFrame()
    out=pd.DataFrame()
    out['asset']=raw[symbol_col].astype(str).map(_normalize_symbol)
    out['date']=pd.to_datetime(raw[date_col],errors='coerce') if date_col else pd.to_datetime(date_str)
    out['date']=out['date'].fillna(pd.to_datetime(date_str))
    out['financing_balance']=pd.to_numeric(raw[fb],errors='coerce')
    out['financing_buy_amount']=pd.to_numeric(raw[buy],errors='coerce')
    out['margin_total_balance']=pd.to_numeric(raw[total],errors='coerce')
    return out[out['asset'].isin(set(symbols))].dropna(subset=['date','asset']).drop_duplicates(['date','asset'])

def build_margin_leverage_panel(*,symbols=None,symbols_file=None,start_date:str,end_date:str,output_root="data/processed/margin_panel/v1",output_dir="outputs/margin_panel",run_name=None,retries=2,retry_wait=1.0,request_sleep=0.5,skip_failed_symbols=True,show_progress=False,progress_every=1,include_calendar_days=False,raw_cache_root="data/raw/margin_detail/v1",use_raw_cache=True,overwrite_cache=False,resume=True,exchanges=None,max_dates=None,request_timeout=None):
    sel=_load_symbols(symbols,symbols_file); run_id=run_name or f"margin_panel_{start_date}_{end_date}"
    art_dir=Path(output_dir)/run_id; art_dir.mkdir(parents=True,exist_ok=True)
    root=Path(output_root); root.mkdir(parents=True,exist_ok=True)
    raw_root=Path(raw_cache_root)
    warnings=[]
    if not use_raw_cache: warnings.append("Raw cache disabled by configuration.")
    if request_timeout is not None: warnings.append("request_timeout not supported by adapters; ignored.")
    dates=pd.date_range(start=start_date,end=end_date,freq="D" if include_calendar_days else "B")
    if max_dates is not None: dates=dates[:max_dates]; warnings.append(f"max_dates limiter active: using first {len(dates)} dates")
    by_ex={"SSE":[s for s in sel if s.startswith('sh')],"SZSE":[s for s in sel if s.startswith('sz')]}
    inferred=[k for k,v in by_ex.items() if v]
    req=(exchanges or "").lower().strip()
    if req in ("","auto"): used=inferred
    elif req=="both": used=["SSE","SZSE"]
    else: used=[x.upper() for x in req.split(',') if x.strip()]
    if set(used)!=set(inferred): warnings.append(f"exchange selection differs from inferred symbols: inferred={inferred}, used={used}")
    fetchers={"SSE":fetch_stock_margin_detail_sse,"SZSE":fetch_stock_margin_detail_szse}
    cache={}; frames=[]; started=time.perf_counter(); step=max(1,int(progress_every))
    planned=len(dates)*len(used); hits=misses=net_attempt=net_fail=0; empty_count={"SSE":0,"SZSE":0}; fail_samples={"SSE":[],"SZSE":[]}; empty_samples={"SSE":[],"SZSE":[]}
    for ex in used:
      syms=by_ex.get(ex,[])
      if not syms: continue
      total=len(dates)
      for i,d in enumerate(dates,1):
        log=show_progress and (i==1 or i==total or i%step==0)
        ds=d.strftime('%Y%m%d'); d2=d.strftime('%Y-%m-%d'); key=(ex,ds)
        if key not in cache:
          cfp=raw_root/f"exchange={ex}"/f"trade_date={d2}"/"data.parquet"
          if use_raw_cache and cfp.exists() and not overwrite_cache:
            cache[key]=pd.read_parquet(cfp); hits+=1
            if log: print(f"[{ex} {i}/{total}] CACHE {d2} rows_raw={len(cache[key])}",flush=True)
          else:
            misses+=1; net_attempt+=1
            if log: print(f"[{ex} {i}/{total}] FETCH {d2} START",flush=True)
            t0=time.perf_counter(); got=None
            for a in range(1,retries+1):
              try: got=fetchers[ex](ds).raw; break
              except Exception:
                if a<retries: time.sleep(retry_wait*a)
            if got is None:
              net_fail+=1; cache[key]=pd.DataFrame();
              if len(fail_samples[ex])<5: fail_samples[ex].append(d2)
              if log: print(f"[{ex} {i}/{total}] FAIL {d2} reason=fetch_error rows_raw=0 rows_selected=0 elapsed={time.perf_counter()-t0:.1f}s total_elapsed={time.perf_counter()-started:.1f}s",flush=True)
            else:
              cache[key]=got if isinstance(got,pd.DataFrame) else pd.DataFrame(got)
              if use_raw_cache:
                cfp.parent.mkdir(parents=True,exist_ok=True); cache[key].to_parquet(cfp,index=False)
              if request_sleep>0: time.sleep(request_sleep)
        raw=cache[key]
        if raw.empty:
          empty_count[ex]+=1
          if len(empty_samples[ex])<5: empty_samples[ex].append(d2)
          if log: print(f"[{ex} {i}/{total}] FAIL {d2} reason=empty rows_raw=0 rows_selected=0 elapsed=0.0s total_elapsed={time.perf_counter()-started:.1f}s",flush=True)
          continue
        n=_normalize_raw_margin(raw,d2,syms)
        if log: print(f"[{ex} {i}/{total}] OK {d2} rows_raw={len(raw)} rows_selected={len(n)} elapsed=0.0s total_elapsed={time.perf_counter()-started:.1f}s",flush=True)
        if not n.empty: frames.append(n)
    if not frames: raise ValueError("No margin panel rows loaded for requested symbols/date range")
    panel=pd.concat(frames,ignore_index=True)
    for dt,g in panel.groupby('date',sort=True):
      p=root/f"trade_date={pd.Timestamp(dt).strftime('%Y-%m-%d')}"; p.mkdir(parents=True,exist_ok=True)
      g.rename(columns={'date':'trade_date','asset':'ts_code'}).to_parquet(p/'data.parquet',index=False)
    per=panel.groupby('asset').size().to_dict(); swd=sorted([s for s,n in per.items() if n>0]); swod=[s for s in sel if s not in set(swd)]
    if swod:
      msg="Symbols with no margin data in date range: "+', '.join(swod)
      if skip_failed_symbols:warnings.append(msg)
      else: raise ValueError(msg)
    (art_dir/'symbols.txt').write_text('\n'.join(sel)+'\n',encoding='utf-8')
    panel.groupby('asset').agg(n_rows=('asset','size'),n_dates=('date','nunique')).reset_index().to_csv(art_dir/'data_quality_summary.csv',index=False)
    for ex,c in empty_count.items():
      if c>0: warnings.append(f"{ex} empty responses skipped: {c} dates sample={empty_samples[ex]}")
    if net_fail>0: warnings.append(f"failed exchange/date requests: {net_fail} sample={fail_samples}")
    manifest={"phase":"18A-2","fetch_strategy":"exchange_date_first_with_raw_cache","start_date":start_date,"end_date":end_date,"n_selected_symbols":len(sel),"selected_symbols":sel,"n_loaded_rows":len(panel),"output_root":str(root),"output_dir":str(art_dir),"raw_cache_root":str(raw_root),"use_raw_cache":use_raw_cache,"overwrite_cache":overwrite_cache,"n_dates_planned":len(dates),"max_dates":max_dates,"exchanges_requested":exchanges or "inferred","exchanges_used":used,"planned_exchange_date_requests":planned,"cache_hits":hits,"cache_misses":misses,"network_requests_attempted":net_attempt,"network_requests_failed":net_fail,"empty_exchange_dates":int(sum(empty_count.values())),"symbols_with_data":swd,"symbols_without_data":swod}
    (art_dir/'panel_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    warnings_fp=write_warnings(art_dir,warnings)
    if show_progress:
      elapsed=time.perf_counter()-started; mins,secs=divmod(int(elapsed),60)
      print(f"Done: fetched={len(swd)} failed={len(swod)} rows={len(panel)} elapsed={mins}m{secs:02d}s",flush=True)
    return {"panel_root":root,"panel_manifest":art_dir/'panel_manifest.json',"warnings":warnings_fp,"symbols":art_dir/'symbols.txt',"data_quality":art_dir/'data_quality_summary.csv'}

def parse_args():
    p=argparse.ArgumentParser();p.add_argument('--symbols',nargs='*',default=None);p.add_argument('--symbols-file',default=None);p.add_argument('--start-date',required=True);p.add_argument('--end-date',required=True);p.add_argument('--output-root',default='data/processed/margin_panel/v1');p.add_argument('--output-dir',default='outputs/margin_panel');p.add_argument('--run-name',default=None);p.add_argument('--retries',type=int,default=2);p.add_argument('--retry-wait',type=float,default=1.0);p.add_argument('--request-sleep',type=float,default=0.5);p.add_argument('--skip-failed-symbols',type=lambda x:str(x).lower()!='false',default=True);p.add_argument('--show-progress',action='store_true');p.add_argument('--progress-every',type=int,default=1);p.add_argument('--include-calendar-days',action='store_true');p.add_argument('--raw-cache-root',default='data/raw/margin_detail/v1');p.add_argument('--use-raw-cache',type=lambda x:str(x).lower()!='false',default=True);p.add_argument('--overwrite-cache',action='store_true');p.add_argument('--resume',type=lambda x:str(x).lower()!='false',default=True);p.add_argument('--exchanges',default=None);p.add_argument('--max-dates',type=int,default=None);p.add_argument('--request-timeout',type=float,default=None);return p.parse_args()

def main():
    a=parse_args(); out=build_margin_leverage_panel(symbols=a.symbols,symbols_file=a.symbols_file,start_date=a.start_date,end_date=a.end_date,output_root=a.output_root,output_dir=a.output_dir,run_name=a.run_name,retries=a.retries,retry_wait=a.retry_wait,request_sleep=a.request_sleep,skip_failed_symbols=a.skip_failed_symbols,show_progress=a.show_progress,progress_every=a.progress_every,include_calendar_days=a.include_calendar_days,raw_cache_root=a.raw_cache_root,use_raw_cache=a.use_raw_cache,overwrite_cache=a.overwrite_cache,resume=a.resume,exchanges=a.exchanges,max_dates=a.max_dates,request_timeout=a.request_timeout); print({k:str(v) for k,v in out.items()})
if __name__=='__main__': main()
