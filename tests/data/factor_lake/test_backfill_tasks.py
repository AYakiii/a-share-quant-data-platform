from qsys.data.factor_lake.backfill_tasks import (
    dry_run_summary,
    export_backfill_tasks_csv,
    filter_tasks,
    generate_tasks_from_default_backfill_plan,
)


def test_generate_tasks_from_default_backfill_plan():
    tasks = generate_tasks_from_default_backfill_plan()
    assert len(tasks) > 0
    families = {t.source_family for t in tasks}
    assert "market_price" in families
    assert "index_market" in families


def test_filter_and_max_tasks():
    tasks = generate_tasks_from_default_backfill_plan()
    p0 = filter_tasks(tasks, priority="P0")
    assert len(p0) > 0
    mp = filter_tasks(tasks, source_family="market_price")
    assert all(t.source_family == "market_price" for t in mp)
    one = filter_tasks(tasks, max_tasks=1)
    assert len(one) == 1


def test_export_and_dry_run_summary(tmp_path):
    tasks = generate_tasks_from_default_backfill_plan()
    tasks_path, summary_path = export_backfill_tasks_csv(tmp_path / "outputs" / "factor_lake_registry", tasks)
    assert tasks_path.exists()
    assert summary_path.exists()
    summary = dry_run_summary(tasks)
    assert "task_count" in summary.columns
