from __future__ import annotations

from qsys.utils import run_akshare_backfill_tasks as mod


def test_run_akshare_backfill_tasks_execute_uses_akshare_wrapper(capsys, tmp_path):
    captured = {}

    def fake_runner(tasks, **kwargs):
        captured["task_count"] = len(tasks)
        captured.update(kwargs)
        return {"ok": True, "task_count": len(tasks)}

    rc = mod.main([
        "--execute",
        "--max-tasks",
        "1",
        "--output-root",
        str(tmp_path / "out"),
        "--metastore-path",
        str(tmp_path / "meta.sqlite"),
    ], runner=fake_runner)
    assert rc == 0
    assert captured["dry_run"] is False
    assert captured["max_tasks"] == 1
    assert captured["task_count"] == 1
    assert '"ok": true' in capsys.readouterr().out
