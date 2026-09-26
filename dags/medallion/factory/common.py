"""Helper bersama untuk factory DAG."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from medallion.config import Platform


def default_args(platform: Platform, owner: str | None) -> dict[str, Any]:
    args: dict[str, Any] = {
        "owner": owner or platform.owner,
        "retries": platform.retries,
        "retry_delay": timedelta(minutes=platform.retry_delay_minutes),
    }
    if platform.email_on_failure:
        # Airflow 3: parameter `email` di task sudah deprecated -> pakai SmtpNotifier
        # (butuh Connection "smtp_default" berisi server SMTP).
        from airflow.providers.smtp.notifications.smtp import SmtpNotifier

        args["on_failure_callback"] = SmtpNotifier(
            to=list(platform.email_on_failure),
            subject="[Airflow] GAGAL {{ ti.dag_id }}.{{ ti.task_id }}",
            html_content=(
                "DAG <b>{{ ti.dag_id }}</b> task <b>{{ ti.task_id }}</b> gagal "
                "(try {{ ti.try_number }}).<br>Run: {{ run_id }}"
            ),
        )
    return args
