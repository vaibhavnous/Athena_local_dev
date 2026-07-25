from typing import Any, Dict, List

from fastapi import APIRouter

from utilis.db import config, get_connection
from utilis.logger import logger

router = APIRouter()


@router.get("/analytics/cost")
def analytics_cost() -> List[Dict[str, Any]]:
    schema = config.get("azure_sql", {}).get("pipeline_schema") or config.get("azure_sql", {}).get("schema_name")
    if not schema:
        return []

    try:
        conn = get_connection()
    except Exception as exc:
        logger.warning("Cost analytics connection failed: %s", exc, extra={"node": "analytics_router"})
        return []

    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT
                CONVERT(varchar(10), CONVERT(date, stored_at), 23) AS cost_date,
                SUM(CASE WHEN LOWER(stage) LIKE '%requirement%' THEN COALESCE(cost_usd, 0) ELSE 0 END) AS stage02_cost,
                SUM(CASE WHEN LOWER(stage) LIKE '%kpi%' OR LOWER(stage) LIKE '%nomination%' THEN COALESCE(cost_usd, 0) ELSE 0 END) AS stage03_cost,
                SUM(COALESCE(cost_usd, 0)) AS total_cost,
                SUM(COALESCE(token_count, 0)) AS total_tokens
            FROM [{schema}].[ai_store]
            WHERE stored_at >= DATEADD(day, -30, SYSUTCDATETIME())
            GROUP BY CONVERT(date, stored_at)
            ORDER BY CONVERT(date, stored_at)
            """
        )
        return [
            {
                "date": str(row[0]),
                "stage02Cost": float(row[1] or 0.0),
                "stage03Cost": float(row[2] or 0.0),
                "totalCost": float(row[3] or 0.0),
                "tokens": int(row[4] or 0),
            }
            for row in cursor.fetchall()
        ]
    except Exception as exc:
        logger.warning("Cost analytics query failed: %s", exc, extra={"node": "analytics_router"})
        return []
    finally:
        conn.close()
