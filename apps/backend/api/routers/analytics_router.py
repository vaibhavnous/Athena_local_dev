from typing import Any, Dict, List

from fastapi import APIRouter

router = APIRouter()


@router.get("/analytics/cost")
def analytics_cost() -> List[Dict[str, Any]]:
    return []
