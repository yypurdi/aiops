from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import logging

from .engine import (
    es_transaction_summary,
    es_search_logs,
    get_service_dependency,
    detect_anomaly_transaction_failure,
    correlate_events_by_keys,
    analyze_root_cause,
    calculate_customer_impact,
)

app = FastAPI(title="Loyalty AI Ops - MCP API")

logger = logging.getLogger("uvicorn")
logger.setLevel(logging.INFO)

class TransactionSummaryResponse(BaseModel):
    transaction_type: str
    total: int
    success: int
    failed: int
    success_rate: float
    failure_rate: float
    avg_duration_ms: Optional[int] = None
    p95_duration_ms: Optional[int] = None

@app.get("/health")
def health():
    return {"status": "ok", "service": "aiops", "time": datetime.utcnow().isoformat() + "Z"}

@app.get("/mcp/get_transaction_summary", response_model=TransactionSummaryResponse)
def get_transaction_summary(
    transaction_type: str = Query(..., description="Transaction type (EARN/REDEEM/..."),
    start_time: str = Query(..., description="ISO8601 start time"),
    end_time: str = Query(..., description="ISO8601 end time"),
):
    # Validate timestamps
    try:
        _ = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        _ = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
    except Exception:
        raise HTTPException(status_code=400, detail="start_time or end_time not ISO8601")
    summary = es_transaction_summary(transaction_type, start_time, end_time)
    return summary

@app.get("/mcp/search_logs")
def search_logs(query: str = Query(..., description="Query string for logs"), size: int = 20):
    result = es_search_logs(query, size=size)
    return result

@app.get("/mcp/get_service_dependency")
def service_dependency(service_name: str = Query(..., description="Service name"), depth: int = 2):
    deps = get_service_dependency(service_name, depth=depth)
    return {"service": service_name, "dependencies": deps}

@app.get("/mcp/detect_anomaly")
def detect_anomaly(transaction_type: str = Query(...), start_time: str = Query(...), end_time: str = Query(...)):
    res = detect_anomaly_transaction_failure(transaction_type, start_time, end_time)
    return res

@app.get("/mcp/correlate_events")
def correlate_events(
    keys: Optional[List[str]] = Query(["trace_id", "request_id", "transaction_id"], description="Correlation keys"),
    start_time: str = Query(...),
    end_time: str = Query(...),
    size: int = Query(100)
):
    res = correlate_events_by_keys(keys, start_time, end_time, size=size)
    return res

@app.get("/mcp/analyze_root_cause")
def rca(transaction_type: str = Query(...), start_time: str = Query(...), end_time: str = Query(...)):
    res = analyze_root_cause(transaction_type, start_time, end_time)
    return res

@app.get("/mcp/calculate_customer_impact")
def customer_impact(start_time: str = Query(...), end_time: str = Query(...)):
    res = calculate_customer_impact(start_time, end_time)
    return res
