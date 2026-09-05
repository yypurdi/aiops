"""
Loyalty AI Ops Engine - minimal implementations to query Elasticsearch, PostgreSQL, and Neo4j.
Functions are synchronous and intentionally simple so you can extend them later.
"""
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple

from elasticsearch import Elasticsearch
import psycopg2
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

ES_URL = os.getenv("ELASTIC_URL", "http://elasticsearch:9200")
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://aiops:aiops_pass@postgres:5432/aiops")
NEO4J_URL = os.getenv("NEO4J_URL", "bolt://neo4j:7687")

# Elasticsearch client
es = Elasticsearch([ES_URL], timeout=30)

# Neo4j driver
try:
    neo4j_driver = GraphDatabase.driver(NEO4J_URL)
except Exception:
    neo4j_driver = None

# Simple Postgres helper (DSN)
def get_pg_conn():
    # POSTGRES_URL format: postgresql://user:pass@host:port/db
    return psycopg2.connect(POSTGRES_URL)

# --- Transaction summary (ES-first, fallback to Postgres) ---
def es_transaction_summary(transaction_type: str, start_iso: str, end_iso: str) -> Dict[str, Any]:
    """
    Query Elasticsearch transaction indices (loyalty-transactions-*) to compute summary.
    """
    index = "loyalty-transactions-*"
    body = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"transaction.type.keyword": transaction_type}},
                    {"range": {"@timestamp": {"gte": start_iso, "lte": end_iso}}}
                ]
            }
        },
        "aggs": {
            "by_status": {
                "terms": {"field": "transaction.status.keyword", "size": 5}
            },
            "avg_duration": {"avg": {"field": "duration_ms"}},
            "p95_duration": {"percentiles": {"field": "duration_ms", "percents": [95]}}
        }
    }
    try:
        resp = es.search(index=index, body=body)
        total = resp["hits"]["total"]["value"] if isinstance(resp["hits"]["total"], dict) else resp["hits"]["total"]
        buckets = {b["key"]: b["doc_count"] for b in resp["aggregations"]["by_status"]["buckets"]}
        success = buckets.get("SUCCESS", 0) or buckets.get("success", 0)
        failed = sum(v for k, v in buckets.items() if k.upper() != "SUCCESS")
        avg = int(resp["aggregations"]["avg_duration"]["value"]) if resp["aggregations"]["avg_duration"]["value"] else None
        p95 = int(resp["aggregations"]["p95_duration"]["values"]["95.0"]) if resp["aggregations"]["p95_duration"]["values"].get("95.0") else None
        return {
            "transaction_type": transaction_type,
            "total": int(total),
            "success": int(success),
            "failed": int(failed),
            "success_rate": round((success / total * 100) if total else 0.0, 2),
            "failure_rate": round((failed / total * 100) if total else 0.0, 2),
            "avg_duration_ms": avg,
            "p95_duration_ms": p95
        }
    except Exception as e:
        logger.exception("ES transaction summary failed: %s", e)
        # fallback: try Postgres
        return pg_transaction_summary(transaction_type, start_iso, end_iso)


def pg_transaction_summary(transaction_type: str, start_iso: str, end_iso: str) -> Dict[str, Any]:
    """
    Fallback to Postgres if Elasticsearch is unavailable.
    Assumes a table transactions(transaction_id, transaction_type, status, duration_ms, created_at, customer_id)
    Adjust queries to match your schema.
    """
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        q = """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success,
                SUM(CASE WHEN status <> 'SUCCESS' THEN 1 ELSE 0 END) as failed,
                AVG(duration_ms) as avg_duration
            FROM transactions
            WHERE transaction_type = %s AND created_at >= %s AND created_at <= %s
        """
        cur.execute(q, (transaction_type, start_iso, end_iso))
        row = cur.fetchone()
        total, success, failed, avg_duration = row
        p95 = None
        # p95 calculation could be added with percentile_disc/statements later
        cur.close()
        conn.close()
        return {
            "transaction_type": transaction_type,
            "total": int(total or 0),
            "success": int(success or 0),
            "failed": int(failed or 0),
            "success_rate": round((success / total * 100) if total else 0.0, 2),
            "failure_rate": round((failed / total * 100) if total else 0.0, 2),
            "avg_duration_ms": int(avg_duration) if avg_duration else None,
            "p95_duration_ms": p95
        }
    except Exception as e:
        logger.exception("Postgres transaction summary failed: %s", e)
        return {
            "transaction_type": transaction_type,
            "total": 0,
            "success": 0,
            "failed": 0,
            "success_rate": 0.0,
            "failure_rate": 0.0,
            "avg_duration_ms": None,
            "p95_duration_ms": None
        }

# --- Search logs (Elasticsearch) ---
def es_search_logs(query: str, size: int = 20) -> Dict[str, Any]:
    index = "loyalty-logs-*"
    body = {
        "query": {
            "query_string": {
                "query": query
            }
        },
        "size": size,
        "sort": [{"@timestamp": {"order": "desc"}}]
    }
    try:
        resp = es.search(index=index, body=body)
        hits = [h["_source"] for h in resp.get("hits", {}).get("hits", [])]
        return {"took_ms": resp.get("took"), "hits": hits}
    except Exception as e:
        logger.exception("search_logs error: %s", e)
        return {"took_ms": 0, "hits": []}

# --- Service dependency (Neo4j) ---
def get_service_dependency(service_name: str, depth: int = 2) -> List[Dict[str, Any]]:
    """
    Return list of dependent services up to given depth.
    Assumes graph has nodes labeled :Service with property name, and relationships :DEPENDS_ON
    """
    cypher = """
    MATCH (s:Service {name: $name})-[:DEPENDS_ON*1..$depth]->(dep:Service)
    RETURN DISTINCT dep.name as name
    """
    try:
        if not neo4j_driver:
            return []
        with neo4j_driver.session() as session:
            result = session.run(cypher, name=service_name, depth=depth)
            deps = [record["name"] for record in result]
            return [{"name": d} for d in deps]
    except Exception as e:
        logger.exception("Neo4j dependency query failed: %s", e)
        return []

# --- Anomaly detection (simple heuristic based on failure rate increase) ---
def detect_anomaly_transaction_failure(transaction_type: str, window_start: str, window_end: str, baseline_window_days: int = 7) -> Dict[str, Any]:
    """
    Very simple anomaly detector:
    - computes failure rate for given window
    - computes baseline failure rate as average failure rate in previous N days (approx)
    - if current / baseline > threshold -> anomaly
    """
    # current window
    cur = es_transaction_summary(transaction_type, window_start, window_end)
    current_failure_rate = cur.get("failure_rate", 0.0)

    # baseline window (naive: shift by baseline_window_days)
    try:
        start_dt = datetime.fromisoformat(window_start.replace("Z", "+00:00"))
        baseline_end_dt = start_dt - timedelta(seconds=1)
        baseline_start_dt = start_dt - timedelta(days=baseline_window_days)
        baseline_start = baseline_start_dt.isoformat()
        baseline_end = baseline_end_dt.isoformat()
    except Exception:
        baseline_start = None
        baseline_end = None

    baseline_failure_rate = 0.0
    if baseline_start and baseline_end:
        baseline = es_transaction_summary(transaction_type, baseline_start, baseline_end)
        baseline_failure_rate = baseline.get("failure_rate", 0.0)

    severity = "OK"
    deviation = None
    confidence = 0.0
    if baseline_failure_rate:
        deviation = (current_failure_rate / baseline_failure_rate) if baseline_failure_rate else None
        if deviation and deviation >= 5:
            severity = "CRITICAL"
            confidence = 0.9
        elif deviation and deviation >= 2:
            severity = "MAJOR"
            confidence = 0.75
        else:
            severity = "WARNING" if deviation and deviation > 1.2 else "OK"
            confidence = 0.5 if severity == "WARNING" else 0.2
    else:
        # no baseline: low confidence
        if current_failure_rate > 1.0:
            severity = "MAJOR"
            confidence = 0.5
        else:
            severity = "OK"
            confidence = 0.1

    return {
        "transaction_type": transaction_type,
        "current_failure_rate": current_failure_rate,
        "baseline_failure_rate": baseline_failure_rate,
        "deviation": deviation,
        "severity": severity,
        "confidence": confidence
    }

# --- Correlate events (by trace_id / request_id /transaction_id) ---
def correlate_events_by_keys(keys: List[str], start_iso: str, end_iso: str, size: int = 100) -> Dict[str, Any]:
    """
    Search logs, api-events, transactions and group by correlation keys provided.
    Returns top matches for each key.
    """
    indices = ["loyalty-logs-*", "loyalty-api-events-*", "loyalty-transactions-*"]
    query = {
        "query": {
            "bool": {
                "must": [
                    {"range": {"@timestamp": {"gte": start_iso, "lte": end_iso}}}
                ],
                "should": [
                    {"exists": {"field": k}} for k in keys
                ],
                "minimum_should_match": 1
            }
        },
        "size": size,
        "sort": [{"@timestamp": {"order": "desc"}}]
    }
    try:
        resp = es.search(index=",".join(indices), body=query)
        hits = [h["_source"] for h in resp.get("hits", {}).get("hits", [])]
        # simple grouping by first matching key found in document
        groups = {}
        for doc in hits:
            for k in keys:
                if k in doc:
                    val = doc.get(k)
                    if val:
                        keyname = f"{k}:{val}"
                        groups.setdefault(keyname, []).append(doc)
                        break
        return {"count": len(hits), "groups": groups}
    except Exception as e:
        logger.exception("correlate_events error: %s", e)
        return {"count": 0, "groups": {}}

# --- Simple Root Cause Analysis (heuristic combining correlation + dependency) ---
def analyze_root_cause(transaction_type: str, start_iso: str, end_iso: str) -> Dict[str, Any]:
    """
    Produce a simple RCA:
    - detect anomalies on transaction failure
    - correlate recent errors and find top service names
    - check dependency graph to see if a dependent service appears in errors
    """
    # 1) anomaly
    anomaly = detect_anomaly_transaction_failure(transaction_type, start_iso, end_iso)

    # 2) get error logs correlated with transaction type
    corr = correlate_events_by_keys(["transaction_id", "trace_id", "request_id"], start_iso, end_iso, size=200)
    # find top services in correlated docs
    svc_count = {}
    for k, docs in corr.get("groups", {}).items():
        for d in docs:
            svc = None
            if "service" in d:
                svc = d["service"].get("name") if isinstance(d["service"], dict) else d.get("service")
            if svc:
                svc_count[svc] = svc_count.get(svc, 0) + 1
    top_services = sorted(svc_count.items(), key=lambda x: x[1], reverse=True)[:5]
    probable_root = top_services[0][0] if top_services else None

    # 3) check dependencies for those top services
    dependency_hits = []
    if probable_root:
        deps = get_service_dependency(probable_root, depth=2)
        dependency_hits = deps

    # simple confidence heuristics
    confidence = anomaly.get("confidence", 0.0)
    if probable_root and anomaly.get("severity") in ("CRITICAL", "MAJOR"):
        confidence = max(confidence, 0.75)
    elif probable_root:
        confidence = max(confidence, 0.5)

    evidence = []
    if anomaly:
        evidence.append(f"Failure rate deviation: {anomaly.get('deviation')}")
    if top_services:
        evidence.append(f"Top services in correlated logs: {top_services}")
    if dependency_hits:
        evidence.append(f"Dependency suggests: {dependency_hits}")

    return {
        "incident_id": f"RCA-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "transaction_type": transaction_type,
        "root_cause": probable_root or "INSUFFICIENT_EVIDENCE",
        "confidence": round(confidence, 2),
        "evidence": evidence
    }

# --- Customer impact (count distinct customers in window) ---
def calculate_customer_impact(start_iso: str, end_iso: str) -> Dict[str, Any]:
    """
    Count affected transactions and distinct customers from loyalty-transactions-* index.
    """
    index = "loyalty-transactions-*"
    body = {
        "size": 0,
        "query": {
            "range": {"@timestamp": {"gte": start_iso, "lte": end_iso}}
        },
        "aggs": {
            "transactions": {"value_count": {"field": "transaction_id.keyword"}},
            "customers": {"cardinality": {"field": "customer_id.keyword"}},
            "points_sum": {"sum": {"field": "points.amount"}}
        }
    }
    try:
        resp = es.search(index=index, body=body)
        tx = resp["aggregations"]["transactions"]["value"]
        customers = resp["aggregations"]["customers"]["value"]
        points = resp["aggregations"]["points_sum"]["value"] if resp["aggregations"]["points_sum"].get("value") else 0
        return {
            "transactions": int(tx),
            "customers": int(customers),
            "points": int(points)
        }
    except Exception as e:
        logger.exception("calculate_customer_impact error: %s", e)
        return {"transactions": 0, "customers": 0, "points": 0}
