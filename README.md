# Loyalty AI Operations - Base Skeleton

This repository contains a minimal skeleton for the Loyalty AI Operations Copilot.

Services:
- FastAPI app that exposes MCP-like endpoints (aiops service)
- Elasticsearch (single-node) + Kibana
- PostgreSQL
- Neo4j
- Qdrant (vector DB)

Quickstart:
1. Build and run:
   docker-compose up --build

2. Health check:
   curl http://localhost:8000/health

3. Example MCP call:
   curl "http://localhost:8000/mcp/get_transaction_summary?transaction_type=EARN&start_time=2026-09-01T08:00:00Z&end_time=2026-09-01T09:00:00Z"

Notes:
- The API endpoints are functional but rely on Elasticsearch/Postgres/Neo4j being populated with appropriate indices/tables.
- For production, secure Elasticsearch/Neo4j/Postgres (do not disable auth), configure persistent storage, resource limits, and monitoring.
