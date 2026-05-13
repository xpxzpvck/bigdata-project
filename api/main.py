import json
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pydantic import BaseModel
from datetime import datetime

app = FastAPI(title="Wikipedia Analytics API")

# Підключення до Cassandra
cluster = Cluster(['cassandra'])
session = cluster.connect('wikipedia_analytics')

# --- Моделі даних ---
class Author(BaseModel):
    name: str
    pages: int
    is_bot: bool

class HourlyReport(BaseModel):
    time_start: str
    time_end: str
    domain: str
    pages_created: int
    unique_authors: int
    bot_percent: float
    top_authors: List[Author]

# --- Ендпоінти Частини B ---

@app.get("/api/reports/hourly", response_model=List[HourlyReport])
async def get_hourly_report(domain: str, hours: int = 6):
    query = """
        SELECT time_start, time_end, domain, pages_created, unique_authors, bot_percent, top_authors 
        FROM hourly_activity_report 
        WHERE domain = %s LIMIT %s
    """
    rows = session.execute(query, (domain, hours))
    
    result = []
    for row in rows:
        result.append(HourlyReport(
            time_start=row.time_start.strftime("%H:%M"),
            time_end=row.time_end.strftime("%H:%M"),
            domain=row.domain,
            pages_created=row.pages_created,
            unique_authors=row.unique_authors,
            bot_percent=row.bot_percent,
            top_authors=json.loads(row.top_authors) # Парсимо JSON-рядок з бази
        ))
    return result

@app.get("/api/analytics/editor-patterns")
async def get_editor_patterns(min_pages: int = 5):
    query = "SELECT * FROM editor_patterns"
    rows = session.execute(query)
    # Фільтрація на стороні API, бо Cassandra не любить нерівності без індексів
    return [row for row in rows if row.total_pages >= min_pages]

# --- Ендпоінти Частини C ---

@app.get("/api/domains")
async def list_domains():
    rows = session.execute("SELECT * FROM domain_stats")
    return list(rows)

@app.get("/api/users/{user_id}/pages")
async def get_pages_by_user(user_id: str, limit: int = 100):
    query = "SELECT * FROM pages_by_user WHERE user_id = %s LIMIT %s"
    rows = session.execute(query, (user_id, limit))
    return list(rows)

@app.get("/api/pages/{page_id}")
async def get_page_details(page_id: str):
    query = "SELECT * FROM page_details WHERE page_id = %s"
    row = session.execute(query, (page_id,)).one()
    if not row:
        raise HTTPException(status_code=404, detail="Page not found")
    return row

@app.get("/api/domains/{domain}/pages")
async def get_pages_by_domain(
    domain: str, 
    from_ts: Optional[datetime] = None, 
    to_ts: Optional[datetime] = None, 
    limit: int = 100
):
    # Базовий запит
    if from_ts and to_ts:
        query = "SELECT * FROM pages_by_domain WHERE domain = %s AND created_at >= %s AND created_at <= %s LIMIT %s"
        rows = session.execute(query, (domain, from_ts, to_ts, limit))
    else:
        query = "SELECT * FROM pages_by_domain WHERE domain = %s LIMIT %s"
        rows = session.execute(query, (domain, limit))
    return list(rows)