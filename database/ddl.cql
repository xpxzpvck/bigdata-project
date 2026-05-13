CREATE KEYSPACE IF NOT EXISTS wikipedia_analytics 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wikipedia_analytics;

-- Таблиця для моніторингу ботів (Task A2)
CREATE TABLE IF NOT EXISTS bot_activity_metrics (
    domain text,
    window_start timestamp,
    window_end timestamp,
    human_count int,
    bot_count int,
    bot_percentage double,
    top_humans list<text>,
    top_bots list<text>,
    PRIMARY KEY ((domain), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Таблиця для моніторингу мовної активності (Task A3)
CREATE TABLE IF NOT EXISTS language_activity (
    domain text,
    window_start timestamp,
    window_end timestamp,
    pages_count int,
    unique_authors int,
    avg_title_length double,
    trend_percent double,
    PRIMARY KEY ((domain), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Таблиця для щогодинних звітів (Task B1)
CREATE TABLE IF NOT EXISTS hourly_activity_report (
    domain text,
    time_start timestamp,
    time_end timestamp,
    pages_created int,
    unique_authors int,
    bot_percent double,
    top_authors text, -- Зберігаємо масив як JSON-рядок для швидкої віддачі в API
    PRIMARY KEY ((domain), time_start)
) WITH CLUSTERING ORDER BY (time_start DESC);

-- B2. Editor Behavior Patterns
CREATE TABLE IF NOT EXISTS editor_patterns (
    user_name text PRIMARY KEY,
    avg_gap_seconds double,
    active_hours list<int>,
    domains_count int,
    specialization text,
    total_pages int
);

-- C1. Domain List (Stats for last hour)
-- Ця таблиця оновлюється стрімінговою джобою A3
CREATE TABLE IF NOT EXISTS domain_stats (
    domain text PRIMARY KEY,
    pages_count int,
    unique_authors int,
    avg_title_length double,
    last_updated timestamp
);

-- C2. Pages by User
CREATE TABLE IF NOT EXISTS pages_by_user (
    user_id text,
    created_at timestamp,
    page_id text,
    page_title text,
    domain text,
    PRIMARY KEY ((user_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

-- C3. Page Details
CREATE TABLE IF NOT EXISTS page_details (
    page_id text PRIMARY KEY,
    page_title text,
    domain text,
    user_name text,
    created_at timestamp
);

-- C4. Pages by Domain
CREATE TABLE IF NOT EXISTS pages_by_domain (
    domain text,
    created_at timestamp,
    page_id text,
    page_title text,
    user_name text,
    PRIMARY KEY ((domain), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);