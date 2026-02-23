"""
9 tools that give the AI agent hands to query the database.

Each tool calls fetch_all/fetch_one directly — same connection pool as the
API routers. No HTTP self-calls, no serialization overhead, no auth needed.

The @tool decorator does three things:
1. Registers the function so LangGraph can execute it
2. Uses the docstring as the tool description (the LLM reads this to decide
   when to call it — vague docstrings = wrong tool choices)
3. Converts the function signature into a parameter schema

Results are capped at 20 rows to stay within the LLM's context window
and keep per-query costs around $0.01.
"""
import json
from decimal import Decimal
from datetime import date, datetime

from langchain_core.tools import tool

from api.database import fetch_all, fetch_one


def _serialize(obj):
    """Handle Decimal and date types that json.dumps can't serialize natively.
    PostgreSQL returns Decimals for ROUND() and date objects for date columns."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (date, datetime)):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def _to_json(data, max_items: int = 20) -> str:
    """Serialize to JSON string, capping list results.
    A query might return 10K rows, but the LLM only needs the top 20
    to answer most questions. Keeps context small and costs low."""
    if isinstance(data, list) and len(data) > max_items:
        data = data[:max_items]
    return json.dumps(data, default=_serialize)


# Reusable weather-flight join clause — matches flights to weather observations
# using precomputed integer hour columns for fast equi-joins
WEATHER_JOIN = """
    JOIN weather_hourly w
        ON f.origin_airport = w.airport_code
        AND f.flight_date = w.observation_date
        AND f.dep_hour = w.hour
"""


# ─────────────────────────────────────────────────────────────
# Tool 1: Carrier Performance Rankings
# ─────────────────────────────────────────────────────────────

@tool
def get_carrier_performance(sort_by: str = "flights") -> str:
    """Get all carriers ranked by performance. sort_by: 'flights', 'delay', 'cancel_rate', or 'on_time'."""
    sort_map = {
        "flights": "total_flights DESC",
        "delay": "avg_delay DESC",
        "cancel_rate": "cancel_rate DESC",
        "on_time": "on_time_pct DESC",
    }
    order = sort_map.get(sort_by, "total_flights DESC")
    rows = fetch_all(f"""
        SELECT
            f.carrier_code,
            c.carrier_name,
            COUNT(*) as total_flights,
            ROUND(AVG(CASE WHEN arr_delay IS NOT NULL THEN arr_delay END)::numeric, 1) as avg_delay,
            ROUND(100.0 * SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) / COUNT(*), 2) as cancel_rate,
            ROUND(100.0 * SUM(CASE WHEN arr_delay <= 0 AND NOT cancelled THEN 1 ELSE 0 END) / COUNT(*), 1) as on_time_pct
        FROM flights f
        JOIN carriers c ON f.carrier_code = c.carrier_code
        GROUP BY f.carrier_code, c.carrier_name
        ORDER BY {order}
    """)
    return _to_json(rows)


# ─────────────────────────────────────────────────────────────
# Tool 2: Single Carrier Deep Dive
# ─────────────────────────────────────────────────────────────

@tool
def get_carrier_details(carrier_code: str) -> str:
    """Get delay breakdown and monthly trend for a specific carrier. Use 2-letter IATA code (e.g. 'AA', 'DL', 'UA')."""
    code = carrier_code.upper()

    # Validate carrier exists before running expensive queries
    carrier = fetch_one(
        "SELECT carrier_code, carrier_name FROM carriers WHERE carrier_code = %(code)s",
        {"code": code},
    )
    if not carrier:
        return json.dumps({"error": f"Carrier '{code}' not found"})

    # BTS delay type averages (only for flights delayed 15+ min)
    breakdown = fetch_one("""
        SELECT
            f.carrier_code,
            c.carrier_name,
            ROUND(AVG(carrier_delay)::numeric, 1) as avg_carrier_delay,
            ROUND(AVG(weather_delay)::numeric, 1) as avg_weather_delay,
            ROUND(AVG(nas_delay)::numeric, 1) as avg_nas_delay,
            ROUND(AVG(security_delay)::numeric, 1) as avg_security_delay,
            ROUND(AVG(late_aircraft_delay)::numeric, 1) as avg_late_aircraft_delay
        FROM flights f
        JOIN carriers c ON f.carrier_code = c.carrier_code
        WHERE f.carrier_code = %(code)s AND f.arr_delay_15 = true
        GROUP BY f.carrier_code, c.carrier_name
    """, {"code": code})

    # Monthly performance trend (chart-ready data)
    monthly = fetch_all("""
        SELECT
            d.month, d.month_name,
            COUNT(*) as total_flights,
            ROUND(AVG(CASE WHEN f.arr_delay IS NOT NULL THEN f.arr_delay END)::numeric, 1) as avg_delay,
            ROUND(100.0 * SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) / COUNT(*), 2) as cancel_rate
        FROM flights f
        JOIN date_dim d ON f.flight_date = d.date_id
        WHERE f.carrier_code = %(code)s
        GROUP BY d.month, d.month_name
        ORDER BY d.month
    """, {"code": code})

    return _to_json({
        "carrier": carrier,
        "delay_breakdown": breakdown,
        "monthly_trend": monthly,
    })


# ─────────────────────────────────────────────────────────────
# Tool 3: Delay Patterns (day-of-week, monthly, BTS breakdown)
# ─────────────────────────────────────────────────────────────

@tool
def get_delay_patterns(carrier: str | None = None, origin: str | None = None) -> str:
    """Get delay patterns by day-of-week, month, and BTS delay type breakdown. Optionally filter by carrier code or origin airport."""
    conditions = ["1=1"]
    params: dict = {}

    if carrier:
        conditions.append("f.carrier_code = %(carrier)s")
        params["carrier"] = carrier.upper()
    if origin:
        conditions.append("f.origin_airport = %(origin)s")
        params["origin"] = origin.upper()

    where = " AND ".join(conditions)

    # Which days of the week have the worst delays?
    by_dow = fetch_all(f"""
        SELECT d.day_of_week, d.day_name,
            COUNT(*) as total_flights,
            ROUND(AVG(CASE WHEN f.arr_delay IS NOT NULL THEN f.arr_delay END)::numeric, 1) as avg_delay,
            ROUND(100.0 * SUM(CASE WHEN f.arr_delay_15 THEN 1 ELSE 0 END) / COUNT(*), 1) as delayed_pct
        FROM flights f
        JOIN date_dim d ON f.flight_date = d.date_id
        WHERE {where}
        GROUP BY d.day_of_week, d.day_name
        ORDER BY d.day_of_week
    """, params)

    # Which months have the worst delays?
    by_month = fetch_all(f"""
        SELECT d.month, d.month_name,
            COUNT(*) as total_flights,
            ROUND(AVG(CASE WHEN f.arr_delay IS NOT NULL THEN f.arr_delay END)::numeric, 1) as avg_delay,
            ROUND(100.0 * SUM(CASE WHEN f.arr_delay_15 THEN 1 ELSE 0 END) / COUNT(*), 1) as delayed_pct
        FROM flights f
        JOIN date_dim d ON f.flight_date = d.date_id
        WHERE {where}
        GROUP BY d.month, d.month_name
        ORDER BY d.month
    """, params)

    # BTS 5-type delay breakdown (only for flights actually delayed 15+ min)
    delayed_where = where + " AND f.arr_delay_15 = true"
    breakdown = fetch_all(f"""
        SELECT * FROM (
            SELECT 'Carrier' as delay_type,
                SUM(CASE WHEN carrier_delay > 0 THEN 1 ELSE 0 END) as flights_affected,
                ROUND(AVG(CASE WHEN carrier_delay > 0 THEN carrier_delay END)::numeric, 1) as avg_minutes
            FROM flights f WHERE {delayed_where}
            UNION ALL
            SELECT 'Weather',
                SUM(CASE WHEN weather_delay > 0 THEN 1 ELSE 0 END),
                ROUND(AVG(CASE WHEN weather_delay > 0 THEN weather_delay END)::numeric, 1)
            FROM flights f WHERE {delayed_where}
            UNION ALL
            SELECT 'NAS',
                SUM(CASE WHEN nas_delay > 0 THEN 1 ELSE 0 END),
                ROUND(AVG(CASE WHEN nas_delay > 0 THEN nas_delay END)::numeric, 1)
            FROM flights f WHERE {delayed_where}
            UNION ALL
            SELECT 'Security',
                SUM(CASE WHEN security_delay > 0 THEN 1 ELSE 0 END),
                ROUND(AVG(CASE WHEN security_delay > 0 THEN security_delay END)::numeric, 1)
            FROM flights f WHERE {delayed_where}
            UNION ALL
            SELECT 'Late Aircraft',
                SUM(CASE WHEN late_aircraft_delay > 0 THEN 1 ELSE 0 END),
                ROUND(AVG(CASE WHEN late_aircraft_delay > 0 THEN late_aircraft_delay END)::numeric, 1)
            FROM flights f WHERE {delayed_where}
        ) t ORDER BY flights_affected DESC
    """, params)

    return _to_json({
        "by_day_of_week": by_dow,
        "by_month": by_month,
        "delay_type_breakdown": breakdown,
    })


# ─────────────────────────────────────────────────────────────
# Tool 4: Route Information
# ─────────────────────────────────────────────────────────────

@tool
def get_route_info(origin: str | None = None, dest: str | None = None) -> str:
    """Get route information. If both origin and dest given, returns specific route detail. If only origin given, returns busiest routes from that airport. If neither, returns top 15 busiest routes overall."""

    # Specific route: origin → destination with carrier breakdown
    if origin and dest:
        origin = origin.upper()
        dest = dest.upper()
        params = {"origin": origin, "dest": dest}
        summary = fetch_one("""
            SELECT COUNT(*) as total_flights,
                ROUND(AVG(CASE WHEN arr_delay IS NOT NULL THEN arr_delay END)::numeric, 1) as avg_delay,
                ROUND(100.0 * SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) / COUNT(*), 2) as cancel_rate
            FROM flights
            WHERE origin_airport = %(origin)s AND dest_airport = %(dest)s
        """, params)
        carriers = fetch_all("""
            SELECT f.carrier_code, c.carrier_name, COUNT(*) as flights,
                ROUND(AVG(CASE WHEN f.arr_delay IS NOT NULL THEN f.arr_delay END)::numeric, 1) as avg_delay
            FROM flights f
            JOIN carriers c ON f.carrier_code = c.carrier_code
            WHERE f.origin_airport = %(origin)s AND f.dest_airport = %(dest)s
            GROUP BY f.carrier_code, c.carrier_name
            ORDER BY flights DESC
        """, params)
        return _to_json({"origin": origin, "destination": dest, "summary": summary, "carriers": carriers})

    # Busiest routes (optionally filtered by origin airport)
    conditions = ["1=1"]
    params = {}
    if origin:
        conditions.append("f.origin_airport = %(origin)s")
        params["origin"] = origin.upper()
    where = " AND ".join(conditions)

    rows = fetch_all(f"""
        SELECT f.origin_airport as origin, a1.city as origin_city,
            f.dest_airport as destination, a2.city as dest_city,
            COUNT(*) as total_flights,
            ROUND(AVG(CASE WHEN f.arr_delay IS NOT NULL THEN f.arr_delay END)::numeric, 1) as avg_delay
        FROM flights f
        JOIN airports a1 ON f.origin_airport = a1.airport_code
        JOIN airports a2 ON f.dest_airport = a2.airport_code
        WHERE {where}
        GROUP BY f.origin_airport, a1.city, f.dest_airport, a2.city
        ORDER BY total_flights DESC
        LIMIT 15
    """, params)
    return _to_json(rows)


# ─────────────────────────────────────────────────────────────
# Tool 5: Weather Impact Analysis
# ─────────────────────────────────────────────────────────────

@tool
def get_weather_impact(condition: str | None = None, airport: str | None = None) -> str:
    """Get weather impact on flights: delays/cancellations by weather condition, temperature band, visibility range, and wind speed. Optionally filter by specific condition ('Snow', 'Rain', 'Fog', etc.) or airport code."""
    conditions = ["w.conditions IS NOT NULL"]
    params: dict = {}
    if airport:
        conditions.append("f.origin_airport = %(airport)s")
        params["airport"] = airport.upper()
    where = " AND ".join(conditions)

    # Impact by weather condition (snow, rain, fog, clear, etc.)
    by_condition = fetch_all(f"""
        SELECT w.conditions, COUNT(*) as total_flights,
            SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) as cancelled,
            ROUND(100.0 * SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) / COUNT(*), 1) as cancel_pct,
            ROUND(AVG(CASE WHEN f.arr_delay IS NOT NULL THEN f.arr_delay END)::numeric, 1) as avg_delay
        FROM flights f {WEATHER_JOIN}
        WHERE {where}
        GROUP BY w.conditions
        ORDER BY avg_delay DESC NULLS LAST
    """, params)

    # If a specific condition was requested, filter to matching rows
    if condition:
        by_condition = [r for r in by_condition if condition.lower() in r["conditions"].lower()]

    # Impact by temperature range
    temp_where = where.replace("w.conditions IS NOT NULL", "w.temperature IS NOT NULL")
    by_temp = fetch_all(f"""
        SELECT
            CASE
                WHEN w.temperature < 32 THEN 'Below Freezing (<32F)'
                WHEN w.temperature < 50 THEN 'Cold (32-50F)'
                WHEN w.temperature < 70 THEN 'Moderate (50-70F)'
                WHEN w.temperature < 85 THEN 'Warm (70-85F)'
                ELSE 'Hot (>85F)'
            END as temp_range,
            COUNT(*) as total_flights,
            ROUND(AVG(CASE WHEN f.arr_delay IS NOT NULL THEN f.arr_delay END)::numeric, 1) as avg_delay,
            ROUND(100.0 * SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) / COUNT(*), 2) as cancel_rate
        FROM flights f {WEATHER_JOIN}
        WHERE {temp_where}
        GROUP BY 1 ORDER BY avg_delay DESC NULLS LAST
    """, params)

    # Impact by visibility range
    vis_where = where.replace("w.conditions IS NOT NULL", "w.visibility IS NOT NULL")
    by_vis = fetch_all(f"""
        SELECT
            CASE
                WHEN w.visibility < 1 THEN 'Very Low (<1 mi)'
                WHEN w.visibility < 3 THEN 'Low (1-3 mi)'
                WHEN w.visibility < 5 THEN 'Moderate (3-5 mi)'
                WHEN w.visibility < 10 THEN 'Good (5-10 mi)'
                ELSE 'Excellent (10+ mi)'
            END as visibility_range,
            COUNT(*) as total_flights,
            ROUND(AVG(CASE WHEN f.arr_delay IS NOT NULL THEN f.arr_delay END)::numeric, 1) as avg_delay,
            ROUND(100.0 * SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) / COUNT(*), 2) as cancel_rate
        FROM flights f {WEATHER_JOIN}
        WHERE {vis_where}
        GROUP BY 1 ORDER BY avg_delay DESC NULLS LAST
    """, params)

    # Impact by wind speed
    wind_where = where.replace("w.conditions IS NOT NULL", "w.wind_speed IS NOT NULL")
    by_wind = fetch_all(f"""
        SELECT
            CASE
                WHEN w.wind_speed < 5 THEN 'Calm (<5 mph)'
                WHEN w.wind_speed < 15 THEN 'Light (5-15 mph)'
                WHEN w.wind_speed < 25 THEN 'Moderate (15-25 mph)'
                WHEN w.wind_speed < 35 THEN 'Strong (25-35 mph)'
                ELSE 'Severe (35+ mph)'
            END as wind_range,
            COUNT(*) as total_flights,
            ROUND(AVG(CASE WHEN f.arr_delay IS NOT NULL THEN f.arr_delay END)::numeric, 1) as avg_delay,
            ROUND(100.0 * SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) / COUNT(*), 2) as cancel_rate
        FROM flights f {WEATHER_JOIN}
        WHERE {wind_where}
        GROUP BY 1 ORDER BY avg_delay DESC NULLS LAST
    """, params)

    return _to_json({
        "by_condition": by_condition,
        "by_temperature": by_temp,
        "by_visibility": by_vis,
        "by_wind": by_wind,
    })


# ─────────────────────────────────────────────────────────────
# Tool 6: Origin vs Destination Weather Comparison
# ─────────────────────────────────────────────────────────────

@tool
def get_weather_origin_vs_dest() -> str:
    """Compare flight performance when bad weather is at origin vs destination vs both (uses visibility < 3 miles as threshold)."""
    # Double weather join: once for origin airport, once for destination
    rows = fetch_all("""
        SELECT
            CASE
                WHEN wo.visibility < 3 AND wd.visibility < 3 THEN 'Bad at BOTH'
                WHEN wo.visibility < 3 THEN 'Bad at ORIGIN only'
                WHEN wd.visibility < 3 THEN 'Bad at DESTINATION only'
                ELSE 'Good at BOTH'
            END as situation,
            COUNT(*) as total_flights,
            SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) as cancelled,
            ROUND(AVG(CASE WHEN f.arr_delay IS NOT NULL THEN f.arr_delay END)::numeric, 1) as avg_delay
        FROM flights f
        JOIN weather_hourly wo
            ON f.origin_airport = wo.airport_code
            AND f.flight_date = wo.observation_date
            AND f.dep_hour = wo.hour
        JOIN weather_hourly wd
            ON f.dest_airport = wd.airport_code
            AND f.flight_date = wd.observation_date
            AND f.dep_hour = wd.hour
        GROUP BY 1
        ORDER BY avg_delay DESC NULLS LAST
    """)
    return _to_json(rows)


# ─────────────────────────────────────────────────────────────
# Tool 7: Weather Cascade Events (storm tracing)
# ─────────────────────────────────────────────────────────────

@tool
def get_cascade_events(event_date: str | None = None) -> str:
    """Get weather cascade events — worst weather disruption days. If event_date given (YYYY-MM-DD), returns per-airport impact and hourly weather for that date. Otherwise returns top 15 worst weather days."""

    # Specific date: drill down into which airports were hit and what the weather looked like
    if event_date:
        airports = fetch_all("""
            SELECT f.origin_airport as airport_code,
                COUNT(*) as total_flights,
                SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) as cancelled,
                SUM(CASE WHEN f.cancelled AND f.cancellation_code = 'B' THEN 1 ELSE 0 END) as weather_cancelled,
                ROUND(100.0 * SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) / COUNT(*), 1) as cancel_pct,
                ROUND(AVG(CASE WHEN f.arr_delay IS NOT NULL THEN f.arr_delay END)::numeric, 1) as avg_delay
            FROM flights f
            WHERE f.flight_date = %(date)s
            GROUP BY f.origin_airport
            HAVING SUM(CASE WHEN f.cancelled AND f.cancellation_code = 'B' THEN 1 ELSE 0 END) >= 5
            ORDER BY weather_cancelled DESC
            LIMIT 15
        """, {"date": event_date})

        # Fetch hour-by-hour weather for the affected airports
        airport_codes = [a["airport_code"] for a in airports]
        hourly = []
        if airport_codes:
            hourly = fetch_all("""
                SELECT airport_code, observation_hour::text as hour,
                    temperature::float as temperature, wind_speed::float as wind_speed,
                    visibility::float as visibility, precipitation::float as precipitation,
                    conditions
                FROM weather_hourly
                WHERE observation_date = %(date)s AND airport_code = ANY(%(codes)s)
                ORDER BY airport_code, observation_hour
            """, {"date": event_date, "codes": airport_codes})

        return _to_json({"flight_date": event_date, "airports": airports, "hourly_weather": hourly}, max_items=50)

    # No date: return the worst weather disruption days ranked by cancellations
    rows = fetch_all("""
        SELECT flight_date::text as flight_date,
            SUM(CASE WHEN cancelled AND cancellation_code = 'B' THEN 1 ELSE 0 END) as weather_cancellations,
            COUNT(*) as total_flights,
            ROUND(AVG(CASE WHEN arr_delay IS NOT NULL THEN arr_delay END)::numeric, 1) as avg_delay
        FROM flights
        GROUP BY flight_date
        HAVING SUM(CASE WHEN cancelled AND cancellation_code = 'B' THEN 1 ELSE 0 END) > 0
        ORDER BY weather_cancellations DESC
        LIMIT 15
    """)
    return _to_json(rows)


# ─────────────────────────────────────────────────────────────
# Tool 8: Airport Information
# ─────────────────────────────────────────────────────────────

@tool
def get_airport_info(airport_code: str | None = None, search: str | None = None) -> str:
    """Get airport information. If airport_code given (e.g. 'JFK'), returns detailed profile. If search given, searches by code/name/city. If neither, returns top 15 airports by flight count."""

    # Specific airport: full profile with stats and top carriers
    if airport_code:
        code = airport_code.upper()
        airport = fetch_one("SELECT * FROM airports WHERE airport_code = %(code)s", {"code": code})
        if not airport:
            return json.dumps({"error": f"Airport '{code}' not found"})

        stats = fetch_one("""
            SELECT
                SUM(CASE WHEN origin_airport = %(code)s THEN 1 ELSE 0 END) as departures,
                SUM(CASE WHEN dest_airport = %(code)s THEN 1 ELSE 0 END) as arrivals,
                ROUND(AVG(CASE WHEN origin_airport = %(code)s AND dep_delay IS NOT NULL THEN dep_delay END)::numeric, 1) as avg_dep_delay,
                ROUND(AVG(CASE WHEN dest_airport = %(code)s AND arr_delay IS NOT NULL THEN arr_delay END)::numeric, 1) as avg_arr_delay,
                ROUND(100.0 * SUM(CASE WHEN cancelled AND (origin_airport = %(code)s OR dest_airport = %(code)s) THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN origin_airport = %(code)s OR dest_airport = %(code)s THEN 1 ELSE 0 END), 0), 2) as cancel_rate
            FROM flights
            WHERE origin_airport = %(code)s OR dest_airport = %(code)s
        """, {"code": code})

        top_carriers = fetch_all("""
            SELECT f.carrier_code, c.carrier_name, COUNT(*) as flights
            FROM flights f
            JOIN carriers c ON f.carrier_code = c.carrier_code
            WHERE f.origin_airport = %(code)s
            GROUP BY f.carrier_code, c.carrier_name
            ORDER BY flights DESC LIMIT 5
        """, {"code": code})

        return _to_json({
            "airport": airport,
            "stats": stats,
            "top_carriers": top_carriers,
        })

    # Search or list: find airports by name/code/city, or return top 15
    conditions = ["1=1"]
    params: dict = {}
    if search:
        conditions.append("(a.airport_code ILIKE %(search)s OR a.airport_name ILIKE %(like)s OR a.city ILIKE %(like)s)")
        params["search"] = search.upper()
        params["like"] = f"%{search}%"

    where = " AND ".join(conditions)
    rows = fetch_all(f"""
        SELECT a.airport_code, a.airport_name, a.city, a.state,
            COALESCE(fc.flight_count, 0) as flight_count
        FROM airports a
        LEFT JOIN (
            SELECT origin_airport, COUNT(*) as flight_count
            FROM flights GROUP BY origin_airport
        ) fc ON a.airport_code = fc.origin_airport
        WHERE {where}
        ORDER BY flight_count DESC
        LIMIT 15
    """, params)
    return _to_json(rows)


# ─────────────────────────────────────────────────────────────
# Tool 9: System Health / Data Inventory
# ─────────────────────────────────────────────────────────────

@tool
def get_system_health() -> str:
    """Get system health info: database status, table row counts, and data date range. Use this to understand what data is available."""
    tables = {}
    for table in ["flights", "carriers", "airports", "date_dim", "weather_observations", "weather_hourly"]:
        r = fetch_one(f"SELECT COUNT(*) as cnt FROM {table}")
        tables[table] = r["cnt"] if r else 0

    dr = fetch_one("SELECT MIN(flight_date) as min_date, MAX(flight_date) as max_date FROM flights")
    date_range = {"min_date": str(dr["min_date"]), "max_date": str(dr["max_date"])} if dr else {}

    return _to_json({"tables": tables, "date_range": date_range})


# ─────────────────────────────────────────────────────────────
# Tool registry — passed to LangGraph and bound to the LLM
# ─────────────────────────────────────────────────────────────

ALL_TOOLS = [
    get_carrier_performance,
    get_carrier_details,
    get_delay_patterns,
    get_route_info,
    get_weather_impact,
    get_weather_origin_vs_dest,
    get_cascade_events,
    get_airport_info,
    get_system_health,
]
