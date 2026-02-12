"""
Database connection helper.
All tasks import this instead of building their own connection strings.
Uses environment variables set in docker-compose.yml.
"""
import os
import psycopg2
from psycopg2.extras import execute_values, execute_batch
from contextlib import contextmanager


def get_db_config():
    """Read DB config from environment variables."""
    return {
        'host': os.environ['FLIGHTS_DB_HOST'],
        'port': int(os.environ['FLIGHTS_DB_PORT']),
        'dbname': os.environ['FLIGHTS_DB_NAME'],
        'user': os.environ['FLIGHTS_DB_USER'],
        'password': os.environ['FLIGHTS_DB_PASSWORD'],
    }


@contextmanager
def get_connection():
    """
    Context manager for database connections.
    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    Auto-commits on success, rolls back on error, always closes.
    """
    conn = psycopg2.connect(**get_db_config())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def bulk_insert(table, columns, rows, conflict_column=None, conflict_action='DO NOTHING'):
    """
    Bulk insert using execute_values (fast batch insert).
    
    Args:
        table: Target table name
        columns: List of column names
        rows: List of tuples (data rows)
        conflict_column: Column for ON CONFLICT (enables upsert/idempotency)
        conflict_action: What to do on conflict (default: skip duplicates)
    
    Returns:
        Number of rows affected
    """
    if not rows:
        return 0
    
    cols = ', '.join(columns)
    
    if conflict_column:
        query = f"""
            INSERT INTO {table} ({cols})
            VALUES %s
            ON CONFLICT ({conflict_column}) {conflict_action}
        """
    else:
        query = f"INSERT INTO {table} ({cols}) VALUES %s"
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, rows, page_size=5000)
            return cur.rowcount

def ensure_dates_exist(dates):
      """
      Ensure all given dates exist in date_dim table.
      Takes a set of date strings ('YYYY-MM-DD') or date objects.
      Adds any missing dates. Skips dates that already exist.

      This is the SINGLE SOURCE OF TRUTH for date generation.
      Used by: generate_date_dim task, load_flights task, any future task.
      """
      from datetime import datetime

      def get_season(month):
          if month in (12, 1, 2): return 'Winter'
          if month in (3, 4, 5): return 'Spring'
          if month in (6, 7, 8): return 'Summer'
          return 'Fall'

      insert_query = """
          INSERT INTO date_dim (date_id, year, quarter, month, day_of_month,
                                day_of_week, day_name, month_name, is_weekend, season)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          ON CONFLICT (date_id) DO NOTHING
      """

      added = 0
      with get_connection() as conn:
          with conn.cursor() as cur:
              for date_val in dates:
                  # Handle both string and date objects
                  if isinstance(date_val, str):
                      d = datetime.strptime(date_val, '%Y-%m-%d')
                  else:
                      d = date_val

                  cur.execute(insert_query, (
                      d.date() if hasattr(d, 'date') else d,
                      d.year,
                      (d.month - 1) // 3 + 1,
                      d.month,
                      d.day,
                      d.weekday(),
                      d.strftime('%A'),
                      d.strftime('%B'),
                      d.weekday() >= 5,
                      get_season(d.month),
                  ))
                  added += 1

      print(f"ensure_dates_exist: processed {added} dates")
      return added


# =============================================
# DATA CONVERSION HELPERS
# =============================================
# Use these when loading data from CSV to handle nulls,
# empty strings, and type conversion safely.

def safe_float(val):
    """Convert value to float, return None if can't."""
    import pandas as pd
    try:
        if pd.isna(val) or val == '':
            return None
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_int(val):
    """Convert value to int, return None if can't."""
    import pandas as pd
    try:
        if pd.isna(val) or val == '':
            return None
        return int(float(val))  # float first handles "123.0"
    except (ValueError, TypeError):
        return None


def safe_str(val):
    """Convert value to stripped string, return None if empty."""
    import pandas as pd
    if pd.isna(val) or val == '':
        return None
    return str(val).strip()


def safe_bool(val):
    """Convert 0/1 or 0.0/1.0 to False/True."""
    try:
        return bool(int(float(val)))
    except (ValueError, TypeError):
        return False