"""
Weather Data Helper Module
==========================
Fetches REAL weather data from Iowa Environmental Mesonet (IEM).

IEM provides free access to ASOS/AWOS airport weather station data.
No registration required. Data goes back decades.

Station IDs = 'K' + airport IATA code (e.g., ATL → KATL)
"""
import urllib.request
import csv
import io
from datetime import datetime, timedelta


# =============================================
# AIRPORT TO WEATHER STATION MAPPING
# =============================================
# Major US airports with their ASOS station IDs.
# Station ID = 'K' + IATA code for most US airports.

AIRPORT_STATION_MAP = {
    # Top 30 US airports by traffic
    'ATL': 'KATL',  # Atlanta Hartsfield-Jackson
    'DFW': 'KDFW',  # Dallas/Fort Worth
    'DEN': 'KDEN',  # Denver International
    'ORD': 'KORD',  # Chicago O'Hare
    'LAX': 'KLAX',  # Los Angeles
    'JFK': 'KJFK',  # New York JFK
    'LAS': 'KLAS',  # Las Vegas
    'MCO': 'KMCO',  # Orlando
    'MIA': 'KMIA',  # Miami
    'CLT': 'KCLT',  # Charlotte
    'SEA': 'KSEA',  # Seattle-Tacoma
    'PHX': 'KPHX',  # Phoenix
    'EWR': 'KEWR',  # Newark
    'SFO': 'KSFO',  # San Francisco
    'IAH': 'KIAH',  # Houston Intercontinental
    'BOS': 'KBOS',  # Boston Logan
    'FLL': 'KFLL',  # Fort Lauderdale
    'MSP': 'KMSP',  # Minneapolis
    'LGA': 'KLGA',  # New York LaGuardia
    'DTW': 'KDTW',  # Detroit
    'PHL': 'KPHL',  # Philadelphia
    'SLC': 'KSLC',  # Salt Lake City
    'DCA': 'KDCA',  # Washington Reagan
    'SAN': 'KSAN',  # San Diego
    'BWI': 'KBWI',  # Baltimore
    'TPA': 'KTPA',  # Tampa
    'AUS': 'KAUS',  # Austin
    'IAD': 'KIAD',  # Washington Dulles
    'BNA': 'KBNA',  # Nashville
    'MDW': 'KMDW',  # Chicago Midway
}


def get_mapped_airports():
    """Return list of airports that have weather station mappings."""
    return list(AIRPORT_STATION_MAP.keys())


def fetch_weather_from_iem(station_id, start_date, end_date):
    """
    Fetch daily weather summary from Iowa Environmental Mesonet.

    IEM provides free ASOS/AWOS data for airport weather stations.
    URL format documented at: https://mesonet.agron.iastate.edu/request/download.phtml

    Args:
        station_id: ASOS station ID (e.g., 'KATL')
        start_date: Start date (datetime or string 'YYYY-MM-DD')
        end_date: End date (datetime or string 'YYYY-MM-DD')

    Returns:
        List of daily weather observation dicts
    """
    # Parse dates if strings
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # IEM daily summary URL
    # Data includes: max/min temp, precipitation, snow, wind
    url = (
      f"https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
      f"station={station_id}"
      f"&data=tmpf&data=dwpf&data=relh&data=sknt&data=vsby&data=p01i"
      f"&year1={start_date.year}&month1={start_date.month}&day1={start_date.day}"
      f"&year2={end_date.year}&month2={end_date.month}&day2={end_date.day}"
      f"&tz=UTC&format=onlycomma&latlon=no&elev=no&missing=M&trace=T"
  )

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = response.read().decode('utf-8')

        # Parse CSV response
        reader = csv.DictReader(io.StringIO(data))
        observations = []

        for row in reader:
            # IEM returns 'M' for missing values
            def safe_float(val):
                if val is None or val == '' or val == 'M' or val == 'T':
                    return None
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return None

            valid_time = row.get('valid', '')
            if not valid_time:
                continue        
            
            obs_date = valid_time.split(' ')[0] if ' ' in valid_time else valid_time
            
            obs = {
                'station_id': station_id,
                'observation_date': obs_date,
                'observation_time': valid_time,
                'avg_temperature': safe_float(row.get('tmpf')),
                'max_temperature': None,  # Not available in hourly
                'min_temperature': None,  # Not available in hourly
                'precipitation': safe_float(row.get('p01i')),
                'snow_depth': None,  # Not in ASOS hourly
                'avg_wind_speed': safe_float(row.get('sknt')),
                'max_wind_speed': None,  # Not available in hourly
                'avg_visibility': safe_float(row.get('vsby')),
                'humidity': safe_float(row.get('relh')),
                'dew_point': safe_float(row.get('dwpf')),
            }

            # Convert wind from knots to mph (1 knot = 1.15078 mph)
            if obs['avg_wind_speed'] is not None:
                obs['avg_wind_speed'] = round(obs['avg_wind_speed'] * 1.15078, 1)
            if obs['max_wind_speed'] is not None:
                obs['max_wind_speed'] = round(obs['max_wind_speed'] * 1.15078, 1)

            # Determine conditions based on weather data
            obs['conditions'] = determine_conditions(obs)

            observations.append(obs)

        return observations

    except Exception as e:
        print(f"Error fetching weather for {station_id}: {e}")
        return []


def determine_conditions(obs):
    """
    Determine weather condition string from observation data.
    Simple heuristic based on precipitation and temperature.
    """
    precip = obs.get('precipitation') or 0
    temp = obs.get('avg_temperature')
    visibility = obs.get('avg_visibility') or 10

    # Snow: precipitation when temp is below freezing
    if precip > 0 and temp is not None and temp < 32:
        return 'Snow'
    elif precip > 0.1:
        return 'Rain'
    elif precip > 0:
        return 'Light Rain'
    elif visibility < 3:
        return 'Fog/Low Visibility'
    elif temp is not None and temp < 32:
        return 'Cold/Clear'
    else:
        return 'Clear'


def fetch_weather_for_airports(airports, start_date, end_date):
    """
    Fetch real weather data for multiple airports from IEM.

    Args:
        airports: List of airport codes (e.g., ['ATL', 'JFK', ...])
        start_date: Start date string 'YYYY-MM-DD'
        end_date: End date string 'YYYY-MM-DD'

    Returns:
        List of weather observation dicts with airport_code added
    """
    all_observations = []

    for airport in airports:
        station_id = AIRPORT_STATION_MAP.get(airport)
        if not station_id:
            print(f"  Skipping {airport} - no station mapping")
            continue

        print(f"  Fetching weather for {airport} ({station_id})...")

        obs_list = fetch_weather_from_iem(station_id, start_date, end_date)

        # Add airport_code to each observation
        for obs in obs_list:
            obs['airport_code'] = airport

        all_observations.extend(obs_list)
        print(f"    Got {len(obs_list)} observations")

    print(f"\nTotal observations fetched: {len(all_observations)}")
    return all_observations


def generate_weather_data(airports, dates):
    """
    Main entry point - fetches REAL weather data from IEM.

    This replaces the old fake data generator.

    Args:
        airports: List of airport codes
        dates: List of date objects or strings

    Returns:
        List of weather observation dicts
    """
    if not dates:
        print("No dates provided!")
        return []

    # Convert dates to strings and find range
    date_strings = []
    for d in dates:
        if hasattr(d, 'strftime'):
            date_strings.append(d.strftime('%Y-%m-%d'))
        else:
            date_strings.append(str(d))

    date_strings.sort()
    start_date = date_strings[0]
    end_date = date_strings[-1]

    print(f"Fetching REAL weather data from IEM")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Airports to fetch: {len(airports)}")

    # Only fetch for airports we have mappings for
    valid_airports = [a for a in airports if a in AIRPORT_STATION_MAP]
    print(f"Airports with station mappings: {len(valid_airports)}")

    # Fetch from IEM
    observations = fetch_weather_for_airports(valid_airports, start_date, end_date)

    # Filter to only dates we need (in case IEM returns extra)
    date_set = set(date_strings)
    filtered = [obs for obs in observations if obs['observation_date'] in date_set]

    print(f"Observations matching our dates: {len(filtered)}")
    return filtered
