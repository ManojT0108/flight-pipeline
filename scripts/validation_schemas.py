"""
Pandera validation schemas.
These run BEFORE data hits the database — catches bad data early.
Each schema defines expected types, ranges, and constraints.
"""
import pandera as pa
from pandera import Column, Check, DataFrameSchema


# =============================================
# FLIGHTS VALIDATION SCHEMA
# =============================================
flights_schema = DataFrameSchema(
    columns={
        # Required fields — must exist and not be null
        "FlightDate": Column(str, nullable=False, 
            description="Flight date in YYYY-MM-DD format"),
        
        "Reporting_Airline": Column(str, nullable=False,
            checks=Check.str_length(min_value=2, max_value=10),
            description="Carrier code like AA, UA, DL"),
        
        "Flight_Number_Reporting_Airline": Column(float, nullable=True,
            checks=Check.greater_than(0),
            description="Flight number, positive integer"),
        
        "Origin": Column(str, nullable=False,
            checks=Check.str_length(min_value=3, max_value=5),
            description="Origin IATA airport code"),
        
        "Dest": Column(str, nullable=False,
            checks=Check.str_length(min_value=3, max_value=5),
            description="Destination IATA airport code"),
        
        # Delay fields — can be null (cancelled flights)
        # Thresholds match quality_checks in DAG (-150 to 5000)
        "DepDelay": Column(float, nullable=True,
            checks=[
                Check.greater_than_or_equal_to(-150),   # Allow 2.5 hours early
                Check.less_than_or_equal_to(5000),      # Cap at ~83 hours (multi-day rebookings)
            ],
            description="Departure delay in minutes, negative = early"),

        "ArrDelay": Column(float, nullable=True,
            checks=[
                Check.greater_than_or_equal_to(-150),
                Check.less_than_or_equal_to(5000),
            ],
            description="Arrival delay in minutes, negative = early"),
        
        # Boolean indicators
        "Cancelled": Column(float, nullable=False,
            checks=Check.isin([0.0, 1.0]),
            description="1 = cancelled, 0 = operated"),
        
        "Diverted": Column(float, nullable=False,
            checks=Check.isin([0.0, 1.0]),
            description="1 = diverted, 0 = normal"),
        
        # Distance — must be positive
        "Distance": Column(float, nullable=True,
            checks=Check.greater_than(0),
            description="Distance between airports in miles"),
        
        # Air time
        "AirTime": Column(float, nullable=True,
            checks=[
                Check.greater_than(0),
                Check.less_than(1500),  # Longest US domestic ~10 hours
            ],
            description="Flight time in minutes"),
    },
    # Don't fail on extra columns — BTS has many we don't use
    strict=False,
    # Coerce types where possible instead of failing
    coerce=True,
)


# =============================================
# AIRPORTS VALIDATION SCHEMA
# =============================================
# airports.dat has no headers, so validation happens after
# we assign column names during loading
airports_schema = DataFrameSchema(
    columns={
        "airport_code": Column(str, nullable=True,
            description="IATA code — some airports don't have one"),
        
        "airport_name": Column(str, nullable=False,
            description="Full airport name"),
        
        "city": Column(str, nullable=True),
        
        "country": Column(str, nullable=False),
        
        "latitude": Column(float, nullable=False,
            checks=[
                Check.greater_than_or_equal_to(-90),
                Check.less_than_or_equal_to(90),
            ]),
        
        "longitude": Column(float, nullable=False,
            checks=[
                Check.greater_than_or_equal_to(-180),
                Check.less_than_or_equal_to(180),
            ]),
    },
    strict=False,
    coerce=True,
)


# =============================================
# WEATHER OBSERVATIONS VALIDATION SCHEMA
# =============================================
weather_schema = DataFrameSchema(
    columns={
        "airport_code": Column(str, nullable=False,
            checks=Check.str_length(min_value=3, max_value=5),
            description="IATA airport code"),

        "observation_date": Column(str, nullable=False,
            description="Observation date in YYYY-MM-DD format"),

        "observation_time": Column(str, nullable=False,
            description="Observation timestamp in YYYY-MM-DD HH:MM format"),
        
        "avg_temperature": Column(float, nullable=True,
            checks=[
                Check.greater_than_or_equal_to(-60),  # Fahrenheit
                Check.less_than_or_equal_to(130),
            ],
            description="Average temperature in Fahrenheit"),

        "max_temperature": Column(float, nullable=True,
            checks=[
                Check.greater_than_or_equal_to(-60),
                Check.less_than_or_equal_to(135),
            ]),

        "min_temperature": Column(float, nullable=True,
            checks=[
                Check.greater_than_or_equal_to(-70),
                Check.less_than_or_equal_to(125),
            ]),

        "avg_wind_speed": Column(float, nullable=True,
            checks=[
                Check.greater_than_or_equal_to(0),
                Check.less_than_or_equal_to(200),  # mph
            ],
            description="Average wind speed in mph"),

        "max_wind_speed": Column(float, nullable=True,
            checks=[
                Check.greater_than_or_equal_to(0),
                Check.less_than_or_equal_to(300),
            ]),

        "avg_visibility": Column(float, nullable=True,
            checks=[
                Check.greater_than_or_equal_to(0),
                Check.less_than_or_equal_to(50),  # miles
            ],
            description="Average visibility in miles"),

        "precipitation": Column(float, nullable=True,
            checks=Check.greater_than_or_equal_to(0),
            description="Precipitation in inches"),

        "snow_depth": Column(float, nullable=True,
            checks=Check.greater_than_or_equal_to(0),
            description="Snow depth in inches"),

        "conditions": Column(str, nullable=True,
            description="Weather conditions (Clear, Rain, Snow, etc.)"),
    },
    strict=False,
    coerce=True,
)
