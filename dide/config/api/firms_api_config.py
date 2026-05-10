import os
from dotenv import load_dotenv

load_dotenv()

FIRMS_API_KEY   = os.getenv("FIRMS_API_KEY")
FIRMS_BASE_URL  = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
FIRMS_SOURCE    = "VIIRS_SNPP_NRT"
FIRMS_AREA      = "world"
FIRMS_DAY_RANGE = 1   # last N days of detections

FIRMS_URL = f"{FIRMS_BASE_URL}/{FIRMS_API_KEY}/{FIRMS_SOURCE}/{FIRMS_AREA}/{FIRMS_DAY_RANGE}"

FIRMS_REQUEST = {
    "timeout": 30,
    "headers": {},   # key is embedded in URL for FIRMS
}

FIRMS_FETCH_INTERVAL_MINUTES = 10

FIRMS_FIELDS = [
    "latitude", "longitude", "brightness",
    "frp", "confidence", "acq_date",
    "acq_time", "satellite", "instrument",
    "daynight",
]