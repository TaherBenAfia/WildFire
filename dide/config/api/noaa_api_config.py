# import os
# from dotenv import load_dotenv

# load_dotenv()

# NOAA_TOKEN    = os.getenv("NOAA_TOKEN")
# NOAA_BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"

# NOAA_REQUEST = {
#     "timeout": 30,
#     "headers": {"token": NOAA_TOKEN},
# }

# NOAA_PARAMS = {
#     "datasetid":  "GHCND",
#     "datatypeid": "TMAX,TMIN,PRCP,AWND",
#     "units":      "metric",
#     "limit":      100,
#     "stationid":  os.getenv("NOAA_STATION_ID", "GHCND:USW00094728"),
# }
NOAA_FETCH_INTERVAL_MINUTES = 30

# NOAA_FIELDS = [
#     "station", "date", "datatype",
#     "value", "attributes",
# ]