"""
Endpoints for Rain Radar (QPE) data
"""

# Radar data endpoints
TRACESET_COLLECTION_DATA = "trace-set-collections/{collection_id}/trace-sets/data"
TRACESET_PIXEL_MAPPINGS = "trace-set-collections/{collection_id}/pixel-mappings/intersects-geometry"

# Constants from Sam's email
RADAR_TRACESET_COLLECTION_ID = 1
RADAR_TRACESET_ID = 3
STORMWATER_ASSET_TYPE_ID = 3541