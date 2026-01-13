"""
Spatial utility functions for calculating pixel-catchment overlap areas.
"""

from typing import Dict, List, Tuple, Optional, Any, TYPE_CHECKING
import json
import logging
from pathlib import Path

# Shapely imports
try:
    from shapely import wkt as shapely_wkt
    from shapely.geometry import box, shape, Polygon
    from shapely.ops import transform as shapely_transform
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False
    if not TYPE_CHECKING:
        Polygon = Any  # Runtime fallback

# Pyproj imports
try:
    from pyproj import Transformer
    PYPROJ_AVAILABLE = True
except ImportError:
    PYPROJ_AVAILABLE = False

# Logger
logger = logging.getLogger(__name__)


# ============================================================================
# COORDINATE TRANSFORMATION
# ============================================================================

def transform_wgs84_to_nztm(geom_wgs84):
    """
    Transform geometry from WGS84 (EPSG:4326) to NZTM (EPSG:2193).
    
    Args:
        geom_wgs84: Shapely geometry in WGS84 coordinates
        
    Returns:
        Shapely geometry in NZTM coordinates
        
    Raises:
        ImportError: If pyproj or shapely not available
    """
    if not PYPROJ_AVAILABLE:
        raise ImportError(
            "pyproj is required for coordinate transformation. "
            "Install with: pip install pyproj"
        )
    
    if not SHAPELY_AVAILABLE:
        raise ImportError(
            "shapely is required for geometry operations. "
            "Install with: pip install shapely"
        )
    
    # Create transformer: WGS84 → NZTM
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:2193", always_xy=True)
    
    # Transform geometry
    geom_nztm = shapely_transform(transformer.transform, geom_wgs84)
    
    return geom_nztm


def create_pixel_geometry_from_index(
    pixel_index: int,
    grid_width: int = 512,
    grid_height: int = 512,
    pixel_size: float = 500.0,
    origin_x: float = 1626250.0,
    origin_y: float = 6108750.0
) -> Optional["Polygon"]:
    """
    Create pixel geometry from pixel index using Sam's grid specifications.
    
    Auckland Council Radar Grid Specifications (from Sam):
    - Grid size: 512 × 512 pixels
    - Pixel size: 500m × 500m
    - Top-left centroid: (1626250.0 E, 6108750.0 N) in NZTM
    - Coordinate system: NZTM (EPSG:2193)
    - Indexing: Row-major (left→right, top→bottom), starts at 0
    
    Args:
        pixel_index: Pixel index (0 to 262,143)
        grid_width: Grid width in pixels (default: 512)
        grid_height: Grid height in pixels (default: 512)
        pixel_size: Pixel size in meters (default: 500)
        origin_x: X coordinate of top-left centroid (NZTM Easting)
        origin_y: Y coordinate of top-left centroid (NZTM Northing)
    
    Returns:
        Shapely Polygon in NZTM coordinates, or None if invalid
    
    Technical Notes:
        - Pixel area: 500m × 500m = 250,000 m²
        - Total pixels: 512 × 512 = 262,144
        - Coverage: ~256 km × 256 km
        - Origin is at CENTROID, so pixel boundary extends ±250m from centroid
    
    Example:
        >>> # Pixel 0 (top-left corner)
        >>> geom = create_pixel_geometry_from_index(0)
        >>> geom.centroid.coords[0]  # (1626250.0, 6108750.0)
        >>> geom.bounds  # (1626000, 6108500, 1626500, 6109000)
        >>> geom.area  # 250000.0 m²
    """
    if not SHAPELY_AVAILABLE:
        logger.warning("Shapely not available")
        return None
    
    # Validate pixel index
    max_index = grid_width * grid_height - 1
    if not (0 <= pixel_index <= max_index):
        logger.warning(f"Invalid pixel index {pixel_index} (max: {max_index})")
        return None
    
    # Calculate row and column from pixel index
    # Row-major ordering: pixel_index = row * grid_width + col
    row = pixel_index // grid_width
    col = pixel_index % grid_width
    
    # Calculate centroid coordinates (NZTM)
    # Note: Y increases downward in pixel indexing, but upward in NZTM
    centroid_x = origin_x + (col * pixel_size)
    centroid_y = origin_y - (row * pixel_size)
    
    # Calculate bounds (centroid ± half pixel size)
    half_size = pixel_size / 2.0  # 250m
    min_x = centroid_x - half_size
    max_x = centroid_x + half_size
    min_y = centroid_y - half_size
    max_y = centroid_y + half_size
    
    # Create rectangular polygon (500m × 500m) in NZTM
    return box(min_x, min_y, max_x, max_y)


# ============================================================================
# SIMPLE WEIGHTING (EQUAL SPLIT)
# ============================================================================

def calculate_pixel_overlap_weights(
    catchments: List[dict],
    pixel_mappings: Dict[int, List[int]]
) -> Dict[Tuple[int, int], float]:
    """
    Calculate area-based weights for pixels that overlap multiple catchments.
    
    Args:
        catchments: List of catchment objects with geometry
        pixel_mappings: Dict mapping catchment_id -> list of pixel_indices
    
    Returns:
        Dict mapping (catchment_id, pixel_index) -> weight (0.0-1.0)
        
    Example:
        {
            (3880862, 115432): 0.6,  # Pixel 115432 is 60% in catchment 3880862
            (3880880, 115432): 0.4,  # Pixel 115432 is 40% in catchment 3880880
        }
    """
    
    # Find which pixels appear in multiple catchments
    pixel_to_catchments = {}
    for catchment_id, pixels in pixel_mappings.items():
        for pixel in pixels:
            if pixel not in pixel_to_catchments:
                pixel_to_catchments[pixel] = []
            pixel_to_catchments[pixel].append(catchment_id)
    
    # Identify overlapping pixels
    overlapping_pixels = {
        pixel: catchments_list
        for pixel, catchments_list in pixel_to_catchments.items()
        if len(catchments_list) > 1
    }
    
    # Calculate weights
    weights = {}
    
    # For non-overlapping pixels: weight = 1.0
    for pixel, catchments_list in pixel_to_catchments.items():
        if len(catchments_list) == 1:
            weights[(catchments_list[0], pixel)] = 1.0
    
    # For overlapping pixels: Calculate area-based weights
    # NOTE: Without actual geometry intersection, we use equal split
    # In production, this should use shapely or similar for real geometry
    for pixel, catchments_list in overlapping_pixels.items():
        # Simple equal split (placeholder)
        # TODO: Replace with actual geometric intersection calculation
        weight = 1.0 / len(catchments_list)
        
        for catchment_id in catchments_list:
            weights[(catchment_id, pixel)] = weight
    
    return weights


def estimate_pixel_area_weights_simple(
    catchments: List[dict],
    pixel_mappings: Dict[int, List[int]]
) -> Dict[Tuple[int, int], float]:
    """
    Simplified weight estimation: equal split for overlapping pixels.
    
    This is a conservative approach that assumes overlapping pixels are
    split equally among catchments. In production, use actual geometry
    intersection calculations.
    
    Args:
        catchments: List of catchment objects
        pixel_mappings: Dict mapping catchment_id -> list of pixel_indices
    
    Returns:
        Dict mapping (catchment_id, pixel_index) -> weight
    """
    
    # Build reverse mapping: pixel -> list of catchments
    pixel_to_catchments = {}
    for catchment_id, pixels in pixel_mappings.items():
        for pixel in pixels:
            if pixel not in pixel_to_catchments:
                pixel_to_catchments[pixel] = []
            pixel_to_catchments[pixel].append(catchment_id)
    
    # Calculate weights
    weights = {}
    for pixel, catchment_list in pixel_to_catchments.items():
        weight = 1.0 / len(catchment_list)
        for catchment_id in catchment_list:
            weights[(catchment_id, pixel)] = weight
    
    return weights


# ============================================================================
# GEOMETRIC WEIGHTING (CALCULATED FROM GRID SPECS)
# ============================================================================

def parse_pixel_geometry_from_api(metadata: Dict[str, Any]) -> Optional["Polygon"]:
    """
    Parse pixel geometry from API metadata response.
    
    NOTE: This function is kept for backward compatibility but is not used
    in the current implementation since the API does not return geometry data.
    
    Args:
        metadata: Pixel metadata dict from API
        
    Returns:
        Shapely Polygon or None if parsing fails
    """
    if not SHAPELY_AVAILABLE:
        logger.warning("Shapely not available")
        return None
    
    try:
        # Check if geometry is in WKT format
        if "geometryWkt" in metadata:
            return shapely_wkt.loads(metadata["geometryWkt"])
        
        # Check if geometry is in GeoJSON format
        if "geometry" in metadata:
            return shape(metadata["geometry"])
        
        # Check if bounds are provided (create box from bounds)
        if "bounds" in metadata:
            bounds = metadata["bounds"]
            # Assuming bounds format: {minX, minY, maxX, maxY}
            return box(
                bounds.get("minX"),
                bounds.get("minY"),
                bounds.get("maxX"),
                bounds.get("maxY")
            )
        
        logger.warning(f"No recognized geometry format in metadata: {metadata.keys()}")
        return None
        
    except Exception as e:
        logger.error(f"Failed to parse geometry from metadata: {e}")
        return None


def fetch_pixel_geometries_from_api(
    pixel_indices: List[int],
    client: Any,  # MoataClient (not used in calculated approach)
    collection_id: int = 1,
    batch_size: int = 150,
) -> Dict[int, "Polygon"]:
    """
    Create pixel geometries from pixel indices using Sam's grid specifications.
    
    NOTE: API metadata endpoint does not return geometry data, so we calculate
    geometries directly from pixel indices using the known grid parameters.
    
    Grid specifications from Sam:
    - Grid size: 512 × 512 pixels
    - Pixel size: 500m × 500m
    - Top-left centroid: (1626250.0 E, 6108750.0 N) NZTM
    - Coordinate system: NZTM (EPSG:2193)
    
    Args:
        pixel_indices: List of pixel indices to create geometries for
        client: MoataClient instance (not used)
        collection_id: TraceSet collection ID (not used)
        batch_size: Batch size (not used)
        
    Returns:
        Dictionary mapping pixel_index -> Polygon geometry (NZTM coordinates)
    """
    if not SHAPELY_AVAILABLE:
        logger.error("Shapely is required for geometric calculations")
        return {}
    
    logger.info(f"Calculating geometries for {len(pixel_indices)} pixels from grid specs...")
    
    # Calculate geometries from pixel indices using Sam's grid specifications
    pixel_geometries = {}
    for pixel_index in pixel_indices:
        geometry = create_pixel_geometry_from_index(pixel_index)
        if geometry:
            pixel_geometries[pixel_index] = geometry
    
    logger.info(f"✓ Created {len(pixel_geometries)} pixel geometries (NZTM coordinates)")
    
    return pixel_geometries


def calculate_geometric_pixel_weights_from_api(
    catchments: List[dict],
    pixel_mappings: Dict[int, List[int]],
    client: Any,  # MoataClient
    collection_id: int = 1,
) -> Dict[Tuple[int, int], float]:
    """
    Calculate geometric weights using pixel geometries calculated from grid specs.
    
    Coordinate Systems:
        - Catchments from API: WGS84 (EPSG:4326) ← Need to transform
        - Pixel geometries (calculated): NZTM (EPSG:2193) ← Already correct
        - Grid specs from Sam:
            * Size: 512×512 pixels
            * Pixel: 500m × 500m
            * Origin: (1626250.0 E, 6108750.0 N) NZTM
    
    Args:
        catchments: List of catchment dictionaries with geometryWkt (WGS84)
        pixel_mappings: Dict mapping catchment_id -> list of pixel_indices
        client: MoataClient instance (not used in current implementation)
        collection_id: TraceSet collection ID (default: 1, not used)
        
    Returns:
        Dict mapping (catchment_id, pixel_index) -> weight (0.0-1.0)
        
    Example:
        >>> from moata_pipeline.moata import create_client
        >>> client = create_client(...)
        >>> weights = calculate_geometric_pixel_weights_from_api(
        ...     catchments=catchments,
        ...     pixel_mappings=pixel_cache,
        ...     client=client
        ... )
    """
    if not SHAPELY_AVAILABLE:
        logger.error("Shapely not available - falling back to equal split")
        return estimate_pixel_area_weights_simple(catchments, pixel_mappings)
    
    # Get all unique pixels
    all_pixels = set()
    for pixels in pixel_mappings.values():
        all_pixels.update(pixels)
    
    logger.info(f"Calculating geometric weights for {len(all_pixels)} unique pixels")
    
    # Create pixel geometries from grid specifications (no API call needed)
    pixel_geometries = fetch_pixel_geometries_from_api(
        pixel_indices=list(all_pixels),
        client=client,
        collection_id=collection_id
    )
    
    if not pixel_geometries:
        logger.error("Failed to create pixel geometries - falling back to equal split")
        return estimate_pixel_area_weights_simple(catchments, pixel_mappings)
    
    # Transform catchment geometries to NZTM
    catchment_geometries = {}
    for catchment in catchments:
        catchment_id = catchment.get("id")
        wkt_str = catchment.get("geometryWkt")
        
        if not catchment_id or not wkt_str:
            continue
        
        try:
            # Parse WGS84 geometry
            geom_wgs84 = shapely_wkt.loads(wkt_str)
            
            # Transform to NZTM (pixel geometries are in NZTM)
            geom_nztm = transform_wgs84_to_nztm(geom_wgs84)
            catchment_geometries[catchment_id] = geom_nztm
            
        except Exception as e:
            logger.warning(f"Failed to parse geometry for catchment {catchment_id}: {e}")
            continue
    
    # Calculate intersection weights
    weights = {}
    
    for catchment_id, pixel_list in pixel_mappings.items():
        catchment_geom = catchment_geometries.get(catchment_id)
        if not catchment_geom:
            # Fallback to equal split for this catchment
            for pixel_index in pixel_list:
                weights[(catchment_id, pixel_index)] = 1.0 / len(pixel_list)
            continue
        
        for pixel_index in pixel_list:
            pixel_geom = pixel_geometries.get(pixel_index)
            if not pixel_geom:
                # Fallback to equal split for this pixel
                weights[(catchment_id, pixel_index)] = 1.0 / len(pixel_list)
                continue
            
            try:
                # Calculate intersection area
                intersection = catchment_geom.intersection(pixel_geom)
                intersection_area = intersection.area
                pixel_area = pixel_geom.area
                
                if pixel_area > 0:
                    weight = intersection_area / pixel_area
                    weights[(catchment_id, pixel_index)] = weight
                else:
                    weights[(catchment_id, pixel_index)] = 0.0
                    
            except Exception as e:
                logger.warning(
                    f"Failed to calculate intersection for catchment {catchment_id}, "
                    f"pixel {pixel_index}: {e}"
                )
                # Fallback
                weights[(catchment_id, pixel_index)] = 1.0 / len(pixel_list)
    
    return weights


# ============================================================================
# FILE I/O
# ============================================================================

def save_pixel_weights(weights: Dict[Tuple[int, int], float], output_path: Path):
    """
    Save pixel weights to JSON file for inspection.
    
    Args:
        weights: Dict mapping (catchment_id, pixel_index) -> weight
        output_path: Path to save JSON file
    """
    
    # Convert tuple keys to string for JSON serialization
    weights_json = {
        f"{catchment_id}_{pixel}": weight
        for (catchment_id, pixel), weight in weights.items()
    }
    
    with open(output_path, 'w') as f:
        json.dump(weights_json, f, indent=2)


def load_pixel_weights(weights_path: Path) -> Dict[Tuple[int, int], float]:
    """
    Load pixel weights from JSON file.
    
    Args:
        weights_path: Path to weights JSON file
    
    Returns:
        Dict mapping (catchment_id, pixel_index) -> weight
    """
    
    with open(weights_path, 'r') as f:
        weights_json = json.load(f)
    
    # Convert string keys back to tuples
    weights = {}
    for key, weight in weights_json.items():
        catchment_id, pixel = key.split('_')
        weights[(int(catchment_id), int(pixel))] = weight
    
    return weights