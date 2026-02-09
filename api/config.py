"""
Configuration settings for the Solar Parks API
"""

import os
from pathlib import Path
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings"""

    # App settings
    APP_NAME: str = "Solar Parks Analysis API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Authentication
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours

    # Default users (for demo purposes - in production use a database)
    DEFAULT_USERNAME: str = "admin"
    DEFAULT_PASSWORD: str = "admin"

    # PostgreSQL settings
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "solar_parks")

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    # File storage
    BASE_DIR: Path = Path(__file__).parent.parent
    DATA_DIR: Path = Path(os.getenv("DATA_DIR", str(Path(__file__).parent.parent / "data")))

    # Geospatial settings
    INDIA_PROJECTED_CRS: int = int(os.getenv("INDIA_PROJECTED_CRS", "24378"))  # India projected CRS for area calculations

    # Processing defaults
    MAX_DISTANCE_CONSIDERED: int = 500  # meters for distance matrix
    DEFAULT_CLUSTERING_DISTANCE: int = 10  # meters for DBSCAN
    DEFAULT_MIN_SAMPLES: int = 2  # minimum samples for DBSCAN

    # Building/Settlement detection
    BUILDING_BUFFER: int = 10  # meters
    SETTLEMENT_EPS: int = 50  # meters for settlement clustering
    SETTLEMENT_MIN_BUILDINGS: int = 5

    # NASA Earthdata credentials for DEM download
    EARTHDATA_USERNAME: str = os.getenv("EARTHDATA_USERNAME", "")
    EARTHDATA_PASSWORD: str = os.getenv("EARTHDATA_PASSWORD", "")

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

# Layer configuration - defines what layers are available and their parameters
AVAILABLE_LAYERS = {
    "buildings": {
        "name": "Buildings",
        "description": "Building footprints with buffer zones",
        "required": False,
        "parameters": {
            "buffer_distance": {
                "type": "int",
                "default": 10,
                "min": 0,
                "max": 50,
                "description": "Buffer distance around buildings in meters",
            }
        },
    },
    "settlements": {
        "name": "Settlements",
        "description": "Clustered building areas forming settlements",
        "required": False,
        "parameters": {
            "clustering_eps": {
                "type": "int",
                "default": 50,
                "min": 10,
                "max": 500,
                "description": "DBSCAN epsilon (max distance between buildings in settlement)",
            },
            "min_buildings": {
                "type": "int",
                "default": 5,
                "min": 2,
                "max": 50,
                "description": "Minimum number of buildings to form a settlement",
            },
        },
    },
    "cropland": {
        "name": "Cropland",
        "description": "Agricultural cropland areas from landcover data",
        "required": False,
        "parameters": {},
    },
    "water": {
        "name": "Water",
        "description": "Open surface water bodies from landcover data",
        "required": False,
        "parameters": {},
    },
    "slopes": {
        "name": "Slopes",
        "description": "Steep slopes from NASA ALOS PALSAR DEM data",
        "required": False,
        "parameters": {
            "include_north_slopes": {
                "type": "bool",
                "default": True,
                "description": "Include north-facing slopes (45-135° aspect, >7° angle)",
            },
            "include_other_slopes": {
                "type": "bool",
                "default": True,
                "description": "Include other-facing slopes (>10° angle)",
            },
            "north_min_angle": {
                "type": "float",
                "default": 7.0,
                "min": 0,
                "max": 90,
                "description": "Minimum slope angle for north-facing slopes (degrees)",
            },
            "other_min_angle": {
                "type": "float",
                "default": 10.0,
                "min": 0,
                "max": 90,
                "description": "Minimum slope angle for other-facing slopes (degrees)",
            },
        },
    },
}

# Export formats available
EXPORT_FORMATS = ["geojson", "kml", "shapefile", "parquet", "csv", "excel"]
