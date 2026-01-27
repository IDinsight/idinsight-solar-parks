# Solar Parks Analysis API

## Dev TO-DO List

DONE:

- Project management page
- Khasra upload
- Settlement layer
- Basic clustering (untested)
- Export and stat calculations
- Add settlement layer
- Export stats excel as expected
- Make layers show up online
- Test clustering
- Export a coloured in KML, I dare you. DONE!
- Added full screen map page
- Fixed: distance_matrix issue after deleting khasras persists...
- clarify colour scheme of map items throughout
- add crops and water
- BUG: Fix stats showing on hover on the map
- BUG: refresh login issue
- make maps take up whole right side and not refresh with every page change
- add slopes WIP

TO DO:

- test and fix slopes
- khasra tooltip before layers etc shows N/A - should show original area.
- BUG: FIX DISTANCE MATRIX ON NEW PROJECT etc. First attempt is always wrong. Better after clearing the first distance matrix but still DBScan seems to be stochastic??
- add per-layer areas to excel output

- use alembic for migrations
- also save usable khasra shape

Later:

- First add buildings, then add settlements. Show all buildings on map always.
- Give ability to turn layers on and off online map
- show inter-khasra distance histogram to help with threshold selection. give suggested threshold.
- clustering based on usable shapes?
- thin-area filtering?
- flood risk?
- host on AWS
- user management with permissions
- khasra api integration

## About

A FastAPI application for analyzing land parcels (khasras) for solar park development in India.

## Architecture

The API uses a **database-first** approach:

- **PostgreSQL/PostGIS** stores all project data including geometries (khasras, layers, parcels)
- **Local file storage** is only used for the distance matrix cache (expensive to compute)
- GeoDataFrames are constructed on-the-fly from database records when needed

## Features

- **Upload khasra boundaries** from KML or GeoJSON files
- **Add constraint layers** (buildings, settlements, water bodies, etc.)
- **Generate settlement layers** automatically from VIDA rooftop data
- **Calculate usable areas** after removing constraints
- **Cluster khasras** into contiguous parcels using DBSCAN algorithm
- **Export results** in multiple formats (GeoJSON, KML, Shapefile, Excel, etc.)

## Prerequisites

### Docker

The API uses a **PostGIS Docker image** (`postgis/postgis:16-3.4-alpine`) for the database. Make sure Docker is installed and running:

- **macOS**: [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
- **Linux**: [Docker Engine](https://docs.docker.com/engine/install/)
- **Windows**: [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)

## Installation

1. Create a virtual environment:

```bash
cd api
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

1. Install dependencies:

```bash
make install
# or manually: pip install -r requirements.txt
```

1. Create a `.env` file (copy from `.env.example`):

```bash
cp .env.example .env
# Edit .env with your settings
```

**Important**: To use the slopes layer feature, you need NASA Earthdata credentials:
- Register for a free account at [NASA Earthdata](https://urs.earthdata.nasa.gov/users/new)
- Add your credentials to the `.env` file:
  ```
  EARTHDATA_USERNAME=your_username
  EARTHDATA_PASSWORD=your_password
  ```

## Configuration

Environment variables (set in `.env` file):

| Variable | Default | Description |
|----------|---------|-------------|
| `SECRET_KEY` | (required) | JWT signing key - change in production |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | 1440 (24h) | Token expiration time |
| `DEFAULT_USERNAME` | admin | Default admin username |
| `DEFAULT_PASSWORD` | admin | Default admin password |
| `POSTGRES_USER` | postgres | PostgreSQL username |
| `POSTGRES_PASSWORD` | postgres | PostgreSQL password |
| `POSTGRES_HOST` | localhost | PostgreSQL host |
| `POSTGRES_PORT` | 5432 | PostgreSQL port |
| `POSTGRES_DB` | solar_parks | PostgreSQL database name |
| `DATA_DIR` | ./data | Directory for cache files (distance matrices) |

## Running the Server

### Quick Start (Recommended)

Use the Makefile commands to manage the database and server:

```bash
# Full setup: install deps, setup database, and run server
make full-setup

# Or step by step:
make setup-db   # Start PostGIS Docker container and initialize tables
make run        # Run the FastAPI server
```

### Makefile Commands

| Command | Description |
|---------|-------------|
| `make install` | Install Python dependencies |
| `make setup-db` | Start PostGIS Docker container and initialize database |
| `make teardown-db` | Stop and remove the PostGIS container |
| `make run` | Run the FastAPI server (assumes database is running) |
| `make run-with-db` | Setup database and run server |
| `make full-setup` | Install deps, setup database, and run server |
| `make clean-docker` | Prune unused Docker resources |

### Manual Server Start

If you prefer to run the server manually (with database already running):

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Or run directly
python main.py
```

The API will be available at `http://localhost:8000`

## Startup Routine

On startup, the API:

1. Connects to PostgreSQL database
2. Creates all tables if they don't exist (via `init_db()`)
3. Initializes the data directory for cache files

## API Documentation

- **Swagger UI**: <http://localhost:8000/docs\>
- **ReDoc**: <http://localhost:8000/redoc\>

## Authentication

The API uses JWT (JSON Web Token) authentication.

### Default Credentials

- Username: `admin`
- Password: `admin`

### Getting a Token

```bash
curl -X POST "http://localhost:8000/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin"
```

### Using the Token

Include the token in the `Authorization` header:

```bash
curl -X GET "http://localhost:8000/projects" \
  -H "Authorization: Bearer <your-token>"
```

---

## Working Endpoints

### Health Check

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check (no auth required) |

### Authentication

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/auth/token` | POST | Get JWT access token |
| `/auth/me` | GET | Get current user info |

### Projects

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/projects` | POST | Create a new project |
| `/projects` | GET | List all projects |
| `/projects/{project_id}` | GET | Get project details |
| `/projects/{project_id}` | DELETE | Delete a project and all data |

### Khasras (Land Parcels)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/projects/{project_id}/khasras` | POST | Upload khasra shapes (KML/GeoJSON) |

Uploads khasra boundaries and stores them in the database. Each khasra geometry is stored in PostGIS with WGS84 (EPSG:4326) coordinates.

### Layers

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/layers/available_builtin` | GET | Get available layer types and parameters |
| `/projects/{project_id}/layers` | GET | List project layers |
| `/projects/{project_id}/layers/custom_upload` | POST | Upload a custom constraint layer |
| `/projects/{project_id}/layers/settlements` | POST | Auto-generate settlement layers from VIDA data |
| `/projects/{project_id}/calculate-areas` | POST | Calculate usable areas after applying layers |

**Layer Processing:**

- Layers can be marked as `is_unusable=true` (area deducted from usable) or `is_unusable=false` (area deducted from available)
- Settlement layer generation fetches building footprints from VIDA S2 rooftop data, clusters them using DBSCAN, and creates settlement polygons

### Clustering

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/projects/{project_id}/cluster` | POST | Cluster khasras into parcels |

Clusters adjacent khasras using DBSCAN algorithm. Parameters:

- `distance_threshold`: Max distance (meters) between khasras to be considered adjacent
- `min_samples`: Minimum khasras to form a cluster

The distance matrix is cached to disk for performance.

### Export

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/projects/{project_id}/export` | POST | Export project data |
| `/projects/{project_id}/download/{data_type}` | GET | Quick download endpoint |

**Export Types:**

- `khasras`: Original khasra boundaries
- `khasras_with_stats`: Khasras with calculated area statistics
- `parcels`: Clustered parcel boundaries
- `layers`: All constraint layers
- `all`: Everything

**Export Formats:**

- GeoJSON, KML, Shapefile, Parquet, CSV, Excel

### Statistics

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/projects/{project_id}/stats` | GET | Get project statistics |

---

## Typical Workflow

1. **Create a project**

```bash
curl -X POST "http://localhost:8000/projects" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"name": "Betul Solar Park", "location": "Betul", "description": "Analysis for Betul district"}'
```

1. **Upload khasra boundaries**

```bash
curl -X POST "http://localhost:8000/projects/{project_id}/khasras" \
  -H "Authorization: Bearer <token>" \
  -F "file=@khasras.kml"
```

1. **Generate settlement layer (automatic)**

```bash
curl -X POST "http://localhost:8000/projects/{project_id}/layers/settlements" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"building_buffer": 10, "settlement_eps": 50, "min_buildings": 5}'
```

1. **Add custom constraint layers (optional)**

```bash
curl -X POST "http://localhost:8000/projects/{project_id}/layers/custom_upload" \
  -H "Authorization: Bearer <token>" \
  -F "file=@water_bodies.kml" \
  -F "layer_name=Water Bodies" \
  -F "is_unusable=true"
```

1. **Calculate usable areas**

```bash
curl -X POST "http://localhost:8000/projects/{project_id}/calculate-areas" \
  -H "Authorization: Bearer <token>"
```

1. **Cluster khasras into parcels**

```bash
curl -X POST "http://localhost:8000/projects/{project_id}/cluster" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"distance_threshold": 25, "min_samples": 2}'
```

1. **Export results**

```bash
curl -X POST "http://localhost:8000/projects/{project_id}/export" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"export_type": "all", "format": "excel"}' \
  --output export.xlsx
```

---

## Project Structure

```
api/
├── main.py           # FastAPI application and endpoints
├── auth.py           # Authentication (JWT)
├── config.py         # Configuration and settings
├── models.py         # Pydantic request/response models
├── database.py       # SQLAlchemy models and database setup
├── services.py       # Business logic and geospatial processing
├── storage.py        # File storage utilities (for distance matrix cache)
├── requirements.txt  # Python dependencies
├── .env.example      # Example environment variables
└── README.md         # This file
```

## Data Storage

### Database (PostgreSQL/PostGIS)

All spatial data is stored in the database:

| Table | Description |
|-------|-------------|
| `projects` | Project metadata, stats, and distance_matrix_path |
| `khasras` | Khasra geometries and calculated statistics |
| `layers` | Layer metadata (name, type, is_unusable) |
| `layer_features` | Individual layer feature geometries |
| `parcels` | Clustered parcel geometries and stats |

### File Storage

Only the distance matrix is stored as a file (numpy `.npy`):

```
data/
├── {project_id}/
│   └── distance_matrix.npy    # Precomputed pairwise distances
└── shared_vida_s2_rooftop_data/
    └── {geohash}.gpkg         # Cached VIDA building footprints
```

## Coordinate Reference Systems

- **Storage**: WGS84 (EPSG:4326) - all geometries in database
- **Area calculations**: India Projected CRS (EPSG:24378) - projected on-the-fly
- **Distance matrix**: India Projected CRS for accurate distance calculations

## Layer Types

### Unusable Layers (deducted from total usable area)

- **Settlements**: Clustered building areas
- **Water Bodies**: Rivers, lakes, ponds
- **Steep Slopes**: Areas with unsuitable terrain

### Unavailable Layers (usable but not currently available)

- **Cropland**: Agricultural areas
- **Isolated Buildings**: Individual buildings with buffer zones
