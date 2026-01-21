# Solar Parks Analysis API

A FastAPI application for analyzing land parcels (khasras) for solar park development.

## Features

- **Upload khasra boundaries** from KML or GeoJSON files
- **Add constraint layers** (buildings, settlements, water, slopes, cropland, etc.)
- **Cluster khasras** into contiguous parcels using DBSCAN algorithm
- **Calculate usable areas** after removing constraints
- **Export results** in multiple formats (GeoJSON, KML, Shapefile, Excel, etc.)

## Installation

1. Create a virtual environment:
```bash
cd api
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. (Optional) Create a `.env` file for configuration:
```env
SECRET_KEY=your-secret-key-here
DEFAULT_USERNAME=admin
DEFAULT_PASSWORD=your-secure-password
```

## Running the Server

```bash
# Development mode with auto-reload
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Or run directly
python main.py
```

The API will be available at `http://localhost:8000`

## API Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Authentication

The API uses JWT (JSON Web Token) authentication. 

### Default Credentials
- Username: `admin`
- Password: `solarparks2024`

### Getting a Token

```bash
curl -X POST "http://localhost:8000/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=solarparks2024"
```

### Using the Token

Include the token in the `Authorization` header:
```bash
curl -X GET "http://localhost:8000/projects" \
  -H "Authorization: Bearer <your-token>"
```

## API Endpoints

### Projects
- `POST /projects` - Create a new project
- `GET /projects` - List all projects
- `GET /projects/{project_id}` - Get project details
- `DELETE /projects/{project_id}` - Delete a project

### Khasras (Land Parcels)
- `POST /projects/{project_id}/khasras` - Upload khasra shapes (KML/GeoJSON)

### Layers
- `GET /layers/available` - Get available layer types and parameters
- `POST /projects/{project_id}/layers` - Upload a custom constraint layer
- `GET /projects/{project_id}/layers` - List project layers
- `POST /projects/{project_id}/calculate-areas` - Calculate usable areas

### Clustering
- `POST /projects/{project_id}/cluster` - Cluster khasras into parcels

### Export
- `POST /projects/{project_id}/export` - Export project data
- `GET /projects/{project_id}/download/{data_type}` - Quick download

### Statistics
- `GET /projects/{project_id}/stats` - Get project statistics

## Workflow Example

1. **Create a project**
```bash
curl -X POST "http://localhost:8000/projects" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"name": "Betul Solar Park", "location": "Betul", "description": "Analysis for Betul district"}'
```

2. **Upload khasra boundaries**
```bash
curl -X POST "http://localhost:8000/projects/{project_id}/khasras" \
  -H "Authorization: Bearer <token>" \
  -F "file=@khasras.kml"
```

3. **Add constraint layers**
```bash
curl -X POST "http://localhost:8000/projects/{project_id}/layers" \
  -H "Authorization: Bearer <token>" \
  -F "file=@water_bodies.kml" \
  -F "layer_name=Water Bodies" \
  -F "is_unusable=true"
```

4. **Calculate usable areas**
```bash
curl -X POST "http://localhost:8000/projects/{project_id}/calculate-areas" \
  -H "Authorization: Bearer <token>"
```

5. **Cluster khasras into parcels**
```bash
curl -X POST "http://localhost:8000/projects/{project_id}/cluster" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"distance_threshold": 25, "min_samples": 2}'
```

6. **Export results**
```bash
curl -X POST "http://localhost:8000/projects/{project_id}/export?export_type=all&format=excel" \
  -H "Authorization: Bearer <token>" \
  --output export.xlsx
```

## Layer Types

### Unusable Layers (deducted from total area)
- **Settlements**: Clustered building areas
- **Water Bodies**: Rivers, lakes, ponds
- **Steep Slopes**: North-facing slopes >7°, other slopes >10°

### Unavailable Layers (usable but not currently available)
- **Cropland**: Agricultural areas
- **Isolated Buildings**: Individual buildings with buffer zones

## Export Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| GeoJSON | .geojson | Web-friendly, good for mapping applications |
| KML | .kml | Google Earth compatible |
| Shapefile | .shp (zipped) | ESRI format for GIS software |
| Parquet | .parquet | Efficient storage for large datasets |
| CSV | .csv | Tabular data without geometry |
| Excel | .xlsx | Multi-sheet workbook with statistics |

## Configuration

Environment variables can be set in a `.env` file or as system environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SECRET_KEY` | auto-generated | JWT signing key |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | 1440 (24h) | Token expiration time |
| `DEFAULT_USERNAME` | admin | Default admin username |
| `DEFAULT_PASSWORD` | solarparks2024 | Default admin password |

## Development

### Project Structure
```
api/
├── main.py           # FastAPI application and endpoints
├── auth.py           # Authentication (JWT)
├── config.py         # Configuration and settings
├── models.py         # Pydantic models
├── services.py       # Business logic and geospatial processing
├── requirements.txt  # Python dependencies
└── README.md         # This file
```

### Running Tests
```bash
pytest tests/
```

## License

MIT License
