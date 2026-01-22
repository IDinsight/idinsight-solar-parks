# Solar Park Analysis - Quick Start Guide

## Prerequisites

### Backend

- Python 3.9+
- PostgreSQL with PostGIS extension
- pip or conda

### Frontend

- Node.js 20.9.0+
- pnpm (or npm/yarn)

## Setup Instructions

### 1. Backend Setup

```bash
# Navigate to API directory
cd api

# Create virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up PostgreSQL database
# Make sure PostgreSQL is running and create a database:
createdb solar_parks
psql solar_parks -c "CREATE EXTENSION postgis;"

# Configure environment variables (optional)
# Create a .env file in api/ directory:
# POSTGRES_USER=postgres
# POSTGRES_PASSWORD=postgres
# POSTGRES_HOST=localhost
# POSTGRES_PORT=5432
# POSTGRES_DB=solar_parks
# SECRET_KEY=your-secret-key-change-in-production

# Run the backend
python main.py
```

Backend will be available at: <http://localhost:8000>
API Documentation: <http://localhost:8000/docs>

### 2. Frontend Setup

```bash
# Navigate to frontend directory
cd frontend

# Install dependencies
pnpm install

# Create environment file
cp .env.local.example .env.local

# Edit .env.local if needed (default should work):
# NEXT_PUBLIC_API_URL=http://localhost:8000

# Run development server
pnpm dev
```

Frontend will be available at: <http://localhost:3000>

## Default Login Credentials

- **Username**: `admin`
- **Password**: `admin`

## Usage Workflow

### 1. Login

Navigate to <http://localhost:3000> and log in with the default credentials.

### 2. Create a Project

- Click "New Project" on the dashboard
- Enter project name and location
- Optionally add a description

### 3. Upload Khasra Boundaries

- Select your project from the dashboard
- In Step 1, upload a KML or GeoJSON file containing land parcel boundaries
- Review the preview and select the ID column
- Confirm to upload to backend

### 4. Add Constraint Layers

- In Step 2, select which layers to overlay (Buildings, Settlements, etc.)
- Click "Add Layers"
- Backend will process the layers and calculate usable areas

### 5. Run Clustering

- In Step 3, configure clustering parameters:
  - Distance Threshold: Maximum distance between adjacent khasras (in meters)
  - Minimum Khasras: Minimum number of khasras per cluster
- Click "Run Clustering"

### 6. Export Results

- In Step 4, download your results in various formats:
  - **GeoJSON**: For web mapping
  - **KML**: For Google Earth
  - **Shapefile**: For GIS software (QGIS, ArcGIS)
  - **Excel**: Complete statistics with all sheets
  - **CSV**: Khasra statistics
  - **Parquet**: Efficient storage format

## Architecture

### Backend (FastAPI + PostgreSQL)

- RESTful API with JWT authentication
- PostgreSQL with PostGIS for spatial operations
- File storage for uploaded data
- DBSCAN clustering algorithm
- Multiple export formats

### Frontend (Next.js + React)

- Server-side rendering with Next.js 15
- TypeScript for type safety
- Tailwind CSS for styling
- Zustand for state management
- Axios for API calls

## API Endpoints

Full API documentation is available at <http://localhost:8000/docs> when the backend is running.

### Key Endpoints

- `POST /auth/token` - Authentication
- `POST /projects` - Create project
- `GET /projects` - List projects
- `POST /projects/{id}/khasras` - Upload khasra boundaries
- `POST /projects/{id}/layers/settlements` - Generate settlement layers
- `POST /projects/{id}/calculate-areas` - Calculate usable areas
- `POST /projects/{id}/cluster` - Run clustering
- `POST /projects/{id}/export` - Export data

## Troubleshooting

### Backend Issues

**Database connection error:**

```bash
# Check if PostgreSQL is running
pg_isready

# Verify database exists
psql -l | grep solar_parks
```

**ModuleNotFoundError:**

```bash
# Reinstall dependencies
pip install -r requirements.txt
```

### Frontend Issues

**Node version error:**

```bash
# Update Node.js to version 20.9.0 or higher
nvm install 20
nvm use 20
```

**API connection error:**

- Verify backend is running on <http://localhost:8000>
- Check NEXT_PUBLIC_API_URL in .env.local

**Authentication error:**

- Clear browser local storage
- Try logging in again with correct credentials

## Development

### Adding New Features

1. **Backend**: Add endpoints in `api/main.py`, services in `api/services.py`
2. **Frontend**:
   - Add API types in `lib/api/types.ts`
   - Add service functions in `lib/api/services.ts`
   - Update components to use new APIs

### Code Structure

```
solar/
├── api/                      # Backend
│   ├── main.py              # FastAPI app and endpoints
│   ├── auth.py              # Authentication logic
│   ├── models.py            # Pydantic models
│   ├── services.py          # Business logic
│   ├── database.py          # Database models
│   ├── config.py            # Configuration
│   └── storage.py           # File storage
├── frontend/                # Frontend
│   ├── app/                 # Next.js pages
│   ├── components/          # React components
│   ├── lib/                 # Utilities and API
│   └── .env.local           # Environment config
└── notebooks/               # Jupyter notebooks (preprocessing)
```

## Production Deployment

### Backend

1. Set proper SECRET_KEY in environment
2. Configure production database
3. Set DEBUG=False
4. Use proper CORS origins
5. Deploy with gunicorn/uvicorn behind nginx

### Frontend

1. Build production bundle: `pnpm build`
2. Set NEXT_PUBLIC_API_URL to production backend
3. Deploy to Vercel, Netlify, or your platform

## Support

For issues or questions:

1. Check the API documentation at /docs
2. Review error messages in browser console (F12)
3. Check backend logs
4. See FRONTEND_INTEGRATION.md for detailed integration notes

## License

[Your License Here]
