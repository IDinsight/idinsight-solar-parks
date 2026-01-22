# 🚀 Quick Reference - Solar Park Analysis

## Start Services

### Backend

```bash
cd api
python main.py
```

→ <http://localhost:8000>
→ Docs: <http://localhost:8000/docs>

### Frontend

```bash
cd frontend
pnpm dev
```

→ <http://localhost:3000>

## Login

- Username: `admin`
- Password: `admin`

## File Formats Supported

### Upload (Input)

- ✅ KML (Google Earth format)
- ✅ GeoJSON (standard web format)

### Export (Output)

- 📊 Excel (.xlsx) - Statistics + multiple sheets
- 🗺️ GeoJSON (.geojson) - Web mapping
- 🌍 KML (.kml) - Google Earth
- 📁 Shapefile (.zip) - GIS software
- 📈 CSV (.zip) - Spreadsheet data
- 🚀 Parquet (.parquet) - Efficient storage

## Workflow Steps

1. **Create Project** → Dashboard → New Project
2. **Upload Khasras** → Select project → Upload KML/GeoJSON
3. **Add Layers** → Choose Buildings/Settlements → Add Layers
4. **Cluster** → Set distance threshold → Run Clustering
5. **Export** → Choose format → Download

## Key Directories

```
api/          → Backend (FastAPI + PostgreSQL)
frontend/     → Frontend (Next.js + React)
notebooks/    → Jupyter notebooks for preprocessing
```

## Environment Files

### Frontend

```bash
# frontend/.env.local
NEXT_PUBLIC_API_URL=http://localhost:8000
```

### Backend (Optional)

```bash
# api/.env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=solar_parks
SECRET_KEY=your-secret-key
```

## Common Commands

### Frontend

```bash
pnpm dev         # Start dev server
pnpm build       # Build for production
pnpm lint        # Run linter
pnpm start       # Start production server
```

### Backend

```bash
python main.py              # Start server
pip install -r requirements.txt  # Install deps
```

## Troubleshooting Quick Fixes

### Backend not connecting?

1. Check if backend is running: <http://localhost:8000/health>
2. Check NEXT_PUBLIC_API_URL in .env.local
3. Clear browser cache and local storage

### Login not working?

1. Check credentials: admin / admin
2. Clear browser local storage
3. Check backend logs for auth errors

### Upload failing?

1. Verify file is valid KML or GeoJSON
2. Check file size (large files may timeout)
3. Check backend logs for processing errors

## Quick Links

- 📖 Full Setup: See [QUICKSTART.md](QUICKSTART.md)
- 🔧 Implementation Details: See [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)
- 🌐 API Docs: <http://localhost:8000/docs> (when backend running)
- 💻 Frontend: <http://localhost:3000>
- 🗄️ Backend: <http://localhost:8000>

## Architecture

```
Browser (React/Next.js)
    ↓ HTTP/REST
Backend (FastAPI)
    ↓ SQL/PostGIS
PostgreSQL Database
    ↓
Local File Storage
```

## State Management

- **Auth**: JWT tokens in localStorage
- **Projects**: Zustand store
- **API**: Axios with interceptors

## Features Summary

✅ User authentication
✅ Project management
✅ KML/GeoJSON upload
✅ Layer overlay (Buildings, Settlements)
✅ Area calculation
✅ DBSCAN clustering
✅ Multi-format export
✅ Real-time preview
✅ Error handling
✅ Loading states

## Support

Need help? Check these docs:

1. [QUICKSTART.md](QUICKSTART.md) - Setup guide
2. [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) - Technical details
3. [FRONTEND_INTEGRATION.md](FRONTEND_INTEGRATION.md) - Integration notes
4. Backend API docs at /docs endpoint

---

**Pro Tip**: Keep both terminal windows (backend and frontend) visible so you can monitor logs in real-time!
