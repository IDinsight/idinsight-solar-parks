"""
File storage utilities for the Solar Parks API
Handles local file storage for GeoDataFrames and other data
"""
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Union
import json

import geopandas as gpd
import numpy as np
import pandas as pd

from config import settings


class FileStorage:
    """Handles local file storage for project data"""
    
    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or settings.DATA_DIR
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def _get_project_dir(self, project_id: str) -> Path:
        """Get or create project directory"""
        project_dir = self.base_path / project_id
        project_dir.mkdir(parents=True, exist_ok=True)
        return project_dir
    
    def _get_layers_dir(self, project_id: str) -> Path:
        """Get or create layers directory for a project"""
        layers_dir = self._get_project_dir(project_id) / "layers"
        layers_dir.mkdir(parents=True, exist_ok=True)
        return layers_dir
    
    # ============ GeoDataFrame Storage ============
    
    def save_geodataframe(
        self,
        gdf: gpd.GeoDataFrame,
        project_id: str,
        filename: str,
        subfolder: Optional[str] = None,
    ) -> str:
        """
        Save a GeoDataFrame to parquet file
        Returns the file path
        """
        if subfolder:
            save_dir = self._get_project_dir(project_id) / subfolder
            save_dir.mkdir(parents=True, exist_ok=True)
        else:
            save_dir = self._get_project_dir(project_id)
        
        # Ensure filename has .parquet extension
        if not filename.endswith(".parquet"):
            filename = f"{filename}.parquet"
        
        file_path = save_dir / filename
        
        # Convert to WGS84 before saving
        if gdf.crs is not None and gdf.crs != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")
        
        gdf.to_parquet(file_path)
        return str(file_path)
    
    def load_geodataframe(self, file_path: str) -> Optional[gpd.GeoDataFrame]:
        """Load a GeoDataFrame from parquet file"""
        path = Path(file_path)
        if not path.exists():
            return None
        return gpd.read_parquet(path)
    
    # ============ NumPy Array Storage ============
    
    def save_numpy_array(
        self,
        arr: np.ndarray,
        project_id: str,
        filename: str,
    ) -> str:
        """Save a NumPy array to file"""
        save_dir = self._get_project_dir(project_id)
        
        if not filename.endswith(".npy"):
            filename = f"{filename}.npy"
        
        file_path = save_dir / filename
        np.save(file_path, arr)
        return str(file_path)
    
    def load_numpy_array(self, file_path: str) -> Optional[np.ndarray]:
        """Load a NumPy array from file"""
        path = Path(file_path)
        if not path.exists():
            return None
        return np.load(path)
    
    # ============ JSON Storage ============
    
    def save_json(
        self,
        data: dict,
        project_id: str,
        filename: str,
    ) -> str:
        """Save dictionary as JSON file"""
        save_dir = self._get_project_dir(project_id)
        
        if not filename.endswith(".json"):
            filename = f"{filename}.json"
        
        file_path = save_dir / filename
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        return str(file_path)
    
    def load_json(self, file_path: str) -> Optional[dict]:
        """Load JSON file as dictionary"""
        path = Path(file_path)
        if not path.exists():
            return None
        with open(path, "r") as f:
            return json.load(f)
    
    # ============ Layer Storage ============
    
    def save_layer(
        self,
        gdf: gpd.GeoDataFrame,
        project_id: str,
        layer_name: str,
    ) -> str:
        """Save a layer GeoDataFrame"""
        return self.save_geodataframe(
            gdf=gdf,
            project_id=project_id,
            filename=f"{layer_name}.parquet",
            subfolder="layers",
        )
    
    def load_layer(self, project_id: str, layer_name: str) -> Optional[gpd.GeoDataFrame]:
        """Load a layer GeoDataFrame"""
        file_path = self._get_layers_dir(project_id) / f"{layer_name}.parquet"
        return self.load_geodataframe(str(file_path))
    
    # ============ Project Management ============
    
    def delete_file(self, file_path: str) -> bool:
        """Delete a specific file"""
        path = Path(file_path)
        if path.exists() and path.is_file():
            path.unlink()
            return True
        return False
    
    def delete_project_files(self, project_id: str) -> bool:
        """Delete all files for a project"""
        project_dir = self._get_project_dir(project_id)
        if project_dir.exists():
            shutil.rmtree(project_dir)
            return True
        return False
    
    def get_project_size(self, project_id: str) -> int:
        """Get total size of project files in bytes"""
        project_dir = self._get_project_dir(project_id)
        if not project_dir.exists():
            return 0
        
        total_size = 0
        for path in project_dir.rglob("*"):
            if path.is_file():
                total_size += path.stat().st_size
        return total_size
    
    def list_project_files(self, project_id: str) -> list:
        """List all files for a project"""
        project_dir = self._get_project_dir(project_id)
        if not project_dir.exists():
            return []
        
        files = []
        for path in project_dir.rglob("*"):
            if path.is_file():
                files.append({
                    "path": str(path.relative_to(project_dir)),
                    "size_bytes": path.stat().st_size,
                    "modified": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
                })
        return files


# Global file storage instance
file_storage = FileStorage()
