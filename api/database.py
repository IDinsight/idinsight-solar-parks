"""
Database configuration and models for the Solar Parks API
Uses SQLAlchemy with GeoAlchemy2 for PostGIS support
"""
import json
import uuid
from datetime import datetime
from typing import Optional

from config import settings
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape, to_shape
from models import ProjectStatus
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy import (
    Enum as SQLEnum,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship, sessionmaker

# Database setup
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


# ============ Database Models ============

class ProjectModel(Base):
    """Project database model"""
    __tablename__ = "projects"

    id = Column(String(36), primary_key=True)
    name = Column(String(100), nullable=False)
    location = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    status = Column(SQLEnum(ProjectStatus), default=ProjectStatus.CREATED)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Cached statistics
    khasra_count = Column(Integer, nullable=True)
    total_area_ha = Column(Float, nullable=True)
    bounds_json = Column(JSONB, nullable=True)
    
    # Distance matrix cache file path (for clustering)
    distance_matrix_path = Column(String, nullable=True)
    
    # Relationships
    khasras = relationship("KhasraModel", back_populates="project", cascade="all, delete-orphan")
    layers = relationship("LayerModel", back_populates="project", cascade="all, delete-orphan")


class KhasraModel(Base):
    """Khasra (land parcel) database model with geometry"""
    __tablename__ = "khasras"
    __table_args__ = (
        UniqueConstraint('project_id', 'khasra_id_unique', name='uq_khasra_project_unique_id'),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    khasra_id = Column(String(100), nullable=False)  # Original khasra ID (can have duplicates)
    khasra_id_unique = Column(String(150), nullable=False)  # Project-unique ID (e.g., "KHASRA_001_0")
    
    # Geometry stored in PostGIS (WGS84)
    geometry = Column(Geometry("MULTIPOLYGON", srid=4326), nullable=False)
    
    # Area statistics
    original_area_ha = Column(Float, nullable=True)
    usable_area_ha = Column(Float, nullable=True)
    unusable_area_ha = Column(Float, nullable=True)
    usable_available_area_ha = Column(Float, nullable=True)
    
    # Clustering
    parcel_id = Column(String(100), nullable=True)
    
    # Additional properties as JSON
    properties = Column(JSONB, nullable=True)
    
    # Relationships
    project = relationship("ProjectModel", back_populates="khasras")


class LayerModel(Base):
    """Constraint layer database model"""
    __tablename__ = "layers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(100), nullable=False)
    layer_type = Column(String(50), nullable=False)
    is_unusable = Column(Boolean, default=True)
    
    # Processing status
    status = Column(String(20), default="in_progress")  # in_progress, successful, failed
    details = Column(Text, nullable=True)  # Current processing step or error message
    
    # Statistics
    feature_count = Column(Integer, nullable=True)
    total_area_ha = Column(Float, nullable=True)
    
    # Parameters used
    parameters = Column(JSONB, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    project = relationship("ProjectModel", back_populates="layers")


class LayerFeatureModel(Base):
    """Individual layer feature with geometry"""
    __tablename__ = "layer_features"

    id = Column(Integer, primary_key=True, autoincrement=True)
    layer_id = Column(Integer, ForeignKey("layers.id", ondelete="CASCADE"), nullable=False)
    khasra_id_unique = Column(String(150), nullable=True)
    
    # Geometry stored in PostGIS
    geometry = Column(Geometry("MULTIPOLYGON", srid=4326), nullable=False)
    
    # Area
    area_ha = Column(Float, nullable=True)
    
    # Additional properties
    properties = Column(JSONB, nullable=True)


class ClusteringRunModel(Base):
    """Clustering run parameters and metadata"""
    __tablename__ = "clustering_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    
    # Clustering parameters
    distance_threshold = Column(Integer, nullable=False)
    min_samples = Column(Integer, nullable=False)
    max_distance_considered = Column(Integer, nullable=False)
    
    # Results summary
    total_parcels = Column(Integer, nullable=True)
    clustered_khasras = Column(Integer, nullable=True)
    unclustered_khasras = Column(Integer, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    parcels = relationship("ParcelModel", back_populates="clustering_run", cascade="all, delete-orphan")


class ParcelModel(Base):
    """Clustered parcel database model"""
    __tablename__ = "parcels"

    id = Column(Integer, primary_key=True, autoincrement=True)
    clustering_run_id = Column(Integer, ForeignKey("clustering_runs.id", ondelete="CASCADE"), nullable=False)
    project_id = Column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    parcel_id = Column(String(100), nullable=False)
    
    # Geometry (convex hull)
    geometry = Column(Geometry("MULTIPOLYGON", srid=4326), nullable=True)
    
    # Statistics
    khasra_count = Column(Integer, nullable=True)
    khasra_ids = Column(Text, nullable=True)  # Comma-separated
    original_area_ha = Column(Float, nullable=True)
    usable_area_ha = Column(Float, nullable=True)
    usable_available_area_ha = Column(Float, nullable=True)
    unusable_area_ha = Column(Float, nullable=True)
    building_count = Column(Integer, nullable=True)
    
    # Layer-specific areas
    layer_areas = Column(JSONB, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    clustering_run = relationship("ClusteringRunModel", back_populates="parcels")


# ============ Database Utilities ============

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database tables"""
    Base.metadata.create_all(bind=engine)


def drop_db():
    """Drop all database tables (use with caution!)"""
    Base.metadata.drop_all(bind=engine)
