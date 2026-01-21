"""
Database configuration and models for the Solar Parks API
Uses SQLAlchemy with GeoAlchemy2 for PostGIS support
"""
import json
from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    Boolean,
    Integer,
    Float,
    ForeignKey,
    create_engine,
    Enum as SQLEnum,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape, from_shape

from config import settings
from models import ProjectStatus

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
    
    # File paths for stored data
    khasras_file_path = Column(String(500), nullable=True)
    stats_file_path = Column(String(500), nullable=True)
    clustered_file_path = Column(String(500), nullable=True)
    parcels_file_path = Column(String(500), nullable=True)
    distance_matrix_path = Column(String(500), nullable=True)
    
    # Cached statistics
    khasra_count = Column(Integer, nullable=True)
    total_area_ha = Column(Float, nullable=True)
    bounds_json = Column(JSONB, nullable=True)
    
    # Relationships
    khasras = relationship("KhasraModel", back_populates="project", cascade="all, delete-orphan")
    layers = relationship("LayerModel", back_populates="project", cascade="all, delete-orphan")


class KhasraModel(Base):
    """Khasra (land parcel) database model with geometry"""
    __tablename__ = "khasras"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    khasra_id = Column(String(100), nullable=False)
    khasra_id_unique = Column(String(150), nullable=False, unique=True)
    
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
    
    # File path for layer data
    file_path = Column(String(500), nullable=True)
    
    # Statistics
    feature_count = Column(Integer, nullable=True)
    total_area_ha = Column(Float, nullable=True)
    
    # Parameters used
    parameters = Column(JSONB, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
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


class ParcelModel(Base):
    """Clustered parcel database model"""
    __tablename__ = "parcels"

    id = Column(Integer, primary_key=True, autoincrement=True)
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
