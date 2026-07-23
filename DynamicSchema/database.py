from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Table, JSON, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./dynamic_schema.db")

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

schema_tags = Table(
    'schema_tags',
    Base.metadata,
    Column('schema_id', Integer, ForeignKey('schemas.id')),
    Column('tag', String)
)

class DBUser(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    username = Column(String, unique=True, index=True)
    organization = Column(String, nullable=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    api_key = Column(String, unique=True)
    
    schemas = relationship("DBSchema", back_populates="created_by")
    usage_logs = relationship("UsageLog", back_populates="user")

class DBSchema(Base):
    __tablename__ = "schemas"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    current_version = Column(String)
    created_by_id = Column(Integer, ForeignKey("users.id"))
    is_public = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    usage_count = Column(Integer, default=0)
    
    created_by = relationship("DBUser", back_populates="schemas")
    versions = relationship("DBSchemaVersion", back_populates="schema")
    tags = relationship("Tag", secondary=schema_tags)

class DBSchemaVersion(Base):
    __tablename__ = "schema_versions"
    
    id = Column(Integer, primary_key=True, index=True)
    schema_id = Column(Integer, ForeignKey("schemas.id"))
    version = Column(String)
    schema_definition = Column(JSON)
    description = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    schema = relationship("DBSchema", back_populates="versions")

class UsageLog(Base):
    __tablename__ = "usage_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    schema_id = Column(Integer, ForeignKey("schemas.id"))
    endpoint = Column(String)
    tokens_used = Column(Integer)
    cost = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("DBUser", back_populates="usage_logs")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Create all tables
Base.metadata.create_all(bind=engine)
