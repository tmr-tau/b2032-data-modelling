from sqlalchemy import Column, Integer, String, Date, Time, Numeric, JSON, ForeignKey, TIMESTAMP, Boolean, UniqueConstraint, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class FctEventSession(Base):
    __tablename__ = 'fct_event_session'
    
    record_id = Column(Integer, primary_key=True)
    hash_key = Column(String(100), nullable=False)
    scenario_id = Column(Integer, ForeignKey('dim_scenarios.scenario_id'))
    version_id = Column(Integer, ForeignKey('dim_schedule_version.version_id'))
    sport_id = Column(Integer, ForeignKey('dim_sports.sport_id'))
    venue_id = Column(Integer, ForeignKey('dim_venues.venue_id'))
    subvenue_id = Column(Integer, ForeignKey('dim_subvenues.subvenue_id'))
    region_id = Column(Integer, ForeignKey('dim_regions.region_id'))
    zone_id = Column(Integer, ForeignKey('dim_zones.zone_id'))
    cluster_id = Column(Integer, ForeignKey('dim_clusters.cluster_id'))
    day_id = Column(Integer, ForeignKey('dim_calendar.day_id'))
    session_id = Column(Integer)
    date_start = Column(Date, nullable=False)
    start_time = Column(Time, nullable=False)
    date_end = Column(Date, nullable=False)
    end_time = Column(Time, nullable=False)
    competition_type = Column(String(1))
    event_type = Column(String(15))
    gross_seats = Column(Integer, nullable=False)
    seat_kill = Column(Numeric(5, 2), nullable=False)
    est_pct_ticksold = Column(Numeric(5, 2), nullable=False)
    net_seats = Column(Integer, nullable=False)
    est_sold_seats = Column(Integer, nullable=False)
    workforce = Column(Integer, nullable=False)
    unticketed = Column(Integer, nullable=False)
    created_at = Column(TIMESTAMP, default='CURRENT_TIMESTAMP')
    created_by = Column(String(50))
    additional_attributes = Column(JSON)
    __table_args__ = (UniqueConstraint('scenario_id', 'version_id', 'hash_key'),)

class FctEventBase(Base):
    __tablename__ = 'fct_event_base'
    
    event_id = Column(Integer, primary_key=True)
    hash_key = Column(String(100), nullable=False, unique=True)
    sport_id = Column(Integer, ForeignKey('dim_sports.sport_id'))
    venue_id = Column(Integer, ForeignKey('dim_venues.venue_id'))
    subvenue_id = Column(Integer, ForeignKey('dim_subvenues.subvenue_id'))
    region_id = Column(Integer, ForeignKey('dim_regions.region_id'))
    zone_id = Column(Integer, ForeignKey('dim_zones.zone_id'))
    cluster_id = Column(Integer, ForeignKey('dim_clusters.cluster_id'))
    day_id = Column(Integer, ForeignKey('dim_calendar.day_id'))
    session_id = Column(Integer)
    competition_type = Column(String(1))
    event_type = Column(String(15))
    created_at = Column(TIMESTAMP, default=func.current_timestamp())
    created_by = Column(String(50))
    last_modified_at = Column(TIMESTAMP, default=func.current_timestamp(), onupdate=func.current_timestamp())

class FctEventDetails(Base):
    __tablename__ = 'fct_event_details'
    
    detail_id = Column(Integer, primary_key=True)
    event_id = Column(Integer, ForeignKey('fct_event_base.event_id'))
    date_start = Column(Date, nullable=False)
    start_time = Column(Time, nullable=False)
    date_end = Column(Date, nullable=False)
    end_time = Column(Time, nullable=False)
    gross_seats = Column(Integer, nullable=False)
    seat_kill = Column(Numeric(5, 2), nullable=False)
    est_pct_ticksold = Column(Numeric(5, 2), nullable=False)
    net_seats = Column(Integer, nullable=False)
    est_sold_seats = Column(Integer, nullable=False)
    workforce = Column(Integer, nullable=False)
    unticketed = Column(Integer, nullable=False)
    additional_attributes = Column(JSON)
    detail_hash = Column(String(100), nullable=False, unique=True)

class DimScenario(Base):
    __tablename__ = 'dim_scenario'
    
    scenario_id = Column(Integer, primary_key=True)
    scenario_name = Column(String(100), nullable=False)
    scenario_description = Column(String)
    created_by = Column(String(50))
    created_at = Column(TIMESTAMP, default=func.current_timestamp())
    category = Column(String(50))
    priority = Column(Integer)

class DimVersion(Base):
    __tablename__ = 'dim_version'
    
    version_id = Column(Integer, primary_key=True)
    scenario_id = Column(Integer, ForeignKey('dim_scenario.scenario_id'))
    version_number = Column(Integer, nullable=False)
    version_code = Column(String(50), nullable=False)
    version_name = Column(String(100))
    version_description = Column(String)
    created_at = Column(TIMESTAMP, default=func.current_timestamp())
    created_by = Column(String(50))
    status = Column(String(20), default='DRAFT')
    __table_args__ = (UniqueConstraint('scenario_id', 'version_number'),)

class VersionEventBridge(Base):
    __tablename__ = 'version_event_bridge'
    
    bridge_id = Column(Integer, primary_key=True)
    version_id = Column(Integer, ForeignKey('dim_version.version_id'))
    event_id = Column(Integer, ForeignKey('fct_event_base.event_id'))
    detail_id = Column(Integer, ForeignKey('fct_event_details.detail_id'))
    is_active = Column(Boolean, default=True)
    added_at = Column(TIMESTAMP, default=func.current_timestamp())
    added_by = Column(String(50))
    __table_args__ = (UniqueConstraint('version_id', 'event_id'),)

# Define other models similarly
