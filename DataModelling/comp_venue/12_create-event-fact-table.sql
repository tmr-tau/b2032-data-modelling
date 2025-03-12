/******************************************************
*
** Create Tables
*
*******************************************************/

-- Create Schedule Version Dimension Table
CREATE TABLE IF NOT EXISTS dim_schedule_version (
    schedule_id SERIAL PRIMARY KEY,
    version_code VARCHAR(50) NOT NULL,
    version_description VARCHAR(255),
    valid_from DATE DEFAULT CURRENT_DATE,
    valid_to DATE DEFAULT '9999-12-31'
);

-- Create the Event Fact Table
CREATE TABLE IF NOT EXISTS test_comp_venue.fct_event_session (
    record_id SERIAL PRIMARY KEY,
    hash_key VARCHAR(100) NOT NULL,
    schedule_version_id VARCHAR(10), --REFERENCES test_comp_venue.dim_schedule_version(version_id),
    current_version_id VARCHAR(10), --REFERENCES test_comp_venue.dim_schedule_version(version_id),
    version_array VARCHAR[], -- schedule_version_id or version_number
    sport_id INTEGER REFERENCES test_comp_venue.dim_sports(sport_id),
    venue_id INTEGER REFERENCES test_comp_venue.dim_venues(venue_id),
    subvenue_id INTEGER REFERENCES test_comp_venue.dim_subvenues(subvenue_id),
    region_id INTEGER REFERENCES test_comp_venue.dim_regions(region_id),
    zone_id INTEGER REFERENCES test_comp_venue.dim_zones(zone_id),
    cluster_id INTEGER REFERENCES test_comp_venue.dim_clusters(cluster_id),
    day_id INTEGER REFERENCES test_comp_venue.dim_calendar(day_id),
    session_id INTEGER,
    date_start DATE NOT NULL,
    start_time TIME NOT NULL,
    date_end DATE NOT NULL,
    end_time TIME NOT NULL,
    competition_type VARCHAR(1),
    event_type VARCHAR(15),
    gross_seats INTEGER NOT NULL,
    seat_kill NUMERIC(5,2) NOT NULL,
    est_pct_ticksold NUMERIC(5,2) NOT NULL,
    net_seats INTEGER NOT NULL,
    est_sold_seats INTEGER NOT NULL,
    workforce INTEGER NOT NULL,
    unticketed INTEGER NOT NULL,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL,
    additional_attributes JSONB
    /*CONSTRAINT fct_event_session_pkey PRIMARY KEY (record_id),
    CONSTRAINT fct_event_session_day_id_fkey FOREIGN KEY (day_id)
        REFERENCES test_comp_venue.dim_calendar (day_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fct_event_session_sport_id_fkey FOREIGN KEY (sport_id)
        REFERENCES test_comp_venue.dim_sports (sport_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fct_event_session_venue_id_fkey FOREIGN KEY (venue_id)
        REFERENCES test_comp_venue.dim_venues (venue_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT event_session_date_range CHECK (valid_to IS NULL OR valid_from < valid_to)*/
);

CREATE TABLE IF NOT EXISTS event_version_changes (
    change_id SERIAL PRIMARY KEY,
    event_session_id INTEGER REFERENCES fct_event_session(id),
    schedule_version_id INTEGER REFERENCES dim_schedule_version(version_code),
    change_type VARCHAR(10) NOT NULL,
    changed_fields JSONB,
    previous_values JSONB,
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Staging Table (WIP)
CREATE TABLE IF NOT EXISTS staging_event_session (
    sport_discipline VARCHAR(100) NOT NULL,
    venue_name VARCHAR(100) NOT NULL,
    subvenue_name VARCHAR(100) NOT NULL,
    region_name VARCHAR(100) NOT NULL,
    zone_name VARCHAR(100) NOT NULL,
    cluster_name VARCHAR(100) NOT NULL,
    event_day INTEGER NOT NULL,
	session_id INTEGER NOT NULL,
    date_start DATE NOT NULL,
    start_time TIME NOT NULL,
    date_end DATE NOT NULL,
    end_time TIME NOT NULL,
    competition_type VARCHAR(1),
    event_type VARCHAR(15),
    gross_seats INTEGER NOT NULL,
    seat_kill NUMERIC(5,2) NOT NULL,
    est_pct_ticksold NUMERIC(5,2) NOT NULL,
    net_seats INTEGER NOT NULL,
    est_sold_seats INTEGER NOT NULL,
    workforce INTEGER NOT NULL,
    unticketed INTEGER NOT NULL,
    additional_attributes JSONB
);

-- FOR TEST - staging table
CREATE TABLE IF NOT EXISTS staging_event_session_id (
    sport_id INTEGER,
    venue_id INTEGER,
    day_id INTEGER,
    session_id INTEGER,
    date_start DATE NOT NULL,
    start_time TIME NOT NULL,
    date_end DATE NOT NULL,
    end_time TIME NOT NULL,
    competition_type VARCHAR(1),
    event_type VARCHAR(15),
    gross_seats INTEGER NOT NULL,
    seat_kill NUMERIC(5,2) NOT NULL,
    est_pct_ticksold NUMERIC(5,2) NOT NULL,
    net_seats INTEGER NOT NULL,
    est_sold_seats INTEGER NOT NULL,
    workforce INTEGER NOT NULL,
    unticketed INTEGER NOT NULL,
    additional_attributes JSONB
);


/******************************************************
** Create Indexes and Partitions
*******************************************************/

-- Indexes
CREATE INDEX idx_event_session_current ON fct_event_session(is_current);
CREATE INDEX idx_event_session_version ON fct_event_session(schedule_version_id);
CREATE INDEX idx_event_session_date ON fct_event_session(day_id);
CREATE INDEX idx_event_session_venue ON fct_event_session(venue_id);
CREATE INDEX idx_event_session_sport ON fct_event_session(sport_id);
CREATE INDEX idx_event_change_version ON event_version_changes(schedule_version_id);

-- Partitioning
-- Partition fct_event_session by schedule_version_key
CREATE TABLE fct_event_session_partitioned (
    LIKE fct_event_session INCLUDING ALL
) PARTITION BY LIST (schedule_version_id);

-- Example partition (create one for each version)
CREATE TABLE fct_event_session_v1 PARTITION OF fct_event_session_partitioned
    FOR VALUES IN (1);


/******************************************************
*
** Create Functions
*
*******************************************************/

-- Trigger Function to generate hash key
CREATE OR REPLACE FUNCTION test_comp_venue.generate_hash_key()
RETURNS TRIGGER AS $$
BEGIN
    NEW.hash_key := md5(
        NEW.sport_id::TEXT || '-' ||
        NEW.venue_id::TEXT || '-' ||
        NEW.day_id::TEXT || '-' ||
        NEW.session_id::TEXT || '-' ||
        NEW.competition_type::TEXT || '-' ||
        NEW.event_type::TEXT || '-' ||
        to_char(NEW.date_start, 'YYYY-MM-DD')
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create or replace the trigger
DROP TRIGGER IF EXISTS set_hash_key ON test_comp_venue.fct_event_session;
CREATE TRIGGER set_hash_key
BEFORE INSERT OR UPDATE ON test_comp_venue.fct_event_session
FOR EACH ROW EXECUTE FUNCTION test_comp_venue.generate_hash_key();

-- Function to update version array
CREATE OR REPLACE FUNCTION update_version_array(current_array INTEGER[], new_version INTEGER)
RETURNS INTEGER[] AS $$
BEGIN
    IF new_version = ANY(current_array) THEN
        RETURN current_array;
    ELSE
        RETURN array_append(current_array, new_version);
    END IF;
END;
$$ LANGUAGE plpgsql;