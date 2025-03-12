CREATE TABLE IF NOT EXISTS fct_event_session (
    record_id SERIAL PRIMARY KEY,
    hash_key VARCHAR(100) NOT NULL,
    scenario_id INTEGER REFERENCES dim_scenarios(scenario_id),
    version_id INTEGER REFERENCES dim_schedule_version(version_id),
    
    -- Foreign keys to dimension tables
    sport_id INTEGER REFERENCES dim_sports(sport_id),
    venue_id INTEGER REFERENCES dim_venues(venue_id),
    subvenue_id INTEGER REFERENCES dim_subvenues(subvenue_id),
    region_id INTEGER REFERENCES dim_regions(region_id),
    zone_id INTEGER REFERENCES dim_zones(zone_id),
    cluster_id INTEGER REFERENCES dim_clusters(cluster_id),
    day_id INTEGER REFERENCES dim_calendar(day_id),
    
    -- Event details
    session_id INTEGER,
    date_start DATE NOT NULL,
    start_time TIME NOT NULL,
    date_end DATE NOT NULL,
    end_time TIME NOT NULL,
    competition_type VARCHAR(1),
    event_type VARCHAR(15),
    
    -- Capacity metrics
    gross_seats INTEGER NOT NULL,
    seat_kill NUMERIC(5,2) NOT NULL,
    est_pct_ticksold NUMERIC(5,2) NOT NULL,
    net_seats INTEGER NOT NULL,
    est_sold_seats INTEGER NOT NULL,
    workforce INTEGER NOT NULL,
    unticketed INTEGER NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50),
    
    -- Flexible attributes
    additional_attributes JSONB,
    
    -- Composite unique constraint to prevent duplicates within a version
    UNIQUE(scenario_id, version_id, hash_key)
);

CREATE OR REPLACE FUNCTION generate_hash_key()
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


"""
Instead of duplicating data or relying on arrays, let's create a more normalized structure that explicitly maps events to versions and scenarios:
"""
-- Create fct_event_base table
-- This table will store the core event properties that rarely change
CREATE TABLE IF NOT EXISTS fct_event_base (
    event_id SERIAL PRIMARY KEY,
    hash_key VARCHAR(100) NOT NULL UNIQUE,
    
    -- Core event properties that rarely change
    sport_id INTEGER REFERENCES dim_sports(sport_id),
    venue_id INTEGER REFERENCES dim_venues(venue_id),
    subvenue_id INTEGER REFERENCES dim_subvenues(subvenue_id),
    region_id INTEGER REFERENCES dim_regions(region_id),
    zone_id INTEGER REFERENCES dim_zones(zone_id),
    cluster_id INTEGER REFERENCES dim_clusters(cluster_id),
    day_id INTEGER REFERENCES dim_calendar(day_id),
    session_id INTEGER,
    competition_type VARCHAR(1),
    event_type VARCHAR(15),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50),
    last_modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create fct_event_details table
-- This table will store the event details that change frequently between versions
CREATE TABLE IF NOT EXISTS fct_event_details (
    detail_id SERIAL PRIMARY KEY,
    event_id INTEGER REFERENCES fct_event_base(event_id),
    
    -- Properties that change frequently between versions
    date_start DATE NOT NULL,
    start_time TIME NOT NULL,
    date_end DATE NOT NULL,
    end_time TIME NOT NULL,
    gross_seats INTEGER NOT NULL,
    seat_kill NUMERIC(5,2) NOT NULL,
    est_pct_ticksold NUMERIC(5,2) NOT NULL,
    net_seats INTEGER NOT NULL,
    est_sold_seats INTEGER NOT NULL,
    workforce INTEGER NOT NULL,
    unticketed INTEGER NOT NULL,
    
    -- Optional attributes
    additional_attributes JSONB,
    
    -- Hash for quick comparison (to detect duplicates)
    detail_hash VARCHAR(100) GENERATED ALWAYS AS (
        md5(
            COALESCE(to_char(date_start, 'YYYY-MM-DD'), '') || 
            COALESCE(to_char(start_time, 'HH24:MI:SS'), '') || 
            COALESCE(to_char(date_end, 'YYYY-MM-DD'), '') || 
            COALESCE(to_char(end_time, 'HH24:MI:SS'), '') || 
            COALESCE(gross_seats::TEXT, '') ||
            COALESCE(seat_kill::TEXT, '') ||
            COALESCE(est_pct_ticksold::TEXT, '') ||
            COALESCE(net_seats::TEXT, '') ||
            COALESCE(est_sold_seats::TEXT, '') ||
            COALESCE(workforce::TEXT, '') ||
            COALESCE(unticketed::TEXT, '')
        )
    ) STORED,
    
    UNIQUE(event_id, detail_hash)
);

-- Scenario Dimension
CREATE TABLE IF NOT EXISTS dim_scenario (
    scenario_id SERIAL PRIMARY KEY,
    scenario_name VARCHAR(100) NOT NULL,
    scenario_description TEXT,
    created_by VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    category VARCHAR(50), -- e.g., "Base", "High Attendance", "Weather Impact"
    priority INTEGER      -- For ordering scenarios
);

-- Version Dimension with scenario relationship
CREATE TABLE IF NOT EXISTS dim_version (
    version_id SERIAL PRIMARY KEY,
    scenario_id INTEGER REFERENCES dim_scenario(scenario_id),
    version_number INTEGER NOT NULL,
    version_code VARCHAR(50) NOT NULL,
    version_name VARCHAR(100),
    version_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50),
    status VARCHAR(20) DEFAULT 'DRAFT', -- DRAFT, PUBLISHED, ARCHIVED
    
    -- Enforce unique version numbers within each scenario
    UNIQUE(scenario_id, version_number)
);

-- Version-Event Bridge Table
-- This table maps events to versions and scenarios
-- It also stores version-specific metadata and tracks active/inactive events
CREATE TABLE IF NOT EXISTS version_event_bridge (
    bridge_id SERIAL PRIMARY KEY,
    version_id INTEGER REFERENCES dim_version(version_id),
    event_id INTEGER REFERENCES fct_event_base(event_id),
    detail_id INTEGER REFERENCES fct_event_details(detail_id),
    
    -- Additional version-specific metadata 
    is_active BOOLEAN DEFAULT TRUE,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    added_by VARCHAR(50),
    
    -- Each event can only have one set of details in each version
    UNIQUE(version_id, event_id)
);

-- View to simplify querying events by version
-- This view joins the bridge table with the event base and details tables
CREATE OR REPLACE VIEW vw_event_by_version AS
SELECT 
    v.scenario_id,
    v.version_id,
    v.version_code,
    s.scenario_name,
    b.event_id,
    eb.hash_key,
    eb.sport_id,
    eb.venue_id,
    eb.subvenue_id,
    eb.region_id,
    eb.zone_id,
    eb.cluster_id,
    eb.day_id,
    eb.session_id,
    eb.competition_type,
    eb.event_type,
    ed.date_start,
    ed.start_time,
    ed.date_end,
    ed.end_time,
    ed.gross_seats,
    ed.seat_kill,
    ed.est_pct_ticksold,
    ed.net_seats,
    ed.est_sold_seats,
    ed.workforce,
    ed.unticketed,
    ed.additional_attributes,
    b.is_active
FROM 
    version_event_bridge b
JOIN dim_version v ON b.version_id = v.version_id
JOIN dim_scenario s ON v.scenario_id = s.scenario_id
JOIN fct_event_base eb ON b.event_id = eb.event_id
JOIN fct_event_details ed ON b.detail_id = ed.detail_id;

-- Function to process events for a specific version
-- This function will insert or update events and details based on the staging data
-- It will also deactivate events that are not present in the current version
CREATE OR REPLACE FUNCTION process_events_for_version(
    p_scenario_id INTEGER,
    p_version_id INTEGER,
    p_creator VARCHAR(50)
)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER := 0;
    v_version_number INTEGER;
    v_scenario_name VARCHAR(100);
    v_source_version_id INTEGER := NULL;
    v_event_id INTEGER;
    v_detail_id INTEGER;
BEGIN
    -- Get scenario and version info
    SELECT scenario_name INTO v_scenario_name
    FROM dim_scenario
    WHERE scenario_id = p_scenario_id;

    SELECT version_number INTO v_version_number
    FROM dim_version
    WHERE version_id = p_version_id;
    
    -- Process records from staging
    FOR event_rec IN 
        SELECT 
            s.sport_id, v.venue_id, 
            CASE 
                WHEN e.subvenue_name = 'Not Applicable' THEN 99
                ELSE sv.subvenue_id
            END AS subvenue_id, 
            r.region_id, z.zone_id, cl.cluster_id, c.day_id,
            e.session_id, e.competition_type, e.event_type,
            e.date_start, e.start_time, e.date_end, e.end_time,
            e.gross_seats, e.seat_kill, e.est_pct_ticksold, 
            e.net_seats, e.est_sold_seats, e.workforce, e.unticketed, 
            e.additional_attributes,
            md5(
                COALESCE(v.venue_id::TEXT, 'NULL') || '-' ||
                COALESCE(s.sport_id::TEXT, 'NULL') || '-' ||
                COALESCE(c.day_id::TEXT, 'NULL') || '-' ||
                e.session_id::TEXT || '-' ||
                e.competition_type::TEXT || '-' ||
                e.event_type::TEXT || '-' ||
                to_char(e.date_start, 'YYYY-MM-DD')
            ) AS hash_key,
            md5(
                COALESCE(to_char(e.date_start, 'YYYY-MM-DD'), '') || 
                COALESCE(to_char(e.start_time, 'HH24:MI:SS'), '') || 
                COALESCE(to_char(e.date_end, 'YYYY-MM-DD'), '') || 
                COALESCE(to_char(e.end_time, 'HH24:MI:SS'), '') || 
                COALESCE(e.gross_seats::TEXT, '') ||
                COALESCE(e.seat_kill::TEXT, '') ||
                COALESCE(e.est_pct_ticksold::TEXT, '') ||
                COALESCE(e.net_seats::TEXT, '') ||
                COALESCE(e.est_sold_seats::TEXT, '') ||
                COALESCE(e.workforce::TEXT, '') ||
                COALESCE(e.unticketed::TEXT, '')
            ) AS detail_hash
        FROM staging_event_session e
        LEFT JOIN dim_venues v ON e.venue_name = v.venue_name
        LEFT JOIN dim_sports s ON e.sport_discipline = s.sport_discipline
        LEFT JOIN dim_subvenues sv on e.subvenue_name = sv.subvenue_name
            AND v.venue_id = sv.venue_id
        LEFT JOIN dim_regions r on e.region_name = r.region_name
        LEFT JOIN dim_zones z on e.zone_name = z.zone_name
        LEFT JOIN dim_clusters cl on e.cluster_name = cl.cluster_name
        LEFT JOIN dim_calendar c ON e.event_day = c.event_day
            AND e.competition_type = c.competition_type
    LOOP
        -- Check if event exists already
        SELECT event_id INTO v_event_id
        FROM fct_event_base
        WHERE hash_key = event_rec.hash_key;
        
        -- If event doesn't exist, create it
        IF v_event_id IS NULL THEN
            INSERT INTO fct_event_base (
                hash_key, sport_id, venue_id, subvenue_id, region_id, 
                zone_id, cluster_id, day_id, session_id, competition_type, 
                event_type, created_by
            ) VALUES (
                event_rec.hash_key, event_rec.sport_id, event_rec.venue_id, 
                event_rec.subvenue_id, event_rec.region_id, event_rec.zone_id, 
                event_rec.cluster_id, event_rec.day_id, event_rec.session_id, 
                event_rec.competition_type, event_rec.event_type, p_creator
            ) RETURNING event_id INTO v_event_id;
        END IF;
        
        -- Check if this exact detail set already exists
        SELECT detail_id INTO v_detail_id
        FROM fct_event_details
        WHERE event_id = v_event_id
        AND detail_hash = event_rec.detail_hash;
        
        -- If details don't exist, create them
        IF v_detail_id IS NULL THEN
            INSERT INTO event_details (
                event_id, date_start, start_time, date_end, end_time,
                gross_seats, seat_kill, est_pct_ticksold, net_seats,
                est_sold_seats, workforce, unticketed, additional_attributes
            ) VALUES (
                v_event_id, event_rec.date_start, event_rec.start_time, 
                event_rec.date_end, event_rec.end_time, event_rec.gross_seats, 
                event_rec.seat_kill, event_rec.est_pct_ticksold, event_rec.net_seats,
                event_rec.est_sold_seats, event_rec.workforce, event_rec.unticketed,
                event_rec.additional_attributes
            ) RETURNING detail_id INTO v_detail_id;
        END IF;
        
        -- Add or update the bridge record for this version
        INSERT INTO version_event_bridge (
            version_id, event_id, detail_id, is_active, added_by
        ) VALUES (
            p_version_id, v_event_id, v_detail_id, TRUE, p_creator
        )
        ON CONFLICT (version_id, event_id)
        DO UPDATE SET 
            detail_id = EXCLUDED.detail_id,
            is_active = TRUE;
            
        v_count := v_count + 1;
    END LOOP;
    
    -- Optionally, deactivate events not in the current version
    UPDATE version_event_bridge
    SET is_active = FALSE
    WHERE version_id = p_version_id
    AND event_id NOT IN (
        SELECT eb.event_id
        FROM event_base eb
        JOIN staging_event_session e ON 
            md5(
                COALESCE(e.venue_id::TEXT, 'NULL') || '-' ||
                COALESCE(e.sport_id::TEXT, 'NULL') || '-' ||
                COALESCE(e.day_id::TEXT, 'NULL') || '-' ||
                e.session_id::TEXT || '-' ||
                e.competition_type::TEXT || '-' ||
                e.event_type::TEXT || '-' ||
                to_char(e.date_start, 'YYYY-MM-DD')
            ) = eb.hash_key
    );
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Compare two versions and get differences
-- This function will compare two versions and return the differences in event details
-- It will return a table with the event_id, hash_key, change_type, and changed_fields
CREATE OR REPLACE FUNCTION compare_versions(
    p_version_id1 INTEGER,
    p_version_id2 INTEGER
) RETURNS TABLE (
    event_id INTEGER,
    hash_key VARCHAR(100),
    change_type VARCHAR(20),
    changed_fields JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH v1 AS (
        SELECT 
            b.event_id, 
            e.hash_key,
            e.sport_id, e.venue_id, e.subvenue_id, e.region_id, e.zone_id, 
            e.cluster_id, e.day_id, e.session_id, e.competition_type, e.event_type,
            d.date_start, d.start_time, d.end_time, d.date_end, 
            d.gross_seats, d.seat_kill, d.est_pct_ticksold, d.net_seats, 
            d.est_sold_seats, d.workforce, d.unticketed,
            d.detail_id
        FROM version_event_bridge b
        JOIN fct_event_base e ON b.event_id = e.event_id
        JOIN fct_event_details d ON b.detail_id = d.detail_id
        WHERE b.version_id = p_version_id1
        AND b.is_active = TRUE
    ),
    v2 AS (
        SELECT 
            b.event_id, 
            e.hash_key,
            e.sport_id, e.venue_id, e.subvenue_id, e.region_id, e.zone_id, 
            e.cluster_id, e.day_id, e.session_id, e.competition_type, e.event_type,
            d.date_start, d.start_time, d.end_time, d.date_end, 
            d.gross_seats, d.seat_kill, d.est_pct_ticksold, d.net_seats, 
            d.est_sold_seats, d.workforce, d.unticketed,
            d.detail_id
        FROM version_event_bridge b
        JOIN fct_event_base e ON b.event_id = e.event_id
        JOIN fct_event_details d ON b.detail_id = d.detail_id
        WHERE b.version_id = p_version_id2
        AND b.is_active = TRUE
    )
    SELECT 
        COALESCE(v1.event_id, v2.event_id),
        COALESCE(v1.hash_key, v2.hash_key),
        CASE 
            WHEN v1.event_id IS NULL THEN 'ADDED' 
            WHEN v2.event_id IS NULL THEN 'REMOVED'
            WHEN v1.detail_id != v2.detail_id THEN 'MODIFIED'
            ELSE 'UNCHANGED'
        END AS change_type,
        CASE
            WHEN v1.event_id IS NULL OR v2.event_id IS NULL THEN NULL
            WHEN v1.detail_id = v2.detail_id THEN NULL
            ELSE jsonb_strip_nulls(jsonb_build_object(
                'date_start', CASE WHEN v1.date_start IS DISTINCT FROM v2.date_start THEN jsonb_build_object('old', v1.date_start, 'new', v2.date_start) ELSE NULL END,
                'start_time', CASE WHEN v1.start_time IS DISTINCT FROM v2.start_time THEN jsonb_build_object('old', v1.start_time, 'new', v2.start_time) ELSE NULL END,
                'date_end', CASE WHEN v1.date_end IS DISTINCT FROM v2.date_end THEN jsonb_build_object('old', v1.date_end, 'new', v2.date_end) ELSE NULL END,
                'end_time', CASE WHEN v1.end_time IS DISTINCT FROM v2.end_time THEN jsonb_build_object('old', v1.end_time, 'new', v2.end_time) ELSE NULL END,
                'gross_seats', CASE WHEN v1.gross_seats IS DISTINCT FROM v2.gross_seats THEN jsonb_build_object('old', v1.gross_seats, 'new', v2.gross_seats) ELSE NULL END,
                'seat_kill', CASE WHEN v1.seat_kill IS DISTINCT FROM v2.seat_kill THEN jsonb_build_object('old', v1.seat_kill, 'new', v2.seat_kill) ELSE NULL END,
                'est_pct_ticksold', CASE WHEN v1.est_pct_ticksold IS DISTINCT FROM v2.est_pct_ticksold THEN jsonb_build_object('old', v1.est_pct_ticksold, 'new', v2.est_pct_ticksold) ELSE NULL END,
                'net_seats', CASE WHEN v1.net_seats IS DISTINCT FROM v2.net_seats THEN jsonb_build_object('old', v1.net_seats, 'new', v2.net_seats) ELSE NULL END,
                'est_sold_seats', CASE WHEN v1.est_sold_seats IS DISTINCT FROM v2.est_sold_seats THEN jsonb_build_object('old', v1.est_sold_seats, 'new', v2.est_sold_seats) ELSE NULL END,
                'workforce', CASE WHEN v1.workforce IS DISTINCT FROM v2.workforce THEN jsonb_build_object('old', v1.workforce, 'new', v2.workforce) ELSE NULL END,
                'unticketed', CASE WHEN v1.unticketed IS DISTINCT FROM v2.unticketed THEN jsonb_build_object('old', v1.unticketed, 'new', v2.unticketed) ELSE NULL END
            ))
        END
    FROM v1
    FULL OUTER JOIN v2 ON v1.event_id = v2.event_id
    WHERE v1.event_id IS NULL OR v2.event_id IS NULL OR v1.detail_id != v2.detail_id;
END;
$$ LANGUAGE plpgsql;