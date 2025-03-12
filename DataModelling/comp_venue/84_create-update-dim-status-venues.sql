-- Table: test.dim_status
CREATE TABLE IF NOT EXISTS test.dim_status (
    status_id SERIAL PRIMARY KEY,
    status_name VARCHAR(255) NOT NULL,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

-- Table: test.dim_venues
CREATE TABLE IF NOT EXISTS test.dim_venues (
    venue_id SERIAL PRIMARY KEY,
    venue_name VARCHAR(100) NOT NULL,
    status_id INTEGER REFERENCES test.dim_status(status_id),
    has_subvenue BOOL,
    region_id INTEGER REFERENCES test.dim_regions(region_id),
    zone_id INTEGER REFERENCES test.dim_zones(zone_id),
    cluster_id INTEGER REFERENCES test.dim_clusters(cluster_id),
    latitude NUMERIC (21, 4),
    longitude NUMERIC (21, 4),
    capacity INTEGER,
    geometry GEOMETRY,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL,
    CONSTRAINT venue_date_range CHECK (valid_from < valid_to)
);

CREATE OR REPLACE FUNCTION test.update_dim_venues()
RETURNS TABLE (expired_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_venues AS (
        SELECT DISTINCT venue_name, region_name, zone_name, cluster_name
        FROM test.staging_event_session
    ),
    venues_to_expire AS (
        UPDATE test.dim_venues v
        SET 
            is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP - INTERVAL '1 microsecond'
        WHERE v.is_current = TRUE
        AND NOT EXISTS (
            SELECT 1 
            FROM staging_venues s
            LEFT JOIN test.dim_regions r ON s.region_name = r.region_name 
				AND r.is_current = TRUE
            LEFT JOIN test.dim_zones z ON s.zone_name = z.zone_name 
				AND z.region_id = r.region_id 
				AND z.is_current = TRUE
            LEFT JOIN test.dim_clusters c ON s.cluster_name = c.cluster_name 
				AND c.zone_id = z.zone_id 
				AND c.region_id = r.region_id 
				AND c.is_current = TRUE
            WHERE s.venue_name = v.venue_name 
				AND r.region_id = v.region_id 
				AND z.zone_id = v.zone_id 
				AND c.cluster_id = v.cluster_id
        )
        RETURNING v.venue_id
    ),
    insert_new_venues AS (
        INSERT INTO test.dim_venues (
            venue_name,
            region_id,
            zone_id,
            cluster_id,
            is_current,
            valid_from,
            valid_to
        )
        SELECT 
            s.venue_name,
            r.region_id,
            z.zone_id,
            c.cluster_id,
            TRUE,
            CURRENT_TIMESTAMP,
            '9999-12-31 23:59:59'::TIMESTAMP
        FROM staging_venues s
        LEFT JOIN test.dim_regions r ON s.region_name = r.region_name 
			AND r.is_current = TRUE
        LEFT JOIN test.dim_zones z ON s.zone_name = z.zone_name 
			AND z.region_id = r.region_id 
			AND z.is_current = TRUE
        LEFT JOIN test.dim_clusters c ON s.cluster_name = c.cluster_name 
			AND c.zone_id = z.zone_id 
			AND c.region_id = r.region_id 
			AND c.is_current = TRUE
        WHERE NOT EXISTS (
            SELECT 1 
            FROM test.dim_venues v
            WHERE v.venue_name = s.venue_name
				AND v.region_id = r.region_id
				AND v.zone_id = z.zone_id
				AND v.cluster_id = c.cluster_id
				AND v.is_current = TRUE
        )
        RETURNING venue_id
    )
    SELECT 
        (SELECT COUNT(*) FROM venues_to_expire),
        (SELECT COUNT(*) FROM insert_new_venues)
    INTO expired_count, inserted_count;
    
    RETURN QUERY SELECT expired_count, inserted_count;
END;
$$ LANGUAGE plpgsql;

SELECT test.update_dim_venues();

SELECT * FROM test.dim_venues;