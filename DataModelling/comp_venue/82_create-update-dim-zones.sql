-- Table: test.dim_zones
CREATE TABLE IF NOT EXISTS test.dim_zones (
    zone_id SERIAL PRIMARY KEY,
    zone_name VARCHAR(255) NOT NULL,
    region_id INTEGER REFERENCES test.dim_regions(region_id),
    geometry GEOMETRY,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

-- Function: test.update_dim_zones
CREATE OR REPLACE FUNCTION test.update_dim_zones()
RETURNS TABLE (expired_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_zones AS (
        SELECT DISTINCT zone_name, region_name
        FROM test.staging_event_session
    ),
    zones_to_expire AS (
        UPDATE test.dim_zones z
        SET 
            is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP - INTERVAL '1 microsecond'
        WHERE z.is_current = TRUE
        AND NOT EXISTS (
            SELECT 1 
            FROM staging_zones s
            LEFT JOIN test.dim_regions r ON s.region_name = r.region_name AND r.is_current = TRUE
            WHERE s.zone_name = z.zone_name AND r.region_id = z.region_id
        )
        RETURNING z.zone_id
    ),
    insert_new_zones AS (
        INSERT INTO test.dim_zones (
            zone_name,
            region_id,
            is_current,
            valid_from,
            valid_to
        )
        SELECT 
            s.zone_name,
            r.region_id,
            TRUE,
            CURRENT_TIMESTAMP,
            '9999-12-31 23:59:59'::TIMESTAMP
        FROM staging_zones s
        LEFT JOIN test.dim_regions r ON s.region_name = r.region_name AND r.is_current = TRUE
        WHERE NOT EXISTS (
            SELECT 1 
            FROM test.dim_zones z
            WHERE z.zone_name = s.zone_name
            AND z.region_id = r.region_id
            AND z.is_current = TRUE
        )
        RETURNING zone_id
    )
    SELECT 
        (SELECT COUNT(*) FROM zones_to_expire),
        (SELECT COUNT(*) FROM insert_new_zones)
    INTO expired_count, inserted_count;
    
    RETURN QUERY SELECT expired_count, inserted_count;
END;
$$ LANGUAGE plpgsql;

SELECT test.update_dim_zones();

SELECT * FROM test.dim_zones