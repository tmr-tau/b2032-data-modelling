-- Region
CREATE TABLE IF NOT EXISTS test.dim_regions (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(255) NOT NULL,
    area_sqkm NUMERIC(20, 6),
    length_km NUMERIC(20, 6),
    geometry GEOMETRY,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

CREATE OR REPLACE FUNCTION test.update_dim_regions()
RETURNS TABLE (expired_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_regions AS (
        SELECT DISTINCT region_name
        FROM test.staging_event_session
    ),
    regions_to_expire AS (
        UPDATE test.dim_regions d
        SET 
            is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP - INTERVAL '1 microsecond'
        WHERE d.is_current = TRUE
        AND d.region_name NOT IN (SELECT region_name FROM staging_regions)
        RETURNING d.region_id
    ),
    insert_new_regions AS (
        INSERT INTO test.dim_regions (
            region_name,
            is_current,
            valid_from,
            valid_to
        )
        SELECT 
            s.region_name,
            TRUE,
            CURRENT_TIMESTAMP,
            '9999-12-31 23:59:59'::TIMESTAMP
        FROM staging_regions s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM test.dim_regions d
            WHERE d.region_name = s.region_name
            AND d.is_current = TRUE
        )
        RETURNING region_id
    )
    SELECT 
        (SELECT COUNT(*) FROM regions_to_expire),
        (SELECT COUNT(*) FROM insert_new_regions)
    INTO expired_count, inserted_count;
    
    RETURN QUERY SELECT expired_count, inserted_count;
END;
$$ LANGUAGE plpgsql;

SELECT test.update_dim_regions();

SELECT * FROM test.dim_regions