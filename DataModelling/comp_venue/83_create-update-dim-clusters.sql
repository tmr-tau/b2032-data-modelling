-- Table: test.dim_clusters
-- Cluster
CREATE TABLE IF NOT EXISTS test.dim_clusters (
    cluster_id SERIAL PRIMARY KEY,
    cluster_name VARCHAR(255) NOT NULL,
    region_id INTEGER REFERENCES test.dim_regions(region_id),
    zone_id INTEGER REFERENCES test.dim_zones(zone_id),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);
	
CREATE OR REPLACE FUNCTION test.update_dim_clusters()
RETURNS TABLE (expired_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_clusters AS (
        SELECT DISTINCT cluster_name, zone_name, region_name
        FROM test.staging_event_session
    ),
    clusters_to_expire AS (
        UPDATE test.dim_clusters c
        SET 
            is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP - INTERVAL '1 microsecond'
        WHERE c.is_current = TRUE
        AND NOT EXISTS (
            SELECT 1 
            FROM staging_clusters s
            LEFT JOIN test.dim_regions r ON s.region_name = r.region_name AND r.is_current = TRUE
            LEFT JOIN test.dim_zones z ON s.zone_name = z.zone_name AND z.region_id = r.region_id AND z.is_current = TRUE
            WHERE s.cluster_name = c.cluster_name AND r.region_id = c.region_id AND z.zone_id = c.zone_id
        )
        RETURNING c.cluster_id
    ),
    insert_new_clusters AS (
        INSERT INTO test.dim_clusters (
            cluster_name,
            region_id,
            zone_id,
            is_current,
            valid_from,
            valid_to
        )
        SELECT 
            s.cluster_name,
            r.region_id,
            z.zone_id,
            TRUE,
            CURRENT_TIMESTAMP,
            '9999-12-31 23:59:59'::TIMESTAMP
        FROM staging_clusters s
        LEFT JOIN test.dim_regions r ON s.region_name = r.region_name AND r.is_current = TRUE
        LEFT JOIN test.dim_zones z ON s.zone_name = z.zone_name AND z.region_id = r.region_id AND z.is_current = TRUE
        WHERE NOT EXISTS (
            SELECT 1 
            FROM test.dim_clusters c
            WHERE c.cluster_name = s.cluster_name
            AND c.region_id = r.region_id
            AND c.zone_id = z.zone_id
            AND c.is_current = TRUE
        )
        RETURNING cluster_id
    )
    SELECT 
        (SELECT COUNT(*) FROM clusters_to_expire),
        (SELECT COUNT(*) FROM insert_new_clusters)
    INTO expired_count, inserted_count;
    
    RETURN QUERY SELECT expired_count, inserted_count;
END;
$$ LANGUAGE plpgsql;

SELECT test.update_dim_clusters();

SELECT * FROM test.dim_clusters