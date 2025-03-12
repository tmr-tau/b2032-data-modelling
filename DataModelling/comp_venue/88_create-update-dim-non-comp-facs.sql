-- None Competition Facilities
CREATE TABLE IF NOT EXISTS test.dim_non_comp_facs (
    nc_fac_id SERIAL PRIMARY KEY,
    nc_fac_name VARCHAR(255) NOT NULL,
	nc_fac_code VARCHAR(10),
	details VARCHAR(255),
	status_id INTEGER REFERENCES test.dim_status(status_id),
    region_id INTEGER REFERENCES test.dim_regions(region_id),
    latitude NUMERIC (21, 4),
    longitude NUMERIC (21, 4),
    geometry GEOMETRY,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

CREATE OR REPLACE FUNCTION test.update_dim_non_comp_facs()
RETURNS TABLE (expired_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_non_comp_facs AS (
        SELECT DISTINCT
            nc_fac_name,
            nc_fac_code,
            details,
            status,
            region_name,
            latitude,
            longitude
        FROM test.staging_event_session -- Adjust this if your staging table is different
    ),
    non_comp_facs_to_expire AS (
        UPDATE test.dim_non_comp_facs ncf
        SET 
            is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP - INTERVAL '1 microsecond'
        WHERE ncf.is_current = TRUE
        AND NOT EXISTS (
            SELECT 1 
            FROM staging_non_comp_facs snc
            JOIN test.dim_status s ON snc.status = s.status AND s.is_current = TRUE
            JOIN test.dim_regions r ON snc.region_name = r.region_name AND r.is_current = TRUE
            WHERE snc.nc_fac_name = ncf.nc_fac_name
              AND snc.nc_fac_code = ncf.nc_fac_code
              AND COALESCE(snc.details, '') = COALESCE(ncf.details, '')
              AND s.status_id = ncf.status_id
              AND r.region_id = ncf.region_id
              AND snc.latitude = ncf.latitude
              AND snc.longitude = ncf.longitude
        )
        RETURNING ncf.nc_fac_id
    ),
    insert_new_non_comp_facs AS (
        INSERT INTO test.dim_non_comp_facs (
            nc_fac_name,
            nc_fac_code,
            details,
            status_id,
            region_id,
            latitude,
            longitude,
            geometry,
            is_current,
            valid_from,
            valid_to
        )
        SELECT 
            snc.nc_fac_name,
            snc.nc_fac_code,
            snc.details,
            s.status_id,
            r.region_id,
            snc.latitude,
            snc.longitude,
            ST_SetSRID(ST_MakePoint(snc.longitude, snc.latitude), 4326),
            TRUE,
            CURRENT_TIMESTAMP,
            '9999-12-31 23:59:59'::TIMESTAMP
        FROM staging_non_comp_facs snc
        JOIN test.dim_status s ON snc.status = s.status AND s.is_current = TRUE
        JOIN test.dim_regions r ON snc.region_name = r.region_name AND r.is_current = TRUE
        WHERE NOT EXISTS (
            SELECT 1 
            FROM test.dim_non_comp_facs ncf
            WHERE ncf.nc_fac_name = snc.nc_fac_name
              AND ncf.nc_fac_code = snc.nc_fac_code
              AND COALESCE(ncf.details, '') = COALESCE(snc.details, '')
              AND ncf.status_id = s.status_id
              AND ncf.region_id = r.region_id
              AND ncf.latitude = snc.latitude
              AND ncf.longitude = snc.longitude
              AND ncf.is_current = TRUE
        )
        RETURNING nc_fac_id
    )
    SELECT 
        (SELECT COUNT(*) FROM non_comp_facs_to_expire),
        (SELECT COUNT(*) FROM insert_new_non_comp_facs)
    INTO expired_count, inserted_count;
    
    RETURN QUERY SELECT expired_count, inserted_count;
END;
$$ LANGUAGE plpgsql;

SELECT test.update_dim_non_comp_facs();

SELECT * FROM test.dim_non_comp_facs