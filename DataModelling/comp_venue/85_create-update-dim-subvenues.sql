-- Sub-Venue
CREATE TABLE IF NOT EXISTS test.dim_subvenues (
    subvenue_id SERIAL PRIMARY KEY,
    subvenue_name VARCHAR(100) NOT NULL,
    venue_id INTEGER REFERENCES test.dim_venues(venue_id),
    capacity INTEGER,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL,
    CONSTRAINT subvenue_date_range CHECK (valid_from < valid_to)
);

CREATE OR REPLACE FUNCTION test.update_dim_subvenues()
RETURNS TABLE (expired_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_subvenues AS (
        SELECT DISTINCT subvenue_name, venue_name
        FROM test.staging_event_session
    ),
    subvenues_to_expire AS (
        UPDATE test.dim_subvenues s
        SET 
            is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP - INTERVAL '1 microsecond'
        WHERE s.is_current = TRUE
        AND NOT EXISTS (
            SELECT 1 
            FROM staging_subvenues ss
            LEFT JOIN test.dim_venues v ON ss.venue_name = v.venue_name AND v.is_current = TRUE
            WHERE ss.subvenue_name = s.subvenue_name AND v.venue_id = s.venue_id
        )
        RETURNING s.subvenue_id
    ),
    insert_new_subvenues AS (
        INSERT INTO test.dim_subvenues (
            subvenue_name,
            venue_id,
            is_current,
            valid_from,
            valid_to
        )
        SELECT 
            ss.subvenue_name,
            v.venue_id,
            TRUE,
            CURRENT_TIMESTAMP,
            '9999-12-31 23:59:59'::TIMESTAMP
        FROM staging_subvenues ss
        LEFT JOIN test.dim_venues v ON ss.venue_name = v.venue_name AND v.is_current = TRUE
        WHERE NOT EXISTS (
            SELECT 1 
            FROM test.dim_subvenues s
            WHERE s.subvenue_name = ss.subvenue_name
            AND s.venue_id = v.venue_id
            AND s.is_current = TRUE
        )
        RETURNING subvenue_id
    )
    SELECT 
        (SELECT COUNT(*) FROM subvenues_to_expire),
        (SELECT COUNT(*) FROM insert_new_subvenues)
    INTO expired_count, inserted_count;
    
    RETURN QUERY SELECT expired_count, inserted_count;
END;
$$ LANGUAGE plpgsql;

SELECT test.update_dim_subvenues();

SELECT * FROM test.dim_subvenues;