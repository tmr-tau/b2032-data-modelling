-- Table: test.dim_sports
-- Sport
CREATE TABLE IF NOT EXISTS test.dim_sports (
    sport_id SERIAL PRIMARY KEY,
    sport_discipline VARCHAR(255) NOT NULL,
    venue_id INTEGER REFERENCES test.dim_venues(venue_id),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

CREATE OR REPLACE FUNCTION test.update_dim_sports()
RETURNS TABLE (expired_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_sports AS (
        SELECT DISTINCT sport_discipline, venue_name
        FROM test.staging_event_session
    ),
    sports_to_expire AS (
        UPDATE test.dim_sports s
        SET 
            is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP - INTERVAL '1 microsecond'
        WHERE s.is_current = TRUE
        AND NOT EXISTS (
            SELECT 1 
            FROM staging_sports ss
            JOIN test.dim_venues v ON ss.venue_name = v.venue_name AND v.is_current = TRUE
            WHERE ss.sport_discipline = s.sport_discipline AND v.venue_id = s.venue_id
        )
        RETURNING s.sport_id
    ),
    insert_new_sports AS (
        INSERT INTO test.dim_sports (
            sport_discipline,
            venue_id,
            is_current,
            valid_from,
            valid_to
        )
        SELECT 
            ss.sport_discipline,
            v.venue_id,
            TRUE,
            CURRENT_TIMESTAMP,
            '9999-12-31 23:59:59'::TIMESTAMP
        FROM staging_sports ss
        JOIN test.dim_venues v ON ss.venue_name = v.venue_name AND v.is_current = TRUE
        WHERE NOT EXISTS (
            SELECT 1 
            FROM test.dim_sports s
            WHERE s.sport_discipline = ss.sport_discipline
            AND s.venue_id = v.venue_id
            AND s.is_current = TRUE
        )
        RETURNING sport_id
    )
    SELECT 
        (SELECT COUNT(*) FROM sports_to_expire),
        (SELECT COUNT(*) FROM insert_new_sports)
    INTO expired_count, inserted_count;
    
    RETURN QUERY SELECT expired_count, inserted_count;
END;
$$ LANGUAGE plpgsql;

SELECT test.update_dim_sports();

SELECT * FROM test.dim_sports