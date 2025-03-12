-- Table: test.dim_calendar

CREATE TABLE IF NOT EXISTS test.dim_calendar (
    day_id SERIAL PRIMARY KEY,
    event_day INTEGER NOT NULL,
    model_date DATE,
    actual_date DATE NOT NULL,
    day_of_week VARCHAR(50) NOT NULL,
    competition_type VARCHAR(50) NOT NULL,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL
);

CREATE OR REPLACE FUNCTION test.update_dim_calendar()
RETURNS TABLE (expired_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_calendar AS (
        SELECT DISTINCT
            event_day,
            date_start AS actual_date,
            competition_type
        FROM test.staging_event_session
    ),
    calendar_to_expire AS (
        UPDATE test.dim_calendar c
        SET 
            is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP - INTERVAL '1 microsecond'
        WHERE c.is_current = TRUE
        AND NOT EXISTS (
            SELECT 1 
            FROM staging_calendar sc
            WHERE sc.event_day = c.event_day
            AND sc.competition_type = c.competition_type
            AND sc.actual_date = c.actual_date
        )
        RETURNING c.day_id
    ),
    insert_new_calendar AS (
        INSERT INTO test.dim_calendar (
            event_day,
--             model_date,
            actual_date,
            day_of_week,
            competition_type,
            is_current,
            valid_from,
            valid_to
        )
        SELECT 
            sc.event_day,
--             sc.model_date,
            sc.actual_date,
            TO_CHAR(sc.actual_date, 'Day') AS day_of_week,
            sc.competition_type,
            TRUE,
            CURRENT_TIMESTAMP,
            '9999-12-31 23:59:59'::TIMESTAMP
        FROM staging_calendar sc
        WHERE NOT EXISTS (
            SELECT 1 
            FROM test.dim_calendar c
            WHERE c.event_day = sc.event_day
            AND c.competition_type = sc.competition_type
            AND c.actual_date = sc.actual_date
            AND c.is_current = TRUE
        )
        RETURNING day_id
    )
    SELECT 
        (SELECT COUNT(*) FROM calendar_to_expire),
        (SELECT COUNT(*) FROM insert_new_calendar)
    INTO expired_count, inserted_count;
    
    RETURN QUERY SELECT expired_count, inserted_count;
END;
$$ LANGUAGE plpgsql;

SELECT test.update_dim_calendar();

SELECT * FROM test.dim_calendar