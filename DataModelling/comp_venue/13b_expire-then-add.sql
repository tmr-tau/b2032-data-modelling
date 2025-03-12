/**********************************************************
** OPTION 2: Flag existing events and insert new records **
***********************************************************/
-- Main event session processing function
CREATE OR REPLACE FUNCTION process_staging_event_session(new_version_number VARCHAR(50))
RETURNS VOID AS $$
DECLARE
    new_version_id VARCHAR(10);
    changed_count INTEGER;
BEGIN
    -- Step 1: Create new schedule version
    INSERT INTO test_comp_venue.dim_schedule_version (version_id, valid_from, valid_to)
    VALUES (new_version_number, CURRENT_TIMESTAMP, '9999-12-31 23:59:59')
    RETURNING version_id INTO new_version_id;

    -- Step 2: Identify new records
    WITH staging_to_fct AS (
        SELECT 
            s.sport_id, v.venue_id, c.day_id,
            e.session_id, e.date_start, e.start_time, e.date_end, e.end_time,
            e.competition_type, e.event_type, e.gross_seats, e.seat_kill, e.est_pct_ticksold, 
            e.net_seats, e.est_sold_seats, e.workforce, e.unticketed, e.additional_attributes
        FROM test_comp_venue.staging_event_session e
        LEFT JOIN test_comp_venue.dim_venues v ON e.venue_name = v.venue_name
        LEFT JOIN test_comp_venue.dim_sports s ON e.sport_discipline = s.sport_discipline
        LEFT JOIN test_comp_venue.dim_calendar c ON e.event_day = c.event_day
    ),
    changed_events AS (
        SELECT 
            md5(
                s.venue_id::TEXT || '-' ||
                s.sport_id::TEXT || '-' ||
                s.day_id::TEXT || '-' ||
                s.session_id::TEXT || '-' ||
                s.competition_type::TEXT || '-' ||
                s.event_type::TEXT || '-' ||
                to_char(s.date_start, 'YYYY-MM-DD')
            ) AS hash_key,
            s.*,
            e.record_id AS old_record_id,
            e.version_array AS old_version_array,
            CASE 
                WHEN e.sport_id IS NULL THEN 'INSERT'
                ELSE 'UPDATE'
            END AS change_type
        FROM staging_to_fct s
        LEFT JOIN test_comp_venue.fct_event_session e ON 
            md5(
                s.venue_id::TEXT || '-' ||
                s.sport_id::TEXT || '-' ||
                s.day_id::TEXT || '-' ||
                s.session_id::TEXT || '-' ||
                s.competition_type::TEXT || '-' ||
                s.event_type::TEXT || '-' ||
                to_char(s.date_start, 'YYYY-MM-DD')
            ) = e.hash_key AND e.is_current = TRUE
        WHERE  e.venue_id IS NULL OR
              (e.start_time != s.start_time OR
               e.date_end != s.date_end OR
               e.end_time != s.end_time OR
               e.gross_seats != s.gross_seats OR
               e.seat_kill != s.seat_kill OR
               e.est_pct_ticksold != s.est_pct_ticksold OR
               e.net_seats != s.net_seats OR
               e.est_sold_seats != s.est_sold_seats OR
               e.workforce != s.workforce OR
			   e.unticketed != s.unticketed)
    )
    -- Step 3: Insert new records for changed events
    , inserted_events AS (
        INSERT INTO test_comp_venue.fct_event_session (
            hash_key, schedule_version_id, current_version_id, version_array, sport_id, venue_id, day_id, session_id, 
            date_start, start_time, date_end, end_time, competition_type, event_type, 
            gross_seats, seat_kill, est_pct_ticksold, net_seats, est_sold_seats, workforce, unticketed,
            valid_from, valid_to, is_current
        )
        SELECT 
            hash_key, new_version_id, new_version_id, 
            CASE 
                WHEN change_type = 'UPDATE' THEN 
                    array_append(old_version_array, new_version_id)
                ELSE 
                    ARRAY[new_version_id] 
            END,
            sport_id, venue_id, day_id, session_id,
            date_start, start_time, date_end, end_time, competition_type, event_type, 
            gross_seats, seat_kill, est_pct_ticksold, net_seats, est_sold_seats, workforce, unticketed,
            CURRENT_TIMESTAMP, '9999-12-31 23:59:59'::TIMESTAMP, TRUE
        FROM changed_events
        RETURNING *
    )
    -- Step 4: Update is_current for old records of modified events
    , update_old_records AS (
        UPDATE test_comp_venue.fct_event_session f
        SET is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP
        FROM changed_events c
        WHERE f.record_id = c.old_record_id
          AND c.change_type = 'UPDATE'
    )
    -- Step 5: Handle deleted events
    , handle_deleted AS (
        UPDATE test_comp_venue.fct_event_session f
        SET is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP,
            current_version_id = new_version_id,
            version_array = array_append(f.version_array, new_version_id)
        WHERE f.is_current = TRUE
        AND NOT EXISTS (
            SELECT 1
            FROM staging_to_fct s
            WHERE f.hash_key = md5(
                s.venue_id::TEXT || '-' ||
                s.sport_id::TEXT || '-' ||
                s.day_id::TEXT || '-' ||
                s.session_id::TEXT || '-' ||
                s.competition_type::TEXT || '-' ||
                s.event_type::TEXT || '-' ||
                to_char(s.date_start, 'YYYY-MM-DD')
            )
        )
    )
    -- Step 6: Update current_version_id and version_array for unchanged records
    , update_unchanged AS (
        UPDATE test_comp_venue.fct_event_session f
        SET current_version_id = new_version_id,
            version_array = array_append(f.version_array, new_version_id)
        WHERE f.is_current = TRUE
        AND EXISTS (
            SELECT 1
            FROM staging_to_fct s
            WHERE f.hash_key = md5(
                s.venue_id::TEXT || '-' ||
                s.sport_id::TEXT || '-' ||
                s.day_id::TEXT || '-' ||
                s.session_id::TEXT || '-' ||
                s.competition_type::TEXT || '-' ||
                s.event_type::TEXT || '-' ||
                to_char(s.date_start, 'YYYY-MM-DD')
            )
        )
        AND NOT EXISTS (
            SELECT 1
            FROM changed_events c
            WHERE f.hash_key = c.hash_key
        )
    )
    -- Count the number of changed records
    SELECT COUNT(*) INTO changed_count FROM inserted_events;

--     -- Clear the staging table
--     TRUNCATE TABLE test_comp_venue.staging_event_session;

    -- Log the number of changed records
    RAISE NOTICE 'Processed % changed records for version %', changed_count, new_version_number;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS event_changes_trigger ON fct_event_session;

TRUNCATE dim_schedule_version RESTART IDENTITY CASCADE;

-- Insert data for Version 1
INSERT INTO staging_event_session (sport_id, venue_id, day_id, event_date, start_time, end_time, date_start, date_end, event_type, gross_seats, seat_kill, est_ticket_sold, net_seats, est_sold_seats, workforce_count)
VALUES
(101, 201, 1, '2024-07-26', '09:00:00', '11:00:00', '2024-07-26', '2024-07-26', 1, 60000, 0.1, 0.80, 59000, 47200, 500),
(102, 202, 1, '2024-07-26', '14:00:00', '16:00:00', '2024-07-26', '2024-07-26', 1, 10000, 0.2, 0.90, 9800, 8820, 100),
(103, 203, 2, '2024-07-27', '10:00:00', '12:00:00', '2024-07-27', '2024-07-27', 1, 5000, 0.1, 0.75, 4900, 3675, 50);

-- ALTER TABLE fct_event_session ADD CONSTRAINT unique_hash_key UNIQUE (hash_key);

SELECT process_staging_event_session('v1');
SELECT * FROM dim_schedule_version;
SELECT * FROM fct_event_session ORDER BY event_session_id;
SELECT * FROM event_version_changes;

-- Update data for Version 2
INSERT INTO staging_event_session (sport_id, venue_id, day_id, event_date, start_time, end_time, date_start, date_end, event_type, gross_seats, seat_kill, est_ticket_sold, net_seats, est_sold_seats, workforce_count)
VALUES
(101, 201, 1, '2024-07-26', '09:00:00', '11:00:00', '2024-07-26', '2024-07-26', 1, 60000, 0.1, 0.80, 59000, 47200, 500),
(102, 202, 1, '2024-07-26', '14:00:00', '17:00:00', '2024-07-26', '2024-07-26', 1, 10000, 0.2, 0.95, 9800, 9310, 100),
(103, 203, 2, '2024-07-27', '10:00:00', '12:00:00', '2024-07-27', '2024-07-27', 1, 5000, 0.1, 0.75, 4900, 3675, 50);

SELECT process_staging_event_session('v2');
SELECT * FROM dim_schedule_version;
SELECT * FROM fct_event_session ORDER BY event_session_id;
SELECT * FROM event_version_changes;

-- Update data for Version 3
INSERT INTO staging_event_session (sport_id, venue_id, day_id, event_date, start_time, end_time, date_start, date_end, event_type, gross_seats, seat_kill, est_ticket_sold, net_seats, est_sold_seats, workforce_count)
VALUES
(101, 201, 1, '2024-07-26', '09:00:00', '11:00:00', '2024-07-26', '2024-07-26', 1, 60000, 0.1, 0.80, 59000, 47200, 500),
(102, 202, 1, '2024-07-26', '14:00:00', '17:00:00', '2024-07-26', '2024-07-26', 1, 10000, 0.2, 0.95, 9800, 9310, 100),
(104, 204, 3, '2024-07-28', '13:00:00', '15:00:00', '2024-07-28', '2024-07-28', 1, 15000, 0.3, 0.7, 14700, 10290, 150);

SELECT process_staging_event_session('v3');
SELECT * FROM dim_schedule_version;
SELECT * FROM fct_event_session ORDER BY event_session_id;
SELECT * FROM event_version_changes;

-- Update data for Version 4
INSERT INTO staging_event_session (sport_id, venue_id, day_id, event_date, start_time, end_time, date_start, date_end, event_type, gross_seats, seat_kill, est_ticket_sold, net_seats, est_sold_seats, workforce_count)
VALUES
(101, 201, 1, '2024-07-26', '09:00:00', '11:00:00', '2024-07-26', '2024-07-26', 1, 60000, 0.1, 0.8, 59000, 47200, 500),
(102, 202, 1, '2024-07-26', '14:00:00', '16:00:00', '2024-07-26', '2024-07-26', 1, 10000, 0.2, 0.9, 9800, 8820, 100),
(103, 203, 2, '2024-07-27', '10:00:00', '12:00:00', '2024-07-27', '2024-07-27', 1, 5000, 0.3, 0.75, 4900, 3675, 50),
(104, 204, 3, '2024-07-28', '13:00:00', '15:00:00', '2024-07-28', '2024-07-28', 1, 15000, 0.25, 0.7, 14700, 10290, 150);

SELECT process_staging_event_session('v4');
SELECT * FROM dim_schedule_version;
SELECT * FROM fct_event_session ORDER BY event_session_id;
SELECT * FROM event_version_changes;

----------------------------------------------------
-- FUNCTIONS TO QUERY FCT_EVENT_SESSION FOR CHANGES
----------------------------------------------------
-- Function to compare changes between two versions
CREATE OR REPLACE FUNCTION get_changes_between_versions(version1 INTEGER, version2 INTEGER)
RETURNS TABLE (
    event_session_id INTEGER,
    change_type TEXT,
    changed_fields JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH v1_events AS (
        SELECT *
        FROM fct_event_session
        WHERE schedule_version_id = version1
    ),
    v2_events AS (
        SELECT *
        FROM fct_event_session
        WHERE schedule_version_id = version2
    )
    SELECT 
        COALESCE(v1.event_session_id, v2.event_session_id),
        CASE 
            WHEN v1.event_session_id IS NULL THEN 'INSERT'
            WHEN v2.event_session_id IS NULL THEN 'DELETE'
            ELSE 'UPDATE'
        END,
        jsonb_strip_nulls(jsonb_build_object(
            'sport_id', CASE WHEN v1.sport_id IS DISTINCT FROM v2.sport_id THEN v2.sport_id ELSE NULL END,
            'venue_id', CASE WHEN v1.venue_id IS DISTINCT FROM v2.venue_id THEN v2.venue_id ELSE NULL END,
            'day_id', CASE WHEN v1.day_id IS DISTINCT FROM v2.day_id THEN v2.day_id ELSE NULL END,
            'event_date', CASE WHEN v1.event_date IS DISTINCT FROM v2.event_date THEN v2.event_date ELSE NULL END,
            'start_time', CASE WHEN v1.start_time IS DISTINCT FROM v2.start_time THEN v2.start_time ELSE NULL END,
            'end_time', CASE WHEN v1.end_time IS DISTINCT FROM v2.end_time THEN v2.end_time ELSE NULL END,
            'date_start', CASE WHEN v1.date_start IS DISTINCT FROM v2.date_start THEN v2.date_start ELSE NULL END,
            'date_end', CASE WHEN v1.date_end IS DISTINCT FROM v2.date_end THEN v2.date_end ELSE NULL END,
            'event_type', CASE WHEN v1.event_type IS DISTINCT FROM v2.event_type THEN v2.event_type ELSE NULL END,
            'gross_seats', CASE WHEN v1.gross_seats IS DISTINCT FROM v2.gross_seats THEN v2.gross_seats ELSE NULL END,
            'seat_kill', CASE WHEN v1.seat_kill IS DISTINCT FROM v2.seat_kill THEN v2.seat_kill ELSE NULL END,
            'est_ticket_sold', CASE WHEN v1.est_ticket_sold IS DISTINCT FROM v2.est_ticket_sold THEN v2.est_ticket_sold ELSE NULL END,
            'net_seats', CASE WHEN v1.net_seats IS DISTINCT FROM v2.net_seats THEN v2.net_seats ELSE NULL END,
            'est_sold_seats', CASE WHEN v1.est_sold_seats IS DISTINCT FROM v2.est_sold_seats THEN v2.est_sold_seats ELSE NULL END,
            'workforce_count', CASE WHEN v1.workforce_count IS DISTINCT FROM v2.workforce_count THEN v2.workforce_count ELSE NULL END
        ))
    FROM v1_events v1
    FULL OUTER JOIN v2_events v2 ON v1.hash_key = v2.hash_key
    WHERE v1.event_session_id IS NULL OR v2.event_session_id IS NULL OR v1 IS DISTINCT FROM v2;
END;
$$ LANGUAGE plpgsql;

-- Function to get changes within a date range
CREATE OR REPLACE FUNCTION get_changes_in_date_range(start_date DATE, end_date DATE)
RETURNS TABLE (
    event_session_id INTEGER,
    change_type TEXT,
    changed_fields JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH date_range_events AS (
        SELECT DISTINCT ON (hash_key) *
        FROM fct_event_session
        WHERE valid_from BETWEEN start_date AND end_date
        ORDER BY hash_key, valid_from DESC
    ),
    previous_events AS (
        SELECT DISTINCT ON (hash_key) *
        FROM fct_event_session
        WHERE valid_from < start_date
        ORDER BY hash_key, valid_from DESC
    )
    SELECT 
        COALESCE(p.event_session_id, d.event_session_id),
        CASE 
            WHEN p.event_session_id IS NULL THEN 'INSERT'
            WHEN d.event_session_id IS NULL THEN 'DELETE'
            ELSE 'UPDATE'
        END,
        jsonb_strip_nulls(jsonb_build_object(
            'sport_id', CASE WHEN p.sport_id IS DISTINCT FROM d.sport_id THEN d.sport_id ELSE NULL END,
            'venue_id', CASE WHEN p.venue_id IS DISTINCT FROM d.venue_id THEN d.venue_id ELSE NULL END,
            'day_id', CASE WHEN p.day_id IS DISTINCT FROM d.day_id THEN d.day_id ELSE NULL END,
            'event_date', CASE WHEN p.event_date IS DISTINCT FROM d.event_date THEN d.event_date ELSE NULL END,
            'start_time', CASE WHEN p.start_time IS DISTINCT FROM d.start_time THEN d.start_time ELSE NULL END,
            'end_time', CASE WHEN p.end_time IS DISTINCT FROM d.end_time THEN d.end_time ELSE NULL END,
            'date_start', CASE WHEN p.date_start IS DISTINCT FROM d.date_start THEN d.date_start ELSE NULL END,
            'date_end', CASE WHEN p.date_end IS DISTINCT FROM d.date_end THEN d.date_end ELSE NULL END,
            'event_type', CASE WHEN p.event_type IS DISTINCT FROM d.event_type THEN d.event_type ELSE NULL END,
            'gross_seats', CASE WHEN p.gross_seats IS DISTINCT FROM d.gross_seats THEN d.gross_seats ELSE NULL END,
            'seat_kill', CASE WHEN p.seat_kill IS DISTINCT FROM d.seat_kill THEN d.seat_kill ELSE NULL END,
            'est_ticket_sold', CASE WHEN p.est_ticket_sold IS DISTINCT FROM d.est_ticket_sold THEN d.est_ticket_sold ELSE NULL END,
            'net_seats', CASE WHEN p.net_seats IS DISTINCT FROM d.net_seats THEN d.net_seats ELSE NULL END,
            'est_sold_seats', CASE WHEN p.est_sold_seats IS DISTINCT FROM d.est_sold_seats THEN d.est_sold_seats ELSE NULL END,
            'workforce_count', CASE WHEN p.workforce_count IS DISTINCT FROM d.workforce_count THEN d.workforce_count ELSE NULL END
        ))
    FROM previous_events p
    FULL OUTER JOIN date_range_events d ON p.hash_key = d.hash_key
    WHERE p.event_session_id IS NULL OR d.event_session_id IS NULL OR p IS DISTINCT FROM d;
END;
$$ LANGUAGE plpgsql;

-- Example usage of the functions:

-- To get changes between version 1 and version 4:
SELECT * FROM get_changes_between_versions(1, 4);

-- To get changes between 2023-11-30 and 2024-01-06:
SELECT * FROM get_changes_in_date_range('2023-11-30'::DATE, '2024-08-12'::DATE);

/******************/
-- Main event session processing function
CREATE OR REPLACE FUNCTION process_staging_event_session(new_version_number VARCHAR(50))
RETURNS VOID AS $$
DECLARE
    new_version_id VARCHAR(10);
    changed_count INTEGER;
BEGIN
    -- Step 1: Create new schedule version
    INSERT INTO test_comp_venue.dim_schedule_version (version_id, valid_from, valid_to)
    VALUES (new_version_number, CURRENT_TIMESTAMP, '9999-12-31 23:59:59')
    RETURNING version_id INTO new_version_id;

    -- Step 2: Identify changed records
    WITH staging_to_fct AS (
        SELECT 
            s.sport_id, v.venue_id, c.day_id,
            e.session_id, e.date_start, e.start_time, e.date_end, e.end_time,
            e.competition_type, e.event_type, e.gross_seats, e.seat_kill, e.est_pct_ticksold, 
            e.net_seats, e.est_sold_seats, e.workforce, e.unticketed, e.additional_attributes
        FROM test_comp_venue.staging_event_session e
        LEFT JOIN test_comp_venue.dim_venues v ON e.venue_name = v.venue_name
        LEFT JOIN test_comp_venue.dim_sports s ON e.sport_discipline = s.sport_discipline
        LEFT JOIN test_comp_venue.dim_calendar c ON e.event_day = c.day_id
    ),
    changed_events AS (
        SELECT 
            s.*,
            f.version_array AS old_version_array,
            CASE 
                WHEN f.record_id IS NULL THEN 'INSERT'
                ELSE 'UPDATE'
            END AS change_type
        FROM staging_to_fct s
        LEFT JOIN test_comp_venue.fct_event_session f ON 
            f.sport_id = s.sport_id AND
            f.venue_id = s.venue_id AND
            f.day_id = s.day_id AND
            f.session_id = s.session_id AND
            f.competition_type = s.competition_type AND
            f.event_type = s.event_type AND
            f.date_start = s.date_start AND
            f.is_current = TRUE
        WHERE f.record_id IS NULL OR  -- New events
              (f.start_time != s.start_time OR 
               f.end_time != s.end_time OR
               f.date_end != s.date_end OR
               f.gross_seats != s.gross_seats OR
               f.seat_kill != s.seat_kill OR
               f.est_pct_ticksold != s.est_pct_ticksold OR
               f.net_seats != s.net_seats OR
               f.est_sold_seats != s.est_sold_seats OR
               f.workforce != s.workforce OR
               f.unticketed != s.unticketed)
    )
    -- Step 3: Insert new records for changed events
    , inserted_events AS (
        INSERT INTO test_comp_venue.fct_event_session (
            schedule_version_id, current_version_id, version_array, sport_id, venue_id, day_id, 
            session_id, date_start, start_time, date_end, end_time, competition_type, event_type, 
            gross_seats, seat_kill, est_pct_ticksold, net_seats, est_sold_seats, workforce, unticketed,
            valid_from, valid_to, is_current, additional_attributes
        )
        SELECT 
            new_version_id, new_version_id, 
            CASE 
                WHEN change_type = 'UPDATE' THEN 
                    array_append(old_version_array, new_version_id)
                ELSE 
                    ARRAY[new_version_id] 
            END,
            sport_id, venue_id, day_id, 
            session_id, date_start, start_time, date_end, end_time, competition_type, event_type, 
            gross_seats, seat_kill, est_pct_ticksold, net_seats, est_sold_seats, workforce, unticketed,
            CURRENT_TIMESTAMP, '9999-12-31 23:59:59'::TIMESTAMP, TRUE, additional_attributes
        FROM changed_events
        RETURNING *
    )
    -- Step 4: Update is_current for old records of modified events
    , update_old_records AS (
        UPDATE test_comp_venue.fct_event_session f
        SET is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP
        FROM changed_events c
        WHERE f.record_id = c.old_record_id
          AND c.change_type = 'UPDATE'
    )
    -- Step 5: Handle deleted events
    , handle_deleted AS (
        UPDATE test_comp_venue.fct_event_session f
        SET is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP,
            current_version_id = new_version_id,
            version_array = array_append(f.version_array, new_version_id)
        WHERE f.is_current = TRUE
        AND NOT EXISTS (
            SELECT 1
            FROM staging_to_fct s
            WHERE f.sport_id = s.sport_id
              AND f.venue_id = s.venue_id
              AND f.day_id = s.day_id
              AND f.session_id = s.session_id
              AND f.competition_type = s.competition_type
              AND f.event_type = s.event_type
              AND f.date_start = s.date_start
        )
    )
    -- Step 6: Update current_version_id and version_array for unchanged records
    , update_unchanged AS (
        UPDATE test_comp_venue.fct_event_session f
        SET current_version_id = new_version_id,
            version_array = array_append(f.version_array, new_version_id)
        WHERE f.is_current = TRUE
        AND EXISTS (
            SELECT 1
            FROM staging_to_fct s
            WHERE f.sport_id = s.sport_id
              AND f.venue_id = s.venue_id
              AND f.day_id = s.day_id
              AND f.session_id = s.session_id
              AND f.event_type = s.event_type
              AND f.competition_type = s.competition_type
              AND f.date_start = s.date_start
        )
        AND NOT EXISTS (
            SELECT 1
            FROM changed_events c
            WHERE f.record_id = c.old_record_id
        )
    )
    -- Count the number of changed records
    SELECT COUNT(*) INTO changed_count FROM inserted_events;

    -- Log the number of changed records
    RAISE NOTICE 'Processed % changed records for version %', changed_count, new_version_number;
END;
$$ LANGUAGE plpgsql;