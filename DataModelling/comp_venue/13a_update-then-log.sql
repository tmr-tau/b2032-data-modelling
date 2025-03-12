/*****************************************************
** OPTION 1: Update existing events and log changes **
*****************************************************/
-- First, let's create the log_event_change function that will be called by the trigger
CREATE OR REPLACE FUNCTION log_event_change(
    p_event_session_id INTEGER,
    p_schedule_version_id INTEGER,
    p_change_type VARCHAR(10),
    p_changed_fields JSONB,
    p_previous_values JSONB
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO event_version_changes (
        event_session_id, schedule_version_id, change_type, changed_fields, previous_values, change_timestamp
    ) VALUES (
        p_event_session_id, p_schedule_version_id, p_change_type, p_changed_fields, p_previous_values, CURRENT_TIMESTAMP
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION track_event_changes()
RETURNS TRIGGER AS $$
DECLARE
    changed_fields_json JSONB := '{}';
    previous_values_json JSONB := '{}';
    change_type_var VARCHAR(10);
    excluded_columns TEXT[] := ARRAY['event_session_id', 'schedule_version_id', 'current_version_id', 'version_array', 'hash_key', 'valid_from', 'valid_to', 'is_current'];
BEGIN
    IF TG_OP = 'INSERT' THEN
        change_type_var := 'INSERT';
        -- For new inserts, log all non-excluded fields as changed fields
        SELECT jsonb_object_agg(key, value)
        INTO changed_fields_json
        FROM jsonb_each(to_jsonb(NEW))
        WHERE key != ALL(excluded_columns);
    ELSIF TG_OP = 'UPDATE' THEN
        -- Check if this is an update or a soft delete
        IF NEW.is_current = FALSE AND OLD.is_current = TRUE THEN
            change_type_var := 'DELETE';
            -- For deleted events, log all previous non-excluded values
            SELECT jsonb_object_agg(key, value)
            INTO previous_values_json
            FROM jsonb_each(to_jsonb(OLD))
            WHERE key != ALL(excluded_columns);
        ELSE
            change_type_var := 'UPDATE';
            -- For updates, compare OLD and NEW to find changed fields
            SELECT 
                jsonb_object_agg(key, new_value) AS changed_fields,
                jsonb_object_agg(key, old_value) AS previous_values
            INTO changed_fields_json, previous_values_json
            FROM (
                SELECT 
                    key,
                    to_jsonb(NEW)->>key AS new_value,
                    to_jsonb(OLD)->>key AS old_value
                FROM jsonb_object_keys(to_jsonb(NEW)) AS k(key)
                WHERE key != ALL(excluded_columns)
            ) AS changes
            WHERE new_value IS DISTINCT FROM old_value;
        END IF;
    ELSE
        -- We're not tracking DELETE operations as they should be handled as soft deletes
        RETURN NULL;
    END IF;

    -- Log the change only if there are changes to track
    IF (change_type_var IN ('UPDATE', 'DELETE') AND previous_values_json != '{}') OR 
       (change_type_var = 'INSERT' AND changed_fields_json != '{}') THEN
        PERFORM log_event_change(
            CASE 
                WHEN change_type_var = 'INSERT' THEN NEW.event_session_id
                ELSE OLD.event_session_id
            END,
            CASE 
                WHEN change_type_var = 'INSERT' THEN NEW.schedule_version_id
                ELSE OLD.schedule_version_id
            END,
            change_type_var,
            CASE 
                WHEN change_type_var = 'DELETE' THEN NULL
                ELSE changed_fields_json
            END,
            CASE 
                WHEN change_type_var = 'INSERT' THEN NULL
                ELSE previous_values_json
            END
        );
    END IF;

    RETURN NULL; -- for AFTER triggers
END;
$$ LANGUAGE plpgsql;

-- Ensure the trigger is properly set up
DROP TRIGGER IF EXISTS event_changes_trigger ON fct_event_session;
CREATE TRIGGER event_changes_trigger
AFTER INSERT OR UPDATE ON fct_event_session
FOR EACH ROW EXECUTE FUNCTION track_event_changes();

-- Add a unique constraint to fct_event_session table
ALTER TABLE fct_event_session ADD CONSTRAINT fct_event_session_hash_key_is_current_unique 
UNIQUE (hash_key, is_current);

-- Now, let's update the process_staging_event_session function
CREATE OR REPLACE FUNCTION process_staging_event_session(new_version_number VARCHAR(50))
RETURNS VOID AS $$
DECLARE
    new_version_id INTEGER;
    changed_count INTEGER;
BEGIN
    -- Insert new version
    INSERT INTO dim_schedule_version (version_number, valid_from, valid_to)
    VALUES (new_version_number, CURRENT_TIMESTAMP, '9999-12-31 23:59:59')
    RETURNING schedule_version_id INTO new_version_id;

    -- Update existing events and insert new ones
    WITH upsert_result AS (
        INSERT INTO fct_event_session (
            hash_key, schedule_version_id, current_version_id, version_array, sport_id, venue_id, day_id, 
            event_date, start_time, end_time, date_start, date_end, event_type, 
            gross_seats, seat_kill, est_ticket_sold, net_seats, est_sold_seats, workforce_count,
            valid_from, valid_to, is_current
        )
        SELECT 
            md5(
                s.venue_id::TEXT || '-' ||
                s.sport_id::TEXT || '-' ||
                s.day_id::TEXT || '-' ||
                to_char(s.event_date, 'YYYY-MM-DD') || '-' ||
                s.event_type::TEXT || '-' ||
                to_char(s.start_time, 'HH24:MI:SS') || '-' ||
                to_char(s.date_start, 'YYYY-MM-DD')
            ),
            new_version_id, new_version_id, 
            ARRAY[new_version_id],
            s.sport_id, s.venue_id, s.day_id, 
            s.event_date, s.start_time, s.end_time, s.date_start, s.date_end, s.event_type, 
            s.gross_seats, s.seat_kill, s.est_ticket_sold, s.net_seats, s.est_sold_seats, s.workforce_count,
            CURRENT_TIMESTAMP, '9999-12-31 23:59:59'::TIMESTAMP, TRUE
        FROM staging_event_session s
        ON CONFLICT (hash_key, is_current) WHERE is_current = TRUE DO UPDATE SET
            schedule_version_id = EXCLUDED.schedule_version_id,
            current_version_id = EXCLUDED.current_version_id,
            version_array = array_append(fct_event_session.version_array, new_version_id),
            sport_id = EXCLUDED.sport_id,
            venue_id = EXCLUDED.venue_id,
            day_id = EXCLUDED.day_id,
            event_date = EXCLUDED.event_date,
            start_time = EXCLUDED.start_time,
            end_time = EXCLUDED.end_time,
            date_start = EXCLUDED.date_start,
            date_end = EXCLUDED.date_end,
            event_type = EXCLUDED.event_type,
            gross_seats = EXCLUDED.gross_seats,
            seat_kill = EXCLUDED.seat_kill,
            est_ticket_sold = EXCLUDED.est_ticket_sold,
            net_seats = EXCLUDED.net_seats,
            est_sold_seats = EXCLUDED.est_sold_seats,
            workforce_count = EXCLUDED.workforce_count,
            valid_from = CURRENT_TIMESTAMP
        WHERE (
            fct_event_session.sport_id != EXCLUDED.sport_id OR
            fct_event_session.venue_id != EXCLUDED.venue_id OR
            fct_event_session.day_id != EXCLUDED.day_id OR
            fct_event_session.event_date != EXCLUDED.event_date OR
            fct_event_session.start_time != EXCLUDED.start_time OR
            fct_event_session.end_time != EXCLUDED.end_time OR
            fct_event_session.date_start != EXCLUDED.date_start OR
            fct_event_session.date_end != EXCLUDED.date_end OR
            fct_event_session.event_type != EXCLUDED.event_type OR
            fct_event_session.gross_seats != EXCLUDED.gross_seats OR
            fct_event_session.seat_kill != EXCLUDED.seat_kill OR
            fct_event_session.est_ticket_sold != EXCLUDED.est_ticket_sold OR
            fct_event_session.net_seats != EXCLUDED.net_seats OR
            fct_event_session.est_sold_seats != EXCLUDED.est_sold_seats OR
            fct_event_session.workforce_count != EXCLUDED.workforce_count
        )
        RETURNING *
    )
    -- Handle deleted events
    , deleted_events AS (
        UPDATE fct_event_session f
        SET is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP,
            current_version_id = new_version_id,
            version_array = array_append(f.version_array, new_version_id)
        WHERE f.is_current = TRUE
        AND NOT EXISTS (
            SELECT 1
            FROM staging_event_session s
            WHERE f.hash_key = md5(
                s.venue_id::TEXT || '-' ||
                s.sport_id::TEXT || '-' ||
                s.day_id::TEXT || '-' ||
                to_char(s.event_date, 'YYYY-MM-DD') || '-' ||
                s.event_type::TEXT || '-' ||
                to_char(s.start_time, 'HH24:MI:SS') || '-' ||
                to_char(s.date_start, 'YYYY-MM-DD')
            )
        )
        RETURNING *
    )
    -- Count changed records
    SELECT COUNT(*) INTO changed_count 
    FROM (
        SELECT * FROM upsert_result
        UNION ALL
        SELECT * FROM deleted_events
    ) AS changes;

    -- Clear staging table
    TRUNCATE TABLE staging_event_session;

    -- Log the number of changed records
    RAISE NOTICE 'Processed % changed records for version %', changed_count, new_version_number;
END;
$$ LANGUAGE plpgsql;

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

WITH versions AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY event_session_id ORDER BY valid_from) AS version_num
    FROM fct_event_session
--     WHERE event_session_id = :event_session_id
)
SELECT 
    v1.event_session_id,
    v1.schedule_version_id AS old_version,
    v2.schedule_version_id AS new_version,
    v1.valid_from AS old_valid_from,
    v2.valid_from AS new_valid_from,
    CASE WHEN v1.venue_id != v2.venue_id THEN 'Changed' ELSE 'Unchanged' END AS venue_change,
    CASE WHEN v1.sport_id != v2.sport_id THEN 'Changed' ELSE 'Unchanged' END AS sport_change
    -- Add more field comparisons as needed
FROM versions v1
JOIN versions v2 ON v1.event_session_id = v2.event_session_id --AND v1.version_num = v2.version_num - 1
WHERE v1.version_num = 'v2' AND v2.version_num = 'v4';