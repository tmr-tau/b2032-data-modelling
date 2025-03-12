-- Step 2: Identify updated events and log changes
    WITH updated_events AS (
        SELECT 
            f.event_session_id AS old_event_session_id,
            f.schedule_version_id AS old_schedule_version_id,
            f.hash_key,
            f.*, s.*
        FROM fct_event_session f
        JOIN staging_event_session s ON f.hash_key = s.hash_key
        WHERE f.is_current = TRUE AND f.schedule_version_id < new_version_id
    ),

    -- Step 3: Update is_current for old records
    update_old_records AS (
        UPDATE fct_event_session
        SET is_current = FALSE, valid_to = CURRENT_TIMESTAMP
        WHERE event_session_id IN (SELECT old_event_session_id FROM updated_events)
        RETURNING *
    ),

    -- Step 4: Log changes in log_event_changes table
    log_changes AS (
        INSERT INTO log_event_changes (
            event_session_id, schedule_version_id, change_type, changed_fields, previous_values, change_timestamp
        )
        SELECT 
            old_event_session_id,
            new_version_id,
            'UPDATE',
            jsonb_strip_nulls(jsonb_build_object(
                'sport_id', CASE WHEN ue.sport_id IS DISTINCT FROM ue.sport_id_new THEN ue.sport_id_new ELSE NULL END,
                'venue_id', CASE WHEN ue.venue_id IS DISTINCT FROM ue.venue_id_new THEN ue.venue_id_new ELSE NULL END,
                'day_id', CASE WHEN ue.day_id IS DISTINCT FROM ue.day_id_new THEN ue.day_id_new ELSE NULL END,
                'event_date', CASE WHEN ue.event_date IS DISTINCT FROM ue.event_date_new THEN ue.event_date_new ELSE NULL END,
                'start_time', CASE WHEN ue.start_time IS DISTINCT FROM ue.start_time_new THEN ue.start_time_new ELSE NULL END,
                'end_time', CASE WHEN ue.end_time IS DISTINCT FROM ue.end_time_new THEN ue.end_time_new ELSE NULL END,
                'date_start', CASE WHEN ue.date_start IS DISTINCT FROM ue.date_start_new THEN ue.date_start_new ELSE NULL END,
                'date_end', CASE WHEN ue.date_end IS DISTINCT FROM ue.date_end_new THEN ue.date_end_new ELSE NULL END,
                'event_type', CASE WHEN ue.event_type IS DISTINCT FROM ue.event_type_new THEN ue.event_type_new ELSE NULL END,
                'gross_seats', CASE WHEN ue.gross_seats IS DISTINCT FROM ue.gross_seats_new THEN ue.gross_seats_new ELSE NULL END,
                'seat_kill', CASE WHEN ue.seat_kill IS DISTINCT FROM ue.seat_kill_new THEN ue.seat_kill_new ELSE NULL END,
                'est_ticket_sold', CASE WHEN ue.est_ticket_sold IS DISTINCT FROM ue.est_ticket_sold_new THEN ue.est_ticket_sold_new ELSE NULL END,
                'net_seats', CASE WHEN ue.net_seats IS DISTINCT FROM ue.net_seats_new THEN ue.net_seats_new ELSE NULL END,
                'est_sold_seats', CASE WHEN ue.est_sold_seats IS DISTINCT FROM ue.est_sold_seats_new THEN ue.est_sold_seats_new ELSE NULL END,
                'workforce_count', CASE WHEN ue.workforce_count IS DISTINCT FROM ue.workforce_count_new THEN ue.workforce_count_new ELSE NULL END,
                'unticketed', CASE WHEN ue.unticketed IS DISTINCT FROM ue.unticketed_new THEN ue.unticketed_new ELSE NULL END,
                'additional_attributes', CASE WHEN ue.additional_attributes IS DISTINCT FROM ue.additional_attributes_new THEN ue.additional_attributes_new ELSE NULL END
            )) AS changed_fields,
            jsonb_strip_nulls(jsonb_build_object(
                'sport_id', CASE WHEN ue.sport_id IS DISTINCT FROM ue.sport_id_new THEN ue.sport_id ELSE NULL END,
                'venue_id', CASE WHEN ue.venue_id IS DISTINCT FROM ue.venue_id_new THEN ue.venue_id ELSE NULL END,
                'day_id', CASE WHEN ue.day_id IS DISTINCT FROM ue.day_id_new THEN ue.day_id ELSE NULL END,
                'event_date', CASE WHEN ue.event_date IS DISTINCT FROM ue.event_date_new THEN ue.event_date ELSE NULL END,
                'start_time', CASE WHEN ue.start_time IS DISTINCT FROM ue.start_time_new THEN ue.start_time ELSE NULL END,
                'end_time', CASE WHEN ue.end_time IS DISTINCT FROM ue.end_time_new THEN ue.end_time ELSE NULL END,
                'date_start', CASE WHEN ue.date_start IS DISTINCT FROM ue.date_start_new THEN ue.date_start ELSE NULL END,
                'date_end', CASE WHEN ue.date_end IS DISTINCT FROM ue.date_end_new THEN ue.date_end ELSE NULL END,
                'event_type', CASE WHEN ue.event_type IS DISTINCT FROM ue.event_type_new THEN ue.event_type ELSE NULL END,
                'gross_seats', CASE WHEN ue.gross_seats IS DISTINCT FROM ue.gross_seats_new THEN ue.gross_seats ELSE NULL END,
                'seat_kill', CASE WHEN ue.seat_kill IS DISTINCT FROM ue.seat_kill_new THEN ue.seat_kill ELSE NULL END,
                'est_ticket_sold', CASE WHEN ue.est_ticket_sold IS DISTINCT FROM ue.est_ticket_sold_new THEN ue.est_ticket_sold ELSE NULL END,
                'net_seats', CASE WHEN ue.net_seats IS DISTINCT FROM ue.net_seats_new THEN ue.net_seats ELSE NULL END,
                'est_sold_seats', CASE WHEN ue.est_sold_seats IS DISTINCT FROM ue.est_sold_seats_new THEN ue.est_sold_seats ELSE NULL END,
                'workforce_count', CASE WHEN ue.workforce_count IS DISTINCT FROM ue.workforce_count_new THEN ue.workforce_count ELSE NULL END,
                'unticketed', CASE WHEN ue.unticketed IS DISTINCT FROM ue.unticketed_new THEN ue.unticketed ELSE NULL END,
                'additional_attributes', CASE WHEN ue.additional_attributes IS DISTINCT FROM ue.additional_attributes_new THEN ue.additional_attributes ELSE NULL END
            )) AS previous_values,
            CURRENT_TIMESTAMP
        FROM updated_events ue
    )

-- Function to identify and log modified events
CREATE OR REPLACE FUNCTION clean_up(new_version_id INTEGER) RETURNS VOID AS $$
BEGIN
    -- Identify modified events and mark old versions as not current
    WITH modified_events AS (
        SELECT 
            e1.event_session_id AS old_event_session_id,
            e1.schedule_version_id AS old_schedule_version_id,
            e1.hash_key,
            e1.*, e2.*
        FROM fct_event_session e1
        JOIN fct_event_session e2 ON e1.hash_key = e2.hash_key
        WHERE f.is_current = FALSE AND e1.schedule_version_id < new_version_id
    ),
    INSERT INTO log_event_changes (
        event_session_id, schedule_version_id, change_type, changed_fields, previous_values, change_timestamp
    )
    SELECT 
        old_event_session_id,
        new_version_id,
        'UPDATE',
        jsonb_strip_nulls(jsonb_build_object(
            'sport_id', CASE WHEN ue.sport_id IS DISTINCT FROM ue.sport_id_new THEN ue.sport_id_new ELSE NULL END,
            'venue_id', CASE WHEN ue.venue_id IS DISTINCT FROM ue.venue_id_new THEN ue.venue_id_new ELSE NULL END,
            'day_id', CASE WHEN ue.day_id IS DISTINCT FROM ue.day_id_new THEN ue.day_id_new ELSE NULL END,
            'event_date', CASE WHEN ue.event_date IS DISTINCT FROM ue.event_date_new THEN ue.event_date_new ELSE NULL END,
            'start_time', CASE WHEN ue.start_time IS DISTINCT FROM ue.start_time_new THEN ue.start_time_new ELSE NULL END,
            'end_time', CASE WHEN ue.end_time IS DISTINCT FROM ue.end_time_new THEN ue.end_time_new ELSE NULL END,
            'date_start', CASE WHEN ue.date_start IS DISTINCT FROM ue.date_start_new THEN ue.date_start_new ELSE NULL END,
            'date_end', CASE WHEN ue.date_end IS DISTINCT FROM ue.date_end_new THEN ue.date_end_new ELSE NULL END,
            'event_type', CASE WHEN ue.event_type IS DISTINCT FROM ue.event_type_new THEN ue.event_type_new ELSE NULL END,
            'gross_seats', CASE WHEN ue.gross_seats IS DISTINCT FROM ue.gross_seats_new THEN ue.gross_seats_new ELSE NULL END,
            'seat_kill', CASE WHEN ue.seat_kill IS DISTINCT FROM ue.seat_kill_new THEN ue.seat_kill_new ELSE NULL END,
            'est_ticket_sold', CASE WHEN ue.est_ticket_sold IS DISTINCT FROM ue.est_ticket_sold_new THEN ue.est_ticket_sold_new ELSE NULL END,
            'net_seats', CASE WHEN ue.net_seats IS DISTINCT FROM ue.net_seats_new THEN ue.net_seats_new ELSE NULL END,
            'est_sold_seats', CASE WHEN ue.est_sold_seats IS DISTINCT FROM ue.est_sold_seats_new THEN ue.est_sold_seats_new ELSE NULL END,
            'workforce_count', CASE WHEN ue.workforce_count IS DISTINCT FROM ue.workforce_count_new THEN ue.workforce_count_new ELSE NULL END,
            'unticketed', CASE WHEN ue.unticketed IS DISTINCT FROM ue.unticketed_new THEN ue.unticketed_new ELSE NULL END,
            'additional_attributes', CASE WHEN ue.additional_attributes IS DISTINCT FROM ue.additional_attributes_new THEN ue.additional_attributes_new ELSE NULL END
        )) AS changed_fields,
        jsonb_strip_nulls(jsonb_build_object(
            'sport_id', CASE WHEN ue.sport_id IS DISTINCT FROM ue.sport_id_new THEN ue.sport_id ELSE NULL END,
            'venue_id', CASE WHEN ue.venue_id IS DISTINCT FROM ue.venue_id_new THEN ue.venue_id ELSE NULL END,
            'day_id', CASE WHEN ue.day_id IS DISTINCT FROM ue.day_id_new THEN ue.day_id ELSE NULL END,
            'event_date', CASE WHEN ue.event_date IS DISTINCT FROM ue.event_date_new THEN ue.event_date ELSE NULL END,
            'start_time', CASE WHEN ue.start_time IS DISTINCT FROM ue.start_time_new THEN ue.start_time ELSE NULL END,
            'end_time', CASE WHEN ue.end_time IS DISTINCT FROM ue.end_time_new THEN ue.end_time ELSE NULL END,
            'date_start', CASE WHEN ue.date_start IS DISTINCT FROM ue.date_start_new THEN ue.date_start ELSE NULL END,
            'date_end', CASE WHEN ue.date_end IS DISTINCT FROM ue.date_end_new THEN ue.date_end ELSE NULL END,
            'event_type', CASE WHEN ue.event_type IS DISTINCT FROM ue.event_type_new THEN ue.event_type ELSE NULL END,
            'gross_seats', CASE WHEN ue.gross_seats IS DISTINCT FROM ue.gross_seats_new THEN ue.gross_seats ELSE NULL END,
            'seat_kill', CASE WHEN ue.seat_kill IS DISTINCT FROM ue.seat_kill_new THEN ue.seat_kill ELSE NULL END,
            'est_ticket_sold', CASE WHEN ue.est_ticket_sold IS DISTINCT FROM ue.est_ticket_sold_new THEN ue.est_ticket_sold ELSE NULL END,
            'net_seats', CASE WHEN ue.net_seats IS DISTINCT FROM ue.net_seats_new THEN ue.net_seats ELSE NULL END,
            'est_sold_seats', CASE WHEN ue.est_sold_seats IS DISTINCT FROM ue.est_sold_seats_new THEN ue.est_sold_seats ELSE NULL END,
            'workforce_count', CASE WHEN ue.workforce_count IS DISTINCT FROM ue.workforce_count_new THEN ue.workforce_count ELSE NULL END,
            'unticketed', CASE WHEN ue.unticketed IS DISTINCT FROM ue.unticketed_new THEN ue.unticketed ELSE NULL END,
            'additional_attributes', CASE WHEN ue.additional_attributes IS DISTINCT FROM ue.additional_attributes_new THEN ue.additional_attributes ELSE NULL END
        )) AS previous_values,
        CURRENT_TIMESTAMP
    FROM update_old_records ue;
END;
$$ LANGUAGE plpgsql;