-- For Analysis

-- Create Views
-- View to get all current event sessions
CREATE VIEW current_event_sessions AS
SELECT e.*, v.venue_name, s.sport_name
FROM fct_event_session e
JOIN dim_venue v ON e.venue_key = v.venue_key
JOIN dim_sport s ON e.sport_key = s.sport_key
WHERE e.is_current = TRUE;

-- View to get all event changes
CREATE VIEW event_changes AS
SELECT c.*, e.event_session_id, v.version_number
FROM event_version_changes c
JOIN fct_event_session e ON c.event_session_id = e.event_session_id
JOIN dim_schedule_version v ON c.schedule_version_id = v.schedule_version_id;

-- Proposed Views
-- TODO: View to get TicketsByDay
-- TODO: View to get TicketsByDayByRegion
-- TODO: View to get TicketsbyRegionbyZonebyCluster
-- TODO: View to get TicketsbyDaybyZonebyCluster

-- Function to get event history
CREATE OR REPLACE FUNCTION get_event_history(p_venue_id VARCHAR(10))
RETURNS TABLE (
    version_number VARCHAR(50),
    event_name VARCHAR(50),
    venue VARCHAR(50),
    start_time TIME,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        v.version_number,
        e.sport_id,
        e.venue_id,
		e.date_start,
        e.start_time,
		e.date_end,
        e.end_time,
        e.valid_from,
        e.valid_to
    FROM fct_event_session e
    JOIN dim_schedule_version v ON e.schedule_version_id = v.schedule_version_id
    WHERE e.venue_id = p_venue_id
    ORDER BY e.valid_from;
END;
$$ LANGUAGE plpgsql;
```
```sql
-- Function to efficiently query records for a specific version
CREATE OR REPLACE FUNCTION get_records_for_version(target_version INTEGER)
RETURNS TABLE (event_session_key INTEGER, /* other columns */) AS $$
BEGIN
    RETURN QUERY
    SELECT event_session_key, /* other columns */
    FROM fct_event_session
    WHERE target_version = ANY(version_array)
      AND is_current = TRUE;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
SELECT * FROM get_records_for_version(2);

-- Alternative query without using a function
SELECT event_session_key, /* other columns */
FROM fct_event_session
WHERE 2 = ANY(version_array)
  AND is_current = TRUE;


-- Sample Queries
-- 1. Get all events for a specific version (e.g., V3):
SELECT * FROM fct_event_session
WHERE 3 = ANY(version_array);
-- 2. Get current events:
SELECT * FROM fct_event_session
WHERE is_current = TRUE;
-- 3. Get events as they were at a specific date:
SELECT * FROM fct_event_session
WHERE valid_from <= '2023-01-03 00:00:00'
  AND valid_to > '2023-01-03 00:00:00';
-- 4. Get events that were part of multiple versions:
SELECT * FROM fct_event_session
WHERE array_length(version_array, 1) > 1;
-- 5. Analyzing changes in event sessions:
SELECT c.event_session_id, c.change_type, c.changed_fields
FROM event_version_changes c
WHERE c.schedule_version_id = 2;
-- 6. Retrieving the current schedule with estimated ticket sales:
SELECT e.*, v.venue_name, s.sport_name
FROM fct_event_session e
JOIN dim_venue v ON e.venue_id = v.venue_id
JOIN dim_sport s ON e.sport_id = s.sport_id
WHERE e.is_current = TRUE;
-- 7. Comparing estimated ticket sales across different schedule versions:
SELECT e1.event_session_id, e1.est_ticket_sold AS v1_sales, 
        e2.est_ticket_sold AS v2_sales,
        e2.est_ticket_sold - e1.est_ticket_sold AS sales_difference
FROM fct_event_session e1
JOIN fct_event_session e2 ON e1.event_session_id = e2.event_session_id
WHERE e1.schedule_version_id = 1 AND e2.schedule_version_id = 2;
