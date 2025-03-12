Create a comprehensive competition schedule version tracking example using the following approach:

1. Use an MD5 hash of venue_id, sport_id, event_type, day_id, date_start, and start_time as the unique identifier for each event.

2. Implement a fact table (fct_event_session) that never deletes records but instead uses valid_from, valid_to, and is_current fields to track changes over time.

3. Use a dimension table (dim_schedule_version) to track different versions of the schedule.

4. Implement an event_version_changes table to explicitly track changes between versions.

5. Handle new, modified, and deleted events as follows:
   - New: Insert a new record with is_current = TRUE
   - Modified: Insert a new record with updated values and is_current = TRUE, update the old record's valid_to and set is_current = FALSE
   - Deleted: Update the existing record's valid_to and set is_current = FALSE

6. Treat modifications or deletions of future events as new records.

Provide examples covering at least four versions of the schedule, including:
- Initial creation of events
- Modification of existing events (changing both key fields and metric fields)
- Deletion of events
- Addition of new events
- Modification of future events

For each version, show:
1. The state of the dim_schedule_version table
2. The state of the fct_event_session table
3. The state of the event_version_changes table

Explain the changes between each version and how they are reflected in the tables. Include examples of SQL queries to retrieve:
1. The current version of the schedule
2. A specific past version of the schedule
3. The latest version before a given date

Use a markdown format similar to the original "Competition Schedule Version Tracking Example" artifact, but adapted to this new approach.
