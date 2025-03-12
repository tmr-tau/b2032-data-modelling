-- Trigger Function to log changes
CREATE OR REPLACE FUNCTION track_event_changes()
RETURNS TRIGGER AS $$
DECLARE
    changed_fields_json JSONB := '{}';
    previous_values_json JSONB := '{}';
    change_type_var VARCHAR(10);
    excluded_columns TEXT[] := ARRAY['schedule_version_id', 'current_version_id', 'version_array', 'hash_key', 'valid_from', 'valid_to', 'is_current'];
BEGIN
    IF TG_OP = 'UPDATE' THEN
        -- First, check if the hash_key exists in the new version
        IF EXISTS (
            SELECT 1 FROM fct_event_session 
            WHERE hash_key = OLD.hash_key AND schedule_version_id > OLD.schedule_version_id
        ) THEN
            -- The hash_key exists in a newer version, so this is an update
            change_type_var := 'UPDATE';
        ELSIF NEW.is_current = FALSE AND OLD.is_current = TRUE THEN
            -- The hash_key doesn't exist in a newer version and is_current changed to false, so this is a delete
            change_type_var := 'DELETE';
        ELSE
            -- This handles other update cases that don't meet the above criteria
            change_type_var := 'UPDATE';
        END IF;
    ELSE
        -- We're not tracking INSERT operations, so return early
        RETURN NEW;
    END IF;

    -- Populate changed_fields_json and previous_values_json
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

    -- Log the change only if there are changes to track
    IF (change_type_var = 'UPDATE' AND changed_fields_json != '{}') OR 
       change_type_var = 'DELETE' THEN
        PERFORM log_event_change(
            OLD.event_session_id,
            OLD.schedule_version_id,
            change_type_var,
            CASE 
                WHEN change_type_var = 'DELETE' THEN NULL 
                ELSE changed_fields_json 
            END,
            previous_values_json
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create or replace the trigger
DROP TRIGGER IF EXISTS event_changes_trigger ON fct_event_session;
CREATE TRIGGER event_changes_trigger
AFTER INSERT OR UPDATE ON fct_event_session
FOR EACH ROW EXECUTE FUNCTION track_event_changes();