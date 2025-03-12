CREATE OR REPLACE PROCEDURE test.staging_to_fact(new_version_number VARCHAR(50))
LANGUAGE plpgsql
AS $$
DECLARE
    staging_process_result INTEGER;
    dim_update_error BOOLEAN := FALSE;
    staging_process_error BOOLEAN := FALSE;
    error_message TEXT;
BEGIN
    -- Step 1: Update dimension tables
    BEGIN
        CALL test.update_all_dimensions();
    EXCEPTION
        WHEN OTHERS THEN
            dim_update_error := TRUE;
            error_message := 'Error updating dimensions: ' || SQLERRM;
            RAISE WARNING '%', error_message;
    END;

    -- Only proceed to process staging data if dimension update was successful
    IF NOT dim_update_error THEN
        -- Step 2: Process the staging data
        BEGIN
            SELECT * INTO staging_process_result 
            FROM test.process_staging_event_session(new_version_number);

            -- Log the results of staging data processing
            RAISE NOTICE 'Staging data processed: % rows affected', staging_process_result;
        EXCEPTION
            WHEN OTHERS THEN
                staging_process_error := TRUE;
                error_message := 'Error processing staging data: ' || SQLERRM;
                RAISE WARNING '%', error_message;
        END;
    END IF;

    -- Final status report
    IF dim_update_error OR staging_process_error THEN
        RAISE EXCEPTION 'Staging to fact process encountered errors. Check the logs for details.';
    ELSE
        RAISE NOTICE 'Staging to fact process completed successfully for version %', new_version_number;
    END IF;
END;
$$;


-- To execute the procedure
BEGIN;
CALL test.staging_to_fact('v0.1');
COMMIT;