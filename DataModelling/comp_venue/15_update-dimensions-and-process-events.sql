-- Main procedure to process staging data
CREATE OR REPLACE PROCEDURE staging_to_fact(new_version_number VARCHAR(50))
LANGUAGE plpgsql
AS $$
DECLARE
    dim_update_result RECORD;
    staging_process_result INTEGER;
BEGIN
    -- Start a transaction
    START TRANSACTION;

    -- Step 1: Update dimension tables
    CALL update_all_dimensions() INTO dim_update_result;
    
    -- Log the results of dimension update
    RAISE NOTICE 'Dimension tables updated: % total changes', 
        (dim_update_result.deleted_count + dim_update_result.inserted_count);

    -- Step 2: Process the staging data
    SELECT * INTO staging_process_result 
    FROM test.process_staging_event_session(new_version_number);

    -- Log the results of staging data processing
    RAISE NOTICE 'Staging data processed: % rows affected', staging_process_result;

    -- Commit the transaction if everything is successful
    COMMIT;

    RAISE NOTICE 'Staging to fact process completed successfully for version %', new_version_number;

EXCEPTION
    WHEN OTHERS THEN
        -- Rollback the transaction if any error occurs
        ROLLBACK;
        RAISE EXCEPTION 'An error occurred during staging to fact process: %', SQLERRM;
END;
$$;

-- To execute the procedure
CALL staging_to_fact('your_new_version_number');