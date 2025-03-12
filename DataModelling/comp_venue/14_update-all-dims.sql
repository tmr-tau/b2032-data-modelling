CREATE OR REPLACE PROCEDURE test.update_all_dimensions()
LANGUAGE plpgsql
AS $$
DECLARE
    region_result RECORD;
    zone_result RECORD;
    cluster_result RECORD;
    venue_result RECORD;
    subvenue_result RECORD;
    sport_result RECORD;
    calendar_result RECORD;
--     status_result RECORD;
    error_occurred BOOLEAN := FALSE;
    error_message TEXT;
BEGIN
    -- Update regions
    BEGIN
        SELECT * INTO region_result FROM test.update_dim_regions();
        RAISE NOTICE 'Regions updated: % expired, % inserted', region_result.expired_count, region_result.inserted_count;
    EXCEPTION WHEN OTHERS THEN
        error_occurred := TRUE;
        error_message := 'Error updating regions: ' || SQLERRM;
    END;

    -- Update zones (depends on regions)
    IF NOT error_occurred THEN
        BEGIN
            SELECT * INTO zone_result FROM test.update_dim_zones();
            RAISE NOTICE 'Zones updated: % expired, % inserted', zone_result.expired_count, zone_result.inserted_count;
        EXCEPTION WHEN OTHERS THEN
            error_occurred := TRUE;
            error_message := 'Error updating zones: ' || SQLERRM;
        END;
    END IF;

    -- Update clusters (depends on regions and zones)
    IF NOT error_occurred THEN
        BEGIN
            SELECT * INTO cluster_result FROM test.update_dim_clusters();
            RAISE NOTICE 'Clusters updated: % expired, % inserted', cluster_result.expired_count, cluster_result.inserted_count;
        EXCEPTION WHEN OTHERS THEN
            error_occurred := TRUE;
            error_message := 'Error updating clusters: ' || SQLERRM;
        END;
    END IF;

    -- Update venues (depends on regions, zones, and clusters)
    IF NOT error_occurred THEN
        BEGIN
            SELECT * INTO venue_result FROM test.update_dim_venues();
            RAISE NOTICE 'Venues updated: % expired, % inserted', venue_result.expired_count, venue_result.inserted_count;
        EXCEPTION WHEN OTHERS THEN
            error_occurred := TRUE;
            error_message := 'Error updating venues: ' || SQLERRM;
        END;
    END IF;

    -- Update subvenues (depends on venues)
    IF NOT error_occurred THEN
        BEGIN
            SELECT * INTO subvenue_result FROM test.update_dim_subvenues();
            RAISE NOTICE 'Subvenues updated: % expired, % inserted', subvenue_result.expired_count, subvenue_result.inserted_count;
        EXCEPTION WHEN OTHERS THEN
            error_occurred := TRUE;
            error_message := 'Error updating subvenues: ' || SQLERRM;
        END;
    END IF;

    -- Update sports (depends on venues)
    IF NOT error_occurred THEN
        BEGIN
            SELECT * INTO sport_result FROM test.update_dim_sports();
            RAISE NOTICE 'Sports updated: % expired, % inserted', sport_result.expired_count, sport_result.inserted_count;
        EXCEPTION WHEN OTHERS THEN
            error_occurred := TRUE;
            error_message := 'Error updating sports: ' || SQLERRM;
        END;
    END IF;

    -- Update calendar
    IF NOT error_occurred THEN
        BEGIN
            SELECT * INTO calendar_result FROM test.update_dim_calendar();
            RAISE NOTICE 'Calendar updated: % expired, % inserted', calendar_result.expired_count, calendar_result.inserted_count;
        EXCEPTION WHEN OTHERS THEN
            error_occurred := TRUE;
            error_message := 'Error updating calendar: ' || SQLERRM;
        END;
    END IF;
	
-- 	-- Update non-competition facilities
-- 	IF NOT error_occurred THEN
-- 		BEGIN
-- 			SELECT * INTO non_comp_facs_result FROM test.update_dim_non_comp_facs();
-- 			RAISE NOTICE 'Non-competition facilities updated: % expired, % inserted', non_comp_facs_result.expired_count, non_comp_facs_result.inserted_count;
-- 		EXCEPTION WHEN OTHERS THEN
-- 			error_occurred := TRUE;
-- 			error_message := 'Error updating non-competition facilities: ' || SQLERRM;
-- 		END;
-- 	END IF;

--     -- Update status
--     IF NOT error_occurred THEN
--         BEGIN
--             SELECT * INTO status_result FROM test.update_dim_status();
--             RAISE NOTICE 'Status updated: % expired, % inserted', status_result.expired_count, status_result.inserted_count;
--         EXCEPTION WHEN OTHERS THEN
--             error_occurred := TRUE;
--             error_message := 'Error updating status: ' || SQLERRM;
--         END;
--     END IF;

    -- Final error handling
    IF error_occurred THEN
        RAISE EXCEPTION '%', error_message;
    ELSE
        RAISE NOTICE 'All dimensions updated successfully.';
    END IF;
END;
$$;

-- Execute the entire update process
CALL test.update_all_dimensions();