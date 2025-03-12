-- Individual update functions for each dimension table

CREATE OR REPLACE FUNCTION test.update_dim_regions()
RETURNS TABLE (updated_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_regions AS (
    SELECT DISTINCT region_name
    FROM test.staging_event_session
),
region_status AS (
    SELECT DISTINCT
        s.region_name,
        CASE 
            WHEN d.region_name IS NULL THEN 'new'
            ELSE 'existing'
        END AS status
    FROM staging_regions s
    LEFT JOIN test.dim_regions d ON s.region_name = d.region_name AND d.is_current = TRUE
),
update_regions AS (
    UPDATE test.dim_regions
    SET 
        is_current = FALSE,
        valid_to = CURRENT_TIMESTAMP
    WHERE is_current = TRUE
    AND region_name NOT IN (SELECT region_name FROM staging_regions)
    RETURNING region_name
),
insert_regions AS (
    INSERT INTO test.dim_regions (region_name, is_current, valid_from)
    SELECT 
        region_name, 
        TRUE,
        CURRENT_TIMESTAMP
    FROM region_status
    WHERE status = 'new'
    ON CONFLICT (region_name) DO UPDATE
    SET 
        is_current = TRUE,
        valid_from = CURRENT_TIMESTAMP,
        valid_to = '9999-12-31 23:59:59'
    RETURNING region_name
)
SELECT 
    (SELECT COUNT(*) FROM update_regions) AS updated_count,
    (SELECT COUNT(*) FROM insert_regions) AS inserted_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_dim_zones()
RETURNS TABLE (updated_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_zones AS (
    SELECT DISTINCT zone_name, region_name
    FROM test.staging_event_session
),
zone_status AS (
    SELECT DISTINCT
        s.zone_name,
        s.region_name,
        CASE 
            WHEN d.zone_name IS NULL THEN 'new'
            ELSE 'existing'
        END AS status
    FROM staging_zones s
    LEFT JOIN test.dim_zones d ON s.zone_name = d.zone_name AND d.is_current = TRUE
),
update_zones AS (
    UPDATE test.dim_zones
    SET 
        is_current = FALSE,
        valid_to = CURRENT_TIMESTAMP
    WHERE is_current = TRUE
    AND zone_name NOT IN (SELECT zone_name FROM staging_zones)
    RETURNING zone_name
),
insert_zones AS (
    INSERT INTO test.dim_zones (zone_name, region_id, is_current, valid_from)
    SELECT 
        zs.zone_name, 
        dr.region_id,
        TRUE,
        CURRENT_TIMESTAMP
    FROM zone_status zs
    LEFT JOIN test.dim_regions dr ON zs.region_name = dr.region_name AND dr.is_current = TRUE
    WHERE zs.status = 'new'
    ON CONFLICT (zone_name) DO UPDATE
    SET 
        is_current = TRUE,
        region_id = EXCLUDED.region_id,
        valid_from = CURRENT_TIMESTAMP,
        valid_to = '9999-12-31 23:59:59'
    RETURNING zone_name
)
SELECT 
    (SELECT COUNT(*) FROM update_zones) AS updated_count,
    (SELECT COUNT(*) FROM insert_zones) AS inserted_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_dim_clusters()
RETURNS TABLE (updated_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_clusters AS (
    SELECT DISTINCT cluster_name, zone_name, region_name
    FROM test.staging_event_session
),
cluster_status AS (
    SELECT DISTINCT
        s.cluster_name,
        s.zone_name,
        s.region_name,
        CASE 
            WHEN d.cluster_name IS NULL THEN 'new'
            ELSE 'existing'
        END AS status
    FROM staging_clusters s
    LEFT JOIN test.dim_clusters d ON s.cluster_name = d.cluster_name AND d.is_current = TRUE
),
update_clusters AS (
    UPDATE test.dim_clusters
    SET 
        is_current = FALSE,
        valid_to = CURRENT_TIMESTAMP
    WHERE is_current = TRUE
    AND cluster_name NOT IN (SELECT cluster_name FROM staging_clusters)
    RETURNING cluster_name
),
insert_clusters AS (
    INSERT INTO test.dim_clusters (cluster_name, region_id, zone_id, is_current, valid_from)
    SELECT 
        cs.cluster_name, 
        dr.region_id,
        dz.zone_id,
        TRUE,
        CURRENT_TIMESTAMP
    FROM cluster_status cs
    LEFT JOIN test.dim_regions dr ON cs.region_name = dr.region_name AND dr.is_current = TRUE
    LEFT JOIN test.dim_zones dz ON cs.zone_name = dz.zone_name AND dz.is_current = TRUE
    WHERE cs.status = 'new'
    ON CONFLICT (cluster_name) DO UPDATE
    SET 
        is_current = TRUE,
        region_id = EXCLUDED.region_id,
        zone_id = EXCLUDED.zone_id,
        valid_from = CURRENT_TIMESTAMP,
        valid_to = '9999-12-31 23:59:59'
    RETURNING cluster_name
)
SELECT 
    (SELECT COUNT(*) FROM update_clusters) AS updated_count,
    (SELECT COUNT(*) FROM insert_clusters) AS inserted_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_dim_venues()
RETURNS TABLE (updated_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_venues AS (
    SELECT DISTINCT venue_name, region_name, zone_name, cluster_name
    FROM test.staging_event_session
),
venue_status AS (
    SELECT DISTINCT
        s.venue_name,
        s.region_name,
        s.zone_name,
        s.cluster_name,
        CASE 
            WHEN d.venue_name IS NULL THEN 'new'
            ELSE 'existing'
        END AS status
    FROM staging_venues s
    LEFT JOIN test.dim_venues d ON s.venue_name = d.venue_name AND d.is_current = TRUE
),
update_venues AS (
    UPDATE test.dim_venues
    SET 
        is_current = FALSE,
        valid_to = CURRENT_TIMESTAMP
    WHERE is_current = TRUE
    AND venue_name NOT IN (SELECT venue_name FROM staging_venues)
    RETURNING venue_name
),
insert_venues AS (
    INSERT INTO test.dim_venues (venue_name, region_id, zone_id, cluster_id, is_current, valid_from)
    SELECT 
        vs.venue_name, 
        dr.region_id,
        dz.zone_id,
        dc.cluster_id,
        TRUE,
        CURRENT_TIMESTAMP
    FROM venue_status vs
    JOIN test.dim_regions dr ON vs.region_name = dr.region_name AND dr.is_current = TRUE
    JOIN test.dim_zones dz ON vs.zone_name = dz.zone_name AND dz.is_current = TRUE
    JOIN test.dim_clusters dc ON vs.cluster_name = dc.cluster_name AND dc.is_current = TRUE
    WHERE vs.status = 'new'
    ON CONFLICT (venue_name) DO UPDATE
    SET 
        is_current = TRUE,
        region_id = EXCLUDED.region_id,
        zone_id = EXCLUDED.zone_id,
        cluster_id = EXCLUDED.cluster_id,
        valid_from = CURRENT_TIMESTAMP,
        valid_to = '9999-12-31 23:59:59'
    RETURNING venue_name
)
SELECT 
    (SELECT COUNT(*) FROM update_venues) AS updated_count,
    (SELECT COUNT(*) FROM insert_venues) AS inserted_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_dim_subvenues()
RETURNS TABLE (updated_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_subvenues AS (
    SELECT DISTINCT subvenue_name, venue_name
    FROM test.staging_event_session
),
subvenue_status AS (
    SELECT DISTINCT
        s.subvenue_name,
        s.venue_name,
        CASE 
            WHEN d.subvenue_name IS NULL THEN 'new'
            ELSE 'existing'
        END AS status
    FROM staging_subvenues s
    LEFT JOIN test.dim_subvenues d ON s.subvenue_name = d.subvenue_name AND d.is_current = TRUE
),
update_subvenues AS (
    UPDATE test.dim_subvenues
    SET 
        is_current = FALSE,
        valid_to = CURRENT_TIMESTAMP
    WHERE is_current = TRUE
    AND subvenue_name NOT IN (SELECT subvenue_name FROM staging_subvenues)
    RETURNING subvenue_name
),
insert_subvenues AS (
    INSERT INTO test.dim_subvenues (subvenue_name, venue_id, is_current, valid_from)
    SELECT 
        ss.subvenue_name, 
        dv.venue_id,
        TRUE,
        CURRENT_TIMESTAMP
    FROM subvenue_status ss
    JOIN test.dim_venues dv ON ss.venue_name = dv.venue_name AND dv.is_current = TRUE
    WHERE ss.status = 'new'
    ON CONFLICT (subvenue_name) DO UPDATE
    SET 
        is_current = TRUE,
        venue_id = EXCLUDED.venue_id,
        valid_from = CURRENT_TIMESTAMP,
        valid_to = '9999-12-31 23:59:59'
    RETURNING subvenue_name
)
SELECT 
    (SELECT COUNT(*) FROM update_subvenues) AS updated_count,
    (SELECT COUNT(*) FROM insert_subvenues) AS inserted_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_dim_sports()
RETURNS TABLE (updated_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_sports AS (
    SELECT DISTINCT sport_discipline, venue_name
    FROM test.staging_event_session
),
sport_status AS (
    SELECT DISTINCT
        s.sport_discipline,
        s.venue_name,
        CASE 
            WHEN d.sport_discipline IS NULL THEN 'new'
            ELSE 'existing'
        END AS status
    FROM staging_sports s
    LEFT JOIN test.dim_sports d ON s.sport_discipline = d.sport_discipline AND d.is_current = TRUE
),
update_sports AS (
    UPDATE test.dim_sports
    SET 
        is_current = FALSE,
        valid_to = CURRENT_TIMESTAMP
    WHERE is_current = TRUE
    AND sport_discipline NOT IN (SELECT sport_discipline FROM staging_sports)
    RETURNING sport_discipline
),
insert_sports AS (
    INSERT INTO test.dim_sports (sport_discipline, venue_id, is_current, valid_from)
    SELECT 
        ss.sport_discipline, 
        dv.venue_id,
        TRUE,
        CURRENT_TIMESTAMP
    FROM sport_status ss
    JOIN test.dim_venues dv ON ss.venue_name = dv.venue_name AND dv.is_current = TRUE
    WHERE ss.status = 'new'
    ON CONFLICT (sport_discipline) DO UPDATE
    SET 
        is_current = TRUE,
        venue_id = EXCLUDED.venue_id,
        valid_from = CURRENT_TIMESTAMP,
        valid_to = '9999-12-31 23:59:59'
    RETURNING sport_discipline
)
SELECT 
    (SELECT COUNT(*) FROM update_sports) AS updated_count,
    (SELECT COUNT(*) FROM insert_sports) AS inserted_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_dim_calendar()
RETURNS TABLE (updated_count INT, inserted_count INT) AS $$
BEGIN
    WITH staging_calendar AS (
    SELECT DISTINCT
        event_day,
        date_start AS model_date,
        competition_type
    FROM test.staging_event_session
    ),
    calendar_status AS (
        SELECT DISTINCT
            s.event_day,
            s.model_date,
            s.competition_type,
            CASE 
                WHEN d.event_day IS NULL THEN 'new'
                ELSE 'existing'
            END AS status
        FROM staging_calendar s
        LEFT JOIN test.dim_calendar d 
            ON s.event_day = d.event_day 
            AND s.competition_type = d.competition_type 
            AND d.is_current = TRUE
    ),
    deactivate_calendar AS (
        UPDATE test.dim_calendar
        SET 
            is_current = FALSE,
            valid_to = CURRENT_TIMESTAMP
        WHERE is_current = TRUE
        AND (d.event_day, d.competition_type) NOT IN (
            SELECT event_day, competition_type 
            FROM staging_calendar
        )
        RETURNING d.event_day, d.competition_type
    ),
    insert_calendar AS (
        INSERT INTO test.dim_calendar (
            event_day, 
            model_date, 
            actual_date, 
            day_of_week, 
            competition_type, 
            is_current, 
            valid_from
        )
        SELECT 
            cs.event_day, 
            cs.model_date,
            cs.model_date AS actual_date, -- Assuming actual_date is the same as model_date for now
            TO_CHAR(cs.model_date, 'Day') AS day_of_week,
            cs.competition_type,
            TRUE,
            CURRENT_TIMESTAMP
        FROM calendar_status cs
        WHERE cs.status = 'new'
        ON CONFLICT (event_day, competition_type) DO UPDATE
        SET 
            is_current = TRUE,
            model_date = EXCLUDED.model_date,
            actual_date = EXCLUDED.actual_date,
            day_of_week = EXCLUDED.day_of_week,
            competition_type = EXCLUDED.competition_type,
            valid_from = CURRENT_TIMESTAMP,
            valid_to = '9999-12-31 23:59:59'
        WHERE 
            dim_calendar.model_date != EXCLUDED.model_date OR
            dim_calendar.actual_date != EXCLUDED.actual_date OR
            dim_calendar.day_of_week != EXCLUDED.day_of_week OR
            dim_calendar.is_current = FALSE
        RETURNING event_day, competition_type, 
            CASE 
                WHEN xmax::text::int > 0 THEN 'UPDATE'
                ELSE 'INSERT'
            END AS operation
    )
    SELECT 
        (SELECT COUNT(*) FROM deactivate_calendar),
        (SELECT COUNT(*) FROM insert_calendar WHERE operation = 'UPDATE'),
        (SELECT COUNT(*) FROM insert_calendar WHERE operation = 'INSERT')

    RETURN QUERY SELECT deactivated_count, updated_count, inserted_count;

END;
$$ LANGUAGE plpgsql;

-- Master stored procedure to orchestrate the entire update process

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
BEGIN
    -- Update regions
    SELECT * INTO region_result FROM test.update_dim_regions();
    RAISE NOTICE 'Regions updated: % deleted, % inserted', region_result.updated_count, region_result.inserted_count;

    -- Update zones
    SELECT * INTO zone_result FROM test.update_dim_zones();
    RAISE NOTICE 'Zones updated: % deleted, % inserted', zone_result.updated_count, zone_result.inserted_count;

    -- Update clusters
    SELECT * INTO cluster_result FROM test.update_dim_clusters();
    RAISE NOTICE 'Clusters updated: % deleted, % inserted', cluster_result.updated_count, cluster_result.inserted_count;

    -- Update venues
    SELECT * INTO venue_result FROM test.update_dim_venues();
    RAISE NOTICE 'Venues updated: % deleted, % inserted', venue_result.updated_count, venue_result.inserted_count;

    -- Update subvenues
    SELECT * INTO subvenue_result FROM test.update_dim_subvenues();
    RAISE NOTICE 'Subvenues updated: % deleted, % inserted', subvenue_result.updated_count, subvenue_result.inserted_count;

    -- Update sports
    SELECT * INTO sport_result FROM test.update_dim_sports();
    RAISE NOTICE 'Sports updated: % deleted, % inserted', sport_result.updated_count, sport_result.inserted_count;

    -- Update calendar
    SELECT * INTO calendar_result FROM test.update_dim_calendar();
    RAISE NOTICE 'Calendar updated: % deleted, % inserted', calendar_result.updated_count, calendar_result.inserted_count;

    COMMIT;
END;
$$;

-- Execute the entire update process
CALL test.update_all_dimensions();