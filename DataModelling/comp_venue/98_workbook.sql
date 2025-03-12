SELECT setval('test.dim_calendar_day_id_seq', 1, true);
SELECT setval('test.dim_clusters_cluster_id_seq', 1, true);
SELECT setval('test.dim_non_comp_facs_nc_fac_id_seq', 1, true);
SELECT setval('test.dim_regions_region_id_seq', 1, true);
SELECT setval('test.dim_schedule_version_record_id_seq', 1, true);
SELECT setval('test.dim_sports_sport_id_seq', 1, true);
SELECT setval('test.dim_status_status_id_seq', 1, true);
SELECT setval('test.dim_subvenues_subvenue_id_seq', 1, true);
SELECT setval('test.dim_venues_venue_id_seq', 1, true);
SELECT setval('test.dim_zones_zone_id_seq', 1, true);
SELECT setval('test.fct_event_session_record_id_seq', 1, true);

SELECT test.update_dim_regions();
SELECT test.update_dim_zones();
SELECT test.update_dim_clusters();
SELECT test.update_dim_venues();
SELECT test.update_dim_subvenues();
SELECT test.update_dim_sports();
SELECT test.update_dim_calendar();

SELECT test.process_staging_event_session('v1.1')

SELECT * FROM test.dim_calendar ORDER BY day_id ASC;

SELECT * FROM test.dim_non_comp_facs ORDER BY nc_fac_id ASC;

SELECT * FROM test.dim_regions ORDER BY region_id ASC;
SELECT * FROM test.dim_zones ORDER BY zone_id ASC;
SELECT * FROM test.dim_clusters ORDER BY cluster_id ASC;

SELECT * FROM test.dim_status ORDER BY status_id ASC;
SELECT * FROM test.dim_venues ORDER BY venue_id ASC;
SELECT * FROM test.dim_subvenues ORDER BY subvenue_id ASC;
SELECT * FROM test.dim_sports ORDER BY sport_id ASC;