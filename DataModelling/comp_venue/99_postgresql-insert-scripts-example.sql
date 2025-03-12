-- Insert dummy data into dim_region
INSERT INTO dim_region (region_id, region_name) VALUES
(1, 'Gold Coast'),
(2, 'Brisbane');

-- Insert dummy data into dim_zone
INSERT INTO dim_zone (zone_id, zone_name) VALUES
(1, 'Zone A'),
(2, 'Zone B');

-- Insert dummy data into dim_cluster
INSERT INTO dim_cluster (cluster_id, cluster_name, region_id, zone_id, is_current) VALUES
(1, 'Cluster 1', 1, 1, TRUE),
(2, 'Cluster 2', 2, 2, TRUE);

-- Insert dummy data into dim_venue
INSERT INTO dim_venue (venue_id, venue_name, region_id, zone_id, cluster_id, is_current) VALUES
(201, 'Stadium A', 1, 1, 1, TRUE),
(202, 'Arena B', 1, 1, 1, TRUE),
(203, 'Court C', 2, 2, 2, TRUE),
(204, 'Pool D', 2, 2, 2, TRUE);

-- Insert dummy data into dim_sport
INSERT INTO dim_sport (sport_id, sport_name, venue_id, venue_name, is_current) VALUES
(101, 'Football', 201, 'Stadium A', TRUE),
(102, 'Basketball', 202, 'Arena B', TRUE),
(103, 'Tennis', 203, 'Court C', TRUE),
(104, 'Swimming', 204, 'Pool D', TRUE);

-- Insert dummy data into dim_calendar
INSERT INTO dim_calendar (day_id, event_day, day_of_week, actual_date, competition_type, is_current) VALUES
(1, 1, '2024-07-26', '2024-07-26', 'Qualifier', TRUE),
(2, 2, '2024-07-27', '2024-07-27', 'Semi-Final', TRUE),
(3, 3, '2024-07-28', '2024-07-28', 'Final', TRUE);
	
-- Insert data for Version 1
INSERT INTO staging_event_session (sport_id, venue_id, day_id, event_date, start_time, end_time, date_start, date_end, event_type, gross_seats, seat_kill, est_ticket_sold, net_seats, est_sold_seats, workforce_count)
VALUES
(101, 201, 1, '2024-07-26', '09:00:00', '11:00:00', '2024-07-26', '2024-07-26', 1, 60000, 0.1, 0.80, 59000, 47200, 500),
(102, 202, 1, '2024-07-26', '14:00:00', '16:00:00', '2024-07-26', '2024-07-26', 1, 10000, 0.2, 0.90, 9800, 8820, 100),
(103, 203, 2, '2024-07-27', '10:00:00', '12:00:00', '2024-07-27', '2024-07-27', 1, 5000, 0.1, 0.75, 4900, 3675, 50);

ALTER TABLE fct_event_session ADD CONSTRAINT unique_hash_key UNIQUE (hash_key);

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