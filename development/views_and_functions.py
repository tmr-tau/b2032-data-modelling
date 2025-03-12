from sqlalchemy import create_engine, DDL
from sqlalchemy.orm import sessionmaker
from models import Base, FctEventBase, FctEventDetails, DimScenario, DimVersion, VersionEventBridge

def create_view_and_function():
    try:
        # Connect to your PostgreSQL database using SQLAlchemy
        engine = create_engine('postgresql+psycopg2://your_db_user:your_db_password@your_db_host/your_db_name')
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Create view vw_event_by_version using DDL
        create_view_ddl = DDL("""
        CREATE OR REPLACE VIEW vw_event_by_version AS
        SELECT 
            v.scenario_id,
            v.version_id,
            v.version_code,
            s.scenario_name,
            b.event_id,
            eb.hash_key,
            eb.sport_id,
            eb.venue_id,
            eb.subvenue_id,
            eb.region_id,
            eb.zone_id,
            eb.cluster_id,
            eb.day_id,
            eb.session_id,
            eb.competition_type,
            eb.event_type,
            ed.date_start,
            ed.start_time,
            ed.date_end,
            ed.end_time,
            ed.gross_seats,
            ed.seat_kill,
            ed.est_pct_ticksold,
            ed.net_seats,
            ed.est_sold_seats,
            ed.workforce,
            ed.unticketed,
            ed.additional_attributes,
            b.is_active
        FROM 
            version_event_bridge b
        JOIN dim_version v ON b.version_id = v.version_id
        JOIN dim_scenario s ON v.scenario_id = s.scenario_id
        JOIN fct_event_base eb ON b.event_id = eb.event_id
        JOIN fct_event_details ed ON b.detail_id = ed.detail_id;
        """)
        session.execute(create_view_ddl)
        
        # Create function compare_versions using DDL
        create_function_ddl = DDL("""
        CREATE OR REPLACE FUNCTION compare_versions(
            p_version_id1 INTEGER,
            p_version_id2 INTEGER
        ) RETURNS TABLE (
            event_id INTEGER,
            hash_key VARCHAR(100),
            change_type VARCHAR(20),
            changed_fields JSONB
        ) AS $$
        BEGIN
            RETURN QUERY
            WITH v1 AS (
                SELECT 
                    b.event_id, 
                    e.hash_key,
                    e.sport_id, e.venue_id, e.subvenue_id, e.region_id, e.zone_id, 
                    e.cluster_id, e.day_id, e.session_id, e.competition_type, e.event_type,
                    d.date_start, d.start_time, d.end_time, d.date_end, 
                    d.gross_seats, d.seat_kill, d.est_pct_ticksold, d.net_seats, 
                    d.est_sold_seats, d.workforce, d.unticketed,
                    d.detail_id
                FROM version_event_bridge b
                JOIN fct_event_base e ON b.event_id = e.event_id
                JOIN fct_event_details d ON b.detail_id = d.detail_id
                WHERE b.version_id = p_version_id1
                AND b.is_active = TRUE
            ),
            v2 AS (
                SELECT 
                    b.event_id, 
                    e.hash_key,
                    e.sport_id, e.venue_id, e.subvenue_id, e.region_id, e.zone_id, 
                    e.cluster_id, e.day_id, e.session_id, e.competition_type, e.event_type,
                    d.date_start, d.start_time, d.end_time, d.date_end, 
                    d.gross_seats, d.seat_kill, d.est_pct_ticksold, d.net_seats, 
                    d.est_sold_seats, d.workforce, d.unticketed,
                    d.detail_id
                FROM version_event_bridge b
                JOIN fct_event_base e ON b.event_id = e.event_id
                JOIN fct_event_details d ON b.detail_id = d.detail_id
                WHERE b.version_id = p_version_id2
                AND b.is_active = TRUE
            )
            SELECT 
                COALESCE(v1.event_id, v2.event_id),
                COALESCE(v1.hash_key, v2.hash_key),
                CASE 
                    WHEN v1.event_id IS NULL THEN 'ADDED' 
                    WHEN v2.event_id IS NULL THEN 'REMOVED'
                    WHEN v1.detail_id != v2.detail_id THEN 'MODIFIED'
                    ELSE 'UNCHANGED'
                END AS change_type,
                CASE
                    WHEN v1.event_id IS NULL OR v2.event_id IS NULL THEN NULL
                    WHEN v1.detail_id = v2.detail_id THEN NULL
                    ELSE jsonb_strip_nulls(jsonb_build_object(
                        'date_start', CASE WHEN v1.date_start IS DISTINCT FROM v2.date_start THEN jsonb_build_object('old', v1.date_start, 'new', v2.date_start) ELSE NULL END,
                        'start_time', CASE WHEN v1.start_time IS DISTINCT FROM v2.start_time THEN jsonb_build_object('old', v1.start_time, 'new', v2.start_time) ELSE NULL END,
                        'date_end', CASE WHEN v1.date_end IS DISTINCT FROM v2.date_end THEN jsonb_build_object('old', v1.date_end, 'new', v2.date_end) ELSE NULL END,
                        'end_time', CASE WHEN v1.end_time IS DISTINCT FROM v2.end_time THEN jsonb_build_object('old', v1.end_time, 'new', v2.end_time) ELSE NULL END,
                        'gross_seats', CASE WHEN v1.gross_seats IS DISTINCT FROM v2.gross_seats THEN jsonb_build_object('old', v1.gross_seats, 'new', v2.gross_seats) ELSE NULL END,
                        'seat_kill', CASE WHEN v1.seat_kill IS DISTINCT FROM v2.seat_kill THEN jsonb_build_object('old', v1.seat_kill, 'new', v2.seat_kill) ELSE NULL END,
                        'est_pct_ticksold', CASE WHEN v1.est_pct_ticksold IS DISTINCT FROM v2.est_pct_ticksold THEN jsonb_build_object('old', v1.est_pct_ticksold, 'new', v2.est_pct_ticksold) ELSE NULL END,
                        'net_seats', CASE WHEN v1.net_seats IS DISTINCT FROM v2.net_seats THEN jsonb_build_object('old', v1.net_seats, 'new', v2.net_seats) ELSE NULL END,
                        'est_sold_seats', CASE WHEN v1.est_sold_seats IS DISTINCT FROM v2.est_sold_seats THEN jsonb_build_object('old', v1.est_sold_seats, 'new', v2.est_sold_seats) ELSE NULL END,
                        'workforce', CASE WHEN v1.workforce IS DISTINCT FROM v2.workforce THEN jsonb_build_object('old', v1.workforce, 'new', v2.workforce) ELSE NULL END,
                        'unticketed', CASE WHEN v1.unticketed IS DISTINCT FROM v2.unticketed THEN jsonb_build_object('old', v1.unticketed, 'new', v2.unticketed) ELSE NULL END
                    ))
                END
            FROM v1
            FULL OUTER JOIN v2 ON v1.event_id = v2.event_id
            WHERE v1.event_id IS NULL OR v2.event_id IS NULL OR v1.detail_id != v2.detail_id;
        END;
        $$ LANGUAGE plpgsql;
        """)
        session.execute(create_function_ddl)
        
        session.commit()
        print("View and function created successfully.")
    
    except Exception as error:
        print("Error while creating view and function:", error)
    
    finally:
        # Close the database session
        session.close()
        print("SQLAlchemy session is closed.")

if __name__ == "__main__":
    create_view_and_function()
