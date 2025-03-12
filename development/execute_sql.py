from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, FctEventSession, DimScenarios, DimScheduleVersion

def execute_sql_statements():
    try:
        # Connect to your PostgreSQL database using SQLAlchemy
        engine = create_engine('postgresql+psycopg2://your_db_user:your_db_password@your_db_host/your_db_name')
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Create tables
        Base.metadata.create_all(engine)
        
        # Example: Add a new record
        new_event = FctEventSession(
            hash_key='example_hash_key',
            scenario_id=1,
            version_id=1,
            sport_id=1,
            venue_id=1,
            subvenue_id=1,
            region_id=1,
            zone_id=1,
            cluster_id=1,
            day_id=1,
            session_id=1,
            date_start='2023-01-01',
            start_time='10:00:00',
            date_end='2023-01-01',
            end_time='12:00:00',
            competition_type='A',
            event_type='Type1',
            gross_seats=100,
            seat_kill=0.0,
            est_pct_ticksold=0.0,
            net_seats=100,
            est_sold_seats=0,
            workforce=10,
            unticketed=0,
            created_by='user',
            additional_attributes={}
        )
        session.add(new_event)
        session.commit()
        
        print("SQL statements executed successfully.")
    
    except Exception as error:
        print("Error while executing SQL statements:", error)
    
    finally:
        # Close the database session
        session.close()
        print("SQLAlchemy session is closed.")

if __name__ == "__main__":
    execute_sql_statements()
