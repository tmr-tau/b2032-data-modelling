-- Create Scenario Dimension
CREATE TABLE IF NOT EXISTS dim_scenarios (
    scenario_id SERIAL PRIMARY KEY,
    scenario_name VARCHAR(100) NOT NULL,
    scenario_description TEXT,
    created_by VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_baseline BOOLEAN DEFAULT FALSE
);

-- Enhance the Schedule Version Dimension
CREATE TABLE IF NOT EXISTS dim_schedule_version (
    version_id SERIAL PRIMARY KEY,
    scenario_id INTEGER REFERENCES dim_scenarios(scenario_id),
    version_number INTEGER NOT NULL,
    version_code VARCHAR(50) NOT NULL,
    version_description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50),
    status VARCHAR(20) DEFAULT 'DRAFT', -- DRAFT, PUBLISHED, ARCHIVED
    UNIQUE(scenario_id, version_number)
);