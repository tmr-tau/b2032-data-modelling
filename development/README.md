# B2032 Data Modelling

## Project Overview
This project is focused on data modelling for B2032 Transport SSOT Data Platfomr. It includes the creation of various database tables, views, and functions to manage and analyze event data.

## Project Structure
- **models.py**: Defines SQLAlchemy ORM models for the database tables.
- **execute_sql.py**: Contains code to create tables and insert sample data using SQLAlchemy.
- **views_and_functions.py**: Defines views and functions using SQLAlchemy's DDL.
- **create_fct.sql**: SQL script to create tables, views, and functions directly in the database.
- **pyproject.toml**: Configuration file for the project dependencies.

## Dependencies
The project requires the following Python packages:
- `psycopg2`
- `boto3`
- `sqlalchemy`

These dependencies are listed in the `pyproject.toml` file.

## Getting Started
1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set Up Database**:
   - Ensure you have a PostgreSQL database set up.
   - Update the database connection details in the Python files.

3. **Create Tables and Insert Data**:
   - Run `execute_sql.py` to create tables and insert sample data.
   ```bash
   python execute_sql.py
   ```

4. **Create Views and Functions**:
   - Run `views_and_functions.py` to create views and functions.
   ```bash
   python views_and_functions.py
   ```

## Usage
- Use the defined ORM models to interact with the database.
- Use the views and functions for advanced data analysis and comparison.
