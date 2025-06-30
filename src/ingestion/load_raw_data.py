import sqlite3
import duckdb
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime
import traceback

# Import centralized configuration
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config import get_config

# Import structured logging
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.logging.structured_logger import get_pipeline_logger, log_execution

# Import secure credential management
from src.security.credential_manager import get_credential_manager

# --- Configuration ---
# Use centralized configuration for paths
config = get_config()
BASE_DIR = config.project_root
DATA_DIR = config.data_folder
RAW_DATA_DIR = config.raw_data_folder
ARTIFACTS_DIR = config.artifacts_folder
DUCKDB_PATH = config.main_database_path

# Use centralized configuration for data sources
SOURCES_CONFIG = config.data_sources_config

# Target schema in DuckDB for the raw data.
TARGET_SCHEMA = "raw"

# Initialize logger and credential manager
logger = get_pipeline_logger("data_ingestion")
credential_manager = get_credential_manager()

# --- Validation Functions ---

@log_execution("validate_dataframe")
def validate_dataframe(df: pd.DataFrame, table_name: str) -> bool:
    """Runs a series of validation checks on a DataFrame."""
    logger.logger.info(
        "Starting dataframe validation",
        table_name=table_name,
        row_count=len(df),
        columns=list(df.columns),
        event_type="validation_start"
    )
    
    is_valid = True
    validation_results = []

    # Example: Check for non-null columns
    if table_name == "raw_customer":
        null_count = df['CustomerId'].isnull().sum()
        if null_count > 0:
            is_valid = False
            validation_results.append({
                "check": "CustomerId_not_null",
                "status": "FAILED",
                "null_count": int(null_count)
            })
            logger.logger.error(
                "Validation failed: CustomerId contains nulls",
                table_name=table_name,
                null_count=int(null_count),
                event_type="validation_failure"
            )
        else:
            validation_results.append({
                "check": "CustomerId_not_null", 
                "status": "PASSED"
            })

    # Check for expected row counts using configuration
    expected_counts = config.expected_row_counts
    if table_name in expected_counts:
        expected_count = expected_counts[table_name]
        actual_count = len(df)
        if actual_count != expected_count:
            is_valid = False
            validation_results.append({
                "check": "expected_row_count",
                "status": "FAILED",
                "expected": expected_count,
                "actual": actual_count,
                "variance": actual_count - expected_count
            })
            logger.logger.error(
                "Validation failed: Row count mismatch",
                table_name=table_name,
                expected_count=expected_count,
                actual_count=actual_count,
                variance=actual_count - expected_count,
                event_type="validation_failure"
            )
        else:
            validation_results.append({
                "check": "expected_row_count",
                "status": "PASSED",
                "count": actual_count
            })

    # Log validation summary
    logger.log_quality_check(
        check_name=f"{table_name}_validation",
        status="PASSED" if is_valid else "FAILED",
        details={
            "validation_results": validation_results,
            "total_checks": len(validation_results),
            "passed_checks": len([r for r in validation_results if r["status"] == "PASSED"])
        }
    )
    
    return is_valid

# --- ETL Functions ---

@log_execution("extract_from_sqlite")
def extract_from_sqlite(db_path: Path, table_name: str) -> pd.DataFrame:
    """Extracts a table from a SQLite database into a pandas DataFrame."""
    start_time = datetime.now()
    
    logger.logger.info(
        "Starting data extraction",
        source_db=str(db_path.name),
        source_table=table_name,
        event_type="extraction_start"
    )
    
    try:
        with sqlite3.connect(db_path) as sqlite_conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", sqlite_conn)
            
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            logger.log_data_operation(
                operation="extract",
                table_name=table_name,
                record_count=len(df),
                duration_ms=duration_ms,
                success=True,
                source_database=str(db_path.name),
                source_type="sqlite"
            )
            
            return df
            
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        logger.log_error(
            error_message=str(e),
            error_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            operation="extract",
            table_name=table_name,
            source_database=str(db_path.name),
            duration_ms=duration_ms
        )
        return None

@log_execution("load_to_duckdb")
def load_to_duckdb(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str) -> bool:
    """Loads a pandas DataFrame into a specific schema and table in DuckDB."""
    target_table_path = f"{TARGET_SCHEMA}.{table_name}"
    start_time = datetime.now()
    
    logger.logger.info(
        "Starting data load",
        target_table=target_table_path,
        record_count=len(df),
        event_type="load_start"
    )
    
    try:
        # Use DuckDB's ability to directly query pandas DataFrames.
        # This is efficient and clean.
        con.execute(f"CREATE OR REPLACE TABLE {target_table_path} AS SELECT * FROM df")
        row_count = con.execute(f"SELECT COUNT(*) FROM {target_table_path}").fetchone()[0]
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        logger.log_data_operation(
            operation="load",
            table_name=target_table_path,
            record_count=row_count,
            duration_ms=duration_ms,
            success=True,
            target_database="duckdb",
            target_schema=TARGET_SCHEMA
        )
        
        return True
        
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        logger.log_error(
            error_message=str(e),
            error_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            operation="load",
            table_name=target_table_path,
            duration_ms=duration_ms
        )
        return False

# --- Main Execution ---

@log_execution("data_ingestion_pipeline")
def main():
    """Main function to orchestrate the EL (Extract-Load) process with validation."""
    
    # Start pipeline run
    run_id = logger.start_pipeline_run(run_type="manual")
    
    logger.logger.info(
        "Starting raw data ingestion pipeline",
        pipeline_run_id=run_id,
        target_schema=TARGET_SCHEMA,
        duckdb_path=str(DUCKDB_PATH),
        event_type="pipeline_start"
    )
    
    # Ensure the directory for the DuckDB file exists.
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    
    all_validations_passed = True
    all_loads_successful = True
    processed_tables = 0
    total_records = 0
    
    pipeline_start_time = datetime.now()
    
    try:
        with duckdb.connect(database=str(DUCKDB_PATH), read_only=False) as duckdb_conn:
            logger.logger.info(
                "Connected to DuckDB warehouse",
                database_path=str(DUCKDB_PATH),
                event_type="database_connection"
            )
            
            # Create schema
            duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
            logger.logger.info(
                "Schema ensured",
                schema_name=TARGET_SCHEMA,
                event_type="schema_creation"
            )

            for db_file, tables_map in SOURCES_CONFIG.items():
                db_path = RAW_DATA_DIR / db_file
                
                logger.logger.info(
                    "Processing source database",
                    source_database=str(db_path),
                    tables_to_process=list(tables_map.keys()),
                    event_type="source_processing_start"
                )
                
                if not db_path.exists():
                    logger.log_error(
                        error_message=f"Source database not found: {db_path}",
                        error_type="FileNotFoundError",
                        source_database=str(db_path)
                    )
                    all_validations_passed = False
                    continue

                for source_table, dest_table in tables_map.items():
                    table_start_time = datetime.now()
                    
                    logger.logger.info(
                        "Processing table",
                        source_table=source_table,
                        dest_table=dest_table,
                        source_database=db_file,
                        event_type="table_processing_start"
                    )
                    
                    df = extract_from_sqlite(db_path, source_table)
                    if df is not None and not df.empty:
                        if validate_dataframe(df, dest_table):
                            load_success = load_to_duckdb(duckdb_conn, df, dest_table)
                            if load_success:
                                processed_tables += 1
                                total_records += len(df)
                                
                                table_duration = int((datetime.now() - table_start_time).total_seconds() * 1000)
                                logger.log_performance_metric(
                                    metric_name="table_processing_time",
                                    value=table_duration,
                                    unit="ms",
                                    table_name=dest_table,
                                    record_count=len(df)
                                )
                            else:
                                all_loads_successful = False
                        else:
                            all_validations_passed = False
                            logger.logger.warning(
                                "Halting load due to validation failure",
                                dest_table=dest_table,
                                event_type="validation_halt"
                            )
                    else:
                        all_validations_passed = False
                        logger.log_error(
                            error_message="No data extracted",
                            error_type="DataExtractionError",
                            dest_table=dest_table
                        )
    
    except Exception as e:
        logger.log_error(
            error_message=f"Database connection failed: {str(e)}",
            error_type=type(e).__name__,
            stack_trace=traceback.format_exc()
        )
        logger.end_pipeline_run(success=False)
        return False
    
    # Pipeline summary
    pipeline_duration = int((datetime.now() - pipeline_start_time).total_seconds() * 1000)
    success = all_validations_passed and all_loads_successful
    
    summary = {
        "processed_tables": processed_tables,
        "total_records": total_records,
        "duration_ms": pipeline_duration,
        "validations_passed": all_validations_passed,
        "loads_successful": all_loads_successful
    }
    
    logger.end_pipeline_run(success=success, summary=summary)
    
    if success:
        logger.logger.info(
            "Pipeline completed successfully",
            **summary,
            event_type="pipeline_success"
        )
    else:
        logger.logger.error(
            "Pipeline completed with errors",
            **summary,
            event_type="pipeline_failure"
        )
    
    return success

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1) 