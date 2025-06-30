"""
DuckDB Optimizations - Werfen Data Pipeline
===========================================

DuckDB-specific optimizations implementing performance best practices,
prepared for migration to Amazon Redshift and Athena.

Author: Lead Software Architect - Werfen Data Team
Date: January 2025
"""

import duckdb
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

# Import existing components
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.logging.structured_logger import get_pipeline_logger

@dataclass
class DuckDBOptimizationConfig:
    """DuckDB optimization configuration"""
    memory_limit: str = "1GB"
    threads: int = 4
    enable_parallel_processing: bool = True
    enable_statistics: bool = True
    enable_optimizer: bool = True
    checkpoint_threshold: str = "100MB"
    wal_mode: bool = True
    
class WerfenDuckDBOptimizer:
    """
    Performance optimizer for DuckDB
    
    AWS equivalents:
    - Memory optimization → Amazon Redshift WLM
    - Parallel processing → Redshift concurrency scaling
    - Statistics → Redshift ANALYZE
    - Optimization → Athena query optimization
    """
    
    def __init__(self, config: Optional[DuckDBOptimizationConfig] = None):
        self.config = config or DuckDBOptimizationConfig()
        self.logger = get_pipeline_logger("duckdb_optimizer")
        
        # AWS equivalent configuration
        self.aws_equivalents = {
            'memory_limit': 'Amazon Redshift: WLM memory allocation',
            'threads': 'Amazon Redshift: Concurrency scaling',
            'parallel_processing': 'AWS Glue: Parallel job execution',
            'statistics': 'Redshift: ANALYZE command',
            'optimizer': 'Athena: Query optimization engine'
        }
        
        self.logger.logger.info(
            "DuckDB optimizer initialized",
            component="duckdb_optimizer",
            memory_limit=self.config.memory_limit,
            threads=self.config.threads,
            event_type="optimizer_init"
        )
    
    def apply_optimizations(self, connection: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
        """Apply all optimizations to DuckDB connection"""
        optimizations_applied = []
        
        try:
            # 1. Configure memory
            connection.execute(f"SET memory_limit='{self.config.memory_limit}'")
            optimizations_applied.append(f"memory_limit={self.config.memory_limit}")
            
            # 2. Configure threads
            connection.execute(f"SET threads={self.config.threads}")
            optimizations_applied.append(f"threads={self.config.threads}")
            
            # 3. Enable parallel processing
            if self.config.enable_parallel_processing:
                connection.execute("SET enable_parallel=true")
                optimizations_applied.append("parallel_processing=enabled")
            
            # 4. Enable automatic statistics
            if self.config.enable_statistics:
                connection.execute("SET enable_statistics_collection=true")
                optimizations_applied.append("statistics_collection=enabled")
            
            # 5. Configure optimizer
            if self.config.enable_optimizer:
                connection.execute("SET optimizer_enable_all=true")
                optimizations_applied.append("optimizer=enabled")
            
            # 6. Configure checkpoint
            connection.execute(f"SET checkpoint_threshold='{self.config.checkpoint_threshold}'")
            optimizations_applied.append(f"checkpoint_threshold={self.config.checkpoint_threshold}")
            
            # 7. Configure WAL mode
            if self.config.wal_mode:
                connection.execute("PRAGMA journal_mode=WAL")
                optimizations_applied.append("wal_mode=enabled")
            
            self.logger.logger.info(
                "DuckDB optimizations applied",
                component="duckdb_optimizer",
                optimizations_count=len(optimizations_applied),
                optimizations=optimizations_applied,
                event_type="optimizations_applied"
            )
            
            return {
                "status": "success",
                "optimizations_applied": optimizations_applied,
                "aws_equivalents": self.aws_equivalents
            }
            
        except Exception as e:
            self.logger.log_error(
                error_message=str(e),
                error_type=type(e).__name__,
                operation="apply_optimizations"
            )
            return {
                "status": "error",
                "error": str(e),
                "optimizations_applied": optimizations_applied
            }
    
    def create_optimized_indexes(self, connection: duckdb.DuckDBPyConnection, 
                               schema: str = "raw") -> Dict[str, Any]:
        """Create optimized indexes for main tables"""
        indexes_created = []
        
        try:
            # Indexes for customer table
            customer_indexes = [
                f"CREATE INDEX IF NOT EXISTS idx_{schema}_customer_id ON {schema}.raw_customer(CustomerId)",
                f"CREATE INDEX IF NOT EXISTS idx_{schema}_customer_country ON {schema}.raw_customer(Country)"
            ]
            
            # Indexes for sales table
            sales_indexes = [
                f"CREATE INDEX IF NOT EXISTS idx_{schema}_sales_customer ON {schema}.raw_sales_quantity(customer_id)",
                f"CREATE INDEX IF NOT EXISTS idx_{schema}_sales_date ON {schema}.raw_sales_quantity(year, month)",
                f"CREATE INDEX IF NOT EXISTS idx_{schema}_sales_material ON {schema}.raw_sales_quantity(material_code)"
            ]
            
            # Indexes for FOC table
            foc_indexes = [
                f"CREATE INDEX IF NOT EXISTS idx_{schema}_foc_customer ON {schema}.raw_free_of_charge_quantity(customer_id)",
                f"CREATE INDEX IF NOT EXISTS idx_{schema}_foc_date ON {schema}.raw_free_of_charge_quantity(year, month)",
                f"CREATE INDEX IF NOT EXISTS idx_{schema}_foc_material ON {schema}.raw_free_of_charge_quantity(material_code)"
            ]
            
            all_indexes = customer_indexes + sales_indexes + foc_indexes
            
            for index_sql in all_indexes:
                connection.execute(index_sql)
                indexes_created.append(index_sql.split("IF NOT EXISTS ")[1].split(" ON ")[0])
            
            self.logger.logger.info(
                "Optimized indexes created",
                component="duckdb_optimizer",
                schema=schema,
                indexes_count=len(indexes_created),
                indexes=indexes_created,
                event_type="indexes_created"
            )
            
            return {
                "status": "success",
                "indexes_created": indexes_created,
                "aws_equivalent": "Amazon Redshift: CREATE INDEX, Athena: Partitioning"
            }
            
        except Exception as e:
            self.logger.log_error(
                error_message=str(e),
                error_type=type(e).__name__,
                operation="create_optimized_indexes"
            )
            return {
                "status": "error",
                "error": str(e),
                "indexes_created": indexes_created
            }
    
    def analyze_table_statistics(self, connection: duckdb.DuckDBPyConnection,
                                schema: str = "raw") -> Dict[str, Any]:
        """Analyze table statistics for optimization"""
        table_stats = {}
        
        try:
            # Get table list
            tables_query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}'
            """
            tables = connection.execute(tables_query).fetchall()
            
            for (table_name,) in tables:
                # Basic statistics
                stats_query = f"""
                SELECT 
                    COUNT(*) as row_count,
                    COUNT(DISTINCT *) as distinct_rows
                FROM {schema}.{table_name}
                """
                
                stats = connection.execute(stats_query).fetchone()
                
                table_stats[table_name] = {
                    "row_count": stats[0] if stats else 0,
                    "distinct_rows": stats[1] if stats else 0,
                    "duplicate_percentage": ((stats[0] - stats[1]) / stats[0] * 100) if stats and stats[0] > 0 else 0
                }
            
            self.logger.logger.info(
                "Table statistics analyzed",
                component="duckdb_optimizer",
                schema=schema,
                tables_analyzed=len(table_stats),
                event_type="statistics_analyzed"
            )
            
            return {
                "status": "success",
                "table_statistics": table_stats,
                "aws_equivalent": "Amazon Redshift: ANALYZE command, Athena: MSCK REPAIR TABLE"
            }
            
        except Exception as e:
            self.logger.log_error(
                error_message=str(e),
                error_type=type(e).__name__,
                operation="analyze_table_statistics"
            )
            return {
                "status": "error",
                "error": str(e)
            }
    
    def optimize_query_plan(self, connection: duckdb.DuckDBPyConnection,
                           query: str) -> Dict[str, Any]:
        """Analyze and optimize query plan"""
        try:
            # Get execution plan
            explain_query = f"EXPLAIN {query}"
            plan = connection.execute(explain_query).fetchall()
            
            # Analyze plan for suggestions
            plan_text = "\n".join([row[0] for row in plan])
            suggestions = self._analyze_execution_plan(plan_text)
            
            self.logger.logger.info(
                "Query plan optimized",
                component="duckdb_optimizer",
                query_length=len(query),
                suggestions_count=len(suggestions),
                event_type="query_plan_optimized"
            )
            
            return {
                "status": "success",
                "execution_plan": plan_text,
                "optimization_suggestions": suggestions,
                "aws_equivalent": "Amazon Redshift: EXPLAIN plan, Athena: Query execution details"
            }
            
        except Exception as e:
            self.logger.log_error(
                error_message=str(e),
                error_type=type(e).__name__,
                operation="optimize_query_plan"
            )
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _analyze_execution_plan(self, plan_text: str) -> List[str]:
        """Analyze execution plan and generate suggestions"""
        suggestions = []
        
        # Basic plan analysis
        if "FULL_SCAN" in plan_text.upper():
            suggestions.append("Consider adding indexes for columns in WHERE clauses")
        
        if "HASH_JOIN" in plan_text.upper():
            suggestions.append("Hash joins detected - ensure adequate memory allocation")
        
        if "SORT" in plan_text.upper():
            suggestions.append("Sort operations detected - consider pre-sorting data")
        
        # AWS suggestions
        suggestions.extend([
            "AWS: Consider using Redshift sortkeys for frequently sorted columns",
            "AWS: Use Athena columnar formats (Parquet) for better compression",
            "AWS: Implement data partitioning for large tables"
        ])
        
        return suggestions

def get_duckdb_optimizer(config: Optional[DuckDBOptimizationConfig] = None) -> WerfenDuckDBOptimizer:
    """Factory function to get DuckDB optimizer"""
    return WerfenDuckDBOptimizer(config) 