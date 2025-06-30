#!/usr/bin/env python3
"""
Werfen ML Pipeline Execution Script
===================================

Main script to run the complete Machine Learning pipeline
for Werfen customer segmentation.

Usage:
    python scripts/run_ml_pipeline.py [options]

Example:
    python scripts/run_ml_pipeline.py --snapshot-date 2024-01-01 --min-transactions 2

Author: Werfen Data Science Team
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from ml_pipeline import WerfenMLPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ml_pipeline.log')
    ]
)

logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Run ML pipeline for Werfen customer segmentation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage examples:
  # Run complete pipeline with default configuration
  python scripts/run_ml_pipeline.py
  
  # Run for a specific date
  python scripts/run_ml_pipeline.py --snapshot-date 2024-01-01
  
  # Filter customers with minimum transactions
  python scripts/run_ml_pipeline.py --min-transactions 3
  
  # Use hybrid feature strategy
  python scripts/run_ml_pipeline.py --feature-strategy hybrid
  
  # Configure K range for optimization
  python scripts/run_ml_pipeline.py --k-min 2 --k-max 6
        """
    )
    
    # Data arguments
    parser.add_argument(
        '--db-path',
        type=str,
        default=None,
        help='Path to DuckDB database (default: dbt_project/target/dev.duckdb)'
    )
    
    parser.add_argument(
        '--snapshot-date',
        type=str,
        default=None,
        help='Specific date for analysis (format: YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--min-transactions',
        type=int,
        default=1,
        help='Minimum number of transactions to include customer (default: 1)'
    )
    
    # Feature engineering arguments
    parser.add_argument(
        '--feature-strategy',
        type=str,
        choices=['business_driven', 'statistical', 'hybrid'],
        default='business_driven',
        help='Feature selection strategy (default: business_driven)'
    )
    
    # Clustering arguments
    parser.add_argument(
        '--k-min',
        type=int,
        default=2,
        help='Minimum K value for optimization (default: 2)'
    )
    
    parser.add_argument(
        '--k-max',
        type=int,
        default=8,
        help='Maximum K value for optimization (default: 8)'
    )
    
    parser.add_argument(
        '--random-state',
        type=int,
        default=42,
        help='Seed for reproducibility (default: 42)'
    )
    
    # Output arguments
    parser.add_argument(
        '--output-dir',
        type=str,
        default='ml_outputs',
        help='Directory to save results (default: ml_outputs)'
    )
    
    parser.add_argument(
        '--no-visualizations',
        action='store_true',
        help='Don\'t create visualizations (faster)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='More detailed logging'
    )
    
    return parser.parse_args()


def validate_arguments(args):
    """Validate input arguments."""
    errors = []
    
    # Validate date if provided
    if args.snapshot_date:
        try:
            datetime.strptime(args.snapshot_date, '%Y-%m-%d')
        except ValueError:
            errors.append("--snapshot-date must be in YYYY-MM-DD format")
    
    # Validate ranges
    if args.min_transactions < 0:
        errors.append("--min-transactions must be >= 0")
    
    if args.k_min < 2:
        errors.append("--k-min must be >= 2")
    
    if args.k_max <= args.k_min:
        errors.append("--k-max must be > --k-min")
    
    if args.random_state < 0:
        errors.append("--random-state must be >= 0")
    
    if errors:
        for error in errors:
            logger.error(error)
        sys.exit(1)


def main():
    """Main function."""
    # Parse and validate arguments
    args = parse_arguments()
    validate_arguments(args)
    
    # Configure detailed logging if requested
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("🚀 Starting Werfen ML pipeline")
    logger.info(f"Configuration:")
    logger.info(f"  - Database: {args.db_path or 'default'}")
    logger.info(f"  - Snapshot date: {args.snapshot_date or 'most recent'}")
    logger.info(f"  - Min. transactions: {args.min_transactions}")
    logger.info(f"  - Feature strategy: {args.feature_strategy}")
    logger.info(f"  - K range: {args.k_min}-{args.k_max}")
    logger.info(f"  - Output directory: {args.output_dir}")
    
    try:
        # Initialize pipeline
        pipeline = WerfenMLPipeline(
            db_path=args.db_path,
            output_dir=args.output_dir,
            random_state=args.random_state
        )
        
        # Execute complete pipeline
        results = pipeline.run_complete_pipeline(
            snapshot_date=args.snapshot_date,
            min_transactions=args.min_transactions,
            feature_strategy=args.feature_strategy,
            k_range=range(args.k_min, args.k_max + 1),
            create_visualizations=not args.no_visualizations
        )
        
        # Show results summary
        logger.info("✅ Pipeline completed successfully")
        logger.info(f"📊 Main results:")
        logger.info(f"  - Execution time: {results['execution_time']}")
        logger.info(f"  - Optimal K: {results['optimal_k']}")
        logger.info(f"  - Model quality: {results['model_quality']['quality_assessment']}")
        
        # Show persona distribution
        if 'persona_distribution' in results:
            logger.info(f"  - Persona distribution:")
            for persona, metrics in results['persona_distribution'].items():
                if isinstance(metrics, dict) and 'Customer Count' in metrics:
                    count = metrics['Customer Count']
                    pct = metrics.get('% of Customers', 0)
                    logger.info(f"    • {persona}: {count:,} customers ({pct:.1f}%)")
        
        # Generated files information
        output_path = Path(args.output_dir)
        logger.info(f"📁 Files generated in: {output_path.absolute()}")
        logger.info(f"  - Results: pipeline_results.json")
        logger.info(f"  - Trained model: trained_model.pkl")
        
        if not args.no_visualizations:
            logger.info(f"  - Visualizations: visualizations/")
        
        logger.info("🎯 ML pipeline completed. Ready for business analysis!")
        
    except Exception as e:
        logger.error(f"❌ Error executing pipeline: {str(e)}")
        if args.verbose:
            logger.exception("Error details:")
        sys.exit(1)


if __name__ == "__main__":
    main() 