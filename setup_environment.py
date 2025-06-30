#!/usr/bin/env python3
"""
Werfen Data Pipeline - Environment Setup Script
==============================================

This script sets up the entire environment needed to run the data pipeline:
- Initializes Apache Airflow
- Configures Great Expectations
- Prepares necessary directories
- Validates dependencies

Author: Daniel (Tech Lead Candidate)
Purpose: Automatic development/production environment setup
"""

import os
import sys
import subprocess
from pathlib import Path
import shutil

# Import centralized configuration
from config import get_config

# Portable configuration
config = get_config()
PROJECT_ROOT = config.project_root
AIRFLOW_HOME = config.airflow_home
DAGS_FOLDER = config.dags_folder
ARTIFACTS_DIR = config.artifacts_folder

def print_banner():
    """Display project banner."""
    print("=" * 70)
    print("🏗️  WERFEN DATA PIPELINE - ENVIRONMENT SETUP")
    print("=" * 70)
    print("🎯 Objective: Setup complete environment for data pipeline")
    print("🔧 Stack: Python + Airflow + dbt + Great Expectations + DuckDB")
    print("📁 Project:", PROJECT_ROOT)
    print("=" * 70)

def check_python_version():
    """Check Python version."""
    print("\n🐍 Checking Python version...")
    
    version = sys.version_info
    if version.major == 3 and version.minor >= 8:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} - Compatible")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} - Python 3.8+ required")
        return False

def create_directories():
    """Create necessary directories."""
    print("\n📁 Creating directory structure...")
    
    directories = [
        AIRFLOW_HOME,
        AIRFLOW_HOME / "logs",
        AIRFLOW_HOME / "plugins",
        ARTIFACTS_DIR,
        PROJECT_ROOT / "logs",
        PROJECT_ROOT / "reports",
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"✅ Directory created: {directory}")

def setup_airflow():
    """Configure Apache Airflow."""
    print("\n🚁 Configuring Apache Airflow...")
    
    try:
        # Configure Airflow environment variables using configuration
        config.apply_airflow_environment()
        
        print("✅ Environment variables configured")
        
        # Initialize Airflow database
        print("🔧 Initializing Airflow database...")
        result = subprocess.run(
            [sys.executable, '-m', 'airflow', 'db', 'init'],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT
        )
        
        if result.returncode == 0:
            print("✅ Airflow database initialized")
        else:
            print("⚠️ Warning initializing Airflow:", result.stderr[:200])
        
        # Create admin user
        print("👤 Creating admin user...")
        create_user_result = subprocess.run([
            sys.executable, '-m', 'airflow', 'users', 'create',
            '--username', 'admin',
            '--firstname', 'Werfen',
            '--lastname', 'DataTeam',
            '--role', 'Admin',
            '--email', config.admin_email,
            '--password', config.admin_password
        ], capture_output=True, text=True, cwd=PROJECT_ROOT)
        
        if create_user_result.returncode == 0:
            print("✅ Admin user created")
            print(f"   📧 Email: {config.admin_email}")
            print(f"   🔑 Password: {config.admin_password}")
        else:
            print("⚠️ Admin user already exists or error in creation")
        
        return True
        
    except Exception as e:
        print(f"❌ Error configuring Airflow: {e}")
        return False

def validate_dbt_setup():
    """Validate dbt configuration."""
    print("\n🔨 Validating dbt configuration...")
    
    try:
        dbt_project_file = PROJECT_ROOT / "dbt_project" / "dbt_project.yml"
        profiles_file = PROJECT_ROOT / "dbt_project" / "profiles.yml"
        
        if dbt_project_file.exists() and profiles_file.exists():
            print("✅ dbt configuration files found")
            
            # Change to dbt directory and run debug
            os.chdir(PROJECT_ROOT / "dbt_project")
            result = subprocess.run(
                [sys.executable, '-m', 'dbt', 'debug'],
                capture_output=True,
                text=True
            )
            
            if "All checks passed!" in result.stdout:
                print("✅ dbt configuration validated successfully")
                return True
            else:
                print("⚠️ Warnings in dbt configuration")
                return True  # Continue even with warnings
        else:
            print("❌ dbt configuration files not found")
            return False
            
    except Exception as e:
        print(f"❌ Error validating dbt: {e}")
        return False
    finally:
        os.chdir(PROJECT_ROOT)

def create_startup_scripts():
    """Create startup scripts."""
    print("\n📜 Creating startup scripts...")
    
    # Define scripts directory
    scripts_dir = PROJECT_ROOT / "src" / "scripts"
    scripts_dir.mkdir(exist_ok=True)

    # Script to start Airflow
    airflow_start_script = scripts_dir / "start_airflow.py"
    airflow_script_content = '''#!/usr/bin/env python3
"""Script to start Apache Airflow"""
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
os.environ['AIRFLOW_HOME'] = str(PROJECT_ROOT / "airflow")
os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = str(PROJECT_ROOT / "dags")

def start_airflow():
    print("🚁 Starting Apache Airflow...")
    print("🌐 Webserver: http://localhost:8080")
    print("👤 User: admin")
    print("🔑 Password: werfen2025")
    print("-" * 50)
    
    try:
        # Start webserver in background
        webserver = subprocess.Popen([
            sys.executable, '-m', 'airflow', 'webserver', '--port', '8080'
        ])
        
        # Start scheduler
        scheduler = subprocess.Popen([
            sys.executable, '-m', 'airflow', 'scheduler'
        ])
        
        print("✅ Airflow started successfully")
        print("   🌐 Webserver PID:", webserver.pid)
        print("   📅 Scheduler PID:", scheduler.pid)
        print("   ⏹️  To stop: Ctrl+C")
        
        # Wait for processes to finish
        webserver.wait()
        scheduler.wait()
        
    except KeyboardInterrupt:
        print("\\n🛑 Stopping Airflow...")
        webserver.terminate()
        scheduler.terminate()
        print("✅ Airflow stopped")

if __name__ == "__main__":
    start_airflow()
'''
    
    with open(airflow_start_script, 'w', encoding='utf-8') as f:
        f.write(airflow_script_content)
    
    # Make executable on Unix systems
    if os.name != 'nt':
        os.chmod(airflow_start_script, 0o755)
    
    print(f"✅ Startup script created: {airflow_start_script}")
    
    # Script to run complete pipeline
    pipeline_script = scripts_dir / "run_pipeline.py"
    pipeline_script_content = '''#!/usr/bin/env python3
"""Script to run the complete pipeline"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def run_complete_pipeline():
    print("🚀 RUNNING COMPLETE WERFEN PIPELINE")
    print("=" * 50)
    
    try:
        # 1. Data ingestion
        print("\\n1️⃣ Running data ingestion...")
        from ingestion.load_raw_data import main as run_ingestion
        if not run_ingestion():
            print("❌ Error in ingestion")
            return False
        
        # 2. Validations
        print("\\n2️⃣ Running validations...")
        from validation.setup_great_expectations import main as run_validation
        if not run_validation():
            print("❌ Error in validations")
            return False
        
        # 3. dbt transformations (staging)
        print("\\n3️⃣ Running dbt transformations...")
        import subprocess
        import os
        
        os.chdir(Path(__file__).parent / "dbt_project")
        
        # dbt run
        result = subprocess.run([sys.executable, '-m', 'dbt', 'run', '--select', 'staging'])
        if result.returncode != 0:
            print("❌ Error in dbt run")
            return False
        
        # dbt test
        result = subprocess.run([sys.executable, '-m', 'dbt', 'test', '--select', 'staging'])
        if result.returncode != 0:
            print("❌ Error in dbt test")
            return False
        
        print("\\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        return True
        
    except Exception as e:
        print(f"❌ Pipeline error: {e}")
        return False

if __name__ == "__main__":
    success = run_complete_pipeline()
    sys.exit(0 if success else 1)
'''
    
    with open(pipeline_script, 'w', encoding='utf-8') as f:
        f.write(pipeline_script_content)
    
    if os.name != 'nt':
        os.chmod(pipeline_script, 0o755)
    
    print(f"✅ Pipeline script created: {pipeline_script}")

def main():
    """Main setup function."""
    print_banner()
    
    success = True
    
    # Check Python
    if not check_python_version():
        success = False
    
    # Create directories
    create_directories()
    
    # Configure Airflow
    if not setup_airflow():
        success = False
    
    # Validate dbt
    if not validate_dbt_setup():
        success = False
    
    # Create scripts
    create_startup_scripts()
    
    print("\n" + "=" * 70)
    if success:
        print("🎉 SETUP COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print("🚀 Next steps:")
        print("   1. Run pipeline: python scripts/run_pipeline.py")
        print("   2. Start Airflow: python scripts/start_airflow.py")
        print("   3. Access UI: http://localhost:8080")
        print("   4. User: admin / Password: werfen2025")
    else:
        print("⚠️ SETUP COMPLETED WITH WARNINGS")
        print("=" * 70)
        print("🔧 Review errors above before continuing")
    
    print("=" * 70)

if __name__ == "__main__":
    main() 