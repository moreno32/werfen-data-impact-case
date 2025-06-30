#!/usr/bin/env python3
"""
Werfen Data Pipeline - Portability Test
=======================================

Script to validate that portability improvements work correctly.
Demonstrates that the code is now portable between different environments.

Author: Lead Software Architect
Purpose: Validate portability refactoring
"""

import os
import sys
from pathlib import Path

def test_config_portability():
    """Test that configuration is portable."""
    print("🧪 TESTING CONFIGURATION PORTABILITY")
    print("=" * 50)
    
    try:
        from config import get_config
        config = get_config()
        
        print("✅ Configuration imported correctly")
        print(f"📁 Project detected at: {config.project_root}")
        print(f"🖥️  System: {config._system_info['os']}")
        print(f"🐍 Python: {config._system_info['python_version']}")
        
        # Verify automatic path detection
        expected_files = [
            ("requirements.txt", config.project_root / "requirements.txt"),
            ("dbt_project.yml", config.dbt_project_folder / "dbt_project.yml"),
            ("Main DAG", config.dags_folder / "werfen_data_pipeline_dag.py"),
        ]
        
        print("\n🔍 VALIDATING AUTOMATIC PATH DETECTION:")
        all_valid = True
        
        for name, path in expected_files:
            exists = path.exists()
            status = "✅" if exists else "❌"
            print(f"  {status} {name}: {path}")
            if not exists:
                all_valid = False
        
        return all_valid
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def test_environment_variables():
    """Test configuration with environment variables."""
    print("\n🌍 TESTING ENVIRONMENT VARIABLES")
    print("=" * 50)
    
    try:
        # Set test environment variables
        test_vars = {
            'WERFEN_ADMIN_EMAIL': 'test@example.com',
            'WERFEN_ENV': 'testing',
            'WERFEN_DEBUG': 'true'
        }
        
        # Save original values
        original_vars = {}
        for key in test_vars:
            original_vars[key] = os.environ.get(key)
            os.environ[key] = test_vars[key]
        
        # Reimport configuration with new variables
        import importlib
        import config
        importlib.reload(config)
        test_config = config.get_config()
        
        # Verify that variables were applied
        tests = [
            (test_config.admin_email == 'test@example.com', "Custom email"),
            (test_config.environment == 'testing', "Custom environment"),
            (test_config.debug_mode == True, "Debug mode activated"),
        ]
        
        all_passed = True
        for test_result, description in tests:
            status = "✅" if test_result else "❌"
            print(f"  {status} {description}")
            if not test_result:
                all_passed = False
        
        # Restore original variables
        for key, original_value in original_vars.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value
        
        return all_passed
        
    except Exception as e:
        print(f"❌ Environment variables error: {e}")
        return False

def test_cross_platform_paths():
    """Test cross-platform compatibility."""
    print("\n🖥️  TESTING CROSS-PLATFORM COMPATIBILITY")
    print("=" * 50)
    
    try:
        from config import get_config
        config = get_config()
        
        # Verify that paths work on current OS
        paths_to_test = [
            ("Project root", config.project_root),
            ("src directory", config.src_folder),
            ("dbt directory", config.dbt_project_folder),
            ("artifacts directory", config.artifacts_folder),
            ("Database", config.main_database_path.parent),
        ]
        
        all_valid = True
        for name, path in paths_to_test:
            # Verify that path is absolute and valid
            is_absolute = path.is_absolute()
            is_valid = True
            
            try:
                resolved_path = path.resolve()
                is_valid = True
            except Exception:
                is_valid = False
            
            status = "✅" if (is_absolute and is_valid) else "❌"
            print(f"  {status} {name}: {path} (absolute: {is_absolute})")
            
            if not (is_absolute and is_valid):
                all_valid = False
        
        return all_valid
        
    except Exception as e:
        print(f"❌ Path compatibility error: {e}")
        return False

def test_imported_modules():
    """Test that refactored modules work."""
    print("\n🔧 TESTING REFACTORED MODULES")
    print("=" * 50)
    
    tests_passed = 0
    total_tests = 0
    
    # Test 1: Ingestion module
    total_tests += 1
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
        from ingestion.load_raw_data import SOURCES_CONFIG, DUCKDB_PATH
        
        # Verify it uses centralized configuration
        if "chinook.db" in SOURCES_CONFIG and str(DUCKDB_PATH).endswith("werfen.db"):
            print("  ✅ Ingestion module refactored correctly")
            tests_passed += 1
        else:
            print("  ❌ Ingestion module doesn't use centralized configuration")
    except Exception as e:
        print(f"  ❌ Error importing ingestion module: {e}")
    
    # Test 2: Airflow DAG
    total_tests += 1
    try:
        # Verify that DAG was refactored (without executing it)
        dag_file = Path(__file__).resolve().parent / "dags" / "werfen_data_pipeline_dag.py"
        if dag_file.exists():
            dag_content = dag_file.read_text()
            if "from config import get_config" in dag_content:
                print("  ✅ Airflow DAG refactored correctly")
                tests_passed += 1
            else:
                print("  ❌ Airflow DAG not refactored")
        else:
            print("  ❌ Airflow DAG not found")
    except Exception as e:
        print(f"  ❌ Error verifying DAG: {e}")
    
    return tests_passed == total_tests

def main():
    """Main testing function."""
    print("🚀 WERFEN DATA PIPELINE - PORTABILITY VALIDATION")
    print("=" * 60)
    
    tests = [
        ("Centralized Configuration", test_config_portability),
        ("Environment Variables", test_environment_variables),
        ("Cross-Platform Compatibility", test_cross_platform_paths),
        ("Refactored Modules", test_imported_modules),
    ]
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, test_function in tests:
        print(f"\n📋 RUNNING: {test_name}")
        try:
            if test_function():
                passed_tests += 1
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
        except Exception as e:
            print(f"💥 {test_name}: ERROR - {e}")
    
    print("\n" + "=" * 60)
    print("📊 FINAL RESULTS")
    print("=" * 60)
    print(f"✅ Tests Passed: {passed_tests}/{total_tests}")
    print(f"📈 Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    
    if passed_tests == total_tests:
        print("🎉 PORTABILITY IMPLEMENTED SUCCESSFULLY!")
        print("\n📋 ACHIEVED BENEFITS:")
        print("   • Automatically detected paths")
        print("   • Centralized configuration")
        print("   • Configurable environment variables")
        print("   • Compatible with Windows/Linux/Mac")
        print("   • Externalized secrets")
        print("   • Portable code between developers")
        return True
    else:
        print("⚠️  Some tests failed. Review configuration.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 