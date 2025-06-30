#!/usr/bin/env python3
"""
Script de Validación del Continuous Training Pipeline
Valida componentes ML, modelos y infraestructura CT
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any
import pandas as pd
import numpy as np

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CTValidator:
    """Validador del Continuous Training Pipeline"""
    
    def __init__(self):
        self.base_path = Path.cwd()
        self.results = {'total_checks': 0, 'passed': 0, 'failed': 0, 'warnings': 0, 'details': []}
    
    def log_result(self, check_name: str, status: str, message: str):
        """Log resultado de validación"""
        self.results['total_checks'] += 1
        
        if status == 'PASS':
            self.results['passed'] += 1
            logger.info(f"✅ {check_name}: {message}")
        elif status == 'FAIL':
            self.results['failed'] += 1
            logger.error(f"❌ {check_name}: {message}")
        elif status == 'WARN':
            self.results['warnings'] += 1
            logger.warning(f"⚠️ {check_name}: {message}")
        
        self.results['details'].append({'check': check_name, 'status': status, 'message': message})
    
    def validate_ct_structure(self) -> bool:
        """Valida estructura de archivos del CT"""
        logger.info("🔍 Validando estructura CT...")
        
        required_files = ['.github/workflows/ct.yml', 'docs/manual_continuous_training.md']
        missing_files = [f for f in required_files if not (self.base_path / f).exists()]
        
        if missing_files:
            self.log_result("CT Structure", "FAIL", f"Missing files: {', '.join(missing_files)}")
            return False
        else:
            self.log_result("CT Structure", "PASS", "All required CT files present")
            return True
    
    def validate_ml_dependencies(self) -> bool:
        """Valida dependencias ML"""
        logger.info("📦 Validando dependencias ML...")
        
        required_packages = ['pandas', 'numpy']
        missing_packages = []
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                missing_packages.append(package)
        
        # Optional ML packages
        optional_packages = ['sklearn', 'joblib']
        optional_missing = []
        
        for package in optional_packages:
            try:
                __import__(package)
            except ImportError:
                optional_missing.append(package)
        
        if missing_packages:
            self.log_result("ML Dependencies", "FAIL", f"Missing required: {', '.join(missing_packages)}")
            return False
        elif optional_missing:
            self.log_result("ML Dependencies", "WARN", f"Missing optional: {', '.join(optional_missing)}")
            return True
        else:
            self.log_result("ML Dependencies", "PASS", "All ML dependencies available")
            return True
    
    def validate_workflow_syntax(self) -> bool:
        """Valida sintaxis del workflow CT"""
        logger.info("📋 Validando sintaxis workflow CT...")
        
        workflow_file = self.base_path / '.github/workflows/ct.yml'
        
        if not workflow_file.exists():
            self.log_result("Workflow Syntax", "FAIL", "CT workflow file not found")
            return False
        
        try:
            with open(workflow_file) as f:
                content = f.read()
            
            # Basic YAML structure checks
            required_sections = ['name:', 'on:', 'jobs:']
            missing_sections = [s for s in required_sections if s not in content]
            
            if missing_sections:
                self.log_result("Workflow Syntax", "FAIL", f"Missing sections: {', '.join(missing_sections)}")
                return False
            
            # Check for CT jobs
            ct_jobs = ['data-validation', 'model-training', 'model-selection']
            present_jobs = [j for j in ct_jobs if j in content]
            
            if len(present_jobs) >= 2:
                self.log_result("Workflow Syntax", "PASS", f"Valid CT workflow with {len(present_jobs)} jobs")
                return True
            else:
                self.log_result("Workflow Syntax", "WARN", "Minimal CT job structure found")
                return True
                
        except Exception as e:
            self.log_result("Workflow Syntax", "FAIL", f"Workflow error: {str(e)}")
            return False
    
    def validate_training_simulation(self) -> bool:
        """Validates training simulation"""
        logger.info("🤖 Validating training simulation...")
        
        try:
            # Create test directories
            (self.base_path / 'models/registry').mkdir(parents=True, exist_ok=True)
            (self.base_path / 'data/training').mkdir(parents=True, exist_ok=True)
            
            # Generate synthetic data
            np.random.seed(42)
            n_samples = 100
            
            features = pd.DataFrame({
                'feature_1': np.random.normal(0, 1, n_samples),
                'feature_2': np.random.uniform(-1, 1, n_samples),
                'feature_3': np.random.exponential(1, n_samples)
            })
            
            target = (features['feature_1'] * 0.5 + features['feature_2'] * 0.3 + 
                     np.random.normal(0, 0.1, n_samples))
            
            train_data = features.copy()
            train_data['target'] = target
            
            # Basic model simulation (if sklearn available)
            try:
                from sklearn.linear_model import LinearRegression
                from sklearn.metrics import r2_score
                
                X = features.values
                y = target.values
                
                model = LinearRegression()
                model.fit(X, y)
                predictions = model.predict(X)
                r2 = r2_score(y, predictions)
                
                if r2 > 0.5:
                    self.log_result("Training Simulation", "PASS", f"Model training successful (R²: {r2:.3f})")
                    return True
                else:
                    self.log_result("Training Simulation", "WARN", f"Low model performance (R²: {r2:.3f})")
                    return True
            
            except ImportError:
                # Without sklearn, just validate data generation
                if len(train_data) > 0 and train_data['target'].notna().all():
                    self.log_result("Training Simulation", "PASS", f"Training data generated ({len(train_data)} samples)")
                    return True
                else:
                    self.log_result("Training Simulation", "FAIL", "Invalid training data generated")
                    return False
                    
        except Exception as e:
            self.log_result("Training Simulation", "FAIL", f"Training simulation failed: {str(e)}")
            return False
    
    def validate_ct_documentation(self) -> bool:
        """Validates CT documentation"""
        logger.info("📚 Validating CT documentation...")
        
        doc_file = self.base_path / 'docs/manual_continuous_training.md'
        
        if not doc_file.exists():
            self.log_result("CT Documentation", "FAIL", "CT documentation not found")
            return False
        
        try:
            with open(doc_file) as f:
                content = f.read()
            
            # Check for key sections
            required_sections = [
                'Continuous Training',
                'AWS',
                'Pipeline',
                'Model',
                'Training'
            ]
            
            present_sections = [s for s in required_sections if s in content]
            
            if len(present_sections) >= 4:
                self.log_result("CT Documentation", "PASS", f"Comprehensive CT documentation ({len(present_sections)}/5 sections)")
                return True
            else:
                self.log_result("CT Documentation", "WARN", f"Basic CT documentation ({len(present_sections)}/5 sections)")
                return True
                
        except Exception as e:
            self.log_result("CT Documentation", "FAIL", f"Documentation error: {str(e)}")
            return False
    
    def validate_aws_readiness(self) -> bool:
        """Validates AWS readiness"""
        logger.info("☁️ Validating AWS readiness...")
        
        doc_file = self.base_path / 'docs/manual_continuous_training.md'
        
        if not doc_file.exists():
            self.log_result("AWS Readiness", "FAIL", "CT documentation not found")
            return False
        
        try:
            with open(doc_file) as f:
                content = f.read()
            
            aws_services = ['SageMaker', 'S3', 'CloudWatch', 'Model Registry', 'Endpoints']
            mentioned_services = [s for s in aws_services if s in content]
            
            if len(mentioned_services) >= 3:
                self.log_result("AWS Readiness", "PASS", f"AWS services documented ({len(mentioned_services)}/5)")
                return True
            else:
                self.log_result("AWS Readiness", "WARN", f"Limited AWS documentation ({len(mentioned_services)}/5)")
                return True
                
        except Exception as e:
            self.log_result("AWS Readiness", "FAIL", f"AWS readiness check failed: {str(e)}")
            return False
    
    def cleanup_test_artifacts(self):
        """Cleans up test artifacts"""
        logger.info("🧹 Cleaning up test artifacts...")
        
        try:
            import shutil
            test_paths = ['models/registry', 'models/production', 'data/training', 'data/validation']
            
            for path in test_paths:
                full_path = self.base_path / path
                if full_path.exists() and full_path.is_dir():
                    # Only remove if it looks like test data
                    if any(f.name.startswith('test_') for f in full_path.iterdir() if f.is_file()):
                        shutil.rmtree(full_path)
        except Exception as e:
            logger.warning(f"Cleanup warning: {str(e)}")
    
    def run_validation(self) -> Dict[str, Any]:
        """Executes complete CT validation"""
        logger.info("🚀 Starting Continuous Training Pipeline validation...")
        
        validations = [
            self.validate_ct_structure,
            self.validate_ml_dependencies,
            self.validate_workflow_syntax,
            self.validate_training_simulation,
            self.validate_ct_documentation,
            self.validate_aws_readiness
        ]
        
        for validation in validations:
            try:
                validation()
            except Exception as e:
                logger.error(f"Validation error in {validation.__name__}: {str(e)}")
                self.log_result(validation.__name__, "FAIL", f"Unexpected error: {str(e)}")
        
        self.cleanup_test_artifacts()
        return self.results
    
    def print_summary(self):
        """Prints validation summary"""
        results = self.results
        total = results['total_checks']
        passed = results['passed']
        failed = results['failed']
        warnings = results['warnings']
        
        print("\n" + "="*60)
        print("🤖 CONTINUOUS TRAINING VALIDATION SUMMARY")
        print("="*60)
        print(f"📊 Total Checks: {total}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"⚠️ Warnings: {warnings}")
        print(f"📈 Success Rate: {(passed/total*100):.1f}%" if total > 0 else "N/A")
        print("="*60)
        
        if failed > 0:
            print("\n❌ FAILED CHECKS:")
            for detail in results['details']:
                if detail['status'] == 'FAIL':
                    print(f"  • {detail['check']}: {detail['message']}")
        
        if warnings > 0:
            print("\n⚠️ WARNINGS:")
            for detail in results['details']:
                if detail['status'] == 'WARN':
                    print(f"  • {detail['check']}: {detail['message']}")
        
        print("\n🎯 CT PIPELINE STATUS:")
        ct_ready = failed == 0
        print(f"  Status: {'✅ READY FOR DEPLOYMENT' if ct_ready else '⚠️ NEEDS ATTENTION'}")
        
        print("\n🚀 NEXT STEPS:")
        if ct_ready:
            print("  • Commit CT workflow to trigger first training run")
            print("  • Monitor model training and selection")
            print("  • Plan AWS SageMaker migration")
        else:
            print("  • Fix failed validation checks")
            print("  • Install missing dependencies if needed")
            print("  • Review CT workflow configuration")
        
        print("="*60)

def main():
    """Función principal"""
    validator = CTValidator()
    
    try:
        results = validator.run_validation()
        validator.print_summary()
        
        # Return appropriate exit code
        sys.exit(1 if results['failed'] > 0 else 0)
            
    except KeyboardInterrupt:
        logger.info("Validation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Validation failed with error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 