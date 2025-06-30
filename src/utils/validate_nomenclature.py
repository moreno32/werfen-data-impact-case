#!/usr/bin/env python3
"""
Nomenclature Validation Script - Werfen Data Pipeline
====================================================

Validates that all models follow the standardized enterprise convention:
{layer}_{domain}_{content}

Author: Lead Software Architect
Purpose: Nomenclature quality assurance
"""

import os
import re
from pathlib import Path
from typing import Dict, List, Tuple

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent
DBT_MODELS_DIR = PROJECT_ROOT / "dbt_project" / "models"

# Valid nomenclature patterns by layer
VALID_PATTERNS = {
    "raw": r"^raw_[a-z]+_[a-z_]+(_(daily|weekly|monthly|yearly))?$",
    "staging": r"^stg_[a-z_]+$",
    "intermediate": r"^int_[a-z_]+$", 
    "marts": r"^(dim_|fct_|mart_)[a-z_]+(_(daily|weekly|monthly|yearly))?$"
}

# Required prefixes by layer
REQUIRED_PREFIXES = {
    "raw": ["raw_"],
    "staging": ["stg_"],
    "intermediate": ["int_"],
    "marts": ["dim_", "fct_", "mart_"]
}

def check_file_naming(layer: str, file_path: Path) -> Tuple[bool, str]:
    """Verify that a file follows the naming convention"""
    file_name = file_path.stem
    
    if layer not in VALID_PATTERNS:
        return True, f"Layer '{layer}' does not require nomenclature validation"
    
    pattern = VALID_PATTERNS[layer]
    if re.match(pattern, file_name):
        return True, f"✅ Correct nomenclature: {file_name}"
    else:
        expected_prefixes = ", ".join(REQUIRED_PREFIXES[layer])
        return False, f"❌ Incorrect nomenclature: {file_name} (must start with: {expected_prefixes})"

def check_model_references(file_path: Path) -> List[Tuple[bool, str]]:
    """Verify {{ ref() }} references in a model"""
    issues = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find all {{ ref('...') }} references
        ref_pattern = r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
        refs = re.findall(ref_pattern, content)
        
        for ref_name in refs:
            # Check if reference follows new convention
            if not any(ref_name.startswith(prefix) for prefixes in REQUIRED_PREFIXES.values() for prefix in prefixes):
                # Check if it's a legacy reference
                legacy_prefixes = ["staging_", "intermediate_", "raw_", "marts_"]
                if any(ref_name.startswith(prefix) for prefix in legacy_prefixes):
                    issues.append((False, f"❌ Legacy reference found: {{ ref('{ref_name}') }}"))
                else:
                    issues.append((False, f"⚠️ Reference without standard prefix: {{ ref('{ref_name}') }}"))
            else:
                issues.append((True, f"✅ Correct reference: {{ ref('{ref_name}') }}"))
    
    except Exception as e:
        issues.append((False, f"❌ Error reading file: {e}"))
    
    return issues

def validate_layer(layer: str) -> Dict[str, List[Tuple[bool, str]]]:
    """Validate a complete layer"""
    layer_dir = DBT_MODELS_DIR / layer
    results = {"file_naming": [], "references": []}
    
    if not layer_dir.exists():
        results["file_naming"].append((False, f"❌ Directory not found: {layer_dir}"))
        return results
    
    # Validate .sql files
    sql_files = list(layer_dir.glob("*.sql"))
    if not sql_files:
        results["file_naming"].append((False, f"⚠️ No .sql files found in {layer}"))
        return results
    
    for sql_file in sql_files:
        # Validate file nomenclature
        is_valid, message = check_file_naming(layer, sql_file)
        results["file_naming"].append((is_valid, message))
        
        # Validate references within file
        ref_results = check_model_references(sql_file)
        results["references"].extend(ref_results)
    
    return results

def generate_validation_report(all_results: Dict) -> str:
    """Generate validation report"""
    report = []
    report.append("# Nomenclature Validation Report")
    report.append("=" * 50)
    
    total_issues = 0
    total_checks = 0
    
    for layer, results in all_results.items():
        report.append(f"\n## Layer: {layer.upper()}")
        report.append("-" * 30)
        
        # File naming validation
        report.append("\n### File Names:")
        for is_valid, message in results["file_naming"]:
            report.append(f"- {message}")
            total_checks += 1
            if not is_valid:
                total_issues += 1
        
        # Reference validation
        if results["references"]:
            report.append("\n### Model References:")
            for is_valid, message in results["references"]:
                report.append(f"- {message}")
                total_checks += 1
                if not is_valid:
                    total_issues += 1
    
    # Summary
    report.append(f"\n## Summary")
    report.append("-" * 20)
    report.append(f"- **Total checks**: {total_checks}")
    report.append(f"- **Issues found**: {total_issues}")
    report.append(f"- **Success rate**: {((total_checks - total_issues) / total_checks * 100):.1f}%" if total_checks > 0 else "N/A")
    
    if total_issues == 0:
        report.append("\n🎉 **ALL VALIDATIONS PASSED!**")
    else:
        report.append(f"\n⚠️ **Found {total_issues} issues that require attention**")
    
    return "\n".join(report)

def check_schema_yml_files():
    """Validate schema.yml files for consistent nomenclature"""
    issues = []
    
    for schema_file in DBT_MODELS_DIR.rglob("schema.yml"):
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Find model names in schema.yml
            model_pattern = r"- name:\s*([a-zA-Z0-9_]+)"
            models = re.findall(model_pattern, content)
            
            for model_name in models:
                # Check if follows new convention
                layer = schema_file.parent.name
                if layer in REQUIRED_PREFIXES:
                    expected_prefixes = REQUIRED_PREFIXES[layer]
                    if not any(model_name.startswith(prefix) for prefix in expected_prefixes):
                        issues.append(f"❌ Model in schema.yml without standard prefix: {model_name} (file: {schema_file.relative_to(PROJECT_ROOT)})")
                    else:
                        issues.append(f"✅ Correct model in schema.yml: {model_name}")
        
        except Exception as e:
            issues.append(f"❌ Error processing {schema_file}: {e}")
    
    return issues

def main():
    """Main validation function"""
    print("🔍 NOMENCLATURE VALIDATION - WERFEN DATA PIPELINE")
    print("=" * 60)
    
    # Check project structure
    if not DBT_MODELS_DIR.exists():
        print(f"❌ Error: {DBT_MODELS_DIR} not found")
        return False
    
    # Validate each layer
    all_results = {}
    layers_to_validate = ["staging", "intermediate", "marts"]
    
    for layer in layers_to_validate:
        print(f"\n🔍 Validating layer: {layer}")
        all_results[layer] = validate_layer(layer)
    
    # Validate schema.yml files
    print(f"\n🔍 Validating schema.yml files...")
    schema_issues = check_schema_yml_files()
    
    # Generate report
    report = generate_validation_report(all_results)
    
    # Add schema.yml validation to report
    if schema_issues:
        report += "\n\n## schema.yml Validation\n"
        report += "-" * 30 + "\n"
        for issue in schema_issues:
            report += f"- {issue}\n"
    
    # Display report in console
    print("\n" + report)
    
    # Save report to file
    report_file = PROJECT_ROOT / "nomenclature_validation_report.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n📊 Report saved to: {report_file}")
    
    # Determine if validation was successful
    total_issues = sum(
        len([r for r in layer_results["file_naming"] + layer_results["references"] if not r[0]])
        for layer_results in all_results.values()
    )
    
    schema_issues_count = len([issue for issue in schema_issues if issue.startswith("❌")])
    total_issues += schema_issues_count
    
    if total_issues == 0:
        print("\n🎉 VALIDATION SUCCESSFUL! All nomenclature is consistent.")
        return True
    else:
        print(f"\n⚠️ Found {total_issues} issues that require attention.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 