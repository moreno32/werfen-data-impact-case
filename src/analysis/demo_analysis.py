#!/usr/bin/env python3
"""
Script de Demostración - Werfen Data Warehouse Analysis
======================================================

Script de demostración que muestra cómo usar el nuevo módulo de análisis
desde la estructura reorganizada en /src.

Uso:
    python demo_analysis.py
"""

from . import DataWarehouseAnalyzer

def main():
    """Demostración del analizador del Data Warehouse"""
    print("🏗️ WERFEN DATA WAREHOUSE ANALYZER - DEMO")
    print("=" * 50)
    print("📍 Running from new location: src/analysis/")
    
    # Crear analizador
    analyzer = DataWarehouseAnalyzer()
    
    # Demostrar funcionalidades principales
    print("\n1️⃣ ESTRUCTURA DEL DATA WAREHOUSE")
    print("-" * 40)
    structure = analyzer.show_dw_structure()
    
    print("\n2️⃣ TABLAS DISPONIBLES")
    print("-" * 40)
    analyzer.show_available_tables()
    
    print("\n3️⃣ ANÁLISIS DE TABLA EJEMPLO")
    print("-" * 40)
    analysis = analyzer.analyze_table_metadata('raw_customer', 'raw', show_sample=True, sample_size=3)
    
    print("\n✅ DEMOSTRACIÓN COMPLETADA")
    print("\n💡 Para usar en notebooks o scripts:")
    print("   from src.analysis import DataWarehouseAnalyzer")
    print("   analyzer = DataWarehouseAnalyzer()")
    print("   analyzer.run_ingestion_pipeline()")
    print("   analyzer.run_dbt_transformations()")

if __name__ == "__main__":
    main() 