# Werfen Data Pipeline - Activación del Entorno
# ===============================================
# Script para activar rápidamente el entorno virtual y verificar el estado

Write-Host "🚀 WERFEN DATA PIPELINE - ACTIVANDO ENTORNO" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green

# Activar entorno virtual
Write-Host "📦 Activando entorno virtual..." -ForegroundColor Yellow
& "$PSScriptRoot\..\..\venv_werfen\Scripts\Activate.ps1"

# Verificar que estamos en el entorno correcto
Write-Host "✅ Entorno virtual activado" -ForegroundColor Green

# Mostrar información del entorno
Write-Host "`n📋 INFORMACIÓN DEL ENTORNO:" -ForegroundColor Cyan
Write-Host "Python: $(python --version)" -ForegroundColor White
Write-Host "dbt: $(dbt --version --no-version-check | Select-String 'installed')" -ForegroundColor White

# Cambiar al directorio del proyecto si no estamos ahí
if (-not (Test-Path "$PSScriptRoot\..\..\README.md")) {
    Write-Host "❌ No estás en el directorio del proyecto" -ForegroundColor Red
    Write-Host "🔄 Usa: cd C:\Users\danie\Downloads\werfen-data-impact-case" -ForegroundColor Yellow
} else {
    Write-Host "✅ Directorio del proyecto: OK" -ForegroundColor Green
}

Write-Host "`n🎯 COMANDOS ÚTILES:" -ForegroundColor Cyan
Write-Host "  python src/utils/check_status.py          - Verificar estado completo" -ForegroundColor White
Write-Host "  python src/ingestion/load_raw_data.py - Cargar datos raw" -ForegroundColor White
Write-Host "  cd dbt_project && dbt run        - Ejecutar modelos dbt" -ForegroundColor White
Write-Host "  cd dbt_project && dbt test       - Ejecutar tests dbt" -ForegroundColor White
Write-Host "  python src/validation/setup_great_expectations.py - Validaciones GE" -ForegroundColor White

Write-Host "`n🎉 ¡Entorno listo para trabajar!" -ForegroundColor Green 