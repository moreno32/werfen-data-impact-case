.PHONY: help setup lint clean data train

# Colores para la salida
YELLOW=\033[0;33m
GREEN=\033[0;32m
RESET=\033[0m

help:
	@echo "Makefile para el Proyecto Werfen Data Impact"
	@echo ""
	@echo "Uso:"
	@echo "  make ${YELLOW}setup${RESET}      - Configura el entorno virtual e instala las dependencias."
	@echo "  make ${YELLOW}lint${RESET}       - Ejecuta el linter (flake8) para verificar la calidad del código."
	@echo "  make ${YELLOW}data${RESET}       - Ejecuta el pipeline de datos (Great Expectations y dbt)."
	@echo "  make ${YELLOW}train${RESET}      - Ejecuta el pipeline de entrenamiento del modelo de ML."
	@echo "  make ${YELLOW}clean${RESET}      - Elimina los artefactos generados y los cachés."
	@echo ""

setup:
	@echo "${GREEN}Configurando el entorno virtual...${RESET}"
	python -m venv .venv
	@echo "${GREEN}Activando el entorno e instalando dependencias de requirements.txt...${RESET}"
	. .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

lint:
	@echo "${GREEN}Ejecutando linter (flake8)...${RESET}"
	flake8 src/

data:
	@echo "${GREEN}Ejecutando el pipeline de datos...${RESET}"
	bash scripts/run_data_pipeline.sh

train:
	@echo "${GREEN}Ejecutando el pipeline de entrenamiento...${RESET}"
	bash scripts/run_training_pipeline.sh

clean:
	@echo "${GREEN}Limpiando artefactos y cachés...${RESET}"
	rm -rf artifacts/*
	rm -rf data/processed/*
	rm -f data/raw/*.duckdb
	rm -rf dbt_project/target dbt_project/logs dbt_project/dbt_packages
	rm -rf great_expectations/uncommitted
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type d -name ".ipynb_checkpoints" -exec rm -r {} + 