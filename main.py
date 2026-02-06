import os
import sys
from src.etl import TitanicETL, get_logger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "data", "raw", "titanic.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "processed", "titanic_clean.csv")

logger = get_logger("Main")

def main():
    etl = TitanicETL()
    try:
        logger.info(">>> Pipeline ETL Iniciado <<<")
        df_raw = etl.extract(INPUT_FILE)
        df_clean = etl.transform(df_raw)
        
        print("\n--- Muestra del Resultado ---")
        print(df_clean.head())
        print("-----------------------------\n")
        
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        etl.load(df_clean, OUTPUT_FILE)
        logger.info(">>> Pipeline Finalizado con Éxito <<<")

    except Exception as e:
        logger.critical(f"Fallo en el proceso: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()