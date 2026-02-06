import logging
import sys
import pandas as pd
import numpy as np

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger

logger = get_logger("TitanicETL")

class TitanicETL:
    def __init__(self):
        pass

    def extract(self, file_path: str) -> pd.DataFrame:
        logger.info(f"Leyendo archivo desde: {file_path}")
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Datos cargados. Filas: {df.shape[0]}, Cols: {df.shape[1]}")
            return df
        except Exception as e:
            logger.error(f"Error en extracción: {e}")
            raise

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Transformando datos...")
        df_t = df.copy()

        # Feature Engineering Vectorizado
        df_t['Title'] = df_t['Name'].str.extract(r',\s*([^\.]+)', expand=False)
        
        # Imputación
        mean_age = df_t['Age'].mean()
        df_t['Age'] = df_t['Age'].fillna(mean_age)
        df_t['Embarked'] = df_t['Embarked'].fillna('S')

        # Lógica de Negocio
        df_t['FamilySize'] = df_t['SibSp'] + df_t['Parch'] + 1
        df_t['IsAlone'] = np.where(df_t['FamilySize'] == 1, 1, 0)
        df_t['Fare'] = df_t['Fare'].round(2)

        cols = ["PassengerId", "Survived", "Pclass", "Title", "Sex", 
                "Age", "FamilySize", "IsAlone", "Fare"]
        
        return df_t[cols]

    def load(self, df: pd.DataFrame, output_path: str) -> None:
        logger.info(f"Guardando CSV en: {output_path}")
        try:
            df.to_csv(output_path, index=False)
            logger.info("✅ Carga finalizada.")
        except Exception as e:
            logger.error(f"Error guardando: {e}")
            raise