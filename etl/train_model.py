import os
import pandas as pd
import joblib
from sklearn.linear_model import LogisticRegression
import logging
import configparser
from datetime import datetime

def train_models():
    # считываем переменные из конфигурационного файла
    config = configparser.ConfigParser()
    config.read("/home/airflow/config.ini")
    log_file = config["BCWD"]["LOG_FILE"].strip("'")
    transform_file_path = config["BCWD"]["TRANSFORM_FILE_PATH"].strip("'")
    model_file_path = config["BCWD"]["MODEL_FILE_PATH"].strip("'")

    # задаём уровень логирования
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s'
    )
    
    try:
        start_time = datetime.now()
        logging.info(f"TRAIN_MODEL.PY. Train script started at {start_time}")

        # получаем csv файлы
        csv_files = [f for f in os.listdir(transform_file_path) if f.endswith('.csv')]

        # проверяем, что csv файл ровно один
        if len(csv_files) == 1:
            filename = csv_files[0]
            csv_file_path = os.path.join(transform_file_path, filename)
            df = pd.read_csv(csv_file_path)

            X = df.iloc[:, 2:]  # первая колонка — это id, а вторая - целевой признак
            y = df.iloc[:,1]  
            
            # обучаем модель
            model = LogisticRegression(max_iter=1000)
            model.fit(X,y)

            # сохраняем модель
            model_filename = f'model_{filename}.pkl'
            os.makedirs(model_file_path, exist_ok=True)
            joblib.dump(model, os.path.join(model_file_path, model_filename))

            logging.info(f"TRAIN_MODEL.PY. Data successfully trained and saved to {model_filename}.")

        elif len(csv_files) > 1:
            logging.error(f"TRAIN_MODEL.PY. Incorrect number of files ({len(csv_files)}) in dir {transform_file_path}.")
            raise # пробрасываем исключение для airflow

        else:
            logging.info(f"TRAIN_MODEL.PY. No data to train in dir {transform_file_path}")

    except Exception as e:
        logging.error(f"TRAIN_MODEL.PY. Error during trainig: {e}")
        raise # пробрасываем исключение для airflow
    finally:
        end_time= datetime.now()
        logging.info(f"TRAIN_MODEL.PY. Training script finished at {end_time}")

if __name__ == "__main__":
    train_models()
