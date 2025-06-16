import os
import pandas as pd
import joblib
from sklearn.linear_model import LogisticRegression
import logging
import configparser
from datetime import datetime
import glob
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import json

def model_pred():
    # считываем переменные из конфигурационного файла
    config = configparser.ConfigParser()
    config.read("/home/airflow/config.ini")
    log_file = config["BCWD"]["LOG_FILE"].strip("'")
    transform_file_path = config["BCWD"]["TRANSFORM_FILE_PATH"].strip("'")
    model_file_path = config["BCWD"]["MODEL_FILE_PATH"].strip("'")
    prediction_file_path = config["BCWD"]["PREDICTION_FILE_PATH"].strip("'")

    # задаём уровень логирования
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s'
    )
    
    try:
        start_time = datetime.now()
        logging.info(f"MODEL_PRED.PY. Model pred script started at {start_time}")

        # получаем модель и данные
        model_files = [f for f in os.listdir(model_file_path) if f.endswith('.pkl')]
        csv_files = [f for f in os.listdir(transform_file_path) if f.endswith('.csv')]

        if len(model_files) == 1 and len(csv_files) == 1:
            model_file_name = model_files[0]
            model_path = os.path.join(model_file_path, model_file_name)
            model = joblib.load(model_path)

            filename = csv_files[0]
            file_path = os.path.join(transform_file_path, filename)
            df = pd.read_csv(file_path)

            # получаем предсказания и метрики
            X = df.iloc[:, 2:]  # первая колонка — это id, а вторая - целевой признак
            y = df.iloc[:,1]  

            y_pred = model.predict(X)
            df['predictions'] = y_pred

            # Вычисление метрик
            accuracy = accuracy_score(y, y_pred)
            precision = precision_score(y, y_pred)
            recall = recall_score(y, y_pred)
            f1 = f1_score(y, y_pred)

            # сохраняем предсказания
            os.makedirs(prediction_file_path, exist_ok=True)
            save_path = os.path.join(prediction_file_path, filename)
            df.to_csv(save_path, index=False)

            # сохраняем метрики
            metrics_result = {
                "model": model_path,
                "Accuracy": accuracy,
                "Precision": precision,
                "Recall": recall,
                "F1": f1
            }
            result_filename = f'metrics_{filename}.json'
            with open(os.path.join(prediction_file_path, result_filename),'w') as f:
                json.dump(metrics_result,f)

        elif len(model_files) > 1 or len(csv_files) > 1:
            logging.error(f"MODEL_PRED.PY. Incorrect number of files and models ({len(csv_files)}, len(model_files)) in dir {transform_file_path} and {model_file_path}.")
            raise # пробрасываем исключение для airflow

        else:
            logging.info(f"MODEL_PRED.PY. No data to pred in dir {transform_file_path} or {model_file_path}")

    except Exception as e:
        logging.error(f"MODEL_PRED.PY. Error during pred: {e}")
        raise # пробрасываем исключение для airflow
    finally:
        end_time= datetime.now()
        logging.info(f"MODEL_PRED.PY. Model pred script finished at {end_time}")

if __name__ == "__main__":
   model_pred()
