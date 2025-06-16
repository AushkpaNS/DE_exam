import requests
from datetime import datetime
import os
import configparser
import logging

def extract_data():
    # считываем переменные из конфигурационного файла
    config = configparser.ConfigParser()
    config.read("/home/airflow/config.ini")
    url = config["BCWD"]["URL"].strip("'")
    log_file = config["BCWD"]["LOG_FILE"].strip("'")
    extract_file_path = config["BCWD"]["EXTRACT_FILE_PATH"].strip("'")

    # задаём уровень логирования
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s'
    )

    try:
        start_time = datetime.now()
        logging.info(f"EXTRACT_DATA.PY. Extract script started at {start_time}")

        # запрашиваем данные
        response = requests.get(url)
        response.raise_for_status()
        data = response.text

        # сохраняем данные
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = extract_file_path + 'data_' + timestamp + '.csv'
        os.makedirs(extract_file_path, exist_ok=True)
        with open(filename, 'w') as f:
            f.write(data)

        logging.info(f"EXTRACT_DATA.PY. Data successfully extracted to: {filename}")
    except Exception as e:
        logging.error(f"EXTRACT_DATA.PY. Error during data extract: {e}")
        raise # пробрасываем исключение для airflow
    finally:
        end_time = datetime.now()
        logging.info(f"EXTRACT_DATA.PY. Extract script finished at {end_time}")

if __name__ == "__main__":
    extract_data()
