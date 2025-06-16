import os
import shutil
import configparser
from datetime import datetime
import logging
import yadisk

def load_data():
    # считываем переменные из конфигурационного файла
    config = configparser.ConfigParser()
    config.read("/home/airflow/config.ini")
    log_file = config["BCWD"]["LOG_FILE"].strip("'")

    YA_DISK_API_TOKEN = config["BCWD"]["YA_DISK_API_TOKEN"].strip("'")
    YA_DISK_DEST_PATH = config["BCWD"]["YA_DISK_DEST_PATH"].strip("'")

    extract_file_path = config["BCWD"]["EXTRACT_FILE_PATH"].strip("'")
    transform_file_path = config["BCWD"]["TRANSFORM_FILE_PATH"].strip("'")
    model_file_path = config["BCWD"]["MODEL_FILE_PATH"].strip("'")
    prediction_file_path = config["BCWD"]["PREDICTION_FILE_PATH"].strip("'")
    archive_extract_file_path = config["BCWD"]["ARCHIVE_EXTRACT_FILE_PATH"].strip("'")
    archive_transform_file_path = config["BCWD"]["ARCHIVE_TRANSFORM_FILE_PATH"].strip("'")
    archive_model_file_path = config["BCWD"]["ARCHIVE_MODEL_FILE_PATH"].strip("'")
    archive_prediction_file_path = config["BCWD"]["ARCHIVE_PREDICTION_FILE_PATH"].strip("'")

    folders = [extract_file_path, transform_file_path, model_file_path, prediction_file_path]
    archive_folders = [archive_extract_file_path, archive_transform_file_path, archive_model_file_path, archive_prediction_file_path]

    # задаём уровень логирования
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s'
    )

    try:
        start_time = datetime.now()
        logging.info(f"LOAD_DATA.PY. Load script started at {start_time}")

        # Перенесём файлы с результатом на yandex disk
        for filename in os.listdir(prediction_file_path):
            client = yadisk.Client(token=YA_DISK_API_TOKEN)
            with client:
                if not client.check_token():
                    raise('Токен подключения к yandex disk недействителен.')
                # Загрузим файл в yandex disk
                dest_file = YA_DISK_DEST_PATH+filename
                if not client.exists(dest_file):
                    # Файла нет, загружаем
                    client.upload(os.path.join(prediction_file_path, filename), dest_file)  
                logging.info(f"LOAD_DATA.PY. Moved to yadisk: {filename} to {dest_file}")

        # переносим файлы в архив
        for source_dir, target_dir in zip(folders, archive_folders):
            # cоздаем целевую папку архива, если она не существует
            os.makedirs(target_dir, exist_ok=True)
            # перебираем файлы в исходной папке и переносим в архив
            for filename in os.listdir(source_dir):
                source_path = os.path.join(source_dir, filename)
                target_path = os.path.join(target_dir, filename)
                # проверяем, что это файл (а не папка)
                if os.path.isfile(source_path):
                    # переносим файл в архив
                    shutil.move(source_path, target_path)
                    logging.info(f"LOAD_DATA.PY. moved to archive: {source_path} to {target_path}")

    except Exception as e:
        logging.error(f"LOAD_DATA.PY. Error during load: {e}")
        raise # пробрасываем исключение для airflow
    finally:
        end_time= datetime.now()
        logging.info(f"LOAD_DATA.PY. Load script finished at {end_time}")

if __name__ == "__main__":
   load_data()
