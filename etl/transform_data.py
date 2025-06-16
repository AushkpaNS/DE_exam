import os
import pandas as pd
import logging
from datetime import datetime
import configparser
from sklearn.preprocessing import StandardScaler

def transform_data():
    # считываем переменные из конфигурационного файла
    config = configparser.ConfigParser()
    config.read("/home/airflow/config.ini")
    log_file = config["BCWD"]["LOG_FILE"].strip("'")
    extract_file_path = config["BCWD"]["EXTRACT_FILE_PATH"].strip("'")
    transform_file_path = config["BCWD"]["TRANSFORM_FILE_PATH"].strip("'")

    # задаём уровень логирования
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s'
    )

    try:
        start_time = datetime.now()
        logging.info(f"TRANSFORM_DATA.PY. Transform script started at {start_time}")

        # получаем csv файлы
        csv_files = [f for f in os.listdir(extract_file_path) if f.endswith('.csv')]

        # проверяем, что csv файл ровно один
        if len(csv_files) == 1:
            filename = csv_files[0]
            csv_file_path = os.path.join(extract_file_path, filename)
            df = pd.read_csv(csv_file_path, header=None)
            # проверяем количество колонок
            if df.shape[1] == 32:
                # удаляем строки с пропусками
                df.dropna(inplace=True)

                # проверям, что в первой колонке целевой признак только со значениями: M и B"
                # удаляем строки с неизвестными значениями
                if not all(df.iloc[:, 1].isin(['M','B'])):
                    df = df[df.iloc[:, 1].isin(['M','B'])]
                
                # 'M' меняем на 1 и 'B' меняем на 0
                mapping = {'M': 1, 'B': 0}
                df.iloc[:, 1] = df.iloc[:, 1].map(mapping)

                # удаляем дубликаты
                df.drop_duplicates(inplace=True)

                # нормализуем данные
                columns_to_normalize = df.columns[2:] # стретьей колонки, так как в первой индекс, а во второй целевой признак
                scaler = StandardScaler()
                df[columns_to_normalize] = scaler.fit_transform(df[columns_to_normalize])

                # сохраняем трансформированные данные
                os.makedirs(transform_file_path, exist_ok=True)
                save_path = os.path.join(transform_file_path, filename)
                df.to_csv(save_path, index=False)

                logging.info(f"TRANSFORM_DATA.PY. Data successfully transformed and saved to {filename}")
            else:
                logging.error(f"TRANSFORM_DATA.PY. Incorrect number of columns ({df.shape[1]}) in file {filename}")
                raise # пробрасываем исключение для airflow

        elif len(csv_files) > 1:
            logging.error(f"TRANSFORM_DATA.PY. Incorrect number of files ({len(csv_files)}) in dir {extract_file_path}.")
            raise # пробрасываем исключение для airflow

        else:
            logging.info(f"TRANSFORM_DATA.PY. No data to transform in dir {extract_file_path}")

    except Exception as e:
        logging.error(f"TRANSFORM_DATA.PY. Error during data transform: {e}")
        raise # пробрасываем исключение для airflow
    finally:
        end_time = datetime.now()
        logging.info(f"TRANSFORM_DATA.PY. Transform script finished at {end_time}")

if __name__ == "__main__":
    transform_data()
