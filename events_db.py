# db_operations/events_db.py
from pymongo import MongoClient, UpdateOne
import logging
import traceback
import config
import pandas as pd
import zipfile
import io


def insert_events_data(file_content, url):
    """Insere dados de eventos no MongoDB, evitando duplicatas."""
    client = None  # Inicializa a variável client
    try:
        client = MongoClient(config.MONGODB_URL)
        db = client[config.DB_NAME]
        collection = db[config.EVENTS_COLLECTION_NAME]

        # --- Lógica de leitura do CSV de EVENTOS ---
        try:
            with zipfile.ZipFile(file_content) as outer_zip:
                for filename in outer_zip.namelist():
                    if filename.lower().endswith('.csv'):
                        with outer_zip.open(filename) as f:
                            df = pd.read_csv(f, sep='\t', header=None)
                            logging.info(f"Arquivo CSV '{filename}' lido (eventos de {url}).")
                            df.dropna(subset=[0], inplace=True)
                            columns_to_drop = [  # Adapte!
                                8, 9, 10, 11, 13, 14, 15,
                                21, 22, 23, 24, 25,
                                37, 43, 44, 51, 52, 59
                            ]
                            existing_columns = [col for col in columns_to_drop if col in df.columns]
                            df.drop(columns=existing_columns, inplace=True, errors='ignore')
                            # --- RENOMEAR COLUNAS (Eventos) ---
                            df = df.rename(columns={
                                0: "GlobalEventID",
                                1: "Day",
                                2: "MonthYear",
                                3: "Year",
                                4: "FractionDate",
                                5: "Actor1Code",
                                6: "Actor1Name",
                                7: "Actor1CountryCode",
                                12: "Actor1Type1Code",
                                16: "Actor2Code",
                                17: "Actor2Name",
                                18: "Actor2CountryCode",
                                19: "Actor2KnownGroupCode",
                                20: "Actor2EthnicCode",
                                26: "IsRootEvent",
                                27: "EventCode",
                                28: "EventBaseCode",
                                29: "EventRootCode",
                                30: "QuadClass",
                                31: "GoldsteinScale",
                                32: "NumMentions",
                                33: "NumSources",
                                34: "NumArticles",
                                35: "AvgTone",
                                36: "Actor1Geo_Type",
                                38: "Actor1Geo_CountryCode",
                                39: "Actor1Geo_ADM1Code",
                                40: "Actor1Geo_ADM2Code",
                                41: "Actor1Geo_Lat",
                                42: "Actor1Geo_Long",
                                45: "Actor2Geo_FullName",
                                46: "Actor2Geo_CountryCode",
                                47: "Actor2Geo_ADM1Code",
                                48: "Actor2Geo_ADM2Code",
                                49: "Actor2Geo_Lat",
                                50: "Actor2Geo_Long",
                                53: "ActionGeo_FullName",
                                54: "ActionGeo_CountryCode",
                                55: "ActionGeo_ADM1Code",
                                56: "ActionGeo_ADM2Code",
                                57: "ActionGeo_Lat",
                                58: "ActionGeo_Long",
                                60: "DATEADDED",
                                61: "SOURCEURL",  # Já existe para eventos
                            })


        except Exception as e:
            logging.error(f"Erro ao ler CSV de eventos de {url}: {e}")
            traceback.print_exc()
            return

        # --- FIM da lógica de leitura ---

        operations = []
        for record in df.to_dict("records"):
            operations.append(
                UpdateOne(
                    {"GlobalEventID": record["GlobalEventID"]},
                    {"$set": record},
                    upsert=True,
                )
            )

        if operations:
            result = collection.bulk_write(operations)
            logging.info(
                f"Eventos de {url}: Inseridos/Atualizados {result.upserted_count} documentos, "
                f"{result.modified_count} modificados."
            )
        else:
            logging.info("Nenhum dado de evento para inserir de {url}.")

    except Exception as e:
        logging.error(f"Erro ao inserir dados de eventos de {url} no MongoDB: {e}")
        traceback.print_exc()
    finally:
        if client:  # Fecha a conexão, se existir
            client.close()