# db_operations/mentions_db.py
from pymongo import MongoClient
import logging
import traceback
import config
import pandas as pd
import zipfile
import io

def insert_mentions_data(file_content, url):
    """Insere dados de menções no MongoDB."""
    client = None
    try:
        client = MongoClient(config.MONGODB_URL)
        db = client[config.DB_NAME]
        collection = db[config.MENTIONS_COLLECTION_NAME]

        # --- Lógica de leitura do CSV de MENÇÕES ---
        df = None  # Inicializa df com None
        try:
            with zipfile.ZipFile(file_content) as outer_zip:
                for inner_zip_name in outer_zip.namelist():
                    if inner_zip_name.lower().endswith('.zip'):
                        with outer_zip.open(inner_zip_name) as inner_zip_file:
                            with zipfile.ZipFile(io.BytesIO(inner_zip_file.read())) as inner_zip:
                                for csv_filename in inner_zip.namelist():
                                    if csv_filename.lower().endswith('.csv'):
                                        with inner_zip.open(csv_filename) as csv_file:
                                            df = pd.read_csv(csv_file, sep='\t', header=None)
                                            logging.info(f"Arquivo CSV '{csv_filename}' lido (menções de {url}).")
                                            df.dropna(subset=[0], inplace=True)
                                            columns_to_drop = []  # Adapte para MENÇÕES!
                                            existing_columns = [col for col in columns_to_drop if col in df.columns]
                                            df.drop(columns=existing_columns, inplace=True, errors='ignore')
                                             # --- RENOMEAR COLUNAS (Menções) ---
                                            df = df.rename(columns={
                                                0: "GlobalEventID",
                                                1: "EventTimeDate",
                                                2: "MentionTimeDate",
                                                3: "MentionType",
                                                4: "MentionSourceName",
                                                5: "MentionIdentifier",
                                                6: "SentenceID",
                                                7: "Actor1CharOffset",
                                                8: "Actor2CharOffset",
                                                9: "ActionCharOffset",
                                                10: "Confidence",
                                                11: "MentionDocLen",
                                                12: "MentionDocTone",
                                                13: "MentionDocTranslationInfo",
                                                14: "Extras"
                                            })
                                            df['SOURCEURL'] = df['MentionIdentifier']  # Crie a coluna SOURCEURL
                                            break  # Sai do loop interno (csv_filename)
                                    else:
                                        logging.debug(f"Arquivo '{csv_filename}' dentro de '{inner_zip_name}' não é um CSV. Ignorando.") #Adicionado para evitar confusões
                                if df is not None:  # Se achou um CSV dentro do zip interno, sai do loop (inner_zip_name)
                                    break
                        if df is not None: #Se achou um zip interno, sai do loop (outer_zip)
                            break
                if df is None: #Se não encontrou nenhum CSV válido
                    logging.warning(f"Nenhum CSV encontrado em {url}")


        except Exception as e:
            logging.error(f"Erro ao ler CSV de menções de {url}: {e}")
            traceback.print_exc()
            return  # Sai da função se houver erro na leitura


        # --- Inserção (Menções) ---
        # Só tenta inserir se o DataFrame foi criado com sucesso
        if df is not None:
            data_to_insert = df.to_dict("records")
            if data_to_insert:
                result = collection.insert_many(data_to_insert)
                logging.info(f"Menções de {url}: Inseridos {len(result.inserted_ids)} documentos.")
            else:
                logging.info(f"Nenhum dado de menção para inserir de {url} (DataFrame vazio).")
        else:
            logging.warning(f"Nenhum dado de menção para inserir de {url} (DataFrame não criado).")


    except Exception as e:
        logging.error(f"Erro ao inserir dados de menções de {url} no MongoDB: {e}")
        traceback.print_exc()

    finally:
        if client:
            client.close()