# db_operations/gkg_db.py
from pymongo import MongoClient
import logging
import traceback
import config
import pandas as pd
import zipfile
import io

def insert_gkg_data(file_content, url):
    """Insere dados do GKG no MongoDB."""
    client = None  # Inicializa a variável client
    try:
        client = MongoClient(config.MONGODB_URL)
        db = client[config.DB_NAME]
        collection = db[config.GKG_COLLECTION_NAME]  # Coleção do GKG

        # --- Lógica de leitura do CSV do GKG ---
        try:
            with zipfile.ZipFile(file_content) as outer_zip:
                for filename in outer_zip.namelist():
                    if filename.lower().endswith('.csv'):
                        with outer_zip.open(filename) as f:
                            # Especifica os tipos para evitar problemas e warnings
                            df = pd.read_csv(f, sep='\t', header=None, dtype={0: str})
                            logging.info(f"Arquivo CSV '{filename}' lido (GKG de {url}).")

                            # GKG V2.1 e V2.0 possuem header, a V1.0 não possui
                            # Verifica se a versão do arquivo é 1.0
                            first_row = df.iloc[0]
                            is_v1 = False
                            try:
                                # Tenta converter para float
                                float(first_row[0])
                                is_v1 = True
                            except (ValueError, TypeError):
                                pass

                            if is_v1:
                                logging.info("Detectado GKG V1.0")
                                # --- LIMPEZA PARCIAL (GKG) ---
                                # Removendo linhas com GKGRECORDID nulo
                                df.dropna(subset=[0], inplace=True)
                                columns_to_drop = []  # Adicione as colunas para remover
                                existing_columns = [col for col in columns_to_drop if col in df.columns]
                                df.drop(columns=existing_columns, inplace=True, errors='ignore')
                                # --- RENOMEAR COLUNAS (GKG v1) ---
                                df = df.rename(columns={
                                    0: "GKGRECORDID",
                                    1: "DATE",
                                    2: "SourceCollectionIdentifier",
                                    3: "SourceCommonName",
                                    4: "DocumentIdentifier",
                                    5: "V2Themes",
                                    6: "V2Locations",
                                    7: "V2Persons",
                                    8: "V2Organizations",
                                    9: "V1Counts",
                                    10: "V2Tone",
                                    11: "V2GCAM",
                                    12: "V2_1EnhancedThemes",
                                    13: "V2_1EnhancedLocations",
                                    14: "V2_1EnhancedPersons",
                                    15: "V2_1EnhancedOrganizations",
                                    16: "V2_1Amounts",
                                    17: "V2_1Quotations",
                                    18: "V2_1AllNames",
                                    19: "V2_1Amounts",
                                    20: "V2_1TranslationInfo",
                                    21: "Extras"
                                })
                            else:
                                logging.info("Detectado GKG V2.0/V2.1")
                                # Remove a primeira linha, que contem o header
                                df = df.iloc[1:, :]
                                # --- LIMPEZA PARCIAL (GKG) ---
                                # Removendo linhas com GKGRECORDID nulo
                                columns = df.columns.tolist()
                                if "GKGRECORDID" not in columns:
                                    logging.error(f"Coluna GKGRECORDID não encontrada no arquivo GKG: {url}")
                                    return
                                df.dropna(subset=["GKGRECORDID"], inplace=True)
                                columns_to_drop = []  # Adicione as colunas para remover
                                existing_columns = [col for col in columns_to_drop if col in df.columns]
                                df.drop(columns=existing_columns, inplace=True, errors='ignore')
                                 # --- RENOMEAR COLUNAS (GKG v2) ---
                                try:  # Utiliza um try para caso algumas colunas estejam faltando
                                    df = df.rename(columns={
                                        "GKGRECORDID": "GKGRECORDID",
                                        "DATE": "DATE",
                                        "SourceCollectionIdentifier": "SourceCollectionIdentifier",
                                        "SourceCommonName": "SourceCommonName",
                                        "DocumentIdentifier": "DocumentIdentifier",
                                        "V2Themes": "V2Themes",
                                        "V2Locations": "V2Locations",
                                        "V2Persons": "V2Persons",
                                        "V2Organizations": "V2Organizations",
                                        "V1Counts": "V1Counts",
                                        "V2Tone": "V2Tone",
                                        "V2GCAM": "V2GCAM",
                                        "V2.1EnhancedThemes": "V2_1EnhancedThemes",
                                        "V2.1EnhancedLocations": "V2_1EnhancedLocations",
                                        "V2.1EnhancedPersons": "V2_1EnhancedPersons",
                                        "V2.1EnhancedOrganizations": "V2_1EnhancedOrganizations",
                                        "V2.1Amounts": "V2_1Amounts",
                                        "V2.1Quotations": "V2_1Quotations",
                                        "V2.1AllNames": "V2_1AllNames",
                                        "V2.1Amounts": "V2_1Amounts",  # Corrigido: V2.1Amounts, não V2.1AllNames
                                        "V2.1TranslationInfo": "V2_1TranslationInfo",
                                        "Extras": "Extras"
                                    })
                                except Exception as e:
                                    logging.warning(f"Erro ao renomear colunas: {e}")
                                    # Se der erro, imprime as colunas para ajudar a debugar
                                    logging.warning(f"Colunas presentes no DataFrame: {df.columns.tolist()}")

        except Exception as e:
            logging.error(f"Erro ao ler CSV do GKG de {url}: {e}")
            traceback.print_exc()
            return

        # --- FIM da lógica de leitura ---

        # Inserção (sem UpdateOne, pois GKGRECORDID é único)
        data_to_insert = df.to_dict("records")
        if data_to_insert:
            result = collection.insert_many(data_to_insert)
            logging.info(f"GKG de {url}: Inseridos {len(result.inserted_ids)} documentos.")
        else:
            logging.info(f"Nenhum dado do GKG para inserir de {url} (DataFrame vazio).")

    except Exception as e:
        logging.error(f"Erro ao inserir dados do GKG de {url} no MongoDB: {e}")
        traceback.print_exc()
    finally:
        if client:  # Fecha a conexão, se existir
            client.close()