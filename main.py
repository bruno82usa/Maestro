# main.py (com detecção de tipo case-insensitive)

import requests
import logging
import time
import config
import utils
import data_extraction1
#import data_extraction2 # REMOVIDO
import url_processing
from db_operations import events_db, mentions_db, gkg_db  # IMPORTANTE
import datetime
import traceback
import os

def main():
    """
    Função principal que orquestra o processo de ETL do GDELT.
    """
    utils.setup_logging(config.LOG_FILE)
    logging.info("Iniciando o processo de ETL do GDELT...")

    try:
        # 1. LER o arquivo gdelt2.txt LOCALMENTE:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        masterfile_path = os.path.join(script_dir, "gdelt2.txt")
        logging.info(f"Lendo a lista de arquivos de: {masterfile_path}")
        try:
            with open(masterfile_path, "r", encoding="utf-8") as f:
                masterfile_content = f.read()
            logging.info(f"Conteúdo do arquivo lido. Tamanho: {len(masterfile_content)} caracteres.")
        except FileNotFoundError:
            logging.error(f"Erro: Arquivo {masterfile_path} não encontrado.")
            return
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo {masterfile_path}: {e}")
            traceback.print_exc()
            return

        # 2. Calcular datas de início e fim (últimos 5 anos, se aplicável):
        today = datetime.date.today()
        if hasattr(config, 'START_DATE') and hasattr(config, 'END_DATE') and config.START_DATE and config.END_DATE:
            # Se START_DATE e END_DATE estiverem definidos em config.py, usa esses valores
            start_date = config.START_DATE
            end_date = config.END_DATE
            start_date_str = start_date.strftime("%Y%m%d")
            end_date_str = end_date.strftime("%Y%m%d")
            logging.info(f"Filtrando dados do período: de {start_date_str} a {end_date_str}")
        else:
            # Caso contrário, calcula os últimos 5 anos
            start_date = today - datetime.timedelta(days=5*365)  # Aproximadamente 5 anos
            start_date_str = start_date.strftime("%Y%m%d")
            end_date_str = today.strftime("%Y%m%d")  # Data atual
            logging.info(f"Filtrando dados dos últimos 5 anos: de {start_date_str} a {end_date_str}")

        # 3. Filtrar URLs por data (DESATIVADO PARA O TESTE, se for o caso):
        urls = []
        line_number = 0
        for line in masterfile_content.splitlines():
            line_number += 1
            line = line.strip()

            if not line:
                logging.debug(f"Linha {line_number}: Linha vazia ignorada.")
                continue

            parts = line.split(" ")
            if len(parts) != 3:
                logging.warning(f"Linha {line_number}: Formato inesperado: {line}")
                continue

            try:
                # Não precisamos mais converter os dois primeiros campos para int
                url = parts[2]

                # --- LÓGICA DE FILTRAGEM POR DATA (DESATIVADA PARA O TESTE, se for o caso) ---
                # ... (mesmo código de antes para filtrar por data, se você quiser) ...
                # date_str = ...
                # if start_date_str <= date_str <= end_date_str: ...

                # --- ADICIONA TODAS AS URLs (ou apenas as filtradas, se você reativar o filtro) ---
                logging.debug(f"Linha {line_number}: URL adicionada: {url}")
                urls.append(url)

            except (ValueError, IndexError) as e:
                logging.warning(f"Linha {line_number}: Formato inesperado: {line} - Erro: {e}")
                continue

        total_urls = len(urls)
        logging.info(f"Total de URLs a serem processadas: {total_urls}")
        processed_count = 0

        # 4. Processar os arquivos em lotes:
        for i in range(0, total_urls, config.BATCH_SIZE):
            batch_urls = urls[i : i + config.BATCH_SIZE]
            logging.info(
                f"Processando lote {i // config.BATCH_SIZE + 1}/{ (total_urls + config.BATCH_SIZE - 1) // config.BATCH_SIZE } ({len(batch_urls)} URLs)"
            )

            for url in batch_urls:
                logging.info(f"Processando URL: {url}")
                file_content = data_extraction1.download_gdelt_file(url)
                if not file_content:
                    logging.warning(f"Falha ao baixar: {url}")
                    continue

                # --- DETECÇÃO DE TIPO DE ARQUIVO (CASE-INSENSITIVE) ---
                url_lower = url.lower()  # Converte a URL para minúsculas
                is_mentions_file = ".translation.mentions.csv.zip" in url_lower
                is_gkg_file = ".gkg.csv.zip" in url_lower
                is_events_file = ".translation.export.csv.zip" in url_lower

                if is_mentions_file:
                    mentions_db.insert_mentions_data(file_content, url)
                elif is_gkg_file:
                    gkg_db.insert_gkg_data(file_content, url)
                #Agora verifica se é um arquivo de eventos
                elif is_events_file:
                    events_db.insert_events_data(file_content, url)
                else:
                    logging.warning(f"Tipo de arquivo desconhecido para URL: {url}") #Caso não seja nenhum dos três.
                    continue  # Pula para a próxima URL

                processed_count += 1
                progress_percentage = (processed_count / total_urls) * 100
                logging.info(f"Progresso: {processed_count}/{total_urls} ({progress_percentage:.2f}%)")

            time.sleep(2)

    except Exception as e:
        logging.error(f"Erro inesperado no processo principal: {e}")
        traceback.print_exc()

    logging.info("Processamento concluído.")


if __name__ == "__main__":
    main()