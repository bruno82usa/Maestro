# related_events.py
from pymongo import MongoClient
import datetime
import logging
import config

def find_related_events(mongodb_url, db_name, events_collection, mentions_collection):
    """
    Identifica eventos relacionados (eventos em cadeia) no MongoDB.

    Procura por eventos subsequentes que:
        - Ocorram dentro de um período de tempo após um evento "gatilho".
        - Tenham códigos CAMEO específicos (configuráveis).
        - Estejam em locais próximos (opcional, com base em raio configurável).
        - (Opcional) Mencione palavras chaves em comum.

    Os eventos relacionados são marcados no MongoDB adicionando-se um campo
    'related_to' ao evento subsequente, que aponta para o ID do evento gatilho.

    Args:
        mongodb_url: String de conexão com o MongoDB.
        db_name: Nome do banco de dados.
        events_collection: Nome da coleção de eventos.
        mentions_collection: Nome da coleção de menções (opcional, para keywords).

    Returns:
        None.  Modifica os documentos no MongoDB diretamente.
    """
    try:
        client = MongoClient(mongodb_url)
        db = client[db_name]
        events = db[events_collection]
        # mentions = db[mentions_collection]  # Descomente se for usar keywords

        # 1. Encontrar Eventos Gatilho:
        trigger_query = {
            "EventRootCode": {"$in": config.TRIGGER_EVENT_TYPES},
            "GoldsteinScale": {"$lt": config.GOLDSTEIN_THRESHOLD},
            "ActionGeo_Lat": {"$exists": True},  # Garante que tem localização
            "ActionGeo_Long": {"$exists": True},
            "related_to": {"$exists": False}  # Que ainda não foram processados
        }
        trigger_events_cursor = events.find(trigger_query)

        # Itera sobre os eventos gatilho
        for trigger_event in trigger_events_cursor:
            trigger_event_id = trigger_event["GlobalEventID"]
            trigger_date = datetime.datetime(
                trigger_event["Year"], trigger_event["MonthYear"] % 100, trigger_event["Day"]
            )
            trigger_lat = trigger_event["ActionGeo_Lat"]
            trigger_lon = trigger_event["ActionGeo_Long"]

            logging.info(f"Processando evento gatilho: {trigger_event_id} em {trigger_date}")

            # 2. Buscar Eventos Subsequentes:
            start_date = trigger_date
            end_date = trigger_date + datetime.timedelta(days=config.TIME_WINDOW_DAYS)

            # Converte datas para o formato numérico do GDELT
            start_date_num = int(start_date.strftime("%Y%m%d"))
            end_date_num = int(end_date.strftime("%Y%m%d"))

            subsequent_events_query = {
                "Day": {"$gte": start_date_num, "$lte": end_date_num},
                "GlobalEventID": {"$ne": trigger_event_id},  # Não relaciona consigo mesmo
                "related_to": {"$exists": False}  # Que ainda não foram relacionados
                # Adicione aqui outros filtros, como códigos CAMEO específicos
                # para os eventos subsequentes, se desejar. Ex:
                # "EventRootCode": {"$in": ["14", "18"]},
            }
            #Verificação de latitude e longitude
            if trigger_lat is not None and trigger_lon is not None:
                subsequent_events_query["ActionGeo_Lat"] = {"$exists": True}
                subsequent_events_query["ActionGeo_Long"] = {"$exists": True}

            # Filtragem geográfica (opcional):
            if config.GEO_DISTANCE_THRESHOLD_KM is not None:
                # Cria um índice 2dsphere (se ainda não existir)
                events.create_index([("loc", "2dsphere")])

                subsequent_events_query["loc"] = {
                    "$nearSphere": {
                        "$geometry": {
                            "type": "Point",
                            "coordinates": [trigger_lon, trigger_lat]  # Ordem: Longitude, Latitude
                        },
                        "$maxDistance": config.GEO_DISTANCE_THRESHOLD_KM * 1000  # Converter km para metros
                    }
                }

            # --- (Opcional) Filtragem por palavras-chave (usando a coleção de menções) ---
            # keyword_filter = []
            # if config.KEYWORDS:
            #     # Consulta a coleção de menções para encontrar URLs que mencionam o evento gatilho
            #     mention_urls = [m["MentionIdentifier"] for m in mentions.find({"GlobalEventID": trigger_event_id}, {"MentionIdentifier": 1})]
            #     for keyword in config.KEYWORDS:
            #         # Cria uma expressão regular para buscar a palavra-chave em qualquer parte do texto
            #         keyword_regex = re.compile(re.escape(keyword), re.IGNORECASE)
            #         keyword_filter.append({"$or": [
            #             {"SOURCEURL": {"$in": mention_urls}, "article_content": {"$regex": keyword_regex}}, #Usar os artigos baixados
            #             #{"SOURCEURL": {"$in": mention_urls}} #Se não for usar os artigos baixados
            #         ]})

            #     if keyword_filter:
            #       subsequent_events_query["$and"] = keyword_filter


            # Executa a consulta para eventos subsequentes
            subsequent_events_cursor = events.find(subsequent_events_query)

            # 3. Armazenar a Relação:
            related_count = 0  # Contador de eventos relacionados
            for subsequent_event in subsequent_events_cursor:
                subsequent_event_id = subsequent_event["GlobalEventID"]
                # Marca o evento subsequente como relacionado ao evento gatilho
                events.update_one(
                    {"GlobalEventID": subsequent_event_id},
                    {"$set": {"related_to": trigger_event_id}}
                )
                related_count += 1
                logging.debug(f"  Evento {subsequent_event_id} relacionado a {trigger_event_id}")

            logging.info(f"  Encontrados {related_count} eventos relacionados.")


    except Exception as e:
        logging.error(f"Erro ao encontrar eventos relacionados: {e}")
        traceback.print_exc()

    finally:
        if 'client' in locals():
            client.close()

# Exemplo de uso (você chamaria isso do main.py, *depois* de importar os dados):
# find_related_events(config.MONGODB_URL, config.DB_NAME, config.EVENTS_COLLECTION_NAME, config.MENTIONS_COLLECTION_NAME)