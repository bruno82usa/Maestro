from pymongo import MongoClient
import config  # Use o seu arquivo config.py

client = MongoClient(config.MONGODB_URL)
db = client[config.DB_NAME]

# As coleções serão criadas automaticamente quando você inserir dados nelas,
# mas você *pode* criá-las explicitamente se quiser:
db.create_collection(config.EVENTS_COLLECTION_NAME)
db.create_collection(config.MENTIONS_COLLECTION_NAME)
db.create_collection(config.GKG_COLLECTION_NAME)

client.close()