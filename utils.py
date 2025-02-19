# utils.py
import logging

def setup_logging(log_file):
    """Configura o logging para gravar em arquivo e no console."""
    logging.basicConfig(
        level=logging.INFO,  # Nível de log: INFO, WARNING, ERROR, DEBUG
        format="%(asctime)s - %(levelname)s - %(message)s",  # Formato
        handlers=[
            logging.FileHandler(log_file, mode='w'),  # Grava em arquivo ('w' sobrescreve)
            logging.StreamHandler()  # Exibe no console
        ]
    )

# --- Exemplo de uso (em outros arquivos, como main.py) ---
# import utils
# import config
# utils.setup_logging(config.LOG_FILE)  # Configura o logging no início do script
# logging.info("Mensagem informativa")
# logging.warning("Aviso!")
# logging.error("Erro!")
# logging.debug("Mensagem de debug (só aparece se o nível for DEBUG)")