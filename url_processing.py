# url_processing.py
import requests
from bs4 import BeautifulSoup
import logging
import traceback
import config  # Importa as configurações

def extract_content_from_urls(df):
    """
    Extrai o conteúdo de texto das URLs em um DataFrame do pandas.

    Args:
        df: DataFrame do pandas com os dados do GDELT.
            Deve ter uma coluna 'SOURCEURL'.

    Returns:
        DataFrame com uma coluna adicional 'article_content' contendo o texto
        extraído (ou string vazia em caso de erro).  O DataFrame original
        NÃO é modificado.
    """

    df_copy = df.copy()  # Cria uma CÓPIA do DataFrame
    df_copy["article_content"] = ""  # Nova coluna para o conteúdo
    success_count = 0  # Contador de sucessos

    for index, row in df_copy.iterrows():  # Itera sobre a CÓPIA
        try:
            article_response = requests.get(row["SOURCEURL"], timeout=config.REQUEST_TIMEOUT)
            article_response.raise_for_status()
            soup = BeautifulSoup(article_response.content, "html.parser")
            # Extrai o texto (método simples, pode precisar de ajustes)
            text = " ".join([p.text for p in soup.find_all("p")])
            df_copy.at[index, "article_content"] = text  # Modifica a CÓPIA
            success_count += 1
            logging.debug(f"Conteúdo extraído de: {row['SOURCEURL']}")

        except requests.exceptions.RequestException as e:
            logging.warning(f"Erro ao baixar URL {row['SOURCEURL']}: {e}")
            df_copy.at[index, "article_content"] = ""  # Define como vazio na CÓPIA

        except Exception as e:
            logging.error(f"Erro ao processar URL {row['SOURCEURL']}: {e}")
            traceback.print_exc()
            df_copy.at[index, "article_content"] = ""  # Define como vazio na CÓPIA

    logging.info(f"Conteúdo extraído de {success_count} URLs (de um total de {len(df_copy)}).")
    return df_copy  # Retorna a CÓPIA modificada