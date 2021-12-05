from base import Base, engine, Session
from article import Article
import pandas as pd
import argparse
import logging
logging.basicConfig(level=logging.INFO)


# Obtenemos una referencia al logger
logger = logging.getLogger(__name__)

# Definimos la función main


def main(filename):
    # Generamos el squema de la BD.
    Base.metadata.create_all(engine)
    # Iniciamos Sesión.
    session = Session()
    # Leemos el archivo csv.
    articles = pd.read_csv(filename, encoding='utf-8')

    logger.info('Iniciando el proceso de carga de artículos a la Base de Datos')
    # Iteramos entre las filas del csv mediante el método iterrows() y vamos cargando los artículos a la base de datos.
    for index, row in articles.iterrows():
        logger.info(
            'Cargando el artículo con uid: {} en la BD'.format(row['uid']))
        article = Article(row['uid'],
                          row['body'],
                          row['host'],
                          row['newspaper_uid'],
                          row['n_tokens_title'],
                          row['title'],
                          row['url'],)
        session.add(article)

    # Guardamos todo
    session.commit()
    session.close()
    logger.info('Terminó el proceso de carga de artícuos a la Base de Datos')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # Creamos el argumento filename
    parser.add_argument('filename',
                        help='El archivo que deseas cargar hacia la Base de Datos',
                        type=str)

    args = parser.parse_args()

    main(args.filename)
