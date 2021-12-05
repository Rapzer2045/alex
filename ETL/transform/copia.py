# Importamos la librería argparse para generar un CLI
from nltk.corpus import stopwords
import nltk
import pandas as pd
from urllib.parse import urlparse
import hashlib
import argparse
# Importamos la librería loggig para mostrar mensajes al usuario
import logging
logging.basicConfig(level=logging.INFO)
# Importamos la librería hashlib para encriptación
# Importamos la librería urlparse para parsean la forma de las url's
# Importamos la librería de pandas para análisi de datos
# Importamos la librería nltk para extraer tokens del texto
# Obtenemos una referencia al logger
logger = logging.getLogger(__name__)

# Definimos la Función principal


def main(file_name):
    logger.info('Iniciando Proceso de limpieza de Datos...')

    # Invocamos a la función para leer los datos.
    df = _read_data(file_name)
    # Invocamos a la función para extraer el newspaper uid
    newspaper_uid = _extract_newspaper_uid(file_name)
    # Invocamos a la funcion para agregar la columna newspaper_uid al Data Frame
    df = _add_newspaper_uid_column(df, newspaper_uid)
    # Invocamos a la función para Extraer el host de las url's
    df = _extract_host(df)
    # Invocamos a la función para Rellenar los títulos faltantes
    df = _fill_missing_titles(df)
    # # Invocamos a la fucnión para generar los uids para las filas.
    df = _generate_uids_for_rows(df)
    # # Invocamos a la fucnión para remover los caracteres \n \r
    df = _remove_scape_characters_from_body(df)
    # # Invocamos a la función para enriquecer el df agregando una columna con los tokens del title y el body.
    df = _data_enrichment(df)
    # # Invocamos a la función para eliminar registros duplicados con base al título
    df = _remove_duplicate_entries(df, 'title')
    # # Invocamos a la función para eliminar registros con valores faltantes
    df = drop_rows_with_missing_values(df)
    # Invocamos a la función para guardar el df un archivo csv.
    _save_data_to_csv(df, file_name)

    return df

####################################################################
# Función para leer los datos del Data Set #
####################################################################


def _read_data(file_name):
    logger.info('Leyendo el archivo {}'.format(file_name))
    # Leemos el archvo csv y lo devolvemos el data frame
    return pd.read_csv(file_name, encoding='utf-8')

####################################################################
# Función para extraer el newspaper uid del nombre del archivo #
####################################################################


def _extract_newspaper_uid(file_name):
    logger.info('Extrayendo el newspaper uid')
    newspaper_uid = file_name.split('_')[0]

    logger.info('Newspaper udi Detectado: {}'.format(newspaper_uid))
    return newspaper_uid

####################################################################
# Función para agregar la columna con el newspaper_uid al df #
####################################################################


def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info('Llenando la columna newspaper_uid con {}'.format(newspaper_uid))
    # Agregamos la nueva columna al df y le pasamos el valor.
    df['newspaper_uid'] = newspaper_uid

    return df

####################################################################
# Función para extraer el host de las url's #
####################################################################


def _extract_host(df):
    logger.info('Extrayendo el host de la url')

    # Generando la nueva columna host
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)

    return df


#######################################################################
# Función para rellenar los títulos faltantes extrayendolos de la url #
#######################################################################
def _fill_missing_titles(df):
    logger.info('Rellenando los títulos faltantes')
    # Obenemos la máscara con los títulos faltantes
    missing_titles_mask = df['title'].isna()

    missing_titles = (df[missing_titles_mask]['url']
                      # mostrar hasta aquí primero\n",
                      .str.extract(r'(?P<missing_titles>[^/]+)$')
                      # Separar el título con base a los guiones\n",
                      .applymap(lambda title: title.split('-'))
                      # Volvemos a unir las palabas con espacios\n",
                      .applymap(lambda title_word_list: ' '.join(title_word_list).capitalize())
                      )

    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']

    return df

############################################################################
# Función para generar los uids para las filas generando un hash de la url #
############################################################################


def _generate_uids_for_rows(df):
    logger.info('Generando los uids para cada fila')
    # Generamos los uid aplicando una función hash al valor de la columna url.
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest())
            )
    # Añadimos una columna al DataFrame y le pasamos la lista de uids"
    df['uid'] = uids
    # Retornamos y establecemso el uid como index del df.
    return df.set_index('uid')

############################################################################
#  Función para remover los caracteres de escape del cuerpo del artículo   #
############################################################################


def _remove_scape_characters_from_body(df):
    logger.info('Eliminando los caracteres de escape del body')

    # Eliminando los saltos de linea que hay en el artículo.
    # stripped_body = (df
    #                  # Obtenemos todas las filas de la columna body\n",
    #                  .apply(lambda row: row['body'], axis=1)
    #                  # convertimos el contenido de body en una lista de letras\n",
    #                  .apply(lambda body: list(body))
    #                  # vamos a iterar en cada una de las letras y a eliminar los caracteres \\n.\n",
    #                  .apply(lambda letters: list(map(lambda letter: letter.replace('\\n', ''), letters)))
    #                  # vamos a iterar en cada una de las letras y a eliminar los caracteres \\r.\n",
    #                  .apply(lambda letters: list(map(lambda letter: letter.replace('\\r', ''), letters)))
    #                  # volvemos a unir las letras\n",
    #                  .apply(lambda letters: ''.join(letters))
    #                  )

    # Eliminando los saltos de linea que hay en el artículo.
    stripped_body = df.apply(lambda row: row['body'].replace(
        '\n', '').replace('\r', ''), axis=1)

    df['body'] = stripped_body

    return df

###############################################################################
#  Función para enriquecer el df añadiendo una columna que cuente los tokens  #
#  (palabras significativas) en el título y cuerpo del artículo               #
###############################################################################


def _data_enrichment(df):
    logger.info('Enriqueciendo el df contando los tokens')

    df['n_tokens_title'] = tokenize_column(df, 'title')
    df['n_tokens_body'] = tokenize_column(df, 'body')

    return df

###############################################################################
#   Función que obtiene los tokens principales para una determinada columna   #
###############################################################################


def tokenize_column(df, column_name):
    # Definiendo los stop_words (palabras no significativas) para español
    stop_words = set(stopwords.words('spanish'))

    return (df
            .dropna()  # Eliminamos filas con NA si aún hubiera algunas.
            # Obtenemos los tokens de todas la filas de la columna (column_name)
            .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
            # Eliminamos todas las plabras que no sean alfanuméticas.\n",
            .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
            # convetir todos los tokens a minúsculas para compararlas con los stop_words\n",
            .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
            # eliminando los stop_words\n",
            .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
            # Obtenemos cuantas palabras son\n",
            .apply(lambda valid_word_list: len(valid_word_list))
            )

##################################################################################
# Función que quita entradas duplicadas del df con el mismo valor en una columna #
##################################################################################


def _remove_duplicate_entries(df, column_name):
    logger.info('Eliminando entradas duplicadas')
    # Eliminando entradas duplicadas con base a una columna
    df.drop_duplicates(subset=[column_name], keep='first', inplace=True)

    return df

##################################################################################
#  Función que elimina registros con valores faltantes (si es que aún los hay)   #
##################################################################################


def drop_rows_with_missing_values(df):
    logger.info('Eliminando entradas con valores faltantes')
    # Eliminando entradas duplicadas con base a una columna
    return df.dropna()


##################################################################################
# Función que guarda los datos del DataFrame en un archivo csv #
##################################################################################


def _save_data_to_csv(df, filename):
    clean_filename = 'clean_{}'.format(filename)
    logger.info(
        'Guardando los datos limpios en el archivo: {}'.format(clean_filename))
    df.to_csv(clean_filename)


##################################################################################
# Inicio de la aplicación #
##################################################################################
if __name__ == '__main__':
    # Creamos un nuevo parser de argumentos
    parser = argparse.ArgumentParser()
    parser.add_argument('file_name',
                        help='La ruta al dataset sucio',
                        type=str)
    # Parseamos los argumentos.
    args = parser.parse_args()
    df = main(args.file_name)

    # Mostramos el Data Frame
    print(df)
