#Importamos la librería argparse para generar un CLI
import argparse
#Importamos la librería loggig para mostrar mensajes al usuario
import logging
logging.basicConfig(level=logging.INFO)
#Importamos la librería hashlib para encriptación
import hashlib
#Importamos la librería urlparse para parsean la forma de las url's
from urllib.parse import urlparse
#Importamos la librería de pandas para análisi de datos
import pandas as pd
#Importamos la librería nltk para extraer tokens del texto
import nltk
from nltk.corpus import stopwords
#Obtenemos una referencia al logger
logger = logging.getLogger(__name__)

#Definimos la Función principal
def main(file_name):
    logger.info('Iniciando Proceso de limpieza de Datos...')
    
    #Invocamos a la función para leer los datos.
    df = _read_data(file_name)
    #Invocamos a la función para extraer el newspaper uid
    newspaper_uid = _extract_newspaper_uid(file_name)
    #Invocamos a la funcion para agregar la columna newspaper_uid al Data Frame
    df = _add_newspaper_uid_column(df, newspaper_uid)
    #Invocamos a la función para Extraer el host de las url's
    df = _extract_host(df)
    #Invocamos a la función para Rellenar los títulos faltantes
    df = _fill_missing_titles(df)
    # #Invocamos a la fucnión para generar los uids para las filas.
    df = _generate_uids_for_rows(df)
    # #Invocamos a la fucnión para remover los caracteres \n \r
    df = _remove_scape_characters_from_body(df)
    # #Invocamos a la función para enriquecer el df agregando una columna con los tokens
    df = _data_enrichment(df)
    # #Invocamos a la función para eliminar registros duplicados con base al título
    df = _remove_duplicate_entries(df, 'title')
    # #Invocamos a la función para eliminar registros con valores faltantes
    #df = drop_rows_with_missing_values(df)
    # #Invocamos a la función para guardar el df un archivo csv.
    _save_data_to_csv(df, file_name)


    return df

####################################################################
# Función para leer los datos del Data Set #
####################################################################
def _read_data(file_name):
    logger.info('Leyendo el archivo {}'.format(file_name))
    #Leemos el archvo csv y lo devolvemos el data frame
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
    #Agregamos la nueva columna al df y le pasamos el valor.
    df['newspaper_uid'] = newspaper_uid

    return df
####################################################################
# Función para extraer el host de las url's #
####################################################################
def _extract_host(df):
    logger.info('Extrayendo de la URL')
    #Generando la nueva columana Host
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    
    return df
#######################################################################
# Función para rellenar los títulos faltantes extrayendolos de la url #
#######################################################################
def _fill_missing_titles(df):
    logger.info('Rellenando titulos faltantes')

    missingTitlesMask = df['title'].isna()
    missingTitlesMask

    missing_titles = (df[missingTitlesMask]['url']
                 .str.extract(r'(?P<missing_titles>[^/]+)$') #extraemos la última parte de la url
                 .applymap(lambda title: title.split('-')) #Separamos el título con base a los guiones
                 .applymap(lambda title_word_list: ' '.join(title_word_list)) #Volvemos a unir las palabras con espacios
    )
    missing_titles

    df.loc[missingTitlesMask, 'title'] = missing_titles.loc[:, 'missing_titles']
    return df
############################################################################
# Función para generar los uids para las filas generando un hash de la url #
############################################################################
def _generate_uids_for_rows(df):

    logger.info('Generar los uids para las filas generando un hash de la url')

    uids = (df
           .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1) #le pasamos las filas a la función lamda y aplicamos md5
           .apply(lambda hash_object: hash_object.hexdigest()) #convirtiendo el hash a valores hexadecimales
    )
    
    #Añadimos una columna al DataFarame y le pasamos la lista de uids
    df['uid'] = uids
    #Establecemos el index de DataFrame
    df.set_index('uid', inplace=True)

    return df
############################################################################
# Función para remover los caracteres de escape del cuerpo del artículo #
############################################################################
def _remove_scape_characters_from_body(df):

    logger.info('Remover los caracteres n r')

    stripped_body = (df
                    .apply(lambda row: row['body'], axis=1) #Obtenemos todas las filas de la columna body
                    .apply(lambda body: list(body)) #convertimos el contenido de body en una lista de letras
                    .apply(lambda letters: list(map(lambda letter: letter.replace('\n', ''), letters))) #vamos a iterar en cada una de las letras y a eliminar los caracteres \n.
                    .apply(lambda letters: list(map(lambda letter: letter.replace('\r', ''), letters))) #vamos a iterar en cada una de las letras y a eliminar los caracteres \r.
                    .apply(lambda letters: ''.join(letters)) #volvemos a unir las letras
                 
                )

    stripped_body

    df['body'] = stripped_body
    return df
###############################################################################
# Función para enriquecer el df añadiendo una columna que cuente los tokens #
# (palabras significativas) en el título y cuerpo del artículo #
###############################################################################

#Definir cuales son nuestros stop words, aquellas palabras que no entran dentro del análisis como artículos, pronombres, etc.
stop_words = set(stopwords.words('spanish'))

def _data_enrichment(df):

    logger.info('Enriquecer el df añadiendo una columna que cuente los tokens')

    df['n_tokens_title'] = tokenize_column(df, 'title')

    return df
###############################################################################
# Función que obtiene los tokens principales para una determinada columna #
###############################################################################
def tokenize_column(df, column_name):

    logger.info('Obtiene los tokens principales para una determinada columna')

    return (df
        .dropna() #Eliminamos filas con NA si aún hubiera algunas.
        .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1) #Obtenemos los tokens de todas la filas de la columna (column_name)
        .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens))) #Eliminamos todas las palabras que no sean alfanuméticas.
        .apply(lambda tokens: list(map(lambda token: token.lower(), tokens))) #convetir todos los tokens a minúsculas para compararlas con los stop_words
        .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list))) #eliminando los stop_words
        .apply(lambda valid_word_list: len(valid_word_list)) #Obtenemos cuantas palabras son
)

##################################################################################
# Función que quita entradas duplicadas del df con el mismo valor en una columna #
##################################################################################
def _remove_duplicate_entries(df, colums):

    logger.info('Quita entradas duplicadas del df con el mismo valor en una columna')

    df.drop_duplicates(subset=['title'], keep='first', inplace=True)
    return df
##################################################################################
# Función que elimina registros con valores faltantes (si es que aún los hay) #
##################################################################################
def drop_rows_with_missing_values(df):

    logger.info('Elimina registros con valores faltantes (si es que aún los hay)')


##################################################################################
# Función que guarda los datos del DataFrame en un archivo csv #
##################################################################################

def _save_data_to_csv(df, filename):
    clean_filename = 'clean_{}'.format(filename)
    logger.info('Guardando los datos limpios en el archivo: {}'.format(clean_filename))
    df.to_csv(clean_filename)
    
##################################################################################
# Inicio de la aplicación #
##################################################################################
if __name__ == '__main__':
    #Creamos un nuevo parser de argumentos
    parser = argparse.ArgumentParser()
    parser.add_argument('file_name',
                        help='La ruta al dataset sucio',
                        type=str)
    #Parseamos los argumentos.
    args = parser.parse_args()
    df = main(args.file_name)

    #Mostramos el Data Frame
    print(df)