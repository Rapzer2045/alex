import subprocess
import logging
from extract.GestionArchivos import GestionArchivos

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def main():
    _extract()
    _transform()
    _load()
    logger.info('Proceso ETL finalizado')

# Método para invocar al proceso de extracción


def _extract():
    logger.info('#####Iniciando el proceso de extracción#####')

    # Corremos un subproceso para ejecutar la primera etapa en la carpeta /extract
    subprocess.run(['python', 'main.py'], cwd='./extract')

    # Movemos los archivos generados al directorio transform
    subprocess.run(['move', r'extract\*.csv', r'transform'], shell=True)
    # Borramos el archivo .rar descargado
    subprocess.run(['del', r'extract\*.rar'], shell=True)

    # Linux
    # subprocess.run(['find', '.', '-name', *.csv', 'exec', 'mv', '{}',
    #    '../transform/{}_.csv'.format(news_sites_uid), ';'], cwd='./extract')
    #subprocess.run(['mv', r'extract\*.csv', r'transform'], shell=True)
    #subprocess.run(['rm', r'extract\*.csv', r'transform'], shell=True)


def _transform():
    logger.info('#####Iniciando el proceso de Transformación#####')

    # Creamos un objeto de la clase GestionArchivos
    gestionArchivos = GestionArchivos()

    # Obtenemos una lista de todos los ficheros CSV que hay dentro de la RUTA que se le envía por parámetro.
    ficheros_csv = gestionArchivos.getFilesCSVFromOrigin('./transform')

    # Iteramos por cada uno de los archivos csv en la lista
    for fichero in ficheros_csv:

        # Corremos un subproceso para ejecutar la segunda etapa en la carpeta /tranform
        subprocess.run(
            ['python', 'main.py', fichero["FICHERO"]], cwd='./transform')
        # Borramos el archivo .csv original
        subprocess.run(['del', fichero["FICHERO"]],
                       shell=True, cwd='./transform')
        # Linux
        #subprocess.run(['pyton', 'main.py', fichero["FICHERO"]], cwd='./transform')
        #subprocess.run(['rm', fichero["FICHERO"]], shell=True, cwd='./transform')

    # Movemos los archivos csv nuevos al directorio load
    subprocess.run(['move', r'transform\*.csv', r'load'], shell=True)
    # subprocess.run(['mv', fichero["FICHERO"], './load/fichero["FICHERO"]], cwd='./transform')


def _load():
    logger.info('#####Iniciando el proceso de Carga#####')

    # Creamos un objeto de la clase GestionArchivos
    gestionArchivos = GestionArchivos()

    # Obtenemos una lista de todos los ficheros CSV que hay dentro de la RUTA que se le envía por parámetro.
    ficheros_csv = gestionArchivos.getFilesCSVFromOrigin('./load')

    # Iteramos por cada uno de los archivos csv en la lista
    for fichero in ficheros_csv:

        # Corremos un subproceso para ejecutar la tercera etapa en la carpeta /load
        subprocess.run(
            ['python', 'main.py', fichero["FICHERO"]], cwd='./load')
        # Borramos el archivo .csv
        subprocess.run(['del', fichero["FICHERO"]],
                       shell=True, cwd='./load')
        # Linux
        #subprocess.run(['pyton', 'main.py', fichero["FICHERO"]], cwd='./load')
        #subprocess.run(['rm', fichero["FICHERO"]], shell=True, cwd='./load')


if __name__ == '__main__':
    main()
