import os
import urllib.request
from pyunpack import Archive


class GestionArchivos():
    RUTA_ENTRADA = None
    RUTA_SALIDA_CSV = None
    RUTA_FICHEROS_PROCESADOS = None

    NOMBRE_FICHERO_DESCARGADO = "descarga.rar"

    ficheros_csv = list()

    def __init__(self):
        print("Gestor de Archivos Creado")

    def getFilesCSVFromOrigin(self, RUTA):
        """ Esta función guarda en la lista recibida la ubicación de todos los 
        ficheros que se encuentran en la RUTA que recibe por parámetro """
        self.ficheros_csv.clear()
        diccionario = None
        self.ficheros_csv = list()
        for root, dirs, files in os.walk(RUTA):
            for file in files:
                if file.endswith(".csv") | file.endswith(".CSV"):

                    diccionario = {"RUTA_ENTRADA": root, "FICHERO": file}

                    self.ficheros_csv.append(diccionario)

        return self.ficheros_csv

    def crear_ruta_salida_si_no_existe(self, RUTA):
        if (not os.path.exists(RUTA)):
            os.makedirs(RUTA)

    def download_datosrar_url(self, URL_DATOS_DOWNLOAD, RUTA_DOWNLOAD):
        print('Iniciando la descarga del archivo con urllib2...')

        self.crear_ruta_salida_si_no_existe(RUTA_DOWNLOAD)

        # Descargamos el archivo indicando las rutas correspondientes.
        urllib.request.urlretrieve(
            URL_DATOS_DOWNLOAD, RUTA_DOWNLOAD + self.NOMBRE_FICHERO_DESCARGADO)

    def unRarFileDownload(self, RUTA_DOWNLOAD, RUTA_DESCARGA):

        self.crear_ruta_salida_si_no_existe(RUTA_DESCARGA)
        # Descomprimimos los datos del .rar
        Archive(RUTA_DOWNLOAD +
                self.NOMBRE_FICHERO_DESCARGADO).extractall(RUTA_DESCARGA)
