"""
Módulo Extract que permite Iniciar el proceso de descarga del archivo con los datos a transformar y analizar desde una ubicación en Internet
"""

from GestionArchivos import GestionArchivos
from datetime import datetime
import json

# Definir el entorno para que se tomen los valores que se encuentran en el config.json.
ENTORNO = "DESARROLLO"

# Obtener las variables de acceso.
with open('./config.json', 'r') as file:
    config = json.load(file)

print("Iniciando el proceso de Extracción...")

# Creamos un objeto de la clase GestionArchivos.
objGestionArchivos = GestionArchivos()

# Invocamos al método para descargar el archivo .rar.
objGestionArchivos.download_datosrar_url(
    URL_DATOS_DOWNLOAD=config[ENTORNO]['URL_DATOS_DOWNLOAD'], RUTA_DOWNLOAD=config[ENTORNO]['RUTA_DOWNLOAD'])

# Invocamos el método para descomprimir el archivo .rar
objGestionArchivos.unRarFileDownload(
    RUTA_DOWNLOAD=config[ENTORNO]['RUTA_DOWNLOAD'], RUTA_DESCARGA=config[ENTORNO]['RUTA_DOWNLOAD'])
