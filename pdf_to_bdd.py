import csv
import shutil
import hashlib
import camelot
import mimetypes
import pandas as pd 
import mysql.connector
from pathlib import Path
from datetime import datetime
import fitz  # pip install pymupdf
import mysql.connector

# RECUPERE LA DATE ET L HEURE DU JOUR
dt = datetime.now()
start_time = dt
print(dt)

# Params de connexion à la BDD
connection_params = {
    'host': "localhost",
    'user': "root",
    'password': "",
    'database': "vues_dynamiques",
}

# CONNEXION A LA BDD
# Avec curseur pour pouvoir executer des requêtes
cnx = mysql.connector.connect(**connection_params)
# cursor = cnx.cursor()
cursor = cnx.cursor(prepared=True)

# ID du ou des modèles entrés par l'admin
models_ids_tuple = tuple(map(int, input("ID du ou des modèles (séparés par des espaces): ").split()))
print("List of models_ids: ", models_ids_tuple)

# Récupération du filename, modele et marque des vues eclatées du ou des modèles
query = "SELECT MO_MA_ID, model_id, filename FROM fichiers as f \
            JOIN modeles as m ON f.model_id = m.MO_ID \
             WHERE f.type = 'exploded_view' AND f.model_id in %s" % (models_ids_tuple,)
            #  WHERE f.type = 'exploded_view' AND f.model_id in {0}""".format(models_ids_tuple)

cursor.execute(query)

for file_datas in cursor:
    
    print('pdf ', file_datas)

    pdf_relpath = Path(f'uploads/{file_datas[0]}/{file_datas[1]}/{file_datas[2]}.pdf')

cursor.close()
cnx.close()