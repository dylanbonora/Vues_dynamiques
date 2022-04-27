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

# RECUPERE LA DATE ET L HEURE DU JOUR
dt = datetime.now()
start_time = dt
print(dt)

# CREER LE DOSSIER CSV pour les données des pièces
csv_dir = Path.cwd() / "csv_pieces"
csv_dir.mkdir(exist_ok=True)

# # CREER LE DOSSIER CSV pour les données des fichiers images
# csv_images_dir = Path.cwd() / "csv_images"
# csv_images_dir.mkdir(exist_ok=True)

# # CREER DOSSIER GLOBAL TEMPORAIRE pour les images
# imgTemp_dir = Path.cwd() / "img_temp"
# imgTemp_dir.mkdir(exist_ok=True)

# # CREER DOSSIER RAPPORTS
# report_dir = Path.cwd() / "Rapports"
# report_dir.mkdir(exist_ok=True)

# LISTE QUI RECUPERERA LES FICHIERS QUI GENERENT DES ERREURS
error_files = []

# create sqlalchemy engine for df.to_sql -> test suspendu : manque autres packages
# suite de test avec csv to sql
# engine = create_engine("mysql://root : ''@localhost/vues_dynamiques")

# try:

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
cursor = cnx.cursor(prepared=True)

# ID du ou des modèles entrés par l'admin
models_ids_list = list(map(int, input("ID du ou des modèles (séparés par des espaces): ").split()))
# print("List of models_ids: ", models_ids_tuple)

# BOUCLE SUR LES MODELES
for idx,model_id in enumerate(models_ids_list):

    query = "SELECT MO_MA_ID, filename FROM fichiers as f \
                JOIN modeles as m ON f.model_id = m.MO_ID \
                WHERE f.type = 'exploded_view' AND f.model_id = %s" % model_id

    cursor.execute(query)
    # for i,file_datas in enumerate(cursor):
    file_datas = cursor.fetchone()

    # print('pdf ', file_datas[1])

    marque_eqpmt = file_datas[0]
    filename = file_datas[1]

    pdf_relpath = Path(f'uploads/{marque_eqpmt}/{model_id}/{filename}.pdf')

    # print('pdf relpath ', pdf_relpath)

    # LISTE QUI RECUPERERA LES DATAFRAMES DU PDF
    dfs_pdf = []

    # BOUCLE SUR LES PAGES DU PDF
    with fitz.open(pdf_relpath) as pages_pdf:
        for iPage in range(len(pages_pdf)):

            # SI C'EST UNE VUE VIESSMANN
            if marque_eqpmt == 4:

                # Liste qui récupérera les données des images du pdf courant
                # img_datas_row = ['', model_id, 'exploded_view']

                # CONVERSION DE LA PAGE EN COURS EN LISTE DE DATAFRAMES
                tables = camelot.read_pdf(
                    f'uploads/{marque_eqpmt}/{model_id}/{filename}.pdf',
                    flavor="stream",
                    table_areas=["0,736,552,62"],
                    pages=f"{iPage+1}",
                )

                # BOUCLE SUR LA OU LES DATAFRAMES DE LA PAGE
                for table in tables:

                    # Si plus de 3 colonnes, c'est une page de tableau
                    if len(table.df.columns) > 3:

                        # NETTOYAGE, REARRANGEMENT DES TABLEAUX

                        # # SUPPRIMER LA COLONNE DES PRIX, colonne index 5
                        table.df.drop(5, axis=1, inplace=True)

                        # Suppression des esapces superflus colonne Designation
                        table.df[2] = table.df[2].str.replace("  ", "")

                        # Insertion d'une colonne à la position 1, pour l'id du modele
                        table.df.insert(0, "piece_ID", None, allow_duplicates=False)
                        # Insertion d'une colonne à la position 1, pour l'id du modele
                        table.df.insert(1, "model_id", model_id, allow_duplicates=False)

                        # # Renommer les index des colonnes (par défaut : entiers)
                        table.df.rename(
                            columns={
                                0: "repere",
                                1: "reference",
                                2: "designation",
                                3: "grpMat",
                                4: "quantite",
                            },
                            inplace=True,
                        )

                        # Ajouter colonne 'page'
                        table.df["page"] = iPage + 1

                        # Ajouter colonne 'Substitution' vide (modèles Chappee)
                        table.df["substitution"] = ''

                        # Ajouter colonne 'created_at' 
                        table.df["created_at"] = dt.strftime('%Y-%m-%d %H:%M:%S')

                        # Ajouter colonne 'updated_at' 
                        table.df["updated_at"] = None

                        # SUPPRIMER LES LIGNES D'EN TETES
                        # en vérifiant la longueur de la valeur dans la colonne 'Position'
                        # si plus de 4 caractères ou égal à 1, c'est un en-tête
                        for ligne, value in enumerate(table.df["repere"]):
                            # repere = "'" + value + "'"

                            if len(value) > 4 or len(value) == 1:
                                table.df.drop(ligne, inplace=True)

                            # table.df["repere"][ligne] =  "'" + value + "'"

                        # On reset les index de ligne sinon bug
                        table.df.reset_index(drop=True, inplace=True)

                        # CHERCHER LES LIGNES 'DOUBLES' (texte qui déborde sur une nouvelle ligne)
                        # ET 'REMETTRE' le texte débordant dans la bonne ligne
                        # Puis supprimer la ligne inutile
                        for ligne, value in enumerate(table.df["repere"]):

                            if value == "":
                                if table.df["designation"][ligne] != "":
                                    table.df["designation"][ligne - 1] += (
                                        " " + table.df["designation"][ligne]
                                    )
                                elif table.df["grpMat"][ligne] != "":
                                    table.df["grpMat"][ligne - 1] += (
                                        " " + table.df["grpMat"][ligne]
                                    )

                                # Supprimer la ligne contenant le texte débordant
                                table.df.drop(ligne, inplace=True)

                        # On reset les index de ligne sinon bug
                        table.df.reset_index(drop=True, inplace=True)

                        # Si valeurs GrpMat décalées dans colonne Designation (bug d'extraction)
                        # Les récupérer et effacer dans Designation
                        for ligne, value in enumerate(table.df["grpMat"]):
                            if value == "":
                                table.df["grpMat"][ligne] = (table.df["designation"][ligne])[-3:]
                                table.df["designation"][ligne] = (table.df["designation"][ligne])[:-3]

                        # On remplace les virgules par des espaces dans 'Designation'
                        table.df["designation"] = table.df["designation"].str.replace(",", "")

                        # On ajoute la dataframe à la liste des dataframes du pdf
                        dfs_pdf.append(table.df)

            # FIN DE LA BOUCLE SUR LES PAGES DU PDF

        # CONCATENER LES DATAFRAMES DU PDF EN UNE SEULE
        # if dfs_pdf != [] and filename not in error_files:
        if dfs_pdf != []:
            dfs_pdf = pd.concat(dfs_pdf)

            # CONVERTIR LA DATAFRAME GLOBALE DU PDF EN CSV
            csv_filepath = Path.cwd() / "csv_pieces" / f"{model_id}_pieces_Vue_Eclatee.csv"
            dfs_pdf.to_csv(csv_filepath, index=False)

            # SUPPRIMER LES GUILLEMETS ET LES ESPACES SUPERFLUS
            with open(csv_filepath, "r", encoding="utf-8") as text:
                csv_text = (
                    text.read().replace('"', "").replace(" ,", ",").replace(", ", ",")
                )

            with open(csv_filepath, "w", encoding="utf-8") as text:
                text.write(csv_text)

# FIN DE LA BOUCLE SUR LES MODELES

csv_pieces_list = list((Path.cwd() / "csv_pieces").glob("*.csv"))

# CONCATENER LES CSV PIECES DE CHAQUE PDF EN UN SEUL
with open("csv_pieces_final.csv", "w", encoding="utf-8") as outfile:
    for i, fname in enumerate(csv_pieces_list):
        with open(fname, "r", encoding="utf-8") as infile:
            # if i != 0:                  # Supprime les en-têtes sauf celui du 1er csv
            infile.readline()
            shutil.copyfileobj(infile, outfile)

# TEST ENVOI CSV EN BDD
datas_pieces = pd.read_csv("csv_pieces_final.csv", header=None, dtype = {0: object, 1: int, 2: str, 3: str, 4: str, 5: str, 6: int, 7: int, 8: object, 10: object})
# print(datas_pieces.head())
# print(datas_pieces.dtypes)

for i, row in datas_pieces.iterrows():
    query = 'INSERT INTO pieces VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    cursor.execute(query, tuple(row))
    cnx.commit()
                                    
print("Nombre de lignes images insérées :", cursor.rowcount)


# except mysql.connector.Error as error:
#     print(f'Erreur mySQL : {error}')

# finally:
#     if cnx.is_connected():
#         cursor.close()
#         cnx.close()
