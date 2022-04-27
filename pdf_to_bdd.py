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

# CREER LE DOSSIER CSV pour les données des fichiers images
csv_images_dir = Path.cwd() / "csv_images"
csv_images_dir.mkdir(exist_ok=True)

# CREER DOSSIER GLOBAL TEMPORAIRE pour les images
imgTemp_dir = Path.cwd() / "img_temp"
imgTemp_dir.mkdir(exist_ok=True)

# # CREER DOSSIER RAPPORTS
# report_dir = Path.cwd() / "Rapports"
# report_dir.mkdir(exist_ok=True)

# LISTE QUI RECUPERERA LES FICHIERS QUI GENERENT DES ERREURS
error_files = []

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
# cnx = mysql.connector.connect(**connection_params)
# cursor = cnx.cursor(prepared=True)
with mysql.connector.connect(**connection_params) as cnx:
    with cnx.cursor(prepared=True) as cursor:
        # ID du ou des modèles entrés par l'admin
        models_ids_list = list(map(int, input("ID du ou des modèles (séparés par des espaces): ").split()))
        # print("List of models_ids: ", models_ids_tuple)

        # BOUCLE SUR LES MODELES
        for model_id in models_ids_list:

            # CREER SOUS-DOSSIER TEMPORAIRE DU NOM DE L'ID du MODELE 
            # pour recevoir les images du pdf, non hashés
            img_pdf_dir = Path.cwd() / "img_temp" / f'{model_id}'
            img_pdf_dir.mkdir(exist_ok=True)
            
            # Requête pour récupérer marque et nom du fichier
            query = "SELECT MO_MA_ID, filename FROM fichiers as f \
                        JOIN modeles as m ON f.model_id = m.MO_ID \
                        WHERE f.type = 'exploded_view' AND f.model_id = %s" % model_id
            cursor.execute(query)            
            file_datas = cursor.fetchone()

            marque_eqpmt = file_datas[0]
            filename = file_datas[1]
            file_relpath = Path(f'uploads/{marque_eqpmt}/{model_id}/{filename}.pdf')

            # Liste qui récupèrera les dataframes du pdf
            dfs_pdf = []

            # LISTE GLOBALE pour le contenu csv des images
            img_datas_rows = []

            # BOUCLE SUR LES PAGES DU PDF
            with fitz.open(file_relpath) as pages_pdf:

                # SI C'EST UNE VUE VIESSMANN
                if marque_eqpmt == 4:

                    for iPage in range(len(pages_pdf)):

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
                            else:
                                # LA PAGE EST UN SCHEMA

                                # Liste qui récupérera les données des images 
                                img_datas_row = ['', model_id, 'exploded_view_picture']

                                # Sauvegarder la page en jpg dans dossier temporaire
                                page = pages_pdf.load_page(iPage)  
                                pix = page.get_pixmap()
                                image_name = f"{str(iPage+1).zfill(3)}_{filename}.jpg"
                                pix.save(f"img_temp/{model_id}/{image_name}", "JPEG")

                                # Hashage du fichier image avec algo md5 et préfixé avec numéro de page sur 3 chiffres
                                with open(f"img_temp/{model_id}/{image_name}", encoding="Latin-1") as img:
                                    data = img.read()
                                    md5hash_img = hashlib.md5((data).encode("utf-8")).hexdigest()
                                    md5hash_img = f"{str(iPage+1).zfill(3)}_{md5hash_img}"

                                # SAUVEGARDER LA PAGE EN JPG dans dossier 'uploads'
                                image_name = f'{md5hash_img}.jpg'
                                pix.save(f"uploads/{marque_eqpmt}/{model_id}/{image_name}", "JPEG")

                                # Taille du fichier image 
                                size = (Path(f"uploads/{marque_eqpmt}/{model_id}/{image_name}")).stat().st_size

                                # Created_at et Updated_at 
                                created_at = dt.strftime('%Y-%m-%d %H:%M:%S')
                                updated_at = None

                                # Type mime des images :
                                # print(mimetypes.guess_type(f"img_temp/{model_id}/{image_name}")) # -> image/jpeg

                                # Ajout du hash, de la taille, l'extension, le type mime et les dates dans la liste des données de l'image courante
                                img_datas_row.extend([md5hash_img, 'jpg', 'image/jpeg', md5hash_img, size, created_at, updated_at])

                                # Copie de cette liste car sinon passée par référence
                                datas_copy = img_datas_row.copy()

                                # Ajout de la liste copiée dans la liste globale des données des images
                                img_datas_rows.append(datas_copy)

                                # Vidage de la liste de l'image courante
                                img_datas_row.clear() 

                    # FIN DE LA BOUCLE SUR LES PAGES DU PDF

                    # CONCATENER LES DATAFRAMES DU PDF EN UNE SEULE
                    # if dfs_pdf != [] and filename not in error_files:
                    if dfs_pdf != []:
                        dfs_pdf = pd.concat(dfs_pdf)

                        # CONVERTIR LA DATAFRAME GLOBALE DU PDF EN CSV
                        csv_filepath = Path.cwd() / "csv_pieces" / f"{model_id}_pieces.csv"
                        dfs_pdf.to_csv(csv_filepath, index=False)

                        # SUPPRIMER LES GUILLEMETS ET LES ESPACES SUPERFLUS
                        with open(csv_filepath, "r", encoding="utf-8") as text:
                            csv_text = (
                                text.read().replace('"', "").replace(" ,", ",").replace(", ", ",")
                            )

                        with open(csv_filepath, "w", encoding="utf-8") as text:
                            text.write(csv_text)

                    # CONVERTIR LA LISTE DES DONNEES DES IMAGES du pdf courant EN CSV
                    csv_images_filepath = Path.cwd() / "csv_images" / f"{model_id}_img.csv"

                    with open(csv_images_filepath, 'w', newline='') as f:
                        write = csv.writer(f)
                        write.writerows(img_datas_rows)

        # FIN DE LA BOUCLE SUR LES MODELES

        # RECUPERER LE NOMBRE DE FICHIERS CSV TRAITES
        csv_images_list = list((Path.cwd() / "csv_images").glob("*.csv"))
        csv_pieces_list = list((Path.cwd() / "csv_pieces").glob("*.csv"))
        processfiles_csv = (len(csv_images_list) + len(csv_pieces_list))

        # CONCATENER LES CSV PIECES DE CHAQUE PDF EN UN SEUL
        with open("csv_pieces_final.csv", "w", encoding="utf-8") as outfile:
            for i, fname in enumerate(csv_pieces_list):
                with open(fname, "r", encoding="utf-8") as infile:
                    # if i != 0:                  # Supprime les en-têtes sauf celui du 1er csv
                    infile.readline()
                    shutil.copyfileobj(infile, outfile)

        # ENVOI CSV PIECES GLOBAL EN BDD
        datas_pieces = pd.read_csv("csv_pieces_final.csv", header=None, dtype = {0: object, 1: int, 2: str, 3: str, 4: str, 5: str, 6: int, 7: int, 8: object, 10: object})

        for i, row in datas_pieces.iterrows():
            query = 'INSERT INTO pieces VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            cursor.execute(query, tuple(row))
            cnx.commit()
                                            
        print("Nombre d'insert de pieces executés' :", cursor.rowcount)

        # CONCATENER LES CSV FICHIERS DE CHAQUE PDF EN UN SEUL
        with open("csv_images_final.csv", "w", encoding="utf-8") as outfile:
            for i, fname in enumerate(csv_images_list):
                with open(fname, "r", encoding="utf-8") as infile:
                    # if i != 0:                  # Supprime les en-têtes sauf celui du 1er csv
                    # infile.readline()
                    shutil.copyfileobj(infile, outfile)

        # ENVOI CSV IMAGES GLOBAL EN BDD
        datas_img = pd.read_csv("csv_images_final.csv", header=None, dtype = {0: object, 1: int, 2: str, 3: str, 4: str, 5: str, 6: str, 7: int, 9: object})

        for i, row in datas_img.iterrows():
            query = 'INSERT INTO fichiers VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            cursor.execute(query, tuple(row))
            cnx.commit()
                                            
        print("Nombre d'insert d'images executés' :", cursor.rowcount)

# except Exception as err:
#     print(f"Erreur : {err}")

