import csv
import re
import sys
from msilib.schema import Error
import shutil
import hashlib
import camelot
import mimetypes
import traceback
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
csv_pieces_dir = Path.cwd() / "csv_pieces"
csv_pieces_dir.mkdir(exist_ok=True)

# CREER LE DOSSIER CSV pour les données des fichiers images
csv_images_dir = Path.cwd() / "csv_images"
csv_images_dir.mkdir(exist_ok=True)

# CREER DOSSIER GLOBAL TEMPORAIRE pour les images
imgTemp_dir = Path.cwd() / "img_temp"
imgTemp_dir.mkdir(exist_ok=True)

# CREER DOSSIER RAPPORTS
report_dir = Path.cwd() / "Rapports"
report_dir.mkdir(exist_ok=True)

# LISTE QUI RECUPERERA LES FICHIERS QUI GENERENT DES ERREURS
error_files = []

# FONCTION DE CONVERSION DES SCHEMAS EN JPG et liste des données
def schemas_to_jpg_and_datas():
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

# Params de connexion à la BDD
connection_params = {
    'host': "localhost",
    'user': "root",
    'password': "",
    'database': "vues_dynamiques",
}

# try:

# OUVERTURE D'UNE CONNEXION A LA BDD
# Avec curseur pour pouvoir executer des requêtes
with mysql.connector.connect(**connection_params) as cnx:
    cnx.autocommit = True

    def input_ids():
        models_ids_list = list(map(str, input("ID du ou des modèles (séparés par des espaces): ").split()))
        return models_ids_list

    def check_digits():
        is_all_digits = all([val.isdigit() for val in models_ids_list])
        return is_all_digits

    models_ids_list = input_ids()
    is_all_digits = check_digits()

    while not is_all_digits:
        print('\n Erreur. Entrez uniquement des nombres')
        models_ids_list = input_ids()
        is_all_digits = check_digits()

    # BOUCLE SUR LES MODELES
    for model_id in models_ids_list:

        # VERIFIER SI DEJA DANS TABLE PIECES 
        query = "SELECT piece_ID FROM pieces \
                    WHERE model_id = %s" % model_id
        cursor = cnx.cursor(buffered=True)
        cursor.execute(query)            
        res = cursor.fetchmany(10)
        # print('10 piece_ID from pieces ', res)

        # SI MODEL_ID DEJA TRAITé
        if res != []:

            # On demande si c'est un Update ou une erreur
            def ask_update():
                is_Maj = input(f"Le modèle {model_id} existe déjà dans la table des pièces \n Tapez 'O' pour mettre à jour (Attention, ceci écrasera les données précédentes) \n Sinon tapez 'N' : ")
                return is_Maj

            is_update = ask_update()

            while not (is_update.lower() == 'o' or is_update.lower() == 'n'):
                print('\n Erreur. Entrez "O" ou "N"')
                is_update = ask_update()

            # Si erreur : on passe au modele suivant ou fin de script
            if is_update.lower() == 'n':
                print("OK pas de MAJ")
                continue
            # Si Update : On supprime les anciennes données du modele 
            elif is_update.lower() == 'o':
                print("OK pour MAJ")
                
                query = "DELETE FROM pieces WHERE model_id = %s" % model_id
                cursor.execute(query)  

                query = "DELETE FROM fichiers\
                WHERE type = 'exploded_view_picture' AND model_id = %s" % model_id 
                cursor.execute(query)  

        # SI PAS DEJA DANS TABLE PIECES, ON TRAITE

        # Requête pour récupérer marque et nom de la vue éclatée
        query = "SELECT MO_MA_ID, filename FROM fichiers as f \
                    JOIN modeles as m ON f.model_id = m.MO_ID \
                    WHERE f.type = 'exploded_view' AND f.model_id = %s" % model_id
        cursor.execute(query)            
        file_datas = cursor.fetchone()

        # print(f'file datas {file_datas}')

        if file_datas == None:
            print(f'Pas de vue éclatée trouvée en BDD pour le modèle {model_id}')
            continue

        else:
            # CREER SOUS-DOSSIER TEMPORAIRE DU NOM DE L'ID du MODELE 
            # pour recevoir les images du pdf, non hashés
            img_pdf_dir = Path.cwd() / "img_temp" / f'{model_id}'
            img_pdf_dir.mkdir(exist_ok=True)
            
            marque_eqpmt = file_datas[0]
            filename = file_datas[1]
            file_relpath = Path(f'uploads/{marque_eqpmt}/{model_id}/{filename}.pdf')

            # Initialisation de l'objet qui récuperera les dataframes Chappee page par page
            # tables = None

            # Liste qui récupèrera les dataframes du pdf
            dfs_pdf = []

            # LISTE GLOBALE pour le contenu csv des images
            img_datas_rows = []

            with fitz.open(file_relpath) as pages_pdf:

                try:

                    # SI C'EST UNE VUE VIESSMANN
                    if marque_eqpmt == 4:

                        # BOUCLE SUR LES PAGES DU PDF
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

                                    # Insertion d'une colonne à la position 0, pour l'id du modele
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

                                        if len(value) > 4 or len(value) == 1:
                                            table.df.drop(ligne, inplace=True)

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
                                    schemas_to_jpg_and_datas()

                            # FIN DE LA BOUCLE SUR LES PAGES VIESSMANN
                    # FIN DU TRAITEMENT VIESSMANN   
                
                    # SI C'EST UNE VUE CHAPPEE
                    if marque_eqpmt == 7:

                        # BOUCLES SUR LES PAGES DU PDF
                        for iPage, page in enumerate(pages_pdf):
                            # Extraction de la 1ere page du pdf pour connaître son type de mise en page 
                            # Et utiliser les paramètres d'extraction correspondants
                            tables_mep = camelot.read_pdf(f'uploads/{marque_eqpmt}/{model_id}/{filename}.pdf', flavor='stream', pages='1')

                            for table_mep in tables_mep:

                                # Si 5,6 ou 7 colonnes -> MEP1 : Tableaux avec colonne 'Substitution'
                                if 5 <= len(table_mep.df.columns) <= 7:
                                    tables = camelot.read_pdf(f'uploads/{marque_eqpmt}/{model_id}/{filename}.pdf', flavor='stream', table_areas=['0,755,400,0'], columns=['65,106,262,319,367'], pages=f'{iPage+1}')

                                # Sinon si 2 ou 4 colonnes -> MEP2 : Tableaux de 4 colonnes sans 'Substitution'
                                elif len(table_mep.df.columns) == 2 or len(table_mep.df.columns) == 4: 
                                    tables = camelot.read_pdf(f'uploads/{marque_eqpmt}/{model_id}/{filename}.pdf', flavor='stream', table_areas=['0,755,580,58'], columns=['62,124,520'], pages=f'{iPage+1}')

                                # Sinon si autre nb de colonnes détéctées -> autre MEP 
                                else:
                                    error_files.append(f'pdf avec autre mise en page : modele {model_id}, filename {filename}') 
                                    tables = None 

                            # Traitement
                            images_infos = page.get_image_info()

                            if images_infos != []:
                                # Si une image dans la page fait plus de 527, c'est une page de schéma
                                if any(img['width'] > 527 for img in images_infos):

                                    schemas_to_jpg_and_datas()

                                # Si aucune image de la page ne dépasse 527 de large ou si aucune image ne fait 14 de large, c'est une page de tableau
                                if not any((img['width'] > 527 or img['width'] == 14) for img in images_infos):
                                    if tables == None:
                                        break
                                    else:
                                        for table in tables:
                                            # NETTOYAGE, REARRANGEMENT, AJOUT DE COLONNES,...
                                            # Si virgule dans colonne 'Quantite' 3
                                            # Garder ce qui précède la virgule
                                            table.df[3] = [value[:(value.find(','))] if ',' in value else value for value in table.df[3]]
                                            # On ajoute '1' pour les valeurs manquantes de 'Quantite'
                                            table.df[3] = ['1' if value == '' else value for value in table.df[3]]
                                            # Si valeur désignation décalée dans colonne 'Quantité'
                                            # On la copie dans colonne 'Designation'
                                            # Et on met '1' dans 'Quantite' 
                                            for ligne,value in enumerate(table.df[3]): 
                                                if len(value) > 3:
                                                    table.df[2][ligne] += value
                                                    table.df[3][ligne] = '1'
                                            # Si pas de valeur dans colonne 'Référence' ou 'Réf. Référenc Description' 
                                            # -> c'est soit une ligne de pied de page
                                            # -> soit une ligne d'en tête 
                                            # On supprime
                                            for ligne,value in enumerate(table.df[1]): 
                                                if value == '' or 'Référenc' in value:
                                                    table.df.drop(ligne, inplace=True)
                                            # On écrase la colonne 'Tarif' avec la colonne 'Quantite' 
                                            table.df[4] = table.df[3]
                                            # On remplace la colonne 'Quantite' par 'GrpMat'
                                            # avec Valeurs 'NA' pour meilleure lisibilité
                                            table.df[3] = 'NA'
                                            # Si colonne 'Substitution' existe en col5 : copie en col7
                                            # Et valeurs 'NA' si pas de valeur, pour meilleure lisibilité
                                            # Puis on écrase col5 pour créer colonne 'Modele'
                                            # Sinon création de col5 'Modele' et col7 'Substitution'
                                            if 5 in table.df.columns:
                                                table.df[6] = table.df[5]  
                                                table.df[6] = ['NA' if value == '' else value for value in table.df[6]]
                                                table.df[5] = iPage+1  
                                            else:
                                                table.df[5] = iPage+1  
                                                table.df[6] = 'NA' 
                                            # Ajouter colonne 'created_at' 
                                            table.df[7] = dt.strftime('%Y-%m-%d %H:%M:%S')
                                            # table.df["created_at"] = dt.strftime('%Y-%m-%d %H:%M:%S')
                                            # Ajouter colonne 'updated_at' 
                                            table.df[8] = None
                                            # table.df["updated_at"] = None
                                            # Réordonner sinon 'Page' après 'Substitution'
                                            table.df.sort_index(axis=1, inplace=True)
                                            # Suppression des esapces superflus
                                            table.df[2] = table.df[2].str.replace('  ','')
                                            # Insertion d'une colonne à la position 0, pour piece id du modele
                                            table.df.insert(0, "piece_ID", None, allow_duplicates=False)
                                            # Insertion d'une colonne à la position 1, pour l'id du modele
                                            table.df.insert(1, "model_id", model_id, allow_duplicates=False)
                                            # On remplace les virgules par des espaces dans 'Designation'
                                            table.df[2] = table.df[2].str.replace(",", "")
                                
                                            # On ajoute la dataframe à la liste des dataframes du pdf
                                            dfs_pdf.append(table.df)
                            else:
                                continue
                        # FIN DE LA BOUCLE SUR LES PAGES CHAPPEE
                        
                    # FIN DU TRAITEMENT CHAPPEE

                    # SI C'EST UNE VUE SAUNIER
                    if marque_eqpmt == 2:

                        # BOUCLES SUR LES PAGES DU PDF
                        for iPage in range(len(pages_pdf)):

                            # CONVERSION DE LA PAGE EN COURS EN LISTE DE DATAFRAMES 
                            tables = camelot.read_pdf(f'uploads/{marque_eqpmt}/{model_id}/{filename}.pdf', flavor='stream', table_areas=['0,800,565,45'], columns=['71,152,333'], pages=f'{iPage+1}')

                            # BOUCLE SUR LA OU LES DATAFRAMES DE LA PAGE
                            for table in tables:

                                # Nettoyage, réarrangement des tableaux
                                # (Si plus de 4 lignes, c'est un tableau, sinon schéma ou autres infos)
                                if len(table.df) > 4:

                                    # Si valeur 'Position' vide, on copie la valeur de 'Désignation'
                                    for ligne,value in enumerate(table.df[0]): 
                                        if value == '':
                                            table.df[0][ligne] = table.df[1][ligne] 

                                    # Si valeur 'Position' commence par 'S' ou 'A' et 8 caractères
                                    # On supprime les 2 zéros finaux
                                    table.df[0] = [value[:-2] if len(value) == 8 and (value[0] == 'S' or value[0] == 'A') else value for value in table.df[0]]

                                    # Si valeur 'Position' 8 caractères et 1er caractère == 0
                                    # On supprime le 1er et les 2 derniers zéros
                                    table.df[0] = [value[1:-2] if (len(value) == 8 and value[0] == '0') else value for value in table.df[0]]

                                    # Si valeur 'Position' 8 caractères et 1er caractère != 0
                                    # On supprime les 2 derniers zéros
                                    table.df[0] = [value[:-2] if (len(value) == 8 and value[0] != '0') else value for value in table.df[0]]

                                    # On supprime les lignes sans Désignation (dûes à des Remarques sur 2 lignes)
                                    for ligne,value in enumerate(table.df[1]): 
                                        if ligne > 4 and value == '':
                                            table.df.drop(ligne, inplace=True)

                                    # Si 'Remplacé par...' dans colonne 'Remarque'
                                    # On garde la nouvelle référence, donc ce qui suit 'Remplacé par '
                                    table.df[3] = [value[13:] if 'Remplacé par' in value else 'NA' for value in table.df[3]]

                                    # On copie la colonne 'Remarque' en colonne 7 pour 'Substitution'
                                    table.df[6] = table.df[3]
                                    
                                    # On crée les colonnes 'GrpMat' et 'Quantite'
                                    # avec Valeurs 'NA' pour meilleure lisibilité
                                    table.df[3] = 'NA'
                                    table.df[4] = 'NA'

                                    # On crée les colonnes 'Page' et 'Substitution'
                                    table.df[5] = iPage+1

                                    # Ajouter colonne 'created_at' 
                                    table.df[7] = dt.strftime('%Y-%m-%d %H:%M:%S')

                                    # Ajouter colonne 'updated_at' 
                                    table.df[8] = None

                                    # Réordonner sinon 'Page' après 'Substitution'
                                    table.df.sort_index(axis=1, inplace=True)

                                    # On supprime les 5 premières lignes qui ne sont pas des données
                                    table.df.drop([0,1,2,3,4], inplace=True)

                                    # On remplace les virgules par des espaces dans 'Designation'
                                    table.df[2] = table.df[2].str.replace(",", "")

                                    # Insertion d'une colonne à la position 0, pour l'id du modele
                                    table.df.insert(0, "piece_ID", None, allow_duplicates=False)
                                    # Insertion d'une colonne à la position 1, pour l'id du modele
                                    table.df.insert(1, "model_id", model_id, allow_duplicates=False)

                                    # On ajoute la dataframe à la liste des dataframes du pdf
                                    dfs_pdf.append(table.df)
                                
                                # Si titre page 'Aide au diagnostic' ou 'Fonctionnement' ou 'basse tension' ou 'Schéma...', 
                                # ce ne sont pas des schémas à traiter
                                elif not any(re.findall('Aide|chéma|Fonc|basse', table.df[0][2])):

                                    schemas_to_jpg_and_datas()

                        # FIN DE LA BOUCLE SUR LES PAGES SAUNIER

                except Exception as err:
                    if model_id not in error_files:
                        error_files.append(model_id)  
                        print(f'Erreur avec modèle {model_id}')
                        print("".join(traceback.TracebackException.from_exception(err).format()))

                        dfs_pdf = []

                # CONCATENER LES DATAFRAMES DU PDF EN UNE SEULE
                if dfs_pdf != [] and model_id not in error_files:
                    dfs_pdf = pd.concat(dfs_pdf)

                    # CONVERTIR LA DATAFRAME GLOBALE DU PDF EN CSV
                    csv_pieces_filepath = Path.cwd() / "csv_pieces" / f"{model_id}_pieces.csv"
                    dfs_pdf.to_csv(csv_pieces_filepath, index=False, header=False)

                    # SUPPRIMER LES GUILLEMETS ET LES ESPACES SUPERFLUS
                    with open(csv_pieces_filepath, "r", encoding="utf-8") as text:
                        csv_text = (
                            text.read().replace('"', "").replace(" ,", ",").replace(", ", ",")
                        )

                    with open(csv_pieces_filepath, "w", encoding="utf-8") as text:
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

    if len(csv_pieces_list) != 0:

        # Fonction pour concaténer les listes de csv
        def concat_csv(csv_filename, csv_list):
            with open(csv_filename, "w", encoding="utf-8") as outfile:
                for i, fname in enumerate(csv_list):
                    with open(fname, "r", encoding="utf-8") as infile:
                        shutil.copyfileobj(infile, outfile)

        # CONCAT des csv pieces
        concat_csv("csv_pieces_final.csv",csv_pieces_list)

      # ENVOI csv pièces concaténé en BDD
        cursor2 = cnx.cursor(prepared=True)

        datas_pieces = pd.read_csv("csv_pieces_final.csv", header=None, dtype = {2: str})

        for i, row in datas_pieces.iterrows():
            query = 'INSERT INTO pieces VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            cursor2.execute(query, tuple(row))
                                            
        print("Nombre d'insert de pieces executés' :", cursor2.rowcount)

        # CONCAT des csv images
        concat_csv("csv_images_final.csv",csv_images_list)

        # ENVOI csv images concaténé en BDD
        datas_img = pd.read_csv("csv_images_final.csv", header=None)

        for i, row in datas_img.iterrows():
            query = 'INSERT INTO fichiers VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            cursor2.execute(query, tuple(row))
                                            
        print("Nombre d'insert d'images executés' :", cursor2.rowcount)

    else:
        print('Aucun traitement effectué')

# CONVERTIR LA LISTE DES FICHIERS 'ERREURS' EN CHAINE DE CARACTERE pour le fichier rapport
error_files_txt = "\n- ".join(error_files)

# RECUPERER LE NOMBRE DE FICHIERS PDF et CSV TRAITES
processfiles_pdf = len(models_ids_list)
processfiles_csv = (len(csv_images_list) + len(csv_pieces_list))

# CREER UN FICHIER TEXTE DE RAPPORT DE TRAITEMENT
with open(f"Rapports/Rapport_Traitement_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.txt", "w+") as report:
    report.write(
        f"""*********************************
REPORTING du {dt:%d/%m/%Y %H:%M:%S}
*********************************

Nombre de fichiers PDF traites : {processfiles_pdf}

Pour chaque PDF, on sort 2 csv : 1 csv_pieces et 1 csv_images

Nombre de fichiers CSV en sortie : {processfiles_csv}

Fichiers non traites : 

- {error_files_txt}"""
    )

# SUPPRESSION DES DOSSIERS TEMPORAIRES
shutil.rmtree(csv_pieces_dir, ignore_errors=True)  # Supprimer le dossier csv_pieces
shutil.rmtree(csv_images_dir, ignore_errors=True)  # Supprimer le dossier csv_images
shutil.rmtree(imgTemp_dir, ignore_errors=True)  # Supprimer les sous-dossiers img_temp 

# # Calcul du temps de traitement :
print("*************************")
time_elapsed = datetime.now() - start_time
print(f"Temps de traitement : (hh:mm:ss.ms)  {time_elapsed}")


