import csv
import shutil
import hashlib
import camelot
import mimetypes
import pandas as pd
from pathlib import Path, PurePath
from datetime import datetime
import fitz  # pip install pymupdf
from pdf2image import convert_from_path

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

# CREER DOSSIER RAPPORTS
report_dir = Path.cwd() / "Rapports"
report_dir.mkdir(exist_ok=True)

# LISTE QUI RECUPERERA LES FICHIERS QUI GENERENT DES ERREURS
error_files = []

# LISTE pour l'en-tête des csv des images
img_datas_cols = ['id','model_id','type','filename','extension','mime_type','hash','file_size','created_at','updated_at']
# LISTE GLOBALE pour le contenu csv des images
img_datas_rows = []

# RECUPERER LES FICHIERS PDF A TRAITER DANS UNE LISTE en scannant le dossier 'uploads'
pdf_files = list((Path.cwd() / "uploads").rglob("*.pdf"))

# BOUCLE SUR LA LISTE DES PDF 
# -> Extraction des tableaux vers CSV PIECES pour export dans table 'pieces' en BDD
# -> Conversion des images et Sauvegarde dans dossier modele correspondant
# -> Création d'un CSV IMAGES pour export dans table 'fichiers' en BDD
for pdf in pdf_files:

    # print(f'pdf {pdf}')
    
    # Récupération des ID Marque et Modele via le chemin du pdf
    path_uploads = Path.cwd() / 'uploads' # Chemin absolu jusqu'à 'uploads' inclus
    pdf_relpath = pdf.relative_to(path_uploads) # Chemin relatif du pdf à partir du dossier Marque_id inclus
    marque_id = pdf_relpath.parts[0]
    model_id = pdf_relpath.parts[1]
  
    # CREER SOUS-DOSSIER TEMPORAIRE DU NOM DE L'ID du MODELE 
    # pour recevoir les images du pdf, non hashés
    filename = pdf.stem
    img_pdf_dir = Path.cwd() / "img_temp" / model_id
    img_pdf_dir.mkdir(exist_ok=True)

    # LISTE QUI RECUPERERA LES DATAFRAMES DU PDF
    dfs_pdf = []

    with fitz.open(pdf) as pages_pdf:

        # BOUCLES SUR LES PAGES DU PDF
        for iPage in range(len(pages_pdf)):

            # Liste qui récupérera les données des images du pdf courant
            img_datas_row = [None, model_id, 'exploded_view']

            # CONVERSION DE LA PAGE EN COURS EN LISTE DE DATAFRAMES
            tables = camelot.read_pdf(
                f'uploads/{marque_id}/{model_id}/{pdf.name}',
                flavor="stream",
                table_areas=["0,736,552,62"],
                pages=f"{iPage+1}",
            )

            # BOUCLE SUR LA OU LES DATAFRAMES DE LA PAGE
            for table in tables:

                # Si plus de 3 colonnes, c'est une page de tableau
                if len(table.df.columns) > 3:

                    try:
                        # NETTOYAGE, REARRANGEMENT DES TABLEAUX

                        # # SUPPRIMER LA COLONNE DES PRIX, colonne index 5
                        table.df.drop(5, axis=1, inplace=True)

                        # Suppression des esapces superflus colonne Designation
                        table.df[2] = table.df[2].str.replace("  ", "")

                        # Insertion d'une colonne à la position 0, pour l'id de la pièce (valeur vide)
                        table.df.insert(0, "piece_ID", None, allow_duplicates=False)

                        # Insertion d'une colonne à la position 1, pour l'id du modele
                        table.df.insert(1, "model_id", model_id, allow_duplicates=False)

                        # # Renommer les index des colonnes (par défaut : entiers)
                        table.df.rename(
                            columns={
                                0: "Position",
                                1: "Reference",
                                2: "Designation",
                                3: "GrpMat",
                                4: "Quantite",
                            },
                            inplace=True,
                        )

                        # Ajouter colonne 'page'
                        table.df["Page"] = iPage + 1

                        # Ajouter colonne 'Substitution' vide (modèles Chappee)
                        table.df["Substitution"] = "NA"

                        # Ajouter colonne 'created_at' 
                        table.df["created_at"] = dt.strftime('%Y-%m-%d %H:%M:%S')

                        # Ajouter colonne 'updated_at' 
                        table.df["updated_at"] = None

                        # SUPPRIMER LES LIGNES D'EN TETES
                        # en vérifiant la longueur de la valeur dans la colonne 'Position'
                        # si plus de 4 caractères ou égal à 1, c'est un en-tête
                        for ligne, value in enumerate(table.df["Position"]):
                            if len(value) > 4 or len(value) == 1:
                                table.df.drop(ligne, inplace=True)

                        # On reset les index de ligne sinon bug
                        table.df.reset_index(drop=True, inplace=True)

                        # CHERCHER LES LIGNES 'DOUBLES' (texte qui déborde sur une nouvelle ligne)
                        # ET 'REMETTRE' le texte débordant dans la bonne ligne
                        # Puis supprimer la ligne inutile
                        for ligne, value in enumerate(table.df["Position"]):
                            if value == "":
                                if table.df["Designation"][ligne] != "":
                                    table.df["Designation"][ligne - 1] += (
                                        " " + table.df["Designation"][ligne]
                                    )
                                elif table.df["GrpMat"][ligne] != "":
                                    table.df["GrpMat"][ligne - 1] += (
                                        " " + table.df["GrpMat"][ligne]
                                    )

                                # Supprimer la ligne contenant le texte débordant
                                table.df.drop(ligne, inplace=True)

                        # On reset les index de ligne sinon bug
                        table.df.reset_index(drop=True, inplace=True)

                        # Si valeurs GrpMat décalées dans colonne Designation (bug d'extraction)
                        # Les récupérer et effacer dans Designation
                        for ligne, value in enumerate(table.df["GrpMat"]):
                            if value == "":
                                table.df["GrpMat"][ligne] = (table.df["Designation"][ligne])[-3:]
                                table.df["Designation"][ligne] = (table.df["Designation"][ligne])[:-3]

                        # On remplace les virgules par des espaces dans 'Designation'
                        table.df["Designation"] = table.df["Designation"].str.replace(",", "")

                        # print(f'pdf : {pdf.stem}')
                        # print(table.df)

                        # On ajoute la dataframe à la liste des dataframes du pdf
                        dfs_pdf.append(table.df)

                    except Exception as err:
                        if pdf.stem not in error_files:
                            error_files.append(pdf.stem)  

                        dfs_pdf = []

                        print(f"Erreur : {err}")
                else:
                    # LA PAGE EST UN SCHEMA
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
                    pix.save(f"uploads/{marque_id}/{model_id}/{image_name}", "JPEG")

                    # Taille du fichier image 
                    size = (Path(f"uploads/{marque_id}/{model_id}/{image_name}")).stat().st_size

                    # Created_at et Updated_at (ou vide avec Current timestamp ?)
                    created_at = dt.strftime('%Y-%m-%d %H:%M:%S')
                    updated_at = None

                    # Type mime des images :
                    # print(mimetypes.guess_type(f"img_temp/{model_id}/{image_name}")) # -> image/jpeg

                    # Ajout du hash, de la taille, l'extension, le type mime et les dates dans la liste des données de l'image courante
                    img_datas_row.extend([image_name, 'jpg', 'image/jpeg', md5hash_img, size, created_at, updated_at])

                    # Copie de cette liste car sinon passée par référence
                    datas_copy = img_datas_row.copy()

                    # Ajout de la liste copiée dans la liste globale des données des images
                    img_datas_rows.append(datas_copy)

                    # Vidage de la liste de l'image courante
                    img_datas_row.clear() 

        # FIN DE LA BOUCLE SUR LES PAGES DU PDF

        # CONCATENER LES DATAFRAMES DU PDF EN UNE SEULE
        if dfs_pdf != [] and pdf.stem not in error_files:
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

        # CONVERTIR LA LISTE DES DONNEES DES IMAGES du pdf courant EN CSV
        csv_images_filepath = Path.cwd() / "csv_images" / f"{model_id}_img_Vue_Eclatee.csv"

        with open(csv_images_filepath, 'w', newline='') as f:
            write = csv.writer(f)
            write.writerows(img_datas_rows)

# FIN DE LA BOUCLE SUR LE DOSSIER PDF

# CONVERTIR LA LISTE DES FICHIERS 'ERREURS' EN CHAINE DE CARACTERE pour le fichier rapport
error_files_txt = "\n- ".join(error_files)

# RECUPERER LE NOMBRE DE FICHIERS PDF TRAITES
processfiles_pdf = len(pdf_files)

# RECUPERER LE NOMBRE DE FICHIERS CSV TRAITES
csv_images_list = list((Path.cwd() / "csv_images").glob("*.csv"))
csv_pieces_list = list((Path.cwd() / "csv_pieces").glob("*.csv"))
processfiles_csv = (len(csv_images_list) + len(csv_pieces_list))

# CREER UN FICHIER TEXTE DE RAPPORT DE TRAITEMENT
with open(
    f"Rapports/Extraction_Viessmann_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.txt", "w+"
) as report:
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

# CONCATENER LES CSV FICHIERS DE CHAQUE PDF EN UN SEUL
with open("csv_images_final_viessmann.csv", "w", encoding="utf-8") as outfile:
    for i, fname in enumerate(csv_images_list):
        with open(fname, "r", encoding="utf-8") as infile:
            # if i != 0:                  # Supprime les en-têtes sauf celui du 1er csv
            infile.readline()
            shutil.copyfileobj(infile, outfile)

# CONCATENER LES CSV PIECES DE CHAQUE PDF EN UN SEUL
with open("csv_pieces_final_viessmann.csv", "w", encoding="utf-8") as outfile:
    for i, fname in enumerate(csv_pieces_list):
        with open(fname, "r", encoding="utf-8") as infile:
            # if i != 0:                  # Supprime les en-têtes sauf celui du 1er csv
            infile.readline()
            shutil.copyfileobj(infile, outfile)

# ***************************************************************************************
# FIN DU SCRIPT
# ***************************************************************************************

# # Calcul du temps de traitement :
print("*************************")
time_elapsed = datetime.now() - start_time
print(f"Temps de traitement : (hh:mm:ss.ms)  {time_elapsed}")
