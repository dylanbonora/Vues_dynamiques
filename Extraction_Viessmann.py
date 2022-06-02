# from tkinter.tix import InputOnly
import camelot
import pandas as pd
from pathlib import Path
import shutil
from datetime import datetime
import fitz  # pip install pymupdf

# RECUPERE LA DATE ET L HEURE DU JOUR
dt = datetime.now()
start_time = dt
print(start_time)

# CREER LE DOSSIER CSV 
csv_dir = Path.cwd() / "csv"
csv_dir.mkdir(exist_ok=True)

# CREER DOSSIER RAPPORTS
report_dir = Path.cwd() / "Rapports"
report_dir.mkdir(exist_ok=True)

# LISTE QUI RECUPERERA LES FICHIERS QUI GENERENT DES ERREURS
error_files = []

# RECUPERER LES FICHIERS PDF A TRAITER DANS UNE LISTE en scannant le dossier PDF
pdf_files = list((Path.cwd() / 'pdf').glob('*.pdf'))

# BOUCLE SUR LA LISTE DES PDF -> Extraction des tableaux PDF vers CSV via Dataframes
for pdf in pdf_files:

    # LISTE QUI RECUPERERA LES DATAFRAMES DU PDF
    dfs_pdf = []

    with fitz.open(pdf) as pages_pdf:

        # BOUCLES SUR LES PAGES DU PDF
        for iPage in range(len(pages_pdf)):

            # CONVERSION DE LA PAGE EN COURS EN LISTE DE DATAFRAMES 
            tables = camelot.read_pdf(f'pdf/{pdf.name}', flavor='stream', table_areas=['0,736,552,62'], pages=f'{iPage+1}')

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

                        # Insertion d'une colonne à la position 0, pour l'id du modele
                        table.df.insert(0, "id", None, allow_duplicates=False)
                        # Insertion d'une colonne à la position 1, pour l'id du modele
                        table.df.insert(1, "model_id", 'model_id', allow_duplicates=False)

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
                        table.df["updated_at"] = dt.strftime('%Y-%m-%d %H:%M:%S')

                        # SUPPRIMER LES LIGNES D'EN TETES
                        # en vérifiant la longueur de la valeur dans la colonne 'repere'
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
                                    table.df.at[ligne - 1, "designation"] += (
                                        " " + table.df["designation"][ligne]
                                    )
                                elif table.df["grpMat"][ligne] != "":
                                    table.df.at[ligne - 1, "grpMat"] += (
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
                                table.df.at[ligne, "grpMat"] = (table.df["designation"][ligne])[-3:]
                                table.df.at[ligne, "designation"] = (table.df["designation"][ligne])[:-3]

                        # On remplace les virgules par un espace dans 'Designation'
                        table.df["designation"] = table.df["designation"].str.replace(",", " ")

                        # On ajoute la dataframe à la liste des dataframes du pdf
                        dfs_pdf.append(table.df)

                    except Exception as err:
                        if pdf.stem not in error_files:
                            error_files.append(pdf.stem)  # On ajoute les noms de fichiers 'erreurs' dans la liste error_files

                        dfs_pdf = []

                        print(f'Erreur : {err}')
        # FIN DE LA BOUCLE SUR LES PAGES DU PDF

        # CONCATENER LES DATAFRAMES DU PDF EN UNE SEULE
        if dfs_pdf != [] and pdf.stem not in error_files:
            dfs_pdf = pd.concat(dfs_pdf)

            # CONVERTIR LA DATAFRAME GLOBALE DU PDF EN CSV 
            csv_filepath = Path.cwd() / 'csv' / f'{pdf.stem}.csv'
            dfs_pdf.to_csv(csv_filepath, index=False)

            # SUPPRIMER LES GUILLEMETS ET LES ESPACES SUPERFLUS
            with open(csv_filepath, "r", encoding='utf-8') as text:
                csv_text = text.read().replace('"', '').replace(' ,', ',').replace(', ', ',')

            with open(csv_filepath, "w", encoding='utf-8') as text:
                text.write(csv_text)

# FIN DE LA BOUCLE SUR LE DOSSIER PDF

# CONVERTIR LA LISTE DES FICHIERS 'ERREURS' EN CHAINE DE CARACTERE pour le fichier rapport
error_files_txt = '\n- '.join(error_files)

# RECUPERER LE NOMBRE DE FICHIERS PDF TRAITES
processfiles_pdf = len(pdf_files)

# RECUPERER LE NOMBRE DE FICHIERS CSV TRAITES
csv_files = list((Path.cwd() / "csv").glob('*.csv'))
processfiles_csv = len(csv_files) 

# CREER UN FICHIER TEXTE DE RAPPORT DE TRAITEMENT 
dt = datetime.now()

with open(f"Rapports/Extraction_Viessmann_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.txt", "w+") as report:
    report.write(f"""*********************************
REPORTING du {dt:%d/%m/%Y %H:%M:%S}
*********************************

Nombre de fichiers PDF traites : {processfiles_pdf}
Nombre de fichiers CSV en sortie : {processfiles_csv}

Fichiers non traites : 

- {error_files_txt}""") 

# CONCATENER LES CSV DE CHAQUE PDF EN UN SEUL
with open('csv_final_viessmann.csv', 'w', encoding='utf-8') as outfile:
    for i, fname in enumerate(csv_files):
        with open(fname, 'r', encoding='utf-8') as infile: 
            if i != 0:                  # Supprime les en-têtes sauf celui du 1er csv
                infile.readline()             
            shutil.copyfileobj(infile, outfile)

# ***************************************************************************************
# FIN DU SCRIPT
# ***************************************************************************************

# # Calcul du temps de traitement :
print('*************************')
time_elapsed = datetime.now() - start_time
print (f'Temps de traitement : (hh:mm:ss.ms)  {time_elapsed}')







