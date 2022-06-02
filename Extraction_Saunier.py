import csv
import camelot
import shutil
import traceback
import pandas as pd
from pathlib import Path
from datetime import datetime
import fitz  # pip install pymupdf
from matplotlib import pyplot as plt

# RECUPERE LA DATE ET L HEURE DU JOUR
start_time = datetime.now()
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

        # Variable pour les pdf sans tableaux à extraire
        no_tab = True

        # BOUCLES SUR LES PAGES DU PDF
        for iPage in range(len(pages_pdf)):

            # CONVERSION DE LA PAGE EN COURS EN LISTE DE DATAFRAMES 
            tables = camelot.read_pdf(f'pdf/{pdf.name}', flavor='stream', table_areas=['0,800,565,45'], columns=['71,152,333'], pages=f'{iPage+1}')

            # BOUCLE SUR LA OU LES DATAFRAMES DE LA PAGE
            for table in tables:

                print('page ', iPage+1, ' len table.df ', len(table.df))
                print(table.df)

                try:
                    # On récupère le nom du modèle
                    if iPage == 0:
                        modele = table.df[0][2].replace('/', '-')

                    # Si tableau de 4 lignes sur toutes les pages
                    # alors aucun tableau à extraire
                    if len(table.df) == 4 and no_tab:
                        no_tab = True
                    else:
                        no_tab = False

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

                        # On copie la colonne 'Remarque' en colonne 7
                        table.df[7] = table.df[3]
                        
                        # On crée les colonnes 'GrpMat' et 'Quantite'
                        # avec Valeurs 'NA' pour meilleure lisibilité
                        table.df[3] = 'NA'
                        table.df[4] = 'NA'

                        # On crée les colonnes 'Modele' et 'Page'
                        table.df[5] = modele
                        table.df[6] = f'Page {iPage+1}'

                        # Réordonner sinon 'Page' après 'Substitution'
                        table.df.sort_index(axis=1, inplace=True)

                        # Renommage des colonnes pour les csv
                        table.df.rename(columns={0: 'Position', 1: 'Reference', 2: 'Designation', 3: 'GrpMat', 4: 'Quantite', 5: 'Modele', 6: 'Page', 7: 'Substitution'}, inplace=True)

                        # On supprime les 5 premières lignes qui ne sont pas des données
                        table.df.drop([0,1,2,3,4], inplace=True)

                        # print('****************')
                        # print('page ', iPage+1)
                        # print('modele :', modele)
                        # print(pdf.name)
                        # print(table.df)

                        # On ajoute la dataframe à la liste des dataframes du pdf
                        dfs_pdf.append(table.df)

                except Exception as err:

                    if pdf.stem not in error_files:
                        error_files.append(f"erreur à l'extraction : {pdf.stem}")  # On ajoute les noms de fichiers 'erreurs' dans la liste error_files

                    print("".join(traceback.TracebackException.from_exception(err).format()))

                    dfs_pdf = []


        # FIN DE LA BOUCLE SUR LES PAGES DU PDF

        # Si pdf sans tableaux à extraire
        if no_tab:
            error_files.append(f'pdf sans tableaux de donnees : {pdf.stem}')  

        # CONCATENER LES DATAFRAMES DU PDF EN UNE SEULE
        if dfs_pdf != [] and pdf.stem not in error_files:
            dfs_pdf = pd.concat(dfs_pdf)

            # CONVERTIR LA DATAFRAME GLOBALE DU PDF EN CSV 
            csv_filepath = Path.cwd() / 'csv' / f'{modele}.csv'
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

with open(f"Rapports/Extraction_Saunier_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.txt", "w+") as report:
    report.write(f"""*********************************
REPORTING du {dt:%d/%m/%Y %H:%M:%S}
*********************************

Nombre de fichiers PDF traites : {processfiles_pdf}
Nombre de fichiers CSV en sortie : {processfiles_csv}

Fichiers non traites : 

- {error_files_txt}""") 

# CONCATENER LES CSV DE CHAQUE PDF EN UN SEUL
with open('csv_final_saunier.csv', 'w', encoding='utf-8') as outfile:
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









