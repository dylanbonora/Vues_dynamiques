import re
import camelot 
import sys
import traceback
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
import pandas as pd
import shutil
from datetime import datetime
import fitz  # pip install pymupdf

# RECUPERE LA DATE ET L HEURE DU JOUR
dt = datetime.now()
start_time = dt
print(dt)

# Envoyer sorties print dans un fichier
sys.stdout = open('stdout.txt', 'w') 

# CREER LE DOSSIER CSV 
csv_dir = Path.cwd() / "csv"
csv_dir.mkdir(exist_ok=True)

# CREER DOSSIER RAPPORTS
report_dir = Path.cwd() / "Rapports"
report_dir.mkdir(exist_ok=True)

# LISTE QUI RECUPERERA LES FICHIERS QUI GENERENT DES ERREURS
error_files = []

pdf_files = list((Path.cwd() / 'pdf').glob('*.pdf'))

for pdf in pdf_files:

    # LISTE QUI RECUPERERA LES DATAFRAMES DU PDF
    dfs_pdf = []

    with fitz.open(pdf) as pages_pdf:

        try:
        # BOUCLES SUR LES PAGES DU PDF
            for iPage, page in enumerate(pages_pdf):
                    # Traitement
                    images_infos = page.get_image_info()

                    if images_infos != []:

                        # Si aucune image de la page ne dépasse 354 de haut, c'est une page de tableau
                        if not any(img['height'] > 354 for img in images_infos):

                            tables = camelot.read_pdf(f'pdf/{pdf.name}', flavor='stream', table_areas=['0,755,400,0'], columns=['65,106,262,319,367'], pages=f'{iPage+1}')

                            for table in tables:
                                # NETTOYAGE, REARRANGEMENT, AJOUT DE COLONNES,...

                                # Si colonne 3 Quantité, vide (décalée)
                                # On la supprime et on réindex
                                if ((table.df[3] == '').all()):
                                    print('vrai')
                                    table.df.drop(3, axis=1, inplace=True)
                                    table.df.columns = range(table.df.columns.size)

                                # Si virgule dans colonne 'Quantite' 3
                                # Garder ce qui précède la virgule
                                table.df[3] = [value[:(value.find(','))] if ',' in value else value for value in table.df[3]]
                                # On ajoute '1' pour les valeurs manquantes de 'Quantite'
                                table.df[3] = ['1' if value == '' else value for value in table.df[3]]

                                # Si valeur désignation décalée dans colonne 'Quantité'
                                # On la copie dans colonne 'Designation'
                                # Et on met '' dans 'Quantite' 
                                for ligne,value in enumerate(table.df[3]): 
                                    if len(value) > 3:
                                        table.df[2][ligne] += value
                                        table.df[3][ligne] = ''

                                # On écrase la colonne 'Tarif' avec la colonne 'Quantite' 
                                table.df[4] = table.df[3]
                                # On remplace la colonne 'Quantite' par 'GrpMat'
                                # avec Valeurs 'NA' pour meilleure lisibilité
                                table.df[3] = 'NA'
                                # Si colonne 'Substitution' existe en col5 : copie en col6
                                # Et valeurs 'NA' si pas de valeur, pour meilleure lisibilité
                                # Puis on écrase col5 pour créer colonne 'page'
                                # Sinon création de col5 'page' et col6 'Substitution'
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
                                # Réordonner sinon 'Page' après 'Substitution'
                                table.df.sort_index(axis=1, inplace=True)
                                # Suppression des esapces superflus
                                table.df[2] = table.df[2].str.replace('  ','')
                                # Insertion d'une colonne à la position 0, pour piece id du modele
                                table.df.insert(0, "piece_ID", None, allow_duplicates=False)
                                # Insertion d'une colonne à la position 1, pour l'id du modele
                                table.df.insert(1, "model_id", 'model_id', allow_duplicates=False)
                                # On remplace les virgules par des espaces dans 'Designation'
                                table.df[2] = table.df[2].str.replace(",", "")

                                # Si pas de valeur dans colonne 'Référence' ou 'Réf. Référenc Description' ou 'De Dietrich'
                                # -> c'est soit une ligne de pied de page
                                # -> soit une ligne d'en tête 
                                # On supprime
                                for ligne,value in enumerate(table.df[1]): 
                                    if any(re.findall('Réf|De', value)) or value == '':
                                        table.df.drop(ligne, inplace=True)

                                # Suppression des esapces superflus
                                table.df[2] = table.df[2].str.replace('  ','')

                                # On remplace les virgules dans repère par des espaces
                                table.df[0] = table.df[0].str.replace(',',' ')

                                print('page ', iPage+1)
                                print(table.df)
                    
                                # On ajoute la dataframe à la liste des dataframes du pdf
                                dfs_pdf.append(table.df)

                        else:                    
                            print('-----------')
                            print(f'pdf {pdf.stem}')
                            print(f'page {iPage+1} : page SCHEMA')
                    else :
                        print('pas d"images')          

        # FIN DE LA BOUCLE SUR LES PAGES DU PDF

        except Exception as err:
            print("".join(traceback.TracebackException.from_exception(err).format()))
            error_files.append(pdf.stem)              
            dfs_pdf = []

        # CONCATENER LES DATAFRAMES DU PDF EN UNE SEULE
        if dfs_pdf != []:
            dfs_pdf = pd.concat(dfs_pdf)

            # CONVERTIR CETTE DATAFRAME GLOBALE DU PDF EN CSV 
            csv_filepath = Path.cwd() / 'csv' / f'{pdf.stem}.csv'
            dfs_pdf.to_csv(csv_filepath, index=False)

            # SUPPRIMER LES GUILLEMETS 
            with open(csv_filepath, "r", encoding='utf-8') as text:
                csv_text = text.read().replace('"', '')

            with open(csv_filepath, "w", encoding='utf-8') as text:
                text.write(csv_text)
        # Si aucun tableau détécté
        elif dfs_pdf == [] and pdf.stem not in error_files:
            error_files.append(pdf.stem)  

# # FIN DE LA BOUCLE SUR LE DOSSIER PDF

# CONVERTIR LA LISTE DES FICHIERS 'ERREURS' EN CHAINE DE CARACTERE pour le fichier rapport
error_files_txt = '\n- '.join(error_files)

# RECUPERER LE NOMBRE DE FICHIERS PDF TRAITES
processfiles_pdf = len(pdf_files)

# RECUPERER LE NOMBRE DE FICHIERS CSV TRAITES
csv_files = list((Path.cwd() / "csv").glob('*.csv'))
processfiles_csv = len(csv_files) 

# CREER LE FICHIER TEXTE DE RAPPORT DE TRAITEMENT 
dt = datetime.now()

with open(f"Rapports/Extraction_DeDietrich_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.txt", "w+") as report:
    report.write(f"""*********************************
REPORTING du {dt:%d/%m/%Y %H:%M:%S}
*********************************

Nombre de fichiers PDF traites : {processfiles_pdf}
Nombre de fichiers CSV en sortie : {processfiles_csv}

Fichiers non traites : 

- {error_files_txt}""") 

# CONCATENER LES CSV DE CHAQUE PDF EN UN SEUL
with open('csv_final_deDietrich.csv', 'w', encoding='utf-8') as outfile:
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
