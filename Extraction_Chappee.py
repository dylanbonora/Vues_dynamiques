import camelot 
import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
import shutil
from datetime import datetime
import fitz  # pip install pymupdf

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

pdf_files = list((Path.cwd() / 'pdf').glob('*.pdf'))

for pdf in pdf_files:

    # LISTE QUI RECUPERERA LES DATAFRAMES DU PDF
    dfs_pdf = []

    # Initialisation de l'objet qui récuperera les dataframes page par page
    # tables = None

    with fitz.open(pdf) as pages_pdf:

        # Extraction de la 1ere page du pdf pour connaître son type de mise en page 
        # Et utiliser les paramètres d'extraction correspondants
        tables_mep = camelot.read_pdf(f'pdf/{pdf.name}', flavor='stream', pages='1')

        # BOUCLES SUR LES PAGES DU PDF
        for iPage, page in enumerate(pages_pdf):

            for table_mep in tables_mep:

                # Si 5,6 ou 7 colonnes -> MEP1 : Tableaux avec colonne 'Substitution'
                if 5 <= len(table_mep.df.columns) <= 7:
                    tables = camelot.read_pdf(f'pdf/{pdf.name}', flavor='stream', table_areas=['0,755,400,0'], columns=['65,106,262,319,367'], pages=f'{iPage+1}')

                # Sinon si 2 ou 4 colonnes -> MEP2 : Tableaux de 4 colonnes sans 'Substitution'
                elif len(table_mep.df.columns) == 2 or len(table_mep.df.columns) == 4: 
                    tables = camelot.read_pdf(f'pdf/{pdf.name}', flavor='stream', table_areas=['0,755,580,58'], columns=['62,124,520'], pages=f'{iPage+1}')

                # Sinon si autre nb de colonnes détéctées -> autre MEP 
                else:
                    error_files.append(f'pdf avec autre mise en page : {pdf.stem}')  
                    tables = None                 

            # Traitement
            images_infos = page.get_image_info()

            if images_infos != []:
                # Si une image dans la page fait plus de 527, c'est une page de schéma
                if any(img['width'] > 527 for img in images_infos):
                    print('-----------')
                    print(f'pdf {pdf.stem}')
                    print(f'page {iPage+1} : page SCHEMA')
                    # schemas_to_jpg_and_datas()

                # Si aucune image de la page ne dépasse 527 de large ou si aucune image ne fait 14 de large, c'est une page de tableau
                if not any((img['width'] > 527 or img['width'] == 14) for img in images_infos):
                # if not any((img['width'] > 527 or img['width'] == 16) for img in images_infos):
                    if tables == None:
                        break
                    else:
                        for table in tables:
                            try:
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
                                    table.df[7] = table.df[5]  
                                    table.df[7] = ['NA' if value == '' else value for value in table.df[7]]
                                    table.df[5] = pdf.stem  
                                else:
                                    table.df[5] = pdf.stem  
                                    table.df[7] = 'NA' 

                                # Ajout de la colonne 'Page' en col6
                                table.df[6] = f'Page {iPage+1}'

                                # Réordonner sinon 'Page' après 'Substitution'
                                table.df.sort_index(axis=1, inplace=True)

                                # Suppression des esapces superflus
                                table.df[2] = table.df[2].str.replace('  ','')

                                # Renommage des colonnes pour les csv
                                table.df.rename(columns={0: 'Position', 1: 'Reference', 2: 'Designation', 3: 'GrpMat', 4: 'Quantite', 5: 'Modele', 6: 'Page', 7: 'Substitution'}, inplace=True)
                    
                                # On ajoute la dataframe à la liste des dataframes du pdf
                                dfs_pdf.append(table.df)

                            except Exception as err:
                                if pdf.stem not in error_files:
                                    error_files.append(f'Erreur lors du traitement: {pdf.stem}')  

                                dfs_pdf = []

                                print(f'Erreur : {err}')

        # FIN DE LA BOUCLE SUR LES PAGES DU PDF

        # CONCATENER LES DATAFRAMES DU PDF EN UNE SEULE
        if dfs_pdf != [] and pdf.stem not in error_files:
            dfs_pdf = pd.concat(dfs_pdf)

            # CONVERTIR CETTE DATAFRAME GLOBALE DU PDF EN CSV 
            csv_filepath = Path.cwd() / 'csv' / f'{pdf.stem}.csv'
            dfs_pdf.to_csv(csv_filepath, index=False)

            # SUPPRIMER LES GUILLEMETS 
            with open(csv_filepath, "r", encoding='utf-8') as text:
                csv_text = text.read().replace('"', '')

            with open(csv_filepath, "w", encoding='utf-8') as text:
                text.write(csv_text)

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

with open(f"Rapports/Extraction_Chappee_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.txt", "w+") as report:
    report.write(f"""*********************************
REPORTING du {dt:%d/%m/%Y %H:%M:%S}
*********************************

Nombre de fichiers PDF traites : {processfiles_pdf}
Nombre de fichiers CSV en sortie : {processfiles_csv}

Fichiers non traites : 

- {error_files_txt}""") 

# CONCATENER LES CSV DE CHAQUE PDF EN UN SEUL
with open('csv_final_chappee.csv', 'w', encoding='utf-8') as outfile:
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
