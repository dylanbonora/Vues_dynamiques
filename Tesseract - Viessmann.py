import re
import shutil
from datetime import datetime
from pathlib import Path
import cv2
import fitz  # this is pymupdf
import pandas as pd
import pytesseract
import tabula
from matplotlib import pyplot as plt
import pypdfium2 as pdfium
from pdf2image import convert_from_path
from pytesseract import Output
import winsound


# from _utils import apply_threshold

# RECUPERE LA DATE ET L HEURE DU JOUR
start_time = datetime.now()
print(start_time)

# Chemin de la commande d'execution de Tesseract
pytesseract.pytesseract.tesseract_cmd = 'C:/Program Files/Tesseract-OCR/tesseract.exe'

# LISTE QUI RECUPERERA LES FICHIERS QUI GENERENT DES ERREURS
error_files = []

# NOMBRE DE LIGNES TOTALES SUR DOSSIER PDF
# NOMBRE DE CODE TOTAUX DETECTES SUR DOSSIER PDF
megaTotal_lines = 0
megaTotal_codes = 0

# Dictionnaire qui récuperera les données
# # pour générer un histogramme
dico_histo = {}

# CREER DOSSIER pour les histogrammes
histo_dir = Path.cwd() / 'Histogrammes'
histo_dir.mkdir(exist_ok=True)

# CREER DOSSIER GLOBAL pour les images
img_dir = Path.cwd() / 'images'
img_dir.mkdir(exist_ok=True)

# CREER DOSSIER RAPPORTS
report_dir = Path.cwd() / 'Rapports'
report_dir.mkdir(exist_ok=True)

# CREER DOSSIER GLOBAL pour les csv OCR globaux de chaque pdf
ocr_dir = Path.cwd() / 'OCR'
ocr_dir.mkdir(exist_ok=True)

# RECUPERER LA lISTE DES PDF
pdf_files = list((Path.cwd() / 'pdf').rglob('*.pdf'))

# ***************************************************************
# BOUCLE SUR LES PDF
# ***************************************************************

for pdf in pdf_files:
    try:
        # CREER SOUS-DOSSIER DU MEME NOM QUE LE PDF pour recevoir les images des schemas du pdf
        filename = pdf.stem      # Nom du fichier pdf sans l'extension
        img_pdf_dir = Path.cwd() / 'images' / filename

        if not img_pdf_dir.exists():

            img_pdf_dir.mkdir(exist_ok=True)

            # ***************************************************************
            # CONVERTIR LES PAGES DE SCHEMAS DU PDF EN IMAGES et sauvegarde en jpg
            # ***************************************************************

            with fitz.open(pdf) as pages_pdf:

                #  CONVERTIR LES PAGES DU PDF EN IMAGES 
                pdf_images = convert_from_path(pdf, 350) 

                # BOUCLE SUR LES PAGES DU PDF
                for iPage in range(len(pages_pdf)):

                    # CONVERTIR LA PAGE EN COURS EN LISTE DE DATAFRAMES 
                    dfs_list = tabula.read_pdf(pdf, encoding='utf-8', pandas_options={'header': None, 'dtype': 'string'}, pages=iPage+1)

                    # SI LISTE VIDE, c'est une page de schéma
                    if dfs_list == []:

                        # SAUVEGARDER LA PAGE EN JPG
                        image_name = f'{str(iPage+1).zfill(2)}_{filename}.jpg' 
                        pdf_images[iPage].save(f'images/{filename}/{image_name}', "JPEG")
        
        # **************************************
        # DETECTER LES CODES (Repères) AVEC TESSERACT 
        # **************************************

        # RECUPERER LA LISTE DES FICHIERS IMAGES DU PDF 
        dossier_images = list((img_pdf_dir).glob('*.jpg'))

        # # CREER DOSSIER - préfixé OCR - DU MEME NOM QUE LE PDF 
        # pour recevoir les CSV des données des codes et le rapport de détection
        # pour chaque page de schéma
        ocr_pdf_dir = Path.cwd() / 'OCR' / f'OCR_{filename}'
        ocr_pdf_dir.mkdir(exist_ok=True)

        if dossier_images == []:

            shutil.rmtree(img_pdf_dir)  # Supprimer le dossier image du pdf

            shutil.rmtree(ocr_pdf_dir)  # Supprimer le dossier OCR du pdf 

            error_files.extend(['pdf sans schemas :', pdf.stem])  # On ajoute le nom du pdf sans schémas dans la liste error_files

            total_codes = 'Aucun schema dans ce pdf'
            taux_de_detection = '0'

        else:
            # INITIALISATION DU NOMBRE DE LIGNES TOTALES SUR FICHIER PDF
            # INITIALISATION DU NB DE CODES DETECTES SUR FICHIER PDF
            total_lines = 0
            total_codes = 0

            # BOUCLE DU TRAITEMENT OCR TESSERACT SUR LES IMAGES 
            for image in dossier_images:

                # LISTE QUI RECUPERERA LES CODES DETECTES AVEC LEURS COORDONNEES RELATIVES, le nom du pdf et la page du schéma
                code_datas = ['Repere,Abscisse(%),Ordonnee(%),Fichier,Page\n']

                # Lecture de l'image et conversion en niveaux de gris
                img = cv2.imread(str(image), cv2.IMREAD_GRAYSCALE) 

                # Agrandissement de l'image avec un facteur x3
                img = cv2.resize(img, None, fx=3, fy=3, interpolation=cv2.INTER_LINEAR)
                # img = cv2.resize(img, (1000,1600))

                # img = cv2.GaussianBlur(img,(5,5),0)
                # img = cv2.bilateralFilter(img,9,75,75)

                # cv2.imshow("image", img[1])
                # cv2.waitKey(0)

                # Paramètres pour Tesseract
                cong = r'--oem 3 --psm 11 -c tessedit_create_tsv=1' 

                # RECUPERATION DES DONNEES DETECTEES dans une variable d :
                # élément reconnu : d['text'] 
                # et coordonnées d'une boîte encadrant cet élément : 
                # d['left'], d['top'] -> coords en haut à gauche de la boîte,  
                # d['width'], d['height'] -> largeur et hauteur de la boîte
                d = pytesseract.image_to_data(img, output_type=Output.DICT, config=cong)

                # Regex pour récupérer uniquement des nombres de 3 ou 4 chiffres (Viessmann)
                pattern = '^[0-9]{3,4}$'

                # Nombre total d'éléments détéctés
                n_boxes = len(d['text'])

                # BOUCLE SUR LES ELEMENTS DETECTES 
                # avec filtre sur niveau de confiance d['conf']
                # Et filtre avec la regex
                for i in range(n_boxes):
                    if int(float(d['conf'][i])) > 55:
                        if re.match(pattern, d['text'][i]):

                            # Nombre de codes détectés dans le pdf
                            total_codes += 1

                            # Nombre de code détéctés totaux
                            megaTotal_codes += 1

                            # Récupération des coordonnées de la boîte de l'élément 
                            (x, y, w, h) = (d['left'][i], d['top'][i], d['width'][i], d['height'][i])

                            # Ajout des données de l'élément dans la liste code_datas
                            # Avec calcul du point central de la boîte, en pourcentage des dimensions de la page (img.shape)
                            code_datas.append(f'{d["text"][i]},{round(((x + w/2)/img.shape[1]*100),2)},{round(((y + h/2)/img.shape[0]*100),2)},{filename},Page {(image.stem)[0:2]}\n')

                # Ecriture de la liste code_datas de l'image courante 
                # dans un fichier CSV du nom du pdf dans dossier OCR du pdf
                with open(f'{ocr_pdf_dir}/{image.stem}.csv', 'w') as datas:
                    datas.write("".join(code_datas)) 
               
            # FIN DE LA BOUCLE DU TRAITEMENT OCR TESSERACT SUR LES IMAGES DU PDF

            # CONCATENER LES CSV DE CHAQUE SCHEMA DU PDF 
                # RECUPERER LES FICHIERS CSV DANS UNE LISTE en scannant le dossier OCR du pdf
            csv_files = list((ocr_pdf_dir).glob('*.csv'))

                # CONCATENATION DANS UN NOUVEAU FICHIER CSV
            with open(f'OCR/OCR_{filename}.csv', 'w') as outfile:
                for i, fname in enumerate(csv_files):
                    with open(fname, 'r') as infile: 
                        if i != 0:                  # Supprime les en-têtes sauf celui du 1er csv
                            infile.readline()   
                        shutil.copyfileobj(infile, outfile)
        
            # **************************************
            # RECUPERER LE NB TOTAL DE LIGNES DU PDF (total_lines : lignes extraites des tableaux)
            # et nb de lignes de tous les pdf (megaTotal_lines) pour les Rapports
            # **************************************

            # CONVERTIR LE PDF EN DATAFRAME pour compter le nombre de lignes avec len(df)
            dfs_pdf = tabula.read_pdf(pdf, encoding='utf-8', pandas_options={'header': None, 'dtype': 'string'}, pages='all')

            if dfs_pdf != []:

                # BOUCLE SUR LES DATAFRAMES DU PDF
                for df in dfs_pdf:

                    # RENOMMER LES INDEX DES COLONNES (par défaut : entiers)
                    # Spécifique VIESSMANN
                    df.rename(columns={0: 'Position', 1: 'Reference', 2: 'Designation', 3: 'GrpMat', 4: 'Quantite'}, inplace=True)

                    # CHERCHER LES LIGNES 'DOUBLES' (texte qui déborde sur une nouvelle ligne)
                    # ET 'REMETTRE' le texte débordant dans la bonne ligne
                    # Puis supprimer la ligne inutile 
                    for ligne,value in enumerate(df['Position']): 
                        if pd.isna(value):

                            if pd.notna(df['Designation'][ligne]):
                                df['Designation'][ligne-1] += ' ' + df['Designation'][ligne]

                            elif pd.notna(df['GrpMat'][ligne]):
                                df['GrpMat'][ligne-1] += ' ' + df['GrpMat'][ligne]

                            # Supprimer la ligne contenant le texte débordant
                            df.drop(ligne, inplace=True) 

                    # SUPPRIMER LES LIGNES D'EN TETES
                    # en vérifiant la longueur de la valeur dans la colonne 'Position'
                    # si plus de 4 caractères, c'est un en-tête
                    headlines = df[df['Position'].map(len) > 4 ].index
                    df.drop(headlines, inplace=True)  

                    # NOMBRE DE LIGNES DANS LE PDF
                    total_lines += len(df)

                    # MEGA TOTAL LIGNES
                    megaTotal_lines += len(df)

            # Si taux détéction du pdf > 100% (repères en double ou triple sur schéma)
            # On considère taux = 100% pour le calcul du taux global
            if total_codes > total_lines:
                 megaTotal_codes -= (total_codes - total_lines)

    
    # FICHIERS PDF GENERANT DES ERREURS
    except Exception as err:
        # Chemin du fichier csv OCR du pdf
        ocr_pdf_file = Path.cwd() / 'OCR' /f'OCR_{filename}.csv'

        if ocr_pdf_file.exists():
            ocr_pdf_file.unlink()  # Supprimer le fichier csv ocr du pdf 'erreur'

        if ocr_pdf_dir.exists():
            shutil.rmtree(ocr_pdf_dir, ignore_errors=True)  # Supprimer le dossier OCR du pdf 'erreur'

        if img_pdf_dir.exists():
            shutil.rmtree(img_pdf_dir)  # Supprimer le dossier Images du pdf 'erreur'

        if pdf.stem not in error_files:
            error_files.append(pdf.stem)  # Ajouter le nom du fichier pdf 'erreur' dans la liste error_files

        # On décompte le nb de codes et le nb de lignes du pdf 'erreur'
        if type(total_codes) == int:
            megaTotal_codes -= total_codes

        megaTotal_lines -= total_lines

        print(f'Erreur : {err}')
    # **************************************
    #  RAPPORT DE DETECTION DU PDF
    # **************************************

    dt = datetime.now()

    taux_de_detection = 0

    ocr_pdf_files = list((ocr_pdf_dir).glob('*.*'))

    if ocr_pdf_files != []:

        with open(f'OCR/OCR_{filename}/Taux_{filename}_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.txt', 'w') as taux:

            if type(total_codes) == int and total_lines > 0:
                taux_de_detection = round((total_codes/total_lines*100),2)
            else:
                taux_de_detection = 0

            taux.write(f"""*********************************
        RAPPORT du {dt:%d/%m/%Y %H:%M:%S}
        *********************************

            Fichier pdf : {filename}

            Codes dans les tableaux : {total_lines}

            Codes detectes par tesseract : {total_codes}

            Taux de detection : {taux_de_detection} %""")

    # Ajout du nom du pdf et du taux pour l'histogramme
    dico_histo[pdf.stem] = taux_de_detection

# FIN DE LA BOUCLE SUR LES PDF

# **************************************************
# CONCATENER LES CSV DE CHAQUE PDF EN UN SEUL
# **************************************************

    # RECUPERER LES FICHIERS CSV DANS UNE LISTE en scannant le dossier OCR 
ocr_files = list((ocr_dir).glob('*.csv'))

    # CONCATENATION DANS UN NOUVEAU FICHIER CSV
with open(f'OCR_Viessmann.csv', 'w') as outfile:
    for i, fname in enumerate(ocr_files):
        with open(fname, 'r') as infile: 
            if i != 0:                  # Supprime les en-têtes sauf celui du 1er csv
                infile.readline()    
            shutil.copyfileobj(infile, outfile)

# ***************************************************
# CREER UN FICHIER TEXTE DE RAPPORT GLOBAL 
# ***************************************************

dt = datetime.now()
    # RECUPERER LE NOMBRE DE FICHIERS PDF TRAITES
processfiles_pdf = len(pdf_files)
    # RECUPERER LE NOMBRE DE FICHIERS CSV dans dossier OCR 
processfiles_ocr = len(ocr_files)
    # CONVERTIR LA LISTE DES FICHIERS 'ERREURS' EN CHAINE DE CARACTERES
error_files_txt = '\n- '.join(error_files)

if megaTotal_lines > 0:
    taux_de_detection = round((megaTotal_codes/megaTotal_lines*100),2)
else :
    taux_de_detection = 0

    # ECRIRE LE RAPPORT DANS UN TXT
with open(f"Rapports/OCR_Viessmann_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.txt", "w+") as report:
    report.write(f"""*********************************
REPORTING du {dt:%d/%m/%Y %H:%M:%S}
*********************************

Script : pdf2img

Nombre de fichiers PDF traites : {processfiles_pdf}
Nombre de fichiers CSV en sortie : {processfiles_ocr}

Fichiers non traites ou Sans schemas : 

- {error_files_txt}

Taux de detection global :

        Codes dans les tableaux : {megaTotal_lines}

        Codes detectes par tesseract : {megaTotal_codes}

        Taux de detection : {taux_de_detection} %
        
Temps de traitement (hh:mm:ss.ms) : {datetime.now() - start_time}""")

# ***********************************************************************
# GRAPHIQUE DES RESULTATS (pour maximum 50 pdf traités)  
# ***********************************************************************
try:
    if processfiles_pdf < 51:
        # Taille du graphique adapté au contenu 
        # avec minimum 12 et 7 pour largeur et hauteur
        plt.rcParams.update({'figure.autolayout': True})
        plt.subplots(figsize=(12,7))

        # Trier le dico par ordre décroissant des taux
        sortedDico = sorted(dico_histo.items(), reverse=True, key=lambda x: x[1]) # x[1] indique qu'on trie sur la valeur (sinon x[0] pour trier sur les clés)

        # Taux de detection et noms des pdf
        valeurs_taux = [val[1] for val in sortedDico]
        pdf_histo = [val[0] for val in sortedDico]

        # Valeurs d'abscisses affichées (de 1 à nombre de pdf traités)
        # plt.xticks(absc_pdf,absc_pdf)

        # Etiquette de l'axe des abscisses
        plt.xlabel('Taux de détection (%)')

        # Titre
        plt.title(f'Taux global : {taux_de_detection}%')

        # Histogramme
        plt.barh(pdf_histo,valeurs_taux, color=([0.2,0.6,0.5]))
        # Taux global
        plt.axvline(taux_de_detection, color='r')

        # Axe des taux de 0 à 100%
        plt.xlim([0,100])

        # Sauvegarde de l'histo en png
        plt.savefig(f'{histo_dir}/Histo_Saunier_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.png')

        # Affichage de l'histogramme
        plt.show()

except Exception as err:
    print(f'Erreur matplot : {err}')

# *********************************************************************************************
#  FIN DU SCRIPT
#  ********************************************************************************************

# # Calcul du temps de traitement du script:
print('*************************')
time_elapsed = datetime.now() - start_time
print (f'Temps de traitement : (hh:mm:ss.ms)  {time_elapsed}')

winsound.PlaySound('sound.wav', winsound.SND_ALIAS)



