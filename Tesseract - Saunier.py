import re
import shutil
from datetime import datetime
from pathlib import Path
from types import NoneType
import cv2
import fitz  # this is pymupdf
import pytesseract
from pytesseract import Output
import camelot 
from matplotlib import pyplot as plt
from pdf2image import convert_from_path
import winsound
import numpy as np

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

# CREER DOSSIER pour les csv OCR globaux de chaque pdf
ocr_dir = Path.cwd() / 'OCR'
ocr_dir.mkdir(exist_ok=True)

# RECUPERER LA lISTE DES PDF
pdf_files = list((Path.cwd() / 'pdf').rglob('*.pdf'))

# ***************************************************************
# BOUCLE SUR LES PDF
# ***************************************************************

for pdf in pdf_files:
    try:
        modele = 'empty'
        img_pdf_dir = Path.cwd() / 'images' / modele

        # ***************************************************************
        # CONVERTIR LES PAGES DE SCHEMAS DU PDF EN IMAGES et sauvegarde en jpg
        # ***************************************************************

        with fitz.open(pdf) as pages_pdf:

            #  CONVERTIR LES PAGES DU PDF EN IMAGES 
            pdf_images = convert_from_path(pdf, 350) 

            # BOUCLE SUR LES PAGES DU PDF
            for iPage in range(len(pages_pdf)):

                # CONVERTIR LA PAGE EN COURS EN LISTE d'objets DE DATAFRAMES 
                tables = camelot.read_pdf(f'pdf/{pdf.name}', flavor='stream', table_areas=['0,790,565,55'], pages=f'{iPage+1}')

                for table in tables:
                    print('page ', iPage+1, ' len table.df ', len(table.df))
                    print(table.df)

                    # SI NB DE LIGNES INFERIEUR A 4, ce n'est pas un tableau de données
                    if len(table.df) < 4:
                        
                        # Si titre page 'Aide au diagnostic' ou 'Fonctionnement' ou 'basse tension' ou 'Schéma...', 
                        # ce ne sont pas des schémas à traiter
                        if not any(re.findall('Aide|chéma|Fonc|basse', table.df[0][2])):

                            if iPage==0:
                                modele = table.df[0][1].replace('/', '-')

                            # CREER SOUS-DOSSIER DU MEME NOM QUE LE PDF (nom du modele) pour recevoir les images des schemas du pdf
                            img_pdf_dir = Path.cwd() / 'images' / modele
                            img_pdf_dir.mkdir(exist_ok=True)                            

                            # SAUVEGARDER LA PAGE EN JPG
                            image_name = f'{str(iPage+1).zfill(2)}_{modele}.jpg' 
                            pdf_images[iPage].save(f'images/{modele}/{image_name}', 'JPEG')

            # FIN DE LA BOUCLE SUR LES PAGES

        # **************************************
        # DETECTER LES CODES (Repères) AVEC TESSERACT 
        # **************************************
        
        dossier_images = list((img_pdf_dir).glob('*.jpg'))

        # # CREER DOSSIER - préfixé OCR - DU MEME NOM QUE LE PDF 
        # pour recevoir les CSV des données des codes et le rapport de détection
        # pour chaque page de schéma
        ocr_pdf_dir = Path.cwd() / 'OCR' / f'OCR_{pdf.stem}_{modele}'
        ocr_pdf_dir.mkdir(exist_ok=True)

        if not img_pdf_dir.exists():

            # shutil.rmtree(ocr_pdf_dir, ignore_errors=True)  # Supprimer les fichiers du dossier OCR du pdf 

            error_files.append(f'pdf sans schemas : {pdf.stem} {modele}')  # On ajoute le nom du pdf sans schémas dans la liste error_files

            total_codes = 'Aucun schema dans ce pdf'
            taux_de_detection = '0'

        else:
            # INITIALISATION DU NB DE CODES DETECTES SUR FICHIER PDF
            total_codes = 0
            total_lines = 0

            # BOUCLE DU TRAITEMENT OCR TESSERACT SUR LES IMAGES 
            for image in dossier_images:

                # LISTE QUI RECUPERERA LES CODES DETECTES AVEC LEURS COORDONNEES RELATIVES, le nom du pdf et la page du schéma
                code_datas = ['Repere,Abscisse(%),Ordonnee(%),Fichier,Page\n']

                # Lecture de l'image et conversion en niveaux de gris
                img = cv2.imread(str(image), cv2.IMREAD_GRAYSCALE) 

                # print(f'{image} {img.shape[0]} {img.shape[1]}')

                # Agrandissement de l'image avec un facteur x3
                img = cv2.resize(img, None, fx=3, fy=3, interpolation=cv2.INTER_LINEAR)

                img = cv2.GaussianBlur(img,(5,5),0)
                # img = cv2.medianBlur(img,5)

                # Flou Moyen + Filtre bilateral
                # img = cv2.blur(img,(5,5))
                img = cv2.bilateralFilter(img,9,75,75)
                # Renforcement des gris
                # Pixels gris au delà de 170 transformés en pixels noirs 255
                # ret, img = cv2.threshold(img,170,255,cv2.THRESH_TOZERO)

                # img = cv2.adaptiveThreshold(img,255,cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY,199,5)

                # Pyramide up (différentes résolutions)
                # img = cv2.pyrUp(img)
                # Erosion
                # kernel = np.ones((5,5),np.uint8)
                # img = cv2.erode(img, kernel, iterations = 1)

                # Paramètres pour Tesseract
                cong = r'--oem 3 --psm 11 -c tessedit_create_tsv=1'

                # RECUPERATION DES DONNEES DETECTEES dans une variable d :
                # élément reconnu : d['text'] 
                # et coordonnées d'une boîte encadrant cet élément : 
                # d['left'], d['top'] -> coords en haut à gauche de la boîte,  
                # d['width'], d['height'] -> largeur et hauteur de la boîte
                d = pytesseract.image_to_data(img, output_type=Output.DICT, config=cong)

                # Regex pour récupérer uniquement les références des schémas Saunier 
                pattern = '^[ASbdg]?([0-9]{5,10})?$'

                # Nombre total d'éléments détéctés
                n_boxes = len(d['text'])

                # BOUCLE SUR LES ELEMENTS DETECTES 
                # avec filtre sur niveau de confiance d['conf]
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
                            code_datas.append(f'{d["text"][i]},{round(((x + w/2)/img.shape[1]*100),2)},{round(((y + h/2)/img.shape[0]*100),2)},{modele},Page {(image.stem)[0:2]}\n')

                # Ecriture de la liste code_datas de l'image courante 
                # dans un fichier CSV du nom du pdf dans dossier OCR du pdf
                with open(f'{ocr_pdf_dir}/{image.stem}.csv', 'w') as datas:
                    datas.write("".join(code_datas)) 
                
            # FIN DE LA BOUCLE DU TRAITEMENT OCR TESSERACT SUR LES IMAGES DU PDF

            # CONCATENER LES CSV DE CHAQUE SCHEMA DU PDF 
            csv_files = list((ocr_pdf_dir).glob('*.csv'))

                # CONCATENATION DANS UN NOUVEAU FICHIER CSV
            with open(f'OCR/OCR_{pdf.stem}_{modele}.csv', 'w') as outfile:
                for i, fname in enumerate(csv_files):
                    with open(fname, 'r') as infile: 
                        if i != 0:                  # Supprime les en-têtes sauf celui du 1er csv
                            infile.readline()   
                        shutil.copyfileobj(infile, outfile)

            # **************************************
            # RECUPERER LE NB TOTAL DE LIGNES DU PDF (total_lines : lignes extraites des tableaux)
            # et nb de lignes de tous les pdf (megaTotal_lines) pour les Rapports
            # **************************************

            # CONVERTIR LE PDF EN DATAFRAME pour compter le nombre de lignes avec len(table.df)
            tables = camelot.read_pdf(f'pdf/{pdf.name}', table_areas=['0,800,565,45'], flavor='stream', pages='all')

            for table in tables:

                    # Nettoyage, réarrangement des tableaux
                    # (Si plus de 3 lignes, c'est un tableau, sinon schéma)
                    if len(table.df) > 4:

                        # On supprime les lignes sans Désignation (dûes à des Remarques sur 2 lignes)
                        for ligne,value in enumerate(table.df[1]): 
                            if ligne > 3 and value == '':
                                table.df.drop(ligne, inplace=True)

                        # On reset les index de ligne sinon bug
                        table.df.reset_index(drop=True, inplace=True)

                        # On supprime les 5 premières lignes qui ne sont pas des données
                        table.df.drop([0,1,2,3,4], inplace=True)

                        total_lines += len(table.df)

                        # MEGA TOTAL LIGNES
                        megaTotal_lines += len(table.df)

            # Si taux détéction du pdf > 100% (repères en double ou triple sur schéma)
            # On considère taux = 100% pour le calcul du taux global
            if total_codes > total_lines:
                 megaTotal_codes -= (total_codes - total_lines)

    # FICHIERS PDF GENERANT DES ERREURS
    except Exception as err:
        # Chemin du fichier csv OCR du pdf
        ocr_pdf_file = Path.cwd() / 'OCR' /f'OCR_{pdf.stem}_{modele}.csv'

        if ocr_pdf_file.exists():
            ocr_pdf_file.unlink()  # Supprimer le fichier csv ocr du pdf 'erreur'

        if ocr_pdf_dir.exists():
            shutil.rmtree(ocr_pdf_dir, ignore_errors=True)  # Supprimer le contenu du dossier OCR du pdf 'erreur'

        if img_pdf_dir.exists():
            shutil.rmtree(img_pdf_dir)  # Supprimer le dossier Images du pdf 'erreur'

        if pdf.stem not in error_files:
            error_files.append(f'{pdf.name} {modele}')  # Ajouter le nom du fichier pdf 'erreur' dans la liste error_files

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

        with open(f'OCR/OCR_{pdf.stem}_{modele}/Taux_{pdf.stem}_{modele}_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.txt', 'w') as taux:

            if type(total_codes) == int and total_lines > 0:
                taux_de_detection = round((total_codes/total_lines*100),2)
            else:
                taux_de_detection = 0

            taux.write(f"""*********************************
        RAPPORT du {dt:%d/%m/%Y %H:%M:%S}
        *********************************

            Fichier pdf : {pdf.name} : {modele}

            Codes dans les tableaux : {total_lines}

            Codes detectes par tesseract : {total_codes}

            Taux de detection : {taux_de_detection} %""")

    # Ajout du nom du pdf et du taux pour l'histogramme
    dico_histo[pdf.stem] = taux_de_detection

# FIN DE LA BOUCLE SUR LES PDF

# **************************************************
# CONCATENER LES CSV DE CHAQUE PDF EN UN SEUL
# **************************************************

ocr_files = list((ocr_dir).glob('*.csv'))

    # CONCATENATION DANS UN NOUVEAU FICHIER CSV
with open(f'OCR_Saunier.csv', 'w') as outfile:
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
with open(f"Rapports/OCR_Saunier_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.txt", "w+") as report:
    report.write(f"""*********************************
REPORTING du {dt:%d/%m/%Y %H:%M:%S}
*********************************

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
        plt.title(f'Taux global : {taux_de_detection}%', color='r')

        # Histogramme
        plt.barh(pdf_histo,valeurs_taux, color=([0.2,0.6,0.5]))
        # Taux global et 50%
        plt.axvline(taux_de_detection, color='r')
        plt.axvline(50, color='b')

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

# # Calcul du temps de traitement :
print('*************************')
time_elapsed = datetime.now() - start_time
print (f'Temps de traitement : (hh:mm:ss.ms)  {time_elapsed}')
# Son de fin de script :)
winsound.PlaySound('sound.wav', winsound.SND_ALIAS)




        
