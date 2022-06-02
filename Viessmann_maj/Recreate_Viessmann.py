import fitz
import shutil
import camelot
import winsound
from pathlib import Path
from weasyprint import HTML
from datetime import datetime
from matplotlib import pyplot as plt

# CREER LE DOSSIER 'Viessman_output' pour recevoir les pdf modifiés
viessmann_output = Path.cwd() / "Viessmann_output"
viessmann_output.mkdir(exist_ok=True)

# DOSSIER TEMP pour les pages des tableaux modifiés
viessmann_temp = Path.cwd() / 'Viessmann_temp' 
viessmann_temp.mkdir(exist_ok=True)

# CREER DOSSIER RAPPORTS
report_dir = Path.cwd() / "Rapports"
report_dir.mkdir(exist_ok=True)

# LISTE QUI RECUPERERA LES FICHIERS QUI GENERENT DES ERREURS
error_files = []

pdf_files = list((Path.cwd() / 'pdf').glob('*.pdf'))

for pdf in pdf_files:

  # CREER DOSSIER OUTPUT du pdf courant
  # pour les pages de tableaux modifiées
  output_pdf_dir = viessmann_temp /pdf.stem
  output_pdf_dir.mkdir(exist_ok=True)

  with fitz.open(pdf) as doc:

    try:

      for iPage in range(len(doc)):

        tables = camelot.read_pdf(f'pdf/{pdf.name}', flavor='stream', table_areas=['0,736,552,62'], pages=f'{iPage+1}')

        for i,table in enumerate(tables):

          # Si plus de 3 colonnes, c'est une page de tableau
          if len(table.df.columns) > 3:

            # # SUPPRIMER LA COLONNE DES PRIX, colonne index 5
            table.df.drop(5, axis=1, inplace=True)

            # Suppression des esapces superflus colonne Designation
            table.df[2] = table.df[2].str.replace('  ','')

            # # Renommer les index des colonnes (par défaut : entiers)
            table.df.rename(columns={0: 'Position', 1: 'Reference', 2: 'Designation', 3: 'GrpMat', 4: 'Quantite'}, inplace=True)

            # SUPPRIMER LES LIGNES D'EN TETES
            # en vérifiant la longueur de la valeur dans la colonne 'Position'
            # si plus de 4 caractères ou égal à 1, c'est un en-tête 
            for ligne,value in enumerate(table.df['Position']): 
                if len(value) > 4 or len(value) == 1:
                    table.df.drop(ligne, inplace=True)   

            # On reset les index de ligne sinon bug
            table.df.reset_index(drop=True, inplace=True)

            # CHERCHER LES LIGNES 'DOUBLES' (texte qui déborde sur une nouvelle ligne)
            # ET 'REMETTRE' le texte débordant dans la bonne ligne
            # Puis supprimer la ligne inutile 
            for ligne,value in enumerate(table.df['Position']): 
                if value == '':
                    if table.df['Designation'][ligne] != '':
                        table.df['Designation'][ligne-1] += ' ' + table.df['Designation'][ligne]
                    elif table.df['GrpMat'][ligne] != '':
                        table.df['GrpMat'][ligne-1] += ' ' + table.df['GrpMat'][ligne]

                    # Supprimer la ligne contenant le texte débordant
                    table.df.drop(ligne, inplace=True) 

            # On reset les index de ligne sinon bug
            table.df.reset_index(drop=True, inplace=True)

            # Si valeurs GrpMat décalées dans colonne Designation (bug d'extraction)
            # Les récupérer et effacer dans Designation
            for ligne,value in enumerate(table.df['GrpMat']): 
                if value == '':
                    table.df['GrpMat'][ligne] =  (table.df['Designation'][ligne])[-3:]
                    table.df['Designation'][ligne] = (table.df['Designation'][ligne])[:-3]

            # Conversion du tableau modifié en tableau html  
            df_to_Html_table = table.df.to_html(classes='mystyle', index=False)

            # Variable avec MEP html contenant le tableau 
            html_string = f'''
            <html>
              <body>
                <header>VIESSMANN</header>
                <div class='modele'>{pdf.stem}</div>
                <div class='table'>{df_to_Html_table}</div>
              </body>
            </html>
            '''

            # Ecriture de la variable 'html' dans une page pdf
            # enregistrée dans le dossier du même nom que le pdf
            HTML(string=html_string).write_pdf(f'{output_pdf_dir}/ {str(iPage+1).zfill(2)}_{pdf.name}', stylesheets=["Viessmann_maj/style.css"])

            # Suppression de la page originale
            doc.delete_page(iPage)

            # Insertion de la nouvelle page
            with fitz.open(f'{output_pdf_dir}/ {str(iPage+1).zfill(2)}_{pdf.name}') as output_pdf:
              doc.insert_pdf(output_pdf, from_page=0, start_at=iPage)

            # Sauvegarde du pdf modifié dans le dossier 'Viessmann_output'
            doc.save(f'Viessmann_output\{pdf.name}')

          # if iPage == (len(doc)-1):
          #   shutil.rmtree(output_pdf_dir, ignore_errors=True)  
            # output_pdf_dir.rmdir()

    except Exception as err:
        print('Erreur ',err)

        if pdf.stem not in error_files:
          error_files.append(pdf.stem)  # On ajoute les noms de fichiers 'erreurs' dans la liste error_files

#  FIN DE LA BOUCLE SUR PDF

# Suppression du dossier temporaire
# shutil.rmtree(viessmann_temp, ignore_errors=True)  
# viessmann_temp.rmdir()

# CREER UN FICHIER TEXTE DE RAPPORT DE TRAITEMENT 
dt = datetime.now()

# CONVERTIR LA LISTE DES FICHIERS 'ERREURS' EN CHAINE DE CARACTERE pour le fichier rapport
if error_files != []:
	error_files_txt = '\n- '.join(error_files)
else:
	error_files_txt = 'Aucun probleme pendant le traitement'

with open(f"Rapports/Recreate_Viessmann_{dt:%d-%m-%Y_%Hh%Mmn%Ss}.txt", "w+") as report:
    report.write(f"""*********************************
REPORTING du {dt:%d/%m/%Y %H:%M:%S}
*********************************

Fichiers non traites : 

- {error_files_txt}""") 

# ***********************************************************************************
# FIN DU SCRIPT
print('finish')
winsound.PlaySound('sound.wav', winsound.SND_ALIAS)


