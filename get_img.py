from pathlib import Path
import fitz  # this is pymupdf
import sys

# Envoyer sorties print dans un fichier
sys.stdout = open('stdout.txt', 'w')

# RECUPERER LA lISTE DES PDF
pdf_files = list((Path.cwd() / 'pdf').rglob('*.pdf'))

# # ***************************************************************
# # BOUCLE SUR LES PDF
# # ***************************************************************

for pdf in pdf_files:
    print(f'pdf {pdf.stem}')

    with fitz.open(pdf) as doc:
        for iPage, page in enumerate(doc):
            print('----------------')
            print(f'page {iPage+1}')

            images_infos = page.get_image_info()

            if images_infos != []:

                for i,img in enumerate(images_infos):
                        print(f'pdf {pdf.stem}')
                        print(f'page {iPage+1}')
                        print('img num :', i, ' image width :',img['width'], ' image height :',img['height'])
                        
            # print('len images infos ', len(images_infos))
                    





