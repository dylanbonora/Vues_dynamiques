# ETAPES POUR EXTRACTION DES PIECES

## 1ERE PHASE : COMPULSER LES PDF
Compulser un certain nombre de pdf pour voir si il y a différents types de mise en page
et si le paramètre table_area  - qui permet de définir une zone à scanner plutôt que toute la page -
est nécessaire

## 2EME PHASE : (au besoin) DETERMINER LES COORDONNEES DU TABLEAU ET/OU DES COLONNES
-> cf plot_table.py

## 3EME PHASE : DISTINGUER LES PAGES DE TABLEAUX DES PAGES DE SCHEMAS
1. D'abord en fonction du nombre de colonnes extraites 

    -> avec la boucle sur les tables du pdf et len(table.df.columns)

2. Ou, en fonction de la taille des images

    -> cf get_img.py

3. Sinon en fonction de différences dans le contenu des dataframes

## NOTES

Pour les ELM qui passent pas :
Tester avec d'autres valeurs de 'line_scale' dans la méthode camelot.read_pdf
--------

Pour les scripts Extraction des 4 marques :
tester à l'occasion le paramètre 'split_text=True' dans la méthode camelot.read_pdf
pour voir si ça règle le problème des valeurs parfois décalées dans la colonne suivante
(au lieu de la logique actuelle)
--------

Si une marque a des images avec de la transparence à l'extraction,
on procède à la place à un recadrage après conversion de la page en images
cf exemple Viessmann dans pdf_to_bdd.py :

Coordonnées récupérées avec un éditeur de pdf
(afficher la règle, unités en points)
Attention avec le zoom facteur 4 (nécessaire pour une bonne résolution)
il faut multiplier les coordonnées d'autant

If we consider (0,0) as top left corner of image called im 
with left-to-right as x direction and 
top-to-bottom as y direction. 
then:

crop_img = im[y1:y2, x1:x2]
--------
