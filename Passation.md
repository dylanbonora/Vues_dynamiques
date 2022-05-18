# ETAPES POUR EXTRACTION DES PIECES

## 1ERE PHASE : DETERMINER LES COORDONNEES DU TABLEAU ET (au besoin) DES COLONNES
-> cf plot_table.py

## 2EME PHASE : DISTINGUER LES PAGES DE TABLEAUX DES PAGES DE SCHEMAS
1. D'abord en fonction du nombre de colonnes extraites 

-> avec la boucle sur les tables du pdf et len(table.df.columns)

2. Sinon, en fonction de la taille des images
-> cf get_img.py