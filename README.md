# Vues Dynamiques
## PDF to BDD to Web Page

https://easysav.com/
EasySAV est une société qui a développé une Application Web et Mobile
qui centralise les Documentations techniques pour les Dépanneurs en génie climatique 
leur permettant ainsi de gagner un temps précieux dans leur activité. 

En tant que stagiaire, j'ai participe à un projet de Vues Eclatées Dynamiques pour les PDF
(ainsi qu'à la consommation d'une API) :

Dans un premier temps, le projet consistait à afficher les informations des pièces
au clic sur les repères des schémas. J'ai donc utilisé le module Tesseract 
pour détecter les repères et générer des fichiers csv à envoyer en base de données

Mais au vu du volume de la documentation en possession d'EasySAV,
le taux de détéction n'a pas été suffisant pour pouvoir poursuivre.

Ainsi, dans un second temps, il a été décidé de reconstituer les PDF en pages web 
afin de rendre les tableaux dynamiques en affchant des informations au clic 

J'ai tout d'abord conçu une mini base de données avec jMerise puis phpMyAdmin
pour correspondre à minima à la BDD de l'application
pour pouvoir effectuer des tests

J'ai ensuite développé un script Python pour :
- Extraire les pièces en fonction des marques d'équipement et les envoyer en base de données
- Convertir les schémas en images
- Constituer et envoyer les données correspondant aux images en BDD
- Gérer les erreurs et générer des rapports

En PYTHON:
Le module fitz m'a permis de manipuler les pdf (ouverture, infos sur les images, conversion de pages en image)
J'ai utilisé les modules tabula puis camelot pour extraire les données des tableaux
Avec les modules numpy et pandas, j'ai pu manipuler les dataframes extraites
J'ai établi la connexion à la base de données avec le module mysql.connector
Les modules Pathlib et shutil m'ont permis de manipuler des fichiers/dossiers 

j'ai utilisé encore d'autres modules pour diverses tâches

Vous pouvez consulter les scripts Tesseract et pdf_to_bdd.py

En PHP:
J'ai effectué des premiers tests d'affichage concluants
avec les scripts index.php et pdo_connect.php

En PHP Symfony:
Avec la participation du Lead Développeur,
nous avons inclus le script Python dans l'application EasySAV
Il a été décidé de l'inclure de façon à le lancer depuis l'interface EasyAdmin





