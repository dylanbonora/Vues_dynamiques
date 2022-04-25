<?php
include 'pdo_connect.php';

// TO DO
// Récupérer l'id du modele en bdd ou dans le nom de fichier ou via slug dans l'url ou hash url' ?

// ******************************************************************************
// REQUETE POUR RECUPERER LES TABLEAUX DE PIECES ET LE NB DE PAGE DE TABLEAUX

// Jointure table pieces avec table modele pour récupérér également l'id de la marque
$query = $pdo->prepare('SELECT repere, designation, MO_ID, MO_MA_ID, page FROM pieces 
                        JOIN modeles ON pieces.model_id = modeles.MO_ID 
                        WHERE MO_ID = 6170 
                        -- WHERE MO_ID = (SELECT MO_ID FROM modeles WHERE MO_SLUG = "slugg") 
                        ORDER BY page');

$query->execute(array());

$tabs_pieces = $query->fetchAll(PDO::FETCH_OBJ);
// $tabs_pieces = $query->fetchAll(PDO::FETCH_ASSOC);

// Tableau qui récupère tous les numéros de page des pièces 
$all_pages_pieces = [];

foreach ($tabs_pieces as $piece => $value) {
  array_push($all_pages_pieces, $tabs_pieces[$piece]->page);
  // array_push($pages_pieces, $tab['page']);
}

// Tableau qui filtre les numéros de page redondants
$pages_pieces = array_unique($all_pages_pieces);
// var_dump($pages_pieces);

// Pour sortir le nombre de page de tableaux
$nb_pages_tabs = (count($pages_pieces));
// var_dump($nb_pages_tabs);

// ID de la marque et du modele
$marque_id = ($tabs_pieces[0]->MO_MA_ID);
$modele_id = ($tabs_pieces[0]->MO_ID);

// ******************************************************************************
// REQUETE POUR RECUPERER LES SCHEMAS ET LE NB DE PAGE DE SCHEMAS

$query = $pdo->prepare('SELECT filename FROM fichiers WHERE model_id = 6170');
// WHERE MO_ID = (SELECT MO_ID FROM modeles WHERE MO_SLUG = "slugg")');

$query->execute(array());

$tabs_vues = $query->fetchAll(PDO::FETCH_OBJ);
// var_dump($tabs_vues);
// $tabs_vues = $query->fetchAll(PDO::FETCH_ASSOC);

$nb_pages_vues = (count($tabs_vues));

// ******************************************************************************
// NOMBRE DE PAGES TOTALES
$nb_pages = $nb_pages_tabs + $nb_pages_vues;
// var_dump($nb_pages);

?>

<!DOCTYPE html>
<html lang="fr">

<head>
  <meta charset="UTF-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="css/bootstrap.min.css">
  <link rel="stylesheet" href="css/style.css">
  <script src="https://unpkg.com/feather-icons"></script>
  <title>Vues Dynamiques</title>
</head>

<body>

  <h1>TEST Vues Dynamiques</h1>

  <section>
    <?php
    // BOUCLE SUR LES PAGES DE LA VUE ECLATEE
    for ($i = 1; $i <= $nb_pages + 1; $i++) :
      // exit();
      // Si la page existe dans la table des pièces
      if (in_array(strval($i), $pages_pieces)) : ?>
          <!-- Alors on affiche les pièces de la page -->
        <div class="tableaux">
          <table class="table table-striped">
            <thead>
              <tr>
                <th>Repere</th>
                <th>Designation</th>
              </tr>
            </thead>
            <tbody>
              <?php
              foreach ($tabs_pieces as $piece => $value) :
                if ($tabs_pieces[$piece]->page == $i) : ?>
                  <tr>
                    <th><?= ($tabs_pieces[$piece]->repere) ?></th>
                    <td><?= ($tabs_pieces[$piece]->designation) ?></td>
                  </tr>
              <?php
                endif;
              endforeach ?>
            </tbody>
          </table>
        </div>
      <?php
      // Sinon, c'est une page de schéma
      else : ?>
        <div class="schema">
          <?php
          // Alors on affiche le schéma
          foreach ($tabs_vues as $vue => $value) :
            if (substr($tabs_vues[$vue]->filename, 0, 3) == $i) : ?>
              <img src="uploads/<?= $marque_id ?>/<?= $modele_id ?>/<?= ($tabs_vues[$vue]->filename) ?>" alt="schéma">
        </div>
        <?php
            endif;
          endforeach;
        endif;
      endfor ?>
  </section>

  <!-- <button type="button" class="btn btn-primary">Bouton</button> -->

</body>