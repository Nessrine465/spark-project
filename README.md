# spark-project

## Présentation

L'objectif de ce projet est de construire un pipeline ETL complet avec Apache Spark à partir du jeu de données **MovieLens**.

Le pipeline suit l'architecture suivante :

```text
CSV bruts
   ↓
Nettoyage
   ↓
Silver Parquet
   ↓
Analyses
   ↓
Gold CSV
```

Le projet couvre l'ingestion des données, le nettoyage, l'écriture d'une couche Silver, la production d'analyses Gold, l'optimisation Spark, l'observation avec la Spark UI, deux explorations au-delà du cours et un bonus MLlib.

---

## Jeu de données

Le projet utilise le dataset **MovieLens**, composé de quatre fichiers CSV :

- `movies.csv`
- `ratings.csv`
- `tags.csv`
- `links.csv`

Les fichiers sources sont stockés dans :

```text
data/raw/
```

Les principales tables utilisées sont :

- `ratings` : notes données par les utilisateurs aux films ;
- `movies` : titres et genres des films ;
- `tags` : mots-clés associés aux films ;
- `links` : identifiants externes des films.

---

## Étape 1 - Ingestion et nettoyage

Les fichiers CSV sont chargés avec des schémas explicites grâce à `StructType`.

Les traitements réalisés sont :

- lecture des fichiers CSV ;
- définition des types des colonnes ;
- affichage des schémas et des premières lignes ;
- suppression des doublons ;
- suppression des valeurs manquantes ;
- filtrage des notes invalides ;
- création de colonnes de date et d'année ;
- normalisation des tags ;
- écriture des données nettoyées au format Parquet.

La couche Silver est écrite dans :

```text
data/output/silver/
```

Les tables `ratings` et `tags` sont partitionnées par année afin de faciliter certaines lectures filtrées.

---

## Étape 2 - Analyses Silver vers Gold

Les analyses sont réalisées uniquement à partir des données Parquet de la couche Silver.

Les résultats sont écrits dans :

```text
data/output/gold/
```

---

### Analyse 1 - Top Rated Movies

Cette analyse identifie les films les mieux notés avec au moins 50 votes.

Elle utilise une agrégation Spark :

```python
groupBy("movieId").agg(count, avg)
```

Résultat produit :

```text
top_rated_movies
```

---

### Analyse 2 - Top Rated Movies With Titles

Cette analyse ajoute le titre et le genre des films aux résultats de l'analyse précédente.

Elle utilise une jointure entre :

- les notes agrégées par film ;
- la table `movies`.

La table `movies` étant plus petite, elle est diffusée avec `broadcast`.

Résultat produit :

```text
top_rated_movies_with_titles
```

---

### Analyse 3 - Top Movies By Genre

Cette analyse classe les films par genre et conserve les 3 meilleurs films de chaque catégorie.

Elle utilise :

- `explode()` pour séparer les genres ;
- `Window.partitionBy("genre")` ;
- `row_number()` pour classer les films dans chaque genre.

Résultat produit :

```text
top_movies_by_genre
```

---

## Optimisations Spark

Deux optimisations principales ont été utilisées.

### Cache

La table `ratings` est mise en cache car elle est réutilisée dans plusieurs traitements.

La Spark UI confirme que le DataFrame est bien stocké en mémoire.

### Broadcast Join

La table `movies` est plus petite que la table `ratings`.

Elle est donc utilisée avec :

```python
F.broadcast(movies)
```

Le plan d'exécution affiche notamment :

```text
BroadcastExchange
BroadcastHashJoin
```

---

## Spark UI

La Spark UI a été consultée sur :

```text
http://localhost:4040
```

Les onglets observés sont :

- Jobs ;
- Stages ;
- DAG Visualization ;
- Storage ;
- SQL/DataFrame.

La Spark UI a permis d'observer :

- les jobs exécutés ;
- les stages Spark ;
- les opérations de shuffle ;
- le cache de la table `ratings` ;
- le `BroadcastExchange` ;
- le `BroadcastHashJoin`.

---

## Exploration 1 - Partition pruning sur Parquet

Cette exploration correspond à la piste **Pushdown mesuré**.

L'objectif est de comparer :

- une lecture complète de la table `ratings` ;
- une lecture filtrée sur la colonne de partition `annee_rating`.

Résultats obtenus :

| Lecture | Nombre de lignes | Temps |
|---|---:|---:|
| Sans filtre | 100836 | 0.11 s |
| Avec filtre `annee_rating = 2018` | 6418 | 0.08
 s |

Le résultat est légèrement contre-intuitif : le filtre n'est pas plus rapide sur ce petit volume de données. Cela peut s'expliquer par le coût fixe de lancement des jobs Spark, qui devient plus important que le gain de lecture sur un dataset de petite taille.

---

## Exploration 2 - UDF Python vs fonction native Spark

Cette exploration correspond à la piste **UDF, pandas_udf et fonction native**.

La même transformation a été écrite de deux façons :

- avec une fonction native Spark (`when`) ;
- avec une UDF Python.

La transformation consiste à créer une catégorie de note :

- `positive` si la note est supérieure ou égale à 4 ;
- `other` sinon.

Résultats obtenus :

| Méthode | Temps |
|---|---:|
| Fonction native Spark | 0.12 s |
| UDF Python | 0.72 s |

La fonction native Spark est plus rapide. Le plan de l'UDF affiche `BatchEvalPython`, ce qui montre que Spark doit passer par Python pour exécuter la transformation. Cela ajoute un coût important.

Conclusion : une UDF Python se justifie surtout lorsqu'une transformation ne peut pas être exprimée avec les fonctions natives Spark.

---

## Bonus MLlib - Système de recommandation ALS

Un mini-modèle de recommandation a été entraîné avec **MLlib** et l'algorithme **ALS**.

Le modèle utilise les colonnes :

- `userId`
- `movieId`
- `rating`

Les données sont séparées en deux parties :

- 80 % pour l'entraînement ;
- 20 % pour le test.

Résultats obtenus :

```text
Temps entraînement + évaluation : 2.28 secondes
RMSE du modèle ALS : 0.8721
```

Le modèle produit ensuite les 3 meilleures recommandations par utilisateur.

Exemple de sortie :

```text
userId | title | genres | score_predit
```

Le `score_predit` représente la préférence estimée par le modèle pour un utilisateur donné. Ce score peut dépasser 5 car il s'agit d'une prédiction du modèle, et non d'une note réelle directement saisie par un utilisateur.

---

## Exécution du projet

Depuis la racine du projet :

```bash
python3 starter-code/pipeline.py
```

Le script exécute automatiquement :

1. l'ingestion des données CSV ;
2. le nettoyage des données ;
3. l'écriture de la couche Silver ;
4. les analyses Gold ;
5. les explorations ;
6. le bonus MLlib.

---

## Arborescence du projet

```text
spark-project/
├── data/
│   ├── raw/
│   └── output/
│       ├── silver/
│       └── gold/
├── starter-code/
│   └── pipeline.py
├── spark_session.py
├── requirements.txt
└── README.md
```

---

## Technologies utilisées

- Python
- Apache Spark
- PySpark
- Parquet
- MLlib
- Git
- GitHub

---

## Conclusion

Ce projet met en place un pipeline Spark complet sur le dataset MovieLens. Il couvre l'ensemble du cycle de traitement : ingestion, nettoyage, écriture en Parquet, analyses métier, optimisations Spark, lecture de la Spark UI, explorations de performance et bonus MLlib.

Le projet montre notamment l'intérêt des fonctions natives Spark par rapport aux UDF Python, l'utilisation du cache, l'utilisation du broadcast join et la possibilité d'entraîner un mini-modèle de recommandation avec MLlib.