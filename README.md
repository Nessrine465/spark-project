# Spark Project - MovieLens ETL Pipeline

## Description

Ce projet a été réalisé avec **Apache Spark (PySpark)** dans le cadre d'un projet de traitement de données.

L'objectif est de construire un pipeline ETL complet à partir du dataset **MovieLens** en suivant une architecture **Bronze → Silver → Gold**.

Le pipeline comprend :
- l'ingestion des fichiers CSV ;
- le nettoyage et la préparation des données ;
- l'écriture d'une couche Silver au format Parquet ;
- la réalisation de plusieurs analyses métier ;
- des optimisations Spark ;
- des explorations de performance ;
- un bonus avec **Spark MLlib**.

---

## Structure du projet

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
├── README.md
└── projects
    └──rapport-modele.md
    └──captures

```

---

## Analyses réalisées

Le projet produit trois analyses :

- **Top Rated Movies** : films les mieux notés avec au moins 50 votes ;
- **Top Rated Movies With Titles** : ajout des titres et des genres grâce à une jointure ;
- **Top Movies By Genre** : classement des trois meilleurs films pour chaque genre avec une Window Function.

---

## Optimisations

Les principales optimisations mises en œuvre dans le pipeline sont :

- mise en cache du DataFrame `ratings` (`cache()`) ;
- `BroadcastHashJoin` pour optimiser la jointure avec la table `movies`.

---

## Explorations

Deux explorations de performances ont été réalisées :

- **Partition Pruning** sur une table Parquet partitionnée ;
- comparaison entre une **fonction native Spark** et une **UDF Python**.

Le projet comprend également un bonus avec **Spark MLlib**, utilisant l'algorithme **ALS** afin de construire un mini système de recommandation de films.

---

## Spark UI

Pendant l'exécution du pipeline, l'interface **Spark UI** est accessible à l'adresse :

```text
http://localhost:4040
```

Elle permet d'observer :

- les **Jobs** exécutés ;
- les **Stages** et leurs tâches ;
- le **DAG (Directed Acyclic Graph)** des traitements ;
- le **Storage**, afin de vérifier la mise en cache des DataFrames ;
- les plans d'exécution **SQL/DataFrame**, permettant de visualiser les optimisations appliquées par Spark.

Une analyse détaillée de la Spark UI est présentée dans le rapport du projet.

---

## Exécution

Depuis la racine du projet :

```bash
python3 starter-code/pipeline.py
```

Le pipeline exécute automatiquement :

1. l'ingestion des données ;
2. le nettoyage ;
3. l'écriture de la couche Silver ;
4. les analyses Gold ;
5. les explorations de performances ;
6. le bonus MLlib.

---

## Technologies utilisées

- Apache Spark (PySpark)
- Spark SQL
- Spark MLlib
- Parquet
- Git
- GitHub