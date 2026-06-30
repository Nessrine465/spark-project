"""Squelette de pipeline data pour le projet du jour 4.

Complétez les sections marquées TODO avec le jeu de données que vous avez choisi
(taxi NYC multi-mois, DVF immobilier, accidents ONISR, ou MovieLens).

Architecture cible (vue en cours) :
    brut (bronze) -> nettoyé (silver, Parquet) -> agrégé (gold, résultats)

Lancement, depuis la racine du projet :
    python starter-code/pipeline.py

L'énoncé complet et la grille : projects/projet-jour-4.md
"""

import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, DoubleType
from spark_session import get_spark

#Chemins des fichiers sources et des dossiers de sortie

MOVIES_CSV = "data/raw/movies.csv"
RATINGS_CSV = "data/raw/ratings.csv"
TAGS_CSV = "data/raw/tags.csv"
LINKS_CSV = "data/raw/links.csv"
SORTIE_SILVER = "data/output/silver"
SORTIE_GOLD = "data/output/gold"

def ingestion(spark):
    """Étape 1a : lire les CSV bruts avec schémas explicites."""

    # Schéma de la table des films
    movies_schema = StructType([
        StructField("movieId", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("genres", StringType(), True),
    ])

    # Schéma des notes
    ratings_schema = StructType([
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("rating", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ])

    # Schéma des tags
    tags_schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("tag", StringType(), True),
        StructField("timestamp", LongType(), True),
    ])
    
    #Schéma des identifiants externes
    links_schema = StructType([
        StructField("movieId", IntegerType(), False),
        StructField("imdbId", IntegerType(), True),
        StructField("tmdbId", IntegerType(), True),
    ])

    #Lecture des quatre fichiers CSV

    movies = spark.read.option("header", True).schema(movies_schema).csv(MOVIES_CSV)
    ratings = spark.read.option("header", True).schema(ratings_schema).csv(RATINGS_CSV)
    tags = spark.read.option("header", True).schema(tags_schema).csv(TAGS_CSV)
    links = spark.read.option("header", True).schema(links_schema).csv(LINKS_CSV)


    #Vérification du contenu des données

    print("=== MOVIES ===")
    movies.printSchema()
    movies.show(5, truncate=False)
    print("Lignes movies :", movies.count())

    print("=== RATINGS ===")
    ratings.printSchema()
    ratings.show(5)
    print("Lignes ratings :", ratings.count())
    ratings.describe("rating").show()

    print("=== TAGS ===")
    tags.printSchema()
    tags.show(5)
    print("Lignes tags :", tags.count())

    print("=== LINKS ===")
    links.printSchema()
    links.show(5)
    print("Lignes links :", links.count())

    return {
        "movies": movies,
        "ratings": ratings,
        "tags": tags,
        "links": links,
    }

def nettoyage(dfs):
    """Étape 1b : nettoyer les données MovieLens."""

    movies = dfs["movies"]
    ratings = dfs["ratings"]
    tags = dfs["tags"]
    links = dfs["links"]

    #Nombre de lignes avant nettoyage
    print("=== AVANT NETTOYAGE ===")
    print("Movies :", movies.count())
    print("Ratings :", ratings.count())
    print("Tags :", tags.count())
    print("Links :", links.count())

    #Suppression des doublons et des valeurs manquantes
    movies_clean = (
        movies
        .dropDuplicates(["movieId"])
        .na.drop(subset=["movieId", "title", "genres"])     
        
    )

    # Nettoyage des notes et création de la date/année
    ratings_clean = (
        ratings
        .dropDuplicates(["userId", "movieId", "timestamp"])
        .na.drop(subset=["userId", "movieId", "rating", "timestamp"])
        .filter((F.col("rating") >= 0.5) & (F.col("rating") <= 5))  
        .withColumn("date_rating", F.to_date(F.from_unixtime(F.col("timestamp"))))
        .withColumn("annee_rating", F.year(F.col("date_rating")))
    )

    # Nettoyage des tags et normalisation du texte
    tags_clean = (
        tags
        .dropDuplicates()
        .dropna(subset=["userId", "movieId", "tag", "timestamp"])
        .filter(F.length(F.trim(F.col("tag"))) > 0)
        .withColumn("tag_clean", F.lower(F.trim(F.col("tag"))))
        .withColumn("tag_date", F.from_unixtime(F.col("timestamp")).cast("timestamp"))
        .withColumn("tag_year", F.year(F.col("tag_date")))
    )
    
    # Suppression des liens incomplets
    links_clean = (
        links
        .dropDuplicates(["movieId"])
        .na.drop(subset=["movieId"])
    )

    # Vérification du contenu après nettoyage
    print("=== APRÈS NETTOYAGE ===")
    print("Movies clean :", movies_clean.count())
    print("Ratings clean :", ratings_clean.count())
    print("Tags clean :", tags_clean.count())
    print("Links clean :", links_clean.count())

    movies_clean.show(5, truncate=False)
    ratings_clean.show(5)
    tags_clean.show(5, truncate=False)
    links_clean.show(5)

    return {
        "movies": movies_clean,
        "ratings": ratings_clean,
        "tags": tags_clean,
        "links": links_clean,
    }


def ecrire_silver(dfs):
    """Étape 1c : écrire la couche silver en Parquet."""

    # Les tables les plus volumineuses sont partitionnées par année
    dfs["movies"].write.mode("overwrite").parquet(f"{SORTIE_SILVER}/movies")

    dfs["ratings"].write.mode("overwrite").partitionBy("annee_rating").parquet(f"{SORTIE_SILVER}/ratings")

    dfs["tags"] \
        .write \
        .mode("overwrite") \
        .partitionBy("tag_year") \
        .parquet(f"{SORTIE_SILVER}/tags")   
    
    dfs["links"].write.mode("overwrite").parquet(f"{SORTIE_SILVER}/links")

    print("Couche silver écrite dans", SORTIE_SILVER)

def transformation_et_analyses(spark):
    """Étape 2 : relire le propre, puis 3 analyses (silver -> gold).

    On relit la couche Parquet nettoyée (pas les données brutes).

    TODO : produire AU MOINS TROIS analyses, dont :
    - une AGRÉGATION (groupBy + agg) ;
    - une JOINTURE (join, idéalement avec F.broadcast sur la petite table) ;
    - une WINDOW FUNCTION (Window.partitionBy(...).orderBy(...), row_number/rank/lag).
    Et au moins UNE OPTIMISATION justifiée : broadcast, cache, ou repartition.
    """
    df = spark.read.parquet(SORTIE_SILVER)

    # Optimisation cache : utile UNIQUEMENT si df est réutilisé par plusieurs analyses.
    df = df.cache()
    df.count()  # matérialise le cache

    # --- Analyse 1 : agrégation -------------------------------------------------
    # TODO : groupBy(...).agg(F.count, F.avg, F.sum...) sur une question métier.
    analyse_1 = None

    # --- Analyse 2 : jointure ---------------------------------------------------
    # TODO : charger une table de référence et la joindre.
    # Pensez à F.broadcast(petite_table) pour éviter un shuffle.
    analyse_2 = None

    # --- Analyse 3 : window function -------------------------------------------
    # TODO : classement / cumul / moyenne glissante par groupe.
    # fenetre = Window.partitionBy("groupe").orderBy(F.desc("metrique"))
    # ... .withColumn("rang", F.row_number().over(fenetre)).filter(F.col("rang") <= 10)
    analyse_3 = None

    if analyse_1 is None or analyse_2 is None or analyse_3 is None:
        raise NotImplementedError(
            "TODO analyses : produisez 3 analyses (agrégation, jointure, window)."
        )

    return {"analyse_1": analyse_1, "analyse_2": analyse_2, "analyse_3": analyse_3}


def ecrire_gold(resultats):
    """Étape 3 : écrire les résultats de synthèse.

    TODO :
    - Écrire chaque résultat (Parquet ou CSV). coalesce(1) est acceptable ICI car les
      résultats agrégés sont PETITS. Ne jamais coalesce(1) un gros DataFrame.
    """
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        df.coalesce(1).write.mode("overwrite").parquet(chemin)
        print("Résultat écrit :", chemin)


def main():
    spark = get_spark("Projet Jour 4 - Mon pipeline")
    print("Spark UI disponible sur http://localhost:4040")

    # Étape 1 : ingestion et nettoyage (bronze -> silver)
    brut = ingestion(spark)
    propre = nettoyage(brut)
    ecrire_silver(propre)

    # Étape 2 : transformation et analyses (silver -> gold)
    # resultats = transformation_et_analyses(spark)

    # Étape 3 : finalisation
    # ecrire_gold(resultats)

    # Garder la session vivante pour explorer la Spark UI.
    # Décommentez la ligne suivante si le pipeline se termine trop vite :
    # input("Spark UI sur http://localhost:4040 - Entree pour quitter...")

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except NotImplementedError as e:
        print()
        print("Pipeline incomplet :", e)
        print("Complétez les sections TODO dans starter-code/pipeline.py.")
        sys.exit(1)
