import sys
import time
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, DoubleType
from spark_session import get_spark

# Chemins des fichiers sources et des dossiers de sortie

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
    
    # Schéma des identifiants externes
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


    # Vérification des données lues

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

    # Nombre de lignes avant nettoyage
    print("=== AVANT NETTOYAGE ===")
    print("Movies :", movies.count())
    print("Ratings :", ratings.count())
    print("Tags :", tags.count())
    print("Links :", links.count())

    # Suppression des doublons et des valeurs manquantes
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

    # Écriture des données nettoyées en Parquet.
    # Les tables contenant une information d'année sont partitionnées.
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
    """Étape 2 : relire la silver et produire 3 analyses gold."""

    # Lecture des données de la couche Silver
    movies = spark.read.parquet(f"{SORTIE_SILVER}/movies")
    ratings = spark.read.parquet(f"{SORTIE_SILVER}/ratings")

    print("=== Lecture Silver ===")
    print("Movies silver :", movies.count())
    print("Ratings silver :", ratings.count())

    # Mise en cache de la table ratings car elle est utilisée plusieurs fois
    ratings = ratings.cache()
    ratings.count()

    # 1. Agrégation : calcul des films les mieux notés

    top_rated_movies = (
        ratings
        .groupBy("movieId")
        .agg(
            F.count("*").alias("nb_votes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne")
        )
        .filter(F.col("nb_votes") >= 50)
        .orderBy(F.desc("note_moyenne"), F.desc("nb_votes"))
    )

    print("=== Analyse 1 : top_rated_movies ===")
    top_rated_movies.show(20, truncate=False)

    # 2. Jointure : ajout du titre et du genre des films les mieux notés
    # Optimisation : Broadcast de la table movies

    debut = time.time()

    top_rated_movies_with_titles = (
        top_rated_movies
        .join(
            F.broadcast(movies),
            on="movieId",
            how="inner"
        )
        .select(
            "movieId",
            "title",
            "genres",
            "nb_votes",
            "note_moyenne"
        )
        .orderBy(F.desc("note_moyenne"), F.desc("nb_votes"))
    )

    # Déclenche l'exécution afin de mesurer le temps réel de la jointure
    top_rated_movies_with_titles.count()

    fin = time.time()
    print("Temps de la jointure avec Broadcast :", round(fin - debut, 2), "secondes")

    # Affichage du plan physique pour vérifier l'utilisation du Broadcast
    top_rated_movies_with_titles.explain()

    print("=== Analyse 2 : top_rated_movies_with_titles ===")
    top_rated_movies_with_titles.show(20, truncate=False)

    # 3. Window function : classer les films par genre et conserver les 3 meilleurs de chaque catégorie.

    films_genres = (
        top_rated_movies_with_titles
        .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
    )

    # Définition de la fenêtre de classement par genre
    fenetre = Window.partitionBy("genre").orderBy(
        F.desc("note_moyenne"),
        F.desc("nb_votes")
    )

    top_movies_by_genre = (
        films_genres
        .withColumn("rang", F.row_number().over(fenetre))
        .filter(F.col("rang") <= 3)
        .select(
            "genre",
            "rang",
            "movieId",
            "title",
            "nb_votes",
            "note_moyenne"
        )
        .orderBy("genre", "rang")
    )

    print("=== Analyse 3 : top_movies_by_genre ===")
    top_movies_by_genre.show(80, truncate=False)


    # Retour des trois analyses pour l'écriture dans la couche Gold
    return {
        "top_rated_movies": top_rated_movies,
        "top_rated_movies_with_titles": top_rated_movies_with_titles,
        "top_movies_by_genre": top_movies_by_genre
    }

def ecrire_gold(resultats):
    """Étape 3 : écrire les résultats de synthèse dans la couche Gold."""

    # Les résultats étant de petite taille, on les écrit en un seul fichier CSV.
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"

        df.coalesce(1) \
          .write \
          .mode("overwrite") \
          .option("header", True) \
          .csv(chemin)

        print("Résultat écrit :", chemin)

def exploration_broadcast_vs_sortmerge(spark):
    """Exploration : comparer BroadcastHashJoin et SortMergeJoin."""

    print("\n=== Exploration : BroadcastHashJoin vs SortMergeJoin ===")

    movies = spark.read.parquet(f"{SORTIE_SILVER}/movies")
    ratings = spark.read.parquet(f"{SORTIE_SILVER}/ratings")

    top_rated_movies = (
        ratings
        .groupBy("movieId")
        .agg(
            F.count("*").alias("nb_votes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne")
        )
        .filter(F.col("nb_votes") >= 50)
    )

    # Test 1 : BroadcastHashJoin forcé
    debut_broadcast = time.time()

    join_broadcast = top_rated_movies.join(
        F.broadcast(movies),
        on="movieId",
        how="inner"
    )

    join_broadcast.count()

    fin_broadcast = time.time()
    temps_broadcast = round(fin_broadcast - debut_broadcast, 2)

    print("\nTemps AVEC BroadcastHashJoin :", temps_broadcast, "secondes")
    join_broadcast.explain()

    # Test 2 : désactiver le broadcast automatique
    ancienne_valeur = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    debut_sortmerge = time.time()

    join_sortmerge = top_rated_movies.join(
        movies,
        on="movieId",
        how="inner"
    )

    join_sortmerge.count()

    fin_sortmerge = time.time()
    temps_sortmerge = round(fin_sortmerge - debut_sortmerge, 2)

    print("\nTemps SANS Broadcast automatique :", temps_sortmerge, "secondes")
    join_sortmerge.explain()

    # Restaurer la configuration Spark initiale
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", ancienne_valeur)

    print("\n=== Conclusion exploration ===")
    print("BroadcastHashJoin :", temps_broadcast, "secondes")
    print("SortMergeJoin / sans broadcast automatique :", temps_sortmerge, "secondes")

def exploration_cache_vs_sans_cache(spark):
    """Exploration bonus : comparer les performances avec et sans cache."""

    print("\n=== Exploration bonus : avec / sans cache ===")

    ratings = spark.read.parquet(f"{SORTIE_SILVER}/ratings")
    movies = spark.read.parquet(f"{SORTIE_SILVER}/movies")

    # Test 1 : sans cache
    debut_sans_cache = time.time()

    result_sans_cache = (
        ratings
        .groupBy("movieId")
        .agg(
            F.count("*").alias("nb_votes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne")
        )
        .filter(F.col("nb_votes") >= 50)
        .join(F.broadcast(movies), on="movieId", how="inner")
    )

    result_sans_cache.count()

    fin_sans_cache = time.time()
    temps_sans_cache = round(fin_sans_cache - debut_sans_cache, 2)

    print("Temps SANS cache :", temps_sans_cache, "secondes")

    # Test 2 : avec cache
    ratings_cache = ratings.cache()
    ratings_cache.count()

    debut_avec_cache = time.time()

    result_avec_cache = (
        ratings_cache
        .groupBy("movieId")
        .agg(
            F.count("*").alias("nb_votes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne")
        )
        .filter(F.col("nb_votes") >= 50)
        .join(F.broadcast(movies), on="movieId", how="inner")
    )

    result_avec_cache.count()

    fin_avec_cache = time.time()
    temps_avec_cache = round(fin_avec_cache - debut_avec_cache, 2)

    print("Temps AVEC cache :", temps_avec_cache, "secondes")

    print("\n=== Conclusion exploration cache ===")
    print("Sans cache :", temps_sans_cache, "secondes")
    print("Avec cache :", temps_avec_cache, "secondes")

def main():
    spark = get_spark("Projet Jour 4 - Mon pipeline")
    print("Spark UI disponible sur http://localhost:4040")

    # Étape 1 : ingestion et nettoyage (bronze -> silver)
    brut = ingestion(spark)
    propre = nettoyage(brut)
    ecrire_silver(propre)

    # Étape 2 : transformation et analyses (silver -> gold)
    resultats = transformation_et_analyses(spark)

    # Étape 3 : écriture de la couche Gold
    ecrire_gold(resultats)

    # Étape 4 : explorations supplémentaires
    exploration_broadcast_vs_sortmerge(spark)
    exploration_cache_vs_sans_cache(spark)

    # Garder la session vivante pour explorer la Spark UI.
    input("Spark UI sur http://localhost:4040 - Entree pour quitter...")

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except NotImplementedError as e:
        print()
        print("Pipeline incomplet :", e)
        print("Complétez les sections TODO dans starter-code/pipeline.py.")
        sys.exit(1)
