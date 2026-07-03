import sys
import time
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, DoubleType
from spark_session import get_spark
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

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

def exploration_pushdown_partition_pruning(spark):
    """Exploration 1 : mesurer le partition pruning sur la table ratings partitionnée."""

    print("\n=== Exploration 1 : partition pruning sur Parquet ===")

    ratings_path = f"{SORTIE_SILVER}/ratings"

    # Test 1 : lecture complète de la table ratings sans filtre.
    # Spark doit parcourir l'ensemble des partitions disponibles.
    debut_sans_filtre = time.time()

    ratings_complet = spark.read.parquet(ratings_path)
    nb_lignes_total = ratings_complet.count()

    fin_sans_filtre = time.time()
    temps_sans_filtre = round(fin_sans_filtre - debut_sans_filtre, 2)

    print("Lecture SANS filtre")
    print("Nombre de lignes :", nb_lignes_total)
    print("Temps :", temps_sans_filtre, "secondes")
    ratings_complet.explain()

    # Test 2 : lecture avec filtre sur la colonne de partition annee_rating.
    # L'objectif est de vérifier si Spark limite la lecture à la partition 2018.
    debut_avec_filtre = time.time()

    ratings_filtre = (
        spark.read.parquet(ratings_path)
        .filter(F.col("annee_rating") == 2018)
    )

    nb_lignes_filtrees = ratings_filtre.count()

    fin_avec_filtre = time.time()
    temps_avec_filtre = round(fin_avec_filtre - debut_avec_filtre, 2)

    print("\nLecture AVEC filtre annee_rating = 2018")
    print("Nombre de lignes :", nb_lignes_filtrees)
    print("Temps :", temps_avec_filtre, "secondes")
    ratings_filtre.explain()

    print("\n=== Conclusion exploration pushdown ===")
    print("Sans filtre :", temps_sans_filtre, "secondes")
    print("Avec filtre partition :", temps_avec_filtre, "secondes")

def exploration_udf_vs_native(spark):
    """Exploration 2 : comparer une UDF Python avec une fonction native Spark."""

    print("\n=== Exploration 2 : UDF vs fonction native ===")

    ratings = spark.read.parquet(f"{SORTIE_SILVER}/ratings")

    # Test 1 : transformation avec une fonction native Spark.
    # Cette version est optimisée par Spark et reste dans le moteur d'exécution JVM.
    debut_native = time.time()

    ratings_native = (
        ratings
        .withColumn(
            "rating_category",
            F.when(F.col("rating") >= 4, "positive").otherwise("other")
        )
    )

    ratings_native.groupBy("rating_category").count().count()

    fin_native = time.time()
    temps_native = round(fin_native - debut_native, 2)

    print("Temps fonction native Spark :", temps_native, "secondes")
    ratings_native.explain()

    # Test 2 : même transformation avec une UDF Python.

    def categoriser_note(rating):
        if rating is None:
            return "unknown"
        if rating >= 4:
            return "positive"
        return "other"

    categoriser_note_udf = F.udf(categoriser_note, StringType())

    debut_udf = time.time()

    ratings_udf = (
        ratings
        .withColumn(
            "rating_category",
            categoriser_note_udf(F.col("rating"))
        )
    )

    ratings_udf.groupBy("rating_category").count().count()

    fin_udf = time.time()
    temps_udf = round(fin_udf - debut_udf, 2)

    print("Temps UDF Python :", temps_udf, "secondes")
    ratings_udf.explain()

    print("\n=== Conclusion exploration UDF ===")
    print("Fonction native Spark :", temps_native, "secondes")
    print("UDF Python :", temps_udf, "secondes")

def bonus_mllib_recommandation(spark):
    """Bonus MLlib : entraîner un mini-modèle de recommandation avec ALS."""

    print("\n=== Bonus MLlib : système de recommandation ALS ===")

    # Lecture des notes nettoyées depuis la couche Silver.
    ratings = spark.read.parquet(f"{SORTIE_SILVER}/ratings")

    # MLlib ALS attend des identifiants utilisateur/film entiers
    # et une note au format float.
    ratings_mllib = ratings.select(
        F.col("userId").cast("int"),
        F.col("movieId").cast("int"),
        F.col("rating").cast("float")
    )

    # Séparation des données en apprentissage et test.
    train, test = ratings_mllib.randomSplit([0.8, 0.2], seed=42)

    # Modèle ALS : algorithme classique de recommandation collaborative.
    als = ALS(
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        rank=10,
        maxIter=5,
        regParam=0.1,
        coldStartStrategy="drop",
        nonnegative=True
    )

    debut = time.time()

    # Entraînement du modèle sur les notes d'apprentissage.
    modele = als.fit(train)

    # Prédiction des notes sur le jeu de test.
    predictions = modele.transform(test)

    # Évaluation avec le RMSE : plus il est faible, plus les prédictions sont proches des notes réelles.
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )

    rmse = evaluator.evaluate(predictions)

    fin = time.time()
    temps = round(fin - debut, 2)

    print("Temps entraînement + évaluation :", temps, "secondes")
    print("RMSE du modèle ALS :", round(rmse, 4))

    # Génération des 3 meilleures recommandations par utilisateur.
    recommandations = modele.recommendForAllUsers(3)

    # Jointure avec movies pour remplacer les movieId par des titres compréhensibles.
    movies = spark.read.parquet(f"{SORTIE_SILVER}/movies")

    recommandations_detaillees = (
        recommandations
        .withColumn("rec", F.explode("recommendations"))
        .select(
            "userId",
            F.col("rec.movieId").alias("movieId"),
            F.round(F.col("rec.rating"), 2).alias("score_predit")
        )
        .join(F.broadcast(movies), on="movieId", how="left")
        .select("userId", "title", "genres", "score_predit")
        .orderBy("userId", F.desc("score_predit"))
    )

    print("Top 3 recommandations détaillées par utilisateur :")
    recommandations_detaillees.filter(F.col("userId") <= 5).show(truncate=False)

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
    exploration_pushdown_partition_pruning(spark)
    exploration_udf_vs_native(spark)

    bonus_mllib_recommandation(spark)

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
