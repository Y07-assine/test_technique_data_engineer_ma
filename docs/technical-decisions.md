# Décisions techniques

Chaque décision est présentée sous la forme : contexte, décision, raisons, compromis.

## Sommaire

1. [Orchestration : Airflow](#orchestration--airflow)
2. [Traitement : PySpark en mode local](#traitement--pyspark-en-mode-local)
3. [Stockage : MinIO (S3)](#stockage--minio-s3)
4. [Format : Parquet](#format--parquet)
5. [Zone raw en CSV](#zone-raw-en-csv)
6. [Quarantaine des lignes invalides](#quarantaine-des-lignes-invalides)
7. [Horodatages en TIMESTAMP_NTZ](#horodatages-en-timestamp_ntz)
8. [Gold dimensionnelle](#gold-dimensionnelle)
9. [Recalcul complet de la gold](#recalcul-complet-de-la-gold)
10. [Médiane exacte](#médiane-exacte)
11. [Exposition : PostgreSQL](#exposition--postgresql)

---

## Orchestration : Airflow

- **Contexte** : le sujet impose Airflow ou Dagster. Le pipeline enchaîne un
  téléchargement lent et faillible (une API publique) et des étapes Spark
  séquentielles.
- **Décision** : Airflow 2.10 avec `LocalExecutor`, un DAG manuel paramétré (`Params` :
  période, `force_download`), et du *dynamic task mapping* pour le téléchargement (une
  tâche par jour, 4 en parallèle).
- **Pourquoi** : standard du marché pour l'orchestration batch. Le mapping par jour donne
  retries, logs et reprise au grain du jour : un jour en échec se rejoue seul. Le DAG ne
  contient que de l'orchestration, la logique vit dans le package `taxi_pipeline`.
- **Compromis** : `LocalExecutor` exécute les tâches dans le conteneur du scheduler, sans
  scalabilité horizontale, mais sans Redis ni Celery à opérer. Les 90 tâches de
  téléchargement d'un trimestre alourdissent un peu la vue *Grid*.

## Traitement : PySpark en mode local

- **Contexte** : le sujet impose Spark. Le volume (1,45 million de lignes par trimestre)
  tient sur un poste, mais le code doit rester valable pour davantage de données.
- **Décision** : PySpark 3.5 en mode `local[*]` dans le conteneur Airflow, une
  SparkSession par tâche, arrêtée en fin de tâche. Uniquement des fonctions Spark
  natives : pas d'UDF Python, pas de pandas. Transformations écrites comme fonctions
  pures sur DataFrames, testées unitairement.
- **Pourquoi** : aucun cluster à maintenir pour ce volume, et un code prêt pour un
  `spark-submit` sur cluster. La SparkSession par tâche libère la mémoire JVM entre les
  étapes. Seules de petites agrégations (comptages, métriques qualité) remontent au
  driver. Le référentiel des zones est joint en *broadcast* : pas de shuffle de la table
  des trajets.
- **Compromis** : un démarrage de JVM par tâche Spark. Pas de scalabilité horizontale.

## Stockage : MinIO (S3)

- **Contexte** : le datalake doit reposer sur un stockage objet et tourner en local.
- **Décision** : MinIO, accédé par le connecteur `s3a://` (`hadoop-aws` 3.3.4, aligné sur
  le Hadoop embarqué par PySpark 3.5). Image `bitnamilegacy/minio`, car les images
  officielles `minio/minio` ne sont plus publiées sur Docker Hub.
- **Pourquoi** : API compatible S3 : le même code fonctionnerait sur AWS S3 en changeant
  l'endpoint et les identifiants.
- **Compromis** : l'image `bitnamilegacy` est figée (plus de mises à jour). Écrire en
  S3 implique un renommage par copie en fin de job (limité par le *committer* v2).

## Format : Parquet

- **Contexte** : bronze, silver et gold sont relues par Spark à chaque étape.
- **Décision** : Parquet compressé snappy, partitionné par `trip_date` pour bronze et
  silver ; un fichier par table pour la gold.
- **Pourquoi** : colonnaire (seules les colonnes utiles sont lues), compressé (644 Mo de
  CSV → 114 Mo en bronze), typé, et lisible par tout l'écosystème (Spark, DuckDB,
  Trino…). La partition par jour permet le *partition pruning* et le rejeu d'un jour.
- **Compromis** : fichiers d'environ 1,3 Mo par jour, plus petits que l'idéal ; une
  partition mensuelle serait préférable à plus grande échelle. Pas de format
  transactionnel (voir limites dans le README).

## Zone raw en CSV

- **Contexte** : l'API renvoie du CSV. La bronze doit être rejouable sans rappeler
  l'API, lente et limitée en débit.
- **Décision** : conserver la réponse de l'API telle quelle (`raw/`, une page par
  fichier, marqueur `_SUCCESS` par jour), puis la convertir en Parquet dans la bronze,
  sans aucune transformation métier (toutes les colonnes en `string`).
- **Pourquoi** : auditabilité (on garde ce que la source a réellement renvoyé), rejeu
  complet hors ligne, et format efficace pour les étapes Spark. La lecture du CSV
  vérifie le header contre le schéma attendu : une dérive de schéma côté source fait
  échouer la bronze au lieu de décaler les colonnes.
- **Compromis** : la donnée est stockée deux fois (CSV + Parquet).

## Quarantaine des lignes invalides

- **Contexte** : environ 2 % des lignes sources sont invalides (trajets vides, montants
  manquants, vitesses impossibles…).
- **Décision** : chaque ligne invalide reçoit une `reject_reason` (la première règle
  violée) et part dans `silver/taxi_trips_rejected/`, au lieu d'être supprimée.
- **Pourquoi** : le taux de rejet devient mesurable (et contrôlé par la quality gate),
  chaque rejet est auditable et une règle trop stricte se détecte.
- **Compromis** : une table supplémentaire à stocker (5 Mo pour le T1 2023).

## Horodatages en TIMESTAMP_NTZ

- **Contexte** : la source fournit l'heure locale de Chicago sans fuseau
  (`2023-01-01T08:00:00.000`).
- **Décision** : typer les horodatages en `TIMESTAMP_NTZ` (*no time zone*).
- **Pourquoi** : un `TIMESTAMP` classique est réinterprété selon le fuseau de la session
  Spark ou du poste. Un décalage de plusieurs heures a été observé pendant le
  développement (poste en heure de Paris). NTZ conserve l'heure exacte de la source :
  dates, heures et jours de semaine dérivés restent justes quel que soit
  l'environnement.
- **Compromis** : il faut se souvenir que ces heures sont celles de Chicago ; un passage
  en UTC demanderait une conversion explicite.

## Gold dimensionnelle

- **Contexte** : la gold d'origine comptait trois tables KPI, chacune répondant à une
  seule question.
- **Décision** : dimensions réutilisables (`dim_date`, `dim_zone`, `dim_payment_type`,
  `dim_time`) et faits à grain explicite, dont les trois tables KPI historiques sont
  dérivées. Pas de fait au grain trajet. Détails dans le [modèle de données](data-model.md).
- **Pourquoi** : de nouvelles questions se posent en SQL par jointure fait-dimension,
  sans nouveau code Spark. Les mesures additives rendent les agrégations sur n'importe
  quelle période exactes. Les KPI historiques gardent leur nom et leur schéma.
- **Compromis** : 11 tables publiées au lieu de 3, et un peu plus de code (catalogue des
  tables, validations). Les tables KPI dupliquent quelques centaines de lignes déjà
  dérivables des faits.

## Recalcul complet de la gold

- **Contexte** : classements et parts de zones dépendent de toute la donnée disponible.
- **Décision** : la gold (dimensions, faits, KPI) est recalculée intégralement à chaque
  run, à partir de toute la silver, et réécrite en overwrite.
- **Pourquoi** : simple et sûr (pas d'état incrémental à maintenir) ; les clés des
  dimensions restent cohérentes avec les faits. À ce volume, le calcul prend moins d'une
  minute.
- **Compromis** : le coût croît avec l'historique. À grande échelle, les faits au grain
  jour deviendraient incrémentaux (remplacement des seules dates traitées).

## Médiane exacte

- **Contexte** : la médiane de durée par jour utilisait `percentile_approx`. La
  comparaison de non-régression a montré que son résultat dépendait de l'ordre de lecture
  des lignes : sur les mêmes données, il changeait sur 5 jours après une simple
  redistribution.
- **Décision** : calculer la médiane exacte (`percentile`, arrondie à 2 décimales).
- **Pourquoi** : un résultat reproductible d'un run à l'autre. Avec environ 16 000 valeurs
  par jour, le calcul exact ne coûte rien.
- **Compromis** : sur de très gros volumes, `percentile` doit trier toutes les valeurs de
  chaque groupe ; l'approximation redeviendrait pertinente, avec une précision explicite.

## Exposition : PostgreSQL

- **Contexte** : l'équipe BI doit pouvoir interroger la gold avec ses outils habituels.
- **Décision** : publication des 11 tables gold dans une base PostgreSQL `analytics`
  (rôle dédié), dans la même instance que les métadonnées Airflow. Écriture Spark JDBC
  dans des tables `__staging`, puis substitution de toutes les tables en une seule
  transaction, avec clés primaires et étrangères.
- **Pourquoi** : tous les outils BI lisent PostgreSQL. La substitution transactionnelle
  garantit que la BI ne voit jamais de modèle partiellement publié, et les clés
  revérifient grain et intégrité référentielle. Une seule instance limite
  l'infrastructure locale.
- **Compromis** : PostgreSQL n'est pas un moteur analytique : il convient aux tables
  gold agrégées, pas à un fait au grain trajet. En production, métadonnées Airflow et
  base d'exposition seraient séparées. Le port hôte est 5433 pour éviter un PostgreSQL
  local sur 5432.
