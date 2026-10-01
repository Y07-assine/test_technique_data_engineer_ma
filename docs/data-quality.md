# Qualité des données

La qualité est contrôlée à trois niveaux :

1. **silver** : règles de validité par ligne, avec quarantaine des lignes invalides ;
2. **quality gate** : contrôles sur l'ensemble de la silver, bloquants s'ils sont CRITICAL ;
3. **gold et publication** : unicité du grain, réconciliation avec la silver, clés
   primaires et étrangères dans PostgreSQL.

Aucune erreur n'est masquée : un contrôle bloquant fait échouer la tâche Airflow avec un
message explicite, et la couche suivante n'est pas produite.

## 1. Validation silver et quarantaine

Code : `src/taxi_pipeline/silver/transform.py`.

| Catégorie | Traitement |
|---|---|
| Typage | Horodatages en `TIMESTAMP_NTZ` (heure locale de Chicago, sans fuseau dans la source), montants en `decimal(10,2)`, durées `long`, distances et coordonnées `double`, zones `int`. |
| Normalisation | Noms explicites (`trip_start_ts`…), chaînes vides → `null`, `trim`, casse homogène des moyens de paiement, colonnes `POINT(...)` redondantes supprimées. |
| Doublons | Un trajet par `trip_id`. |
| Colonnes dérivées | `trip_date`, `trip_hour`, `day_of_week`, `trip_minutes`, `avg_speed_mph`, `tip_pct`, `amount_gap`, `is_amount_consistent`. |

**Règles de rejet**, évaluées dans cet ordre (la première règle violée est retenue comme
raison) :

| Raison | Condition |
|---|---|
| `missing_trip_id` | `trip_id` absent |
| `invalid_start_timestamp` / `invalid_end_timestamp` | horodatage absent ou non parsable |
| `end_before_start` | fin antérieure au début |
| `missing_duration` / `invalid_duration` | durée absente, négative ou > 24 h |
| `missing_distance` / `invalid_distance` | distance absente, négative ou > 500 miles |
| `empty_trip` | 0 seconde et 0 mile |
| `implausible_speed` | vitesse > 90 mph (trajets d'au moins 60 s) |
| `missing_amount` | `fare` ou `trip_total` absent |
| `negative_amount` | `fare` ou `trip_total` négatif |
| `implausible_amount` | `trip_total` > 1 000 $ |

Les lignes rejetées ne sont pas supprimées : elles sont écrites dans
`silver/taxi_trips_rejected/` avec leur `reject_reason`, leur fichier source et la date
de rejet. Le taux de rejet est donc mesurable et chaque rejet est auditable.

Résultat sur le T1 2023 : 29 323 rejets sur 1 446 439 lignes (2,0 %) :

| Raison | Lignes |
|---|---:|
| `empty_trip` | 27 212 |
| `missing_amount` | 1 242 |
| `implausible_speed` | 531 |
| `missing_duration` | 253 |
| `invalid_end_timestamp` | 27 |
| `implausible_amount` | 25 |
| `invalid_distance` | 19 |
| `end_before_start` | 9 |
| `missing_distance` | 5 |

## 2. Constat : cohérence des montants

Au premier run, le contrôle WARNING `amount_consistency_rate` a échoué : seuls 75 % des
trajets vérifiaient `trip_total = fare + tips + tolls + extras`. Ventilation par moyen de
paiement sur la première semaine de 2023 :

| Paiement | Écart 0 $ | Écart 0,50 $ | Écart 1,00 $ | Autre |
|---|---|---|---|---|
| Cash | 25 802 | — | — | 1 |
| Credit Card | 8 556 | 17 958 | 53 | 1 |
| Mobile | 10 047 | 2 160 | 1 | 8 |
| Prcard, Unknown, Dispute, No Charge | 17 412 | — | — | — |

L'écart n'est pas du bruit : c'est un frais fixe propre aux paiements électroniques, non
détaillé dans la source. Plutôt que d'abaisser le seuil, la règle modélise ce frais : un
écart de 0,50 $ ou 1,00 $ (tolérance 0,05 $) sur *Credit Card* ou *Mobile* est jugé
cohérent, tout autre écart est signalé. L'écart reste disponible dans `amount_gap`, et
la ligne n'est jamais rejetée pour ce motif. Après correction, le taux de cohérence
est de 99,99 % sur le T1 2023.

## 3. Quality gate

Code : `src/taxi_pipeline/quality/`. Toutes les métriques sont calculées en **une seule
agrégation Spark** (un seul scan de la silver de la période). Le rapport est journalisé
dans les logs de la tâche et écrit en JSON dans
`s3://datalake/quality_reports/period=<début>_<fin>/report.json`.

| Contrôle | Sévérité | Nombre |
|---|---|---|
| Colonnes obligatoires présentes | CRITICAL | 1 |
| Nombre de lignes > 0 | CRITICAL | 1 |
| Tous les jours de la période présents | CRITICAL | 1 |
| Aucun doublon de `trip_id` | CRITICAL | 1 |
| Dates dans la période | CRITICAL | 1 |
| Fin ≥ début | CRITICAL | 1 |
| Durées, distances et montants dans les bornes | CRITICAL | 3 |
| Taux de rejet ≤ `QUALITY_MAX_REJECT_RATE` (10 %) | CRITICAL | 1 |
| Aucun `null` sur les 7 colonnes obligatoires | CRITICAL | 7 |
| Taux de nullité ≤ 25 % sur les 5 colonnes optionnelles (zones, taxi, paiement, compagnie) | WARNING | 5 |
| Cohérence des montants ≥ 95 % | WARNING | 1 |

Soit **23 contrôles**, dont 17 CRITICAL. Un CRITICAL en échec lève `DataQualityError`,
qui liste les contrôles fautifs et l'emplacement du rapport. La tâche `quality_checks`
n'a pas de retry : l'échec est déterministe. Les seuils sont configurables
(`QUALITY_*` dans `.env`).

## 4. Garde-fous gold

Code : `src/taxi_pipeline/gold/validation.py`. Exécutés sur les tables en cache, **avant
toute écriture** :

- **Unicité du grain** : pour chacune des 11 tables, nombre de lignes = nombre de clés
  distinctes, et aucune clé nulle. Le grain est déclaré une seule fois, dans
  `gold/tables.py`.
- **Réconciliation** : chacun des 4 faits partitionne la silver. Son total de trajets et
  de revenu doit donc égaler exactement celui de la silver.

Toute violation lève `GoldValidationError` : rien n'est écrit ni publié.

## 5. Intégrité à la publication

PostgreSQL applique les mêmes garanties, dans la transaction de publication :

- **11 clés primaires** : le grain de chaque table ;
- **7 clés étrangères** : `date_key` des 4 faits vers `dim_date`, plus `zone_key`,
  `time_key` et `payment_type_key` vers leur dimension.

Une violation annule toute la publication ; la version précédente reste en place. Les
tests unitaires vérifient aussi l'absence de clé orpheline entre faits et dimensions.

## 6. Non-régression lors du passage à la gold dimensionnelle

Les trois tables KPI publiées par l'ancienne gold ont été exportées avant la migration,
puis comparées cellule par cellule à celles produites par le modèle dimensionnel sur le
T1 2023 :

| Table | Lignes | Cellules comparées | Différences |
|---|---|---|---|
| `gold_daily_metrics` | 90 | 1 170 | 1 |
| `gold_pickup_zone_metrics` | 78 | 780 | 0 |
| `gold_hourly_demand` | 168 | 1 344 | 0 |

La seule différence est la médiane de durée du 2023-02-07 (13,18 contre 13,2 min). Elle
ne vient pas du modèle mais de `percentile_approx`, utilisé par les deux versions : sur
les mêmes données, son résultat changeait sur 5 jours après une simple
redistribution des lignes. La médiane exacte de ce jour vaut 13,2. La médiane est
depuis calculée exactement (`percentile`), ce qui la rend reproductible
(voir [décisions techniques](technical-decisions.md#médiane-exacte)).
