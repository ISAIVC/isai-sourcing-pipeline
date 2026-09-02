# CLAUDE.md

Mémoire partagée du projet ISAI Sourcing. **Toute session Claude (Cowork ou Claude Code) qui travaille sur un des 3 repos doit lire ce fichier en démarrant, et ajouter une ligne au journal d'avancement en bas après un changement notable** (décision, bug corrigé, piège découvert, feature livrée). La section "Vue d'ensemble" et le "Journal d'avancement" sont dupliqués à l'identique dans les 3 repos (back/front/pipeline) — répercuter tout ajout dans les 3 fichiers pour rester synchronisé.

## Vue d'ensemble du système

Le projet ISAI Sourcing (outil de sourcing investissement pour l'équipe ISAI) est réparti sur 3 repos GitHub (org `ISAIVC`) qui fonctionnent ensemble :

- **`isai-sourcing-front`** — dashboard React 19 / Vite / Chakra UI 3. Les analystes y consultent et filtrent les sociétés, lancent des recherches sémantiques/concurrents, poussent vers Attio, déclenchent le pipeline d'ingestion.
- **`isai-sourcing-back`** — Supabase (Postgres + Auth + RLS + Edge Functions Deno). Source de vérité du schéma, des vues (`sourcing_view`, `sourcing_mv`) et des fonctions SQL de matching (recherche floue, similarité vectorielle).
- **`isai-sourcing-pipeline`** — Python / Prefect / Terraform. Scrape, enrichit (LLM, embeddings) et score les sociétés ; écrit dans les mêmes tables Supabase. Orchestré par Prefect Cloud, tourne sur AWS ECS (Terraform), déployé par GitHub Actions.

**Flux type** : front (page Ingestion) → edge function `run-prefect-pipeline` → déploiement Prefect Cloud → tâches sur ECS → écriture dans Supabase → front relit `sourcing_view`/`sourcing_mv`.

Projet Supabase principal : ref `blfkamqmdmgkykcjyopd` (voir aussi note dans la section back sur un second ref rencontré pour les secrets Prefect — à clarifier).

## Ce repo : isai-sourcing-pipeline

Flows Prefect dans `src/flows/`, tasks dans `src/tasks/`, config dans `src/config/`, utilitaires dans `src/utils/` (dont `db.py` — retry/backoff, pagination via `.range()`). Infra AWS ECS dans `terraform/`. Worker Dockerisé (`Dockerfile`), déploiement via GitHub Actions (`prefect deploy`, `deploy.yml`). Docs de référence dans `how_tos/` (parsing Tracxn, réconciliation founders/funding).

### Pièges connus

**Ingestion Tracxn — header row différent par feuille** (`src/tasks/ingest_traxcn_export.py`) : `_find_header_row` ne scanne QUE la feuille Companies pour "Domain Name", puis applique cet index à toutes les feuilles via `pd.read_excel(..., header=header_row)`. Les exports Tracxn n'ont pas le même nombre de lignes de métadonnées par feuille (vérifié sur l'export de juillet 2026 : Companies → header à l'index 6, Funding Rounds et People → index 5). Résultat : Funding/People lisent une ligne trop bas, la vraie ligne d'en-tête devient une ligne de données, et `parse_column_names` plante (`AttributeError: 'int' object has no attribute 'lower'`) — Companies passe très bien, donc l'échec a l'air de venir d'ailleurs.
- Workaround sans déploiement : supprimer la ligne "Asterisk (*) denotes..." de la feuille Companies avant réupload, pour réaligner les 3 feuilles sur l'index 5.
- Fix propre (pas encore fait au 25/08/2026) : détecter le header par feuille dans `load_and_clean_excel` au lieu d'une fois sur Companies. Le commit `d1743bc` ("Detect Tracxn header row dynamically") a réglé le header glissant sur Companies mais pas la divergence entre feuilles.

**`business_processing_flow(auto=True)` ne converge jamais** : liste de travail = `scraped − fresh_complete`, tirage aléatoire (`random.shuffle`) de 200/heure. `fresh_complete` exige les 6 colonnes de `TASK_REPRESENTATIVE_COLUMNS` non-nulles, mais **`embed_textual_dimensions` n'y figure pas** — l'absence d'embedding n'est détectée qu'indirectement (elle fait planter `compute_scores` en aval, qui laisse `solution_fit_cg = None`). Mesuré le 25/08/2026 : 2520 domaines sur 72407 scrapés restent sans embedding indéfiniment (~0.57% de chance/heure/domaine sur un pool de ~35300).
- Pour rattraper un lot précis : lancer `business-processing-deployment` avec `domains` (liste JSON plate de strings — **champ séparé**, ne pas coller l'objet paramètres entier dedans), `auto: false`, en activant seulement `embed_textual_dimensions` + `compute_scores`. Les longs tableaux se tronquent au copier-coller dans le formulaire Prefect : découper avec `ntile(5)`.
- Fix proposé, pas fait : remplacer `random.shuffle` par un ordre déterministe (jamais traités d'abord, puis `updated_at` le plus ancien) pour que le pool draine réellement.

**Scoring `solution_fit_cg`/`solution_fit_by`** (`compute_scores.py`) : copie du plus proche voisin (1-NN), pas un modèle. Texte = `detailed_solution` + `use_cases` de la dernière ligne `web_scraping_enrichment` réussie, embeddé en `gemini-embedding-001` (768 dims, `task_type=CLASSIFICATION`), comparé par produit scalaire à un pool de sociétés scorées manuellement (`solution_fit_cg_manual`/`_by_manual`), score du gagnant (`argmax`) copié tel quel.
- `global_fund_score` n'est PAS calculé par le pipeline — c'est un champ analyste lu via `sourcing_view` (`COALESCE(manual, bcv.global_fund_score)`). Ne pas s'en servir pour vérifier si le scoring a tourné : utiliser `solution_fit_cg`.
- Faiblesses connues (mesurées le 25/08/2026 sur 69896/71480 lignes scorées) : pas de seuil de similarité (un match sans rapport est quand même copié, rien ne le signale) ; espace d'embedding anisotrope (les pires matchs de juillet sont à ~0.81 cosine — un seuil absolu n'a pas de sens, seul le rang relatif compte ; le mean-centering serait le gain qualité le plus probable) ; k=1 (une référence mal étiquetée contamine tout son voisinage) ; une référence sans embedding est silencieusement écartée du pool, sans log.
- Avant de changer quoi que ce soit : mesurer en leave-one-out sur le pool de référence via l'opérateur `<=>` (pgvector) en SQL, et annoter en priorité les sociétés les plus loin de toute référence.

**Rotation de la clé API Prefect Cloud** — stockée à 3 endroits qui expirent ensemble :
1. Secrets d'edge function Supabase (projet `nhszmpinlumqrfnrflrm`, eu-west-3) : `PREFECT_API_KEY` + `PREFECT_ORG`/`PREFECT_WORKSPACE`/`PREFECT_PIPELINE_DEPLOYMENT_ID`/`PREFECT_PUSH_ATTIO_DEPLOYMENT_ID`, utilisés par `run-prefect-pipeline` et `push-to-attio`.
2. Secrets GitHub Actions du repo (`environment: development` → vérifier d'abord les secrets de cet environnement, puis ceux du repo), utilisés par `prefect deploy` dans `deploy.yml`.
3. `terraform/terraform-variables.env` (champ `api_key` dans `TF_VAR_prefect_config`), utilisé par le provider Prefect Terraform — pas besoin d'`apply` juste pour le mettre à jour.
- Symptôme d'expiration (vu le 25/08/2026) : le bouton "Confirm: Run Pipeline" du front échoue avec "Failed to start the pipeline" ; les logs de l'edge function montrent `401 Invalid authentication credentials` (un 404 indiquerait plutôt un mauvais org/workspace/deployment id).
- Après `supabase secrets set PREFECT_API_KEY=...`, forcer un cold start avec `supabase functions deploy run-prefect-pipeline` — une instance déjà chaude sert encore l'ancienne valeur pendant quelques minutes.
- Risque associé, pas corrigé au 25/08/2026 : `.gitignore` référence `terraform/terraform-var.env` au lieu du vrai nom `terraform-variables.env` → ce fichier est tracké dans git. Valeurs actuelles = placeholders `****` (vérifié sur tout l'historique), mais un `apply` local avec de vraies valeurs suivi d'un commit fuiterait la service role key Supabase, les tokens Attio, Google et Dealroom.

### Dette technique connue (audit du 13/07/2026)

- 🔴 Security group ECS ouvert à `0.0.0.0/0` sur tous les ports, combiné à `assignPublicIp: ENABLED` et des subnets publics → les tasks (qui portent tous les secrets en variables d'environnement) sont exposées.
- 🟠 IAM trop permissif (`secretsmanager:GetSecretValue` sur `Resource=*`, `CloudWatchFullAccess` là où `logs:CreateLogStream/PutLogEvents` suffirait), clés IAM statiques jamais rotées pour Prefect.
- 🟠 State Terraform S3 non chiffré, sans lock, contenant les tokens en clair (Supabase service role, Attio, Dealroom, Google).
- 🟠 Panne API Attio avalée silencieusement → écrase `in_attio=False`/`attio_status` en base sans jamais déclencher les retries Prefect (`pull_attio_status.py`).
- 🟠 Troncature silencieuse à 1000 lignes (défaut PostgREST) sur les tables de référence non paginées (`fuzzy_matching_metrics.py`, `compute_scores.py`) — matching incomplet garanti sur `global_2000` (2000 lignes par construction).
- 🟠 Aucun test sur 15+ tasks de transformation (fuzzy matching, réconciliation, scoring) — risque n°1 de régression.
- 🟡 Prompt injection → SSRF possible dans le crawler (`website_crawling.py`) : le markdown crawlé, non fiable, est passé à Gemini qui renvoie des URLs crawlées sans validation de domaine ni blocage d'IP privées.
- Détail complet : `AUDIT.md` section 2, dans le dossier local `Datadriven_sourcing` (pas encore versionné).

## Journal d'avancement (partagé — dupliqué dans les 3 repos, garder synchronisé)

Ajoutez une ligne datée à chaque décision, bug corrigé, ou piège découvert. Une phrase courte + fichier concerné si utile.

- **2026-07-13** — Audit complet sécurité/qualité des 3 repos (voir `AUDIT.md`/`AUDIT_FRONT.md`, pour l'instant uniquement dans le dossier local `Datadriven_sourcing`, pas versionné). 5 actions prioritaires identifiées : JWT sur les edge functions, fermer l'accès `anon` aux tables Crunchbase/Tracxn, fermer l'ingress ECS, fiabiliser Attio/pagination pipeline, introduire des tests.
- **2026-08-06** — Piège découvert : une nouvelle table Supabase sans policy RLS bloque silencieusement tout accès front.
- **2026-08-25** — Incident : ingestion Tracxn cassée par un header row qui diffère par feuille (workaround manuel appliqué, fix propre pas encore fait) ; clé API Prefect Cloud expirée a cassé le déclenchement du pipeline depuis le front (401) — procédure de rotation documentée (3 emplacements).
- **2026-08-25** — Mesure : le mode auto du business processing ne converge pas (2520/72407 domaines sans embedding) ; le scoring `solution_fit` (1-NN) a des faiblesses identifiées (pas de seuil, espace anisotrope, k=1) — à mesurer avant de corriger.
- **~2026-09** — Feature livrée : recherche de concurrents par similarité vectorielle (`match_competitors`), ~14s/recherche (pas d'index HNSW), seuil de similarité 0.5 non calibré. Reranking Cohere et croisement avec `competitors_cg`/`competitors_by` envisagés en évolution.
- **2026-09-02** — Mise en place de ce `CLAUDE.md` partagé (dans les 3 repos) comme mémoire de projet versionnée, pour que Simon et son collègue (et leurs sessions Claude respectives) partagent le même contexte.
