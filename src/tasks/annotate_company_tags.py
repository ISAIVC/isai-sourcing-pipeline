"""
This task annotates the companies with AI for columns "gtm_target" "gtm_target_by"
"business_model" "all_industries_served_sorted" "tech_tags_dynamic" "scope"
"primary_sector_served_cg" "primary_industry_served_cg" "primary_sector_served_by"
"primary_industry_served_by".
 
Scope (CG/BY/BOTH) is derived from the industries served, each with a Gemini-provided
confidence score (0-100), via a confidence-weighted signed ratio in
deduced_industry_tags() -- see SCOPE_THRESHOLD and that function's docstring below for
the detail. Calibrated on a 189-company blind test against known Attio ground truth,
see claude/calibration-seuil-formule.md in the ISAI-sourcing-fix Cowork project for the
full methodology and numbers. Single Gemini call, same cadence as before -- no change
to prefect.yaml.
"""
 
from typing import Literal, Optional
 
from prefect import task
from pydantic import BaseModel, Field, create_model
 
from src.config.clients import get_qa_model, get_supabase_client
from src.utils.db import fetch_in_batches, keep_latest_per_domain, upsert_in_batches
from src.utils.logger import get_logger
from src.utils.qa_model import ModelName, Question
 
# NOUVEAU -- seuil retenu après calibration sur l'échantillon de test aveugle (189
# sociétés, vérité Attio). Fourchette validée : 10-15. Isolé en constante pour pouvoir
# le retoucher facilement sans replonger dans deduced_industry_tags(). Voir
# claude/calibration-seuil-formule.md pour le détail du raisonnement.
SCOPE_THRESHOLD = 12
 
SYSTEM_PROMPT = """
**Role:**
You are a Senior Investment Analyst at ISAI, a leading entrepreneur-focused venture capital firm. Your expertise lies in deep-dive company analysis for two specific strategic funds:
1. **ISAI Cap Venture:** Focused on B2B software and services that align with Capgemini's ecosystem (Enterprise AI, Cloud, Digital Transformation, Sustainability).
2. **ISAI Build Venture:** Focused on "ConTech" and "PropTech" in partnership with Bouygues (Sustainable construction, smart cities, energy efficiency, infrastructure).
 
**Objective:**
Your task is to define tags for a company based on the provided information
 
**Input Context:**
You will be provided with an detailed description of a company.
 
**Analysis Guidelines:**
1. **Industries served:** The most important part of this analysis are the industries served by the provided company.
   - List industries in order of relevance, from the industry most central to the company's core business, to the least central.
   - Only include an industry if the company's product or solution is genuinely built for or focused on that industry. A company that could theoretically be used by many industries, but is not built for one specifically, is NOT a match for those industries.
   - For industries describing a physical activity or a specific market vertical (energy production, grid, data centers, public works, quarries & carriers, real estate), only match the BY physical category if the company itself performs that physical activity -- not if it merely sells software, hardware, or data to companies that do; route those instead to the matching CG market category (e.g. Renewable Energy for energy-market software, Grid for grid-market software) or Software & Technology providers if no dedicated CG counterpart exists. Building construction is the exception to this rule -- read its own description carefully, since it covers the construction/AEC industry as a target market, not only physical builders.
   - If the company sells a horizontal product usable across most industries with no specific industry focus, select "Horizontal / Cross-industry" instead of forcing a specific industry match. NOTE : cette catégorie n'existe pas encore dans le référentiel `industries` (aucune ligne NEUTRAL/Horizontal en base) -- cette consigne restera sans effet réel tant qu'elle n'est pas ajoutée par ailleurs (suivi dans un autre ticket). Ne pas retirer cette ligne du prompt : elle deviendra utile dès que la catégorie sera créée.
   - Do not pad the list to reach the maximum. A company with one clear industry focus should return one industry, not several loosely related ones.
   - For each matched industry, give a confidence score from 0 to 100: 90-100 = core/primary focus, unambiguous; 60-89 = real, significant use case but not the sole focus; 30-59 = plausible but secondary or tangential use case; below 30 = do not include the industry at all.
2. **The GTM Target:** The ambition is to identify the nature of the targetted clients. We want a first generic labellisation (gtm_target) but also a more refined labelisation for Build Venture (gtm_target_by). It's possible that this refined labellisation is not relevant, in that case leave it empty.
3. **Business Model:** Identify the business model of the company among the provided list
4. **Business Map:** Associate a business from the list if relevant, else set to None
5. **Technology tags:** For the tech tags there is no specific list, you are free to put tags but keep it concise, accurate and relevant for VC sourcing!
6. **No Hallucinations:** Do not hallucinate any information, only use the information provided in the input context
 
**Output Format:**
Return only a valid JSON object following the `CompanyTags` schema provided.
 
### Fields and Tags Descriptions:
"""
# MODIFIÉ ci-dessus :
#   - ligne 2 du Role : "VINCI" -> "Bouygues" (coquille trouvée dans le prompt de prod
#     actuel, indépendante du reste -- à confirmer volontaire ou non avant d'appliquer).
#   - point 1 des Analysis Guidelines : passé de 1 ligne générique à la consigne
#     détaillée validée sur 189 sociétés en test aveugle (règle physique vs vendeur,
#     exception Building construction, rubrique de confiance 0-100).
# Le référentiel des industries lui-même (47 lignes, descriptions par industrie)
# N'EST PAS dans cette chaîne -- il est injecté dynamiquement plus bas via
# `industry_tags_description`, exactement comme aujourd'hui.
# ÉTAT SUPABASE (à jour au 8 septembre 2026, déjà appliqué -- rien à refaire ici) :
#   - Table `industries` mise à jour : descriptions corrigées sur Building construction,
#     Public works, Real estate, Quarries & Carriers, Roads, Grid, Power Generation,
#     Renewable Energy, Automotive, Industrial Equipment, Aerospace & Defense,
#     Retail Banking, Asset Management, Capital Markets, Pharma/Life sciences,
#     Software & Technology providers.
#   - 4 nouvelles lignes ajoutées : Insurance / Reinsurance (CG), Cybersecurity (CG),
#     Healthcare Payor (CG), Industrial / Multi-technical maintenance (BY).
#   - Détail complet : claude/modifications-referentiel-industries-appliquees.md
#     (projet Cowork "ISAI-sourcing-fix").
#   - Catégorie NEUTRAL / "Horizontal / Cross-industry" : PAS créée (hors périmètre de
#     ce lot, suivie dans le ticket de calibration séparé). Voir note dans le prompt
#     ci-dessus, point 1.
 
 
# NOUVEAU -- objet imbriqué pour porter la confiance par industrie. Nécessaire pour
# que deduced_industry_tags() puisse faire la moyenne pondérée (avant,
# sorted_industries_served était une simple liste de strings, sans aucune confiance).
class IndustryMatch(BaseModel):
    industry: Literal["fake_industry", "fake_industry_2"]
    confidence: int = Field(
        ...,
        ge=0,
        le=100,
        description=(
            "Confidence in this industry match, 0-100. 90-100: core/primary focus. "
            "60-89: real but secondary use case. 30-59: tangential. Below 30: do not include."
        ),
    )
 
 
class CompanyTags(BaseModel):
    sorted_industries_served: list[IndustryMatch] = Field(  # MODIFIÉ : list[Literal[...]] -> list[IndustryMatch]
        ...,
        description="All the industries served by the company among the list, each with a confidence score. Sorted from most relevant to least relevant.",
        max_length=4,
        min_length=1,
    )
    small_explanation_of_industries_sorting: str = Field(
        ...,
        description="A small explanation of why you sorted the industries the way you did",
    )
    gtm_target: Literal["fake_gtm_target", "fake_gtm_target_2"] = Field(
        ..., description="The go to market target (the type of clients) of the company"
    )
    # Optional pydantic field
    gtm_target_by: Optional[Literal["fake_gtm_target", "fake_gtm_target_2"]] = Field(
        None,
        description="The go to market target (the type of clients) of the company but among a really specific client typology, if not relevant set to None",
    )
    business_model: Literal["fake_bm", "fake_bm_2"] = Field(
        ..., description="The business model of the company"
    )
    business_map: Optional[Literal["fake_business", "fake_business_2"]] = Field(
        None,
        description="Associate a business from the list if relevant, else set to None",
    )
    tech_tags: list[str] = Field(..., description="Technology tags for this company")
 
 
def build_model_from_schema(schema: dict, model_name: str = None, defs: dict = None):
    model_name = model_name or schema.get("title", "DynamicModel")
    defs = schema.get("$defs", {}) if defs is None else defs  # NOUVEAU
    properties = schema.get("properties", {})
    required_fields = set(schema.get("required", []))
    fields = {}
 
    for field_name, field_schema in properties.items():
        field_type, field_default = _resolve_field(
            field_schema, field_name, field_name in required_fields, defs  # MODIFIÉ : + defs
        )
        description = field_schema.get("description", "")
        fields[field_name] = (
            field_type,
            Field(default=field_default, description=description),
        )
 
    return create_model(model_name, **fields)
 
 
def _resolve_field(field_schema: dict, field_name: str, is_required: bool, defs: dict):  # MODIFIÉ : + defs
    # NOUVEAU -- Handle $ref (nested object, e.g. IndustryMatch). Sans ce bloc,
    # list[IndustryMatch] casse : pydantic génère un schema avec
    # items = {"$ref": "#/$defs/IndustryMatch"} au lieu d'un schema inline, et
    # l'ancien code ne savait pas le résoudre.
    if "$ref" in field_schema:
        ref_name = field_schema["$ref"].split("/")[-1]
        nested_schema = defs[ref_name]
        nested_model = build_model_from_schema(nested_schema, ref_name, defs)
        return nested_model, ... if is_required else None
 
    # Handle anyOf (Optional types)
    if "anyOf" in field_schema:
        types = field_schema["anyOf"]
        non_null = [t for t in types if t.get("type") != "null"]
        has_null = any(t.get("type") == "null" for t in types)
 
        if non_null:
            inner_type, _ = _resolve_field(non_null[0], field_name, True, defs)  # MODIFIÉ : + defs
            if has_null:
                final_type = Optional[inner_type]
                default = field_schema.get("default", None)
                return final_type, default
            return inner_type, ... if is_required else None
 
    # Handle enum at top level (overrides anyOf enum if present)
    top_enum = field_schema.get("enum")
 
    # Handle string with enum -> Literal
    if field_schema.get("type") == "string" and (top_enum or field_schema.get("enum")):
        enum_values = top_enum or field_schema.get("enum")
        literal_type = Literal[tuple(enum_values)]
        return literal_type, ... if is_required else None
 
    # Handle array
    if field_schema.get("type") == "array":
        items = field_schema.get("items", {})
        item_type, _ = _resolve_field(items, field_name, True, defs)  # MODIFIÉ : + defs
        return list[item_type], ... if is_required else None
 
    # Handle basic types
    type_map = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "object": dict,
    }
 
    base_type = type_map.get(field_schema.get("type"), str)
 
    # String with enum
    if base_type is str and top_enum:
        return Literal[tuple(top_enum)], ... if is_required else None
 
    return base_type, ... if is_required else None
 
 
def build_response_model_dynamically() -> tuple[BaseModel, dict, dict, str]:
    client = get_supabase_client()
 
    # Industries parsing
    industries_data = client.table("industries").select("*").execute().data
    industry_tags_description = "### Industries:\n"
    for industry in industries_data:
        industry_tags_description += (
            f"{industry['industry']}: {industry['description']}\n"
        )
    industry_tags_description = industry_tags_description.strip()
    industry_to_scope_mapping = {r["industry"]: r["scope"] for r in industries_data}
    industry_to_sector_mapping = {r["industry"]: r["sector"] for r in industries_data}
 
    # GTM parsing
    gtm_data = (
        client.table("gtm_target").select("*").in_("scope", ["ALL"]).execute().data
    )
    gtm_tags_description = "### GTM Target:\n"
    for gtm in gtm_data:
        gtm_tags_description += f"{gtm['target']}: {gtm['description']}\n"
    gtm_tags_description = gtm_tags_description.strip()
 
    # GTM BY parsing
    gtm_by_data = (
        client.table("gtm_target").select("*").in_("scope", ["BY"]).execute().data
    )
    gtm_by_tags_description = "### GTM Target (Build Venture specific):\n"
    for gtm_by in gtm_by_data:
        gtm_by_tags_description += f"{gtm_by['target']}: {gtm_by['description']}\n"
    gtm_by_tags_description = gtm_by_tags_description.strip()
 
    # Business model parsing
    business_model_data = client.table("business_models").select("*").execute().data
    business_model_tags_description = "### Business Models:\n"
    for business_model in business_model_data:
        business_model_tags_description += (
            f"{business_model['name']}: {business_model['description']}\n"
        )
    business_model_tags_description = business_model_tags_description.strip()
 
    # Business map parsing
    business_map_data = client.table("business_mapping").select("*").execute().data
    business_map_tags_description = "### Business Maps:\n"
    for business_map in business_map_data:
        business_map_tags_description += (
            f"{business_map['name']}: {business_map['description']}\n"
        )
    business_map_tags_description = business_map_tags_description.strip()
 
    final_description = " ".join(
        [
            industry_tags_description,
            gtm_tags_description,
            gtm_by_tags_description,
            business_model_tags_description,
            business_map_tags_description,
        ]
    )
 
    pydantic_model = CompanyTags.model_json_schema()
    # MODIFIÉ -- l'enum des industries se pose maintenant sur le $defs.IndustryMatch.industry
    # imbriqué, plus sur properties.sorted_industries_served.items (qui est un $ref depuis
    # que sorted_industries_served est devenu list[IndustryMatch]).
    pydantic_model["$defs"]["IndustryMatch"]["properties"]["industry"]["enum"] = [
        industry["industry"] for industry in industries_data
    ]
    pydantic_model["properties"]["gtm_target"]["enum"] = [
        gtm["target"] for gtm in gtm_data
    ]
    pydantic_model["properties"]["gtm_target_by"]["anyOf"][0]["enum"] = [
        gtm_by["target"] for gtm_by in gtm_by_data
    ]
    pydantic_model["properties"]["business_model"]["enum"] = [
        business_model["name"] for business_model in business_model_data
    ]
    pydantic_model["properties"]["business_map"]["anyOf"][0]["enum"] = [
        business_map["name"] for business_map in business_map_data
    ]
 
    return (
        build_model_from_schema(pydantic_model, "CompanyTagsResponse"),
        industry_to_scope_mapping,
        industry_to_sector_mapping,
        final_description,
    )
 
 
def build_company_description_from_web_enrichment(record: dict) -> str:
    description = ""
    for dim, content in [
        ("Description", record["description"]),
        ("Detailed Solution", record["detailed_solution"]),
        ("Key Features", record["key_features"]),
        ("Use Cases", record["use_cases"]),
        ("Tech Description", record["tech_description"]),
        ("Industries Served", record["industries_served_description"]),
        ("Key Clients", record["key_clients"]),
        ("Key Partners", record["key_partners"]),
    ]:
        if content is not None:
            description += f"###{dim}\n{str(content)}\n\n"
    return description
 
 
def retrieve_companies_web_enrichement(domains: list[str]) -> list[dict]:
    records = fetch_in_batches(
        get_supabase_client(), "web_scraping_enrichment", "domain", domains
    )
    records = keep_latest_per_domain(records)
    # Drop all the record with empty description
    records = [record for record in records if record["description"] is not None]
    return records
 
 
def deduced_industry_tags(  # MODIFIÉ -- même nom qu'avant, signature étendue avec `confidences`
    listed_industries: list[str],
    confidences: list[int],
    industry_to_scope_mapping: dict,
    industry_to_sector_mapping: dict,
    threshold: float = SCOPE_THRESHOLD,
) -> dict:
    """
    Détermine le scope et les secteurs de la société à partir des industries servies
    ET de leur confiance -- remplace l'ancienne logique (ET logique strict sur les 2
    premières industries, qui diluait un match BY précis noyé par un match CG générique,
    et forçait BOTH sur une liste vide).
 
    scope : ratio signé pondéré par confiance sur TOUTES les industries CG/BY listées
            (les matches NEUTRAL sont ignorés du calcul de ratio, comme aujourd'hui) :
              ratio = 100 * sum(confidence si CG sinon -confidence) / sum(confidence)
            > +threshold -> CG, < -threshold -> BY, sinon BOTH.
            None si aucune industrie CG/BY n'a été matchée (liste vide ou 100% NEUTRAL) --
            avant, ce cas tombait dans le `else` et produisait BOTH à tort. En pratique,
            tant qu'aucune catégorie NEUTRAL n'existe dans `industries` (voir note en
            tête de fichier), ce cas restera rare/quasi jamais déclenché puisque
            min_length=1 force toujours au moins une industrie CG ou BY.
    primary_sector_served_cg / primary_industry_served_cg / *_by : logique inchangée
    par rapport à l'original (indépendante du scope lui-même).
 
    NOTE -- le modèle Pydantic dynamique généré par build_model_from_schema() ne
    reprend PAS les validators de CompanyTags (il est reconstruit depuis le JSON
    schema via create_model, sans les méthodes/validators de la classe d'origine).
    Le garde-fou de cohérence de longueur listed_industries/confidences doit donc
    être fait ici en Python plutôt que via un validator Pydantic -- fallback
    défensif ci-dessous plutôt qu'un crash, pour ne pas perdre tout le batch sur
    une réponse Gemini malformée.
    """
    if len(confidences) != len(listed_industries):  # NOUVEAU -- garde-fou défensif
        logger = get_logger()
        logger.warning(
            "Mismatched industries/confidences length "
            f"({listed_industries} / {confidences}) -- padding with confidence 50."
        )
        confidences = (list(confidences) + [50] * len(listed_industries))[: len(listed_industries)]
 
    cg_by_pairs = [
        (industry, conf)
        for industry, conf in zip(listed_industries, confidences)
        if industry_to_scope_mapping.get(industry) in ("CG", "BY")
    ]
 
    if not cg_by_pairs:
        scope = None  # MODIFIÉ -- avant : tombait dans le else -> "BOTH" à tort
    else:
        signed_total = sum(
            conf if industry_to_scope_mapping[industry] == "CG" else -conf
            for industry, conf in cg_by_pairs
        )
        abs_total = sum(conf for _, conf in cg_by_pairs)
        ratio = 100 * signed_total / abs_total if abs_total else 0
        if ratio > threshold:
            scope = "CG"
        elif ratio < -threshold:
            scope = "BY"
        else:
            scope = "BOTH"
 
    # --- logique inchangée par rapport à l'original ---
    first_cg_industry = None
    first_by_industry = None
 
    for industry in listed_industries:
        if industry_to_scope_mapping.get(industry) == "CG" and first_cg_industry is None:
            first_cg_industry = industry
        if industry_to_scope_mapping.get(industry) == "BY" and first_by_industry is None:
            first_by_industry = industry
        if first_cg_industry and first_by_industry:
            break
 
    all_cg_industries = [
        industry
        for industry in listed_industries
        if industry_to_scope_mapping.get(industry) == "CG"
    ]
    all_by_industries = [
        industry
        for industry in listed_industries
        if industry_to_scope_mapping.get(industry) == "BY"
    ]
 
    all_cg_sectors = [
        industry_to_sector_mapping[industry] for industry in all_cg_industries
    ]
    all_by_sectors = [
        industry_to_sector_mapping[industry] for industry in all_by_industries
    ]
 
    unique_cg_sectors = set(all_cg_sectors)
    unique_by_sectors = set(all_by_sectors)
 
    if len(unique_cg_sectors) == 1:
        primary_sector_served_cg = list(unique_cg_sectors)[0]
    else:
        primary_sector_served_cg = "cross_sector"
    if len(unique_by_sectors) == 1:
        primary_sector_served_by = list(unique_by_sectors)[0]
    else:
        primary_sector_served_by = "cross_sector"
 
    return {
        "scope": scope,
        "primary_sector_served_cg": primary_sector_served_cg,
        "primary_industry_served_cg": first_cg_industry,
        "primary_sector_served_by": primary_sector_served_by,
        "primary_industry_served_by": first_by_industry,
    }
 
 
def build_upsert_record(
    source_records: list[dict],
    answers: list[CompanyTags],
    industry_to_scope_mapping: dict,
    industry_to_sector_mapping: dict,
) -> list[dict]:
    upsert_records = []
    for source_record, answer in zip(source_records, answers):
        if answer is None:
            continue
        # MODIFIÉ -- answer.sorted_industries_served est maintenant une liste
        # d'objets IndustryMatch (industry + confidence), plus une liste de strings.
        industry_names = [m.industry for m in answer.sorted_industries_served]
        industry_confidences = [m.confidence for m in answer.sorted_industries_served]
        industry_tags = deduced_industry_tags(  # MODIFIÉ -- appel avec `confidences` en plus
            industry_names,
            industry_confidences,
            industry_to_scope_mapping,
            industry_to_sector_mapping,
        )
        upsert_records.append(
            {
                "domain": source_record["domain"],
                "all_industries_served_sorted": industry_names,  # inchangé : liste de strings côté DB
                "gtm_target": answer.gtm_target,
                "gtm_target_by": answer.gtm_target_by,
                "business_model": answer.business_model,
                "tech_tags_dynamic": answer.tech_tags,
                "scope": industry_tags["scope"],
                "primary_sector_served_cg": industry_tags["primary_sector_served_cg"],
                "primary_industry_served_cg": industry_tags[
                    "primary_industry_served_cg"
                ],
                "primary_sector_served_by": industry_tags["primary_sector_served_by"],
                "primary_industry_served_by": industry_tags[
                    "primary_industry_served_by"
                ],
                "business_mapping": answer.business_map,
            }
        )
 
    return upsert_records
 
 
def call_qa_by_batches(
    questions: list[Question], model_name: ModelName, batch_size: int = 50
):
    answers = []
    qa_model = get_qa_model()
    logger = get_logger()
    logger.info(f"Calling QA model for {len(questions)} domains")
    nb_batches = (len(questions) // batch_size) + 1 * (len(questions) % batch_size != 0)
    for i in range(0, len(questions), batch_size):
        logger.info(f"Processing batch {i // batch_size + 1}/{nb_batches}")
        batch = questions[i : i + batch_size]
        answers.extend(qa_model(batch, model_name=model_name))
    qa_model.log_cost(logger)
 
    return answers
 
 
@task(name="annotate_company_tags")  # inchangé -- même tâche Prefect, même déclaration,
def annotate_company_tags(domains: list[str]):  # donc même cadence dans prefect.yaml
    logger = get_logger()
    logger.info(f"Starting annotate company tags for {len(domains)} domains")
    (
        pydantic_model,
        industry_to_scope_mapping,
        industry_to_sector_mapping,
        final_description,
    ) = build_response_model_dynamically()
    records = retrieve_companies_web_enrichement(domains)
    logger.info(f"Retrieved {len(records)} records from web enrichment")
    system_prompt = SYSTEM_PROMPT + final_description
    requests = []
    for record in records:
        description = build_company_description_from_web_enrichment(record)
 
        question = Question(
            text_content=description,
            pydantic_model=pydantic_model,
            system_prompt=system_prompt,
            question="Please provide the tags for this company",
        )
        requests.append(question)
    logger.info("Calling model for tagging")
    responses = call_qa_by_batches(requests, ModelName.GEMINI_3_FLASH_PREVIEW)
    logger.info(f"Annotated {len(responses)} companies")
    records_to_upsert = build_upsert_record(
        records, responses, industry_to_scope_mapping, industry_to_sector_mapping
    )
 
    upsert_in_batches(
        get_supabase_client(),
        "business_computed_values",
        records_to_upsert,
        on_conflict="domain",
        logger=logger,
    )
 
 
# =============================================================================
# TODO -- à faire / vérifier avant un déploiement large :
# =============================================================================
#
# 1. TEST SUR VRAI GEMINI (pas seulement Claude) -- tout le test aveugle (89,9-96,6%
#    selon la lecture) a tourné sur Claude simulant Gemini, jamais sur le vrai
#    `GEMINI_3_FLASH_PREVIEW` de prod, et jamais sur ce schéma imbriqué ($ref/$defs)
#    précisément. Recommandé : un tout petit batch (5-10 sociétés) avant le batch
#    d'août en entier, pour vérifier que l'appel structured output passe sans erreur.
#
# 2. IMPACT DU scope=None -- avant, l'ancienne logique ne produisait jamais None
#    (toujours CG/BY/BOTH). Maintenant, une société sans aucun match CG/BY produirait
#    scope=None -- mais tant que la catégorie NEUTRAL n'existe pas dans `industries`
#    (voir note dans SYSTEM_PROMPT), ce cas restera rare voire inexistant en pratique,
#    puisque min_length=1 force toujours au moins une industrie CG ou BY. À revérifier
#    le jour où NEUTRAL sera ajouté : `business_processing.py` utilise `scope` comme
#    colonne représentative pour décider si une société a déjà été traitée
#    (`TASK_REPRESENTATIVE_COLUMNS["annotate_company_tags"] = "scope"` ->
#    `_get_fresh_complete_domains()` exclut les domaines où scope n'est pas NULL) --
#    un scope=None légitime y serait alors perçu comme "jamais traité" et retraité en
#    boucle en mode auto. Pas bloquant pour un test manuel sur le batch d'août.
#
# 3. CONFIANCE PAR INDUSTRIE NON PERSISTÉE -- elle sert à calculer `scope` dans
#    deduced_industry_tags() mais n'est stockée nulle part dans business_computed_values
#    (seul `all_industries_served_sorted`, liste de noms, part en base). À décider si
#    utile de la garder (nouvelle colonne) ou si le calcul en aval suffit.
#
# 4. CHOIX D'ARCHITECTURE POUR LA CONFIANCE -- ce fichier implémente l'option
#    "objet imbriqué" (list[IndustryMatch], confiance garantie appariée à
#    l'industrie par le schema JSON lui-même). Alternative plus rapide à coder
#    mais moins sûre : deux listes parallèles (sorted_industries_served: list[str]
#    inchangé + un nouveau champ industries_confidence: list[int]) qui évite de
#    toucher build_model_from_schema()/_resolve_field(), mais qui repose sur un
#    appariement positionnel entre les deux listes que le schema JSON ne garantit
#    pas -- à trancher avec l'équipe selon le temps dispo pour tester.
#
# 5. Coquille VINCI -> Bouygues (ligne du Role) : déjà corrigée dans ce fichier ;
#    à confirmer avec l'équipe que c'était bien une coquille et pas volontaire,
#    par acquis de conscience, avant de merger.
# =============================================================================