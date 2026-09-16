"""Map SIGNOR records while retaining source-specific routing and evidence policies."""

from typing import Any, Iterable

import koza
import pandas as pd
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Association,
    CausalMechanismQualifierEnum,
    ChemicalAffectsGeneAssociation,
    ChemicalEntity,
    ChemicalEntityToChemicalEntityAssociation,
    ChemicalGeneInteractionAssociation,
    DirectionQualifierEnum,
    GeneAffectsChemicalAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    GeneRegulatesGeneAssociation,
    KnowledgeLevelEnum,
    MacromolecularComplex,
    NamedThing,
    PairwiseGeneToGeneInteraction,
    Protein,
)
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import INFORES_SIGNOR, build_association_knowledge_sources
from translator_ingest.util.transform_utils import entity_id

SIGNOR_SOURCES = build_association_knowledge_sources(primary=INFORES_SIGNOR)

BIOLINK_CAUSES = "biolink:causes"
BIOLINK_AFFECTS = "biolink:affects"
BIOLINK_REGULATES = "biolink:regulates"

# Source labels are kept verbatim; unsupported values must fail explicitly.
_MECHANISM_QUALIFIERS: dict[str, CausalMechanismQualifierEnum | None] = {
    "transcriptional regulation": CausalMechanismQualifierEnum.transcriptional_regulation,
    "translation regulation": CausalMechanismQualifierEnum.translational_regulation,
    # No suitable precursor qualifier is currently emitted by this ingest.
    "precursor of": None,
    "binding": CausalMechanismQualifierEnum.binding,
    "stabilization": CausalMechanismQualifierEnum.stabilization,
    "destabilization": CausalMechanismQualifierEnum.destabilization,
    "cleavage": CausalMechanismQualifierEnum.cleavage,
    "isomerization": CausalMechanismQualifierEnum.isomerization,
    "chemical inhibition": CausalMechanismQualifierEnum.inhibition,
    "chemical activation": CausalMechanismQualifierEnum.activation,
    "catalytic activity": CausalMechanismQualifierEnum.catalytic_activity,
    "small molecule catalysis": CausalMechanismQualifierEnum.catalytic_activity,
    "gtpase-activating protein": CausalMechanismQualifierEnum.gtpase_activation,
    "guanine nucleotide exchange factor": CausalMechanismQualifierEnum.guanyl_nucleotide_exchange,
    "relocalization": CausalMechanismQualifierEnum.relocalization,
    "chemical modification": CausalMechanismQualifierEnum.chemical_modification,
    "post transcriptional regulation": CausalMechanismQualifierEnum.post_transcriptional_regulation,
    "post translational modification": CausalMechanismQualifierEnum.molecular_modification,
    "phosphorylation": CausalMechanismQualifierEnum.phosphorylation,
    "dephosphorylation": CausalMechanismQualifierEnum.dephosphorylation,
    "neddylation": CausalMechanismQualifierEnum.neddylation,
    "lipidation": CausalMechanismQualifierEnum.lipidation,
    "tyrosination": CausalMechanismQualifierEnum.tyrosination,
    "carboxylation": CausalMechanismQualifierEnum.carboxylation,
    "ubiquitination": CausalMechanismQualifierEnum.ubiquitination,
    "monoubiquitination": CausalMechanismQualifierEnum.monoubiquitination,
    "polyubiquitination": CausalMechanismQualifierEnum.polyubiquitination,
    "deubiquitination": CausalMechanismQualifierEnum.deubiquitination,
    "acetylation": CausalMechanismQualifierEnum.acetylation,
    "oxidation": CausalMechanismQualifierEnum.oxidation,
    "deacetylation": CausalMechanismQualifierEnum.deacetylation,
    "glycosylation": CausalMechanismQualifierEnum.glycosylation,
    "deglycosylation": CausalMechanismQualifierEnum.deglycosylation,
    "methylation": CausalMechanismQualifierEnum.methylation,
    "demethylation": CausalMechanismQualifierEnum.demethylation,
    "trimethylation": CausalMechanismQualifierEnum.trimethylation,
    "sumoylation": CausalMechanismQualifierEnum.sumoylation,
    "desumoylation": CausalMechanismQualifierEnum.desumoylation,
    "ADP-ribosylation": CausalMechanismQualifierEnum.ADP_ribosylation,
    "palmitoylation": CausalMechanismQualifierEnum.palmitoylation,
    "hydroxylation": CausalMechanismQualifierEnum.hydroxylation,
    "s-nitrosylation": CausalMechanismQualifierEnum.s_nitrosylation,
}

# The index selects the positive (0) or negative (1) category-dependent direction.
_EFFECT_QUALIFIERS: dict[str, tuple[GeneOrGeneProductOrChemicalEntityAspectEnum, int]] = {
    "up-regulates": (GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance, 0),
    "up-regulates activity": (GeneOrGeneProductOrChemicalEntityAspectEnum.activity, 0),
    "up-regulates quantity": (GeneOrGeneProductOrChemicalEntityAspectEnum.abundance, 0),
    "up-regulates quantity by expression": (GeneOrGeneProductOrChemicalEntityAspectEnum.expression, 0),
    "up-regulates quantity by stabilization": (GeneOrGeneProductOrChemicalEntityAspectEnum.stability, 0),
    "down-regulates": (GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance, 1),
    "down-regulates activity": (GeneOrGeneProductOrChemicalEntityAspectEnum.activity, 1),
    "down-regulates quantity": (GeneOrGeneProductOrChemicalEntityAspectEnum.abundance, 1),
    "down-regulates quantity by destabilization": (GeneOrGeneProductOrChemicalEntityAspectEnum.stability, 1),
    "down-regulates quantity by repression": (GeneOrGeneProductOrChemicalEntityAspectEnum.expression, 1),
}

# Each supported regulatory route selects a primary and an optional DIRECT edge class.
# Chemical and smallmolecule categories are deliberately not interchangeable here.
_REGULATORY_ASSOCIATIONS: dict[tuple[str, str], tuple[type[Association], type[Association]]] = {
    ("protein", "protein"): (GeneRegulatesGeneAssociation, PairwiseGeneToGeneInteraction),
    ("protein", "chemical"): (GeneAffectsChemicalAssociation, ChemicalGeneInteractionAssociation),
    ("chemical", "protein"): (ChemicalAffectsGeneAssociation, ChemicalGeneInteractionAssociation),
    ("smallmolecule", "protein"): (ChemicalAffectsGeneAssociation, ChemicalGeneInteractionAssociation),
    ("smallmolecule", "chemical"): (
        ChemicalEntityToChemicalEntityAssociation,
        ChemicalEntityToChemicalEntityAssociation,
    ),
    ("smallmolecule", "smallmolecule"): (
        ChemicalEntityToChemicalEntityAssociation,
        ChemicalEntityToChemicalEntityAssociation,
    ),
}


def _resolve_association_types(record: dict[str, Any]) -> tuple[type[Association], type[Association] | None] | None:
    """Select an emitted route after validating the record's mechanism and effect.

    Complex membership is a separate, single-edge route. Unrecognized category
    combinations and recognized effects without a route are intentionally filtered.

    >>> _resolve_association_types({
    ...     "subject_category": "protein", "object_category": "complex", "EFFECT": "form complex"
    ... }) == (Association, None)
    True
    >>> _resolve_association_types({
    ...     "subject_category": "chemical", "object_category": "chemical", "EFFECT": "up-regulates"
    ... }) is None
    True
    """
    # Retain short-circuit behavior: unsupported subjects do not require object fields.
    if record["subject_category"] not in ("protein", "chemical", "smallmolecule"):
        return None
    categories = (record["subject_category"], record["object_category"])
    if categories == ("protein", "complex") and record["EFFECT"] == "form complex":
        return Association, None
    if not record["EFFECT"] or record["EFFECT"] not in _EFFECT_QUALIFIERS:
        return None
    return _REGULATORY_ASSOCIATIONS.get(categories)


def _map_causal_mechanism(mechanism: str | None) -> CausalMechanismQualifierEnum | None:
    """Map a SIGNOR mechanism, preserving empty values and unknown-value errors.

    >>> _map_causal_mechanism("binding") == CausalMechanismQualifierEnum.binding
    True
    >>> _map_causal_mechanism("precursor of") is None
    True
    """
    if not mechanism:
        return None
    if mechanism not in _MECHANISM_QUALIFIERS:
        raise NotImplementedError(f"Effect {mechanism} could not be mapped to required qualifiers.")
    return _MECHANISM_QUALIFIERS[mechanism]


def _map_effect_qualifiers(
    effect: str | None,
    directions: tuple[DirectionQualifierEnum, DirectionQualifierEnum],
) -> tuple[GeneOrGeneProductOrChemicalEntityAspectEnum | None, DirectionQualifierEnum | None]:
    """Map a SIGNOR effect using the direction pair chosen for the entity categories.

    >>> directions = (DirectionQualifierEnum.upregulated, DirectionQualifierEnum.downregulated)
    >>> aspect, direction = _map_effect_qualifiers("down-regulates quantity", directions)
    >>> aspect == GeneOrGeneProductOrChemicalEntityAspectEnum.abundance
    True
    >>> direction == DirectionQualifierEnum.downregulated
    True
    """
    if not effect or effect in ("form complex", "unknown"):
        return None, None
    if effect not in _EFFECT_QUALIFIERS:
        raise NotImplementedError(f"Effect {effect} could not be mapped to required qualifiers.")
    aspect, direction_index = _EFFECT_QUALIFIERS[effect]
    return aspect, directions[direction_index]


def _species_context(taxon: str | None) -> str | None:
    """Return a taxon CURIE, treating only "-1" and None as unknown.

    >>> _species_context("9606")
    'NCBITaxon:9606'
    >>> _species_context("-1") is None
    True
    """
    if taxon == "-1" or taxon is None:
        return None
    return "NCBITaxon:" + taxon


def _anatomical_context(record: dict[str, Any]) -> list[str] | None:
    """Split cell or tissue context, preferring any non-None cell value.

    Preserve empty entries and whitespace. Read tissue data only when cell
    data is None, retaining the existing missing-field behavior.

    >>> _anatomical_context({"CELL_DATA": "", "TISSUE_DATA": "UBERON:1"})
    ['']
    >>> _anatomical_context({"CELL_DATA": None, "TISSUE_DATA": "UBERON:1;UBERON:2"})
    ['UBERON:1', 'UBERON:2']
    """
    if record["CELL_DATA"] is not None:
        return [f"{p}" for p in record["CELL_DATA"].split(";")]
    elif record["TISSUE_DATA"] is not None:
        return [f"{p}" for p in record["TISSUE_DATA"].split(";")]
    return None


def _apply_evidence(
    associations: Iterable[Association],
    publications: list[str] | None,
    supporting_text: list[str],
    confidence_score: Any,
) -> None:
    """Attach truthy evidence values, leaving falsey values (including zero scores) unassigned."""
    for association in associations:
        if publications:
            association.publications = publications
        if supporting_text:
            association.supporting_text = supporting_text
        if confidence_score:
            association.has_confidence_score = confidence_score


def get_latest_version() -> str:
    """Return the release version of the mirrored SIGNOR dataset."""
    # SIGNOR has some issues with downloading the latest data programmatically.
    # In the short term we implemented downloading it from our own server,
    # so the data version is static. We would like to do something like following when that is fixed.
    #
    # SIGNOR doesn't provide a great way to get the version,
    # but this link serves a file named something like "Oct2025_release.txt"
    # signor_latest_release_url = "https://signor.uniroma2.it/releases/getLatestRelease.php"
    # signor_latest_response = requests.post(signor_latest_release_url)
    # signor_latest_response.raise_for_status()
    # extract the version from the file name
    # file_name = signor_latest_response.headers['Content-Disposition']
    # file_name = file_name.replace("attachment; filename=", "").replace("_release.txt",
    #
    #
    # also note that currently the file we have on the RENCI server corresponds to a date but that's the download date
    # the actual version is
    return "2026_March"


@koza.prepare_data(tag="signor_parsing")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    """Aggregate source evidence, rename fields, and filter unsupported source categories."""
    ## convert the input dataframe into pandas df format
    source_df = pd.DataFrame(data)

    ## Only select needed columns
    sele_cols = [
        "ENTITYA",
        "ENTITYB",
        "TYPEA",
        "TYPEB",
        "IDA",
        "IDB",
        "EFFECT",
        "MECHANISM",
        "TAX_ID",
        "CELL_DATA",
        "TISSUE_DATA",
        "DIRECT",
        "SCORE",
        "SENTENCE",
        "PMID",
    ]
    source_subset_df = source_df[sele_cols].drop_duplicates()

    ## include some basic quality control steps here
    ## Drop nan values
    source_subset_df = source_subset_df.dropna(subset=["ENTITYA", "ENTITYB"])

    ## Implement logic to aggregate source records into a single edge based on SPO + qualifier pair (subject_name, subject_category, object_name, object_category, MECHANISM, EFFECT, DIRECT)
    group_cols = [
        "ENTITYA",
        "ENTITYB",
        "TYPEA",
        "TYPEB",
        "IDA",
        "IDB",
        "EFFECT",
        "MECHANISM",
        "TAX_ID",
        "CELL_DATA",
        "TISSUE_DATA",
        "DIRECT",
        "SCORE",
    ]

    source_agg_df = source_subset_df.groupby(group_cols, as_index=False, dropna=False).agg(
        {"PMID": lambda x: "|".join(x.dropna().astype(str)), "SENTENCE": lambda x: "|".join(x.dropna().astype(str))}
    )

    ## rename those columns into desired format
    source_agg_df.rename(
        columns={
            "ENTITYA": "subject_name",
            "TYPEA": "subject_category",
            "ENTITYB": "object_name",
            "TYPEB": "object_category",
        },
        inplace=True,
    )

    ## replace all 'miR-34' to 'miR-34a' in two columns subject_category and object_category in the pandas dataframe
    source_agg_df["subject_name"] = source_agg_df["subject_name"].replace("miR-34", "miR-34a")
    source_agg_df["object_name"] = source_agg_df["object_name"].replace("miR-34", "miR-34a")

    ## remove those rows with category in fusion protein or stimulus from source_df for now, and expecting biolink team to add those new categories
    source_agg_df = source_agg_df[
        (source_agg_df["subject_category"].str.lower() != "fusion protein")
        & (source_agg_df["object_category"].str.lower() != "fusion protein")
    ]
    source_agg_df = source_agg_df[
        (source_agg_df["subject_category"].str.lower() != "stimulus")
        & (source_agg_df["object_category"].str.lower() != "stimulus")
    ]

    ## only drop rows missing fields required to build a valid record
    required_cols = ["subject_name", "object_name", "IDA", "IDB"]

    return source_agg_df.dropna(subset=required_cols).drop_duplicates().to_dict(orient="records")


def _transform_record(record: dict[str, Any]) -> KnowledgeGraph | None:
    """Build one record graph, preserving validation order and per-edge field placement.

    Validate mechanism and effect mappings before filtering category/effect pairs,
    preserving errors even for records that would otherwise emit no edges.
    """
    publications = [f"PMID:{p}" for p in record["PMID"].split("|")] if record["PMID"] else None
    species = _species_context(record["TAX_ID"])
    anatomy = _anatomical_context(record)
    supporting_text = [s.strip() for s in record["SENTENCE"].split("|")] if record.get("SENTENCE") else []
    confidence_score = record["SCORE"]

    # SIGNOR derives regulation from entity categories, not an Endogenous field.
    if record["subject_category"] in ("protein", "complex") and record["object_category"] in ("protein", "complex"):
        predicate = BIOLINK_REGULATES
        directions = (DirectionQualifierEnum.upregulated, DirectionQualifierEnum.downregulated)
    else:
        predicate = BIOLINK_AFFECTS
        directions = (DirectionQualifierEnum.increased, DirectionQualifierEnum.decreased)

    mechanism = _map_causal_mechanism(record["MECHANISM"])
    aspect, direction = _map_effect_qualifiers(record["EFFECT"], directions)
    association_types = _resolve_association_types(record)
    if association_types is None:
        return None
    primary_type, interaction_type = association_types

    subject: NamedThing
    target: NamedThing
    if record["subject_category"] == "protein":
        subject = Protein(id="UniProtKB:" + record["IDA"], name=record["subject_name"])
    else:
        subject = ChemicalEntity(id=record["IDA"], name=record["subject_name"])

    if record["object_category"] == "protein":
        target = Protein(id="UniProtKB:" + record["IDB"], name=record["object_name"])
    elif record["object_category"] == "complex":
        target = MacromolecularComplex(id="SIGNOR:" + record["IDB"], name=record["object_name"])
    else:
        target = ChemicalEntity(id=record["IDB"], name=record["object_name"])

    # Read DIRECT before constructing edges, but never require it for complex membership.
    is_direct = interaction_type is not None and bool(record["DIRECT"] == "YES")
    common_attributes: dict[str, Any] = {
        "subject": subject.id,
        "object": target.id,
        "sources": SIGNOR_SOURCES,
        "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
        "agent_type": AgentTypeEnum.manual_agent,
    }
    primary_attributes: dict[str, Any] = {}
    interaction_attributes: dict[str, Any] = {}

    if primary_type is Association:
        # Complex membership has neither regulation qualifiers nor species/anatomy.
        predicate = "biolink:part_of"
    elif primary_type is ChemicalEntityToChemicalEntityAssociation:
        # Chemical-pair edges omit regulation qualifiers; only the primary has species.
        primary_attributes["species_context_qualifier"] = species
    else:
        primary_attributes = {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_aspect_qualifier": aspect,
            "object_direction_qualifier": direction,
            "species_context_qualifier": species,
        }
        interaction_attributes = {
            **primary_attributes,
            "causal_mechanism_qualifier": mechanism,
        }
        # Gene-regulation primaries omit mechanism and anatomy. Chemical/gene
        # primaries include both, while their physical-interaction edges omit anatomy.
        if primary_type is not GeneRegulatesGeneAssociation:
            primary_attributes.update(
                causal_mechanism_qualifier=mechanism,
                anatomical_context_qualifier=anatomy,
            )

    edges = [
        primary_type(
            id=entity_id(),
            predicate=predicate,
            **common_attributes,
            **primary_attributes,
        )
    ]
    if is_direct and interaction_type is not None:
        edges.append(
            interaction_type(
                id=entity_id(),
                predicate="biolink:directly_physically_interacts_with",
                **common_attributes,
                **interaction_attributes,
            )
        )

    _apply_evidence(edges, publications, supporting_text, confidence_score)
    return KnowledgeGraph(nodes=[subject, target], edges=edges)


@koza.transform(tag="signor_parsing")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    """Combine record graphs in input order, retaining duplicate nodes and an empty batch graph."""
    nodes: list[NamedThing] = []
    edges: list[Association] = []

    for record in data:
        graph = _transform_record(record)
        if graph is not None:
            nodes.extend(graph.nodes)
            edges.extend(graph.edges)

    return [KnowledgeGraph(nodes=nodes, edges=edges)]
