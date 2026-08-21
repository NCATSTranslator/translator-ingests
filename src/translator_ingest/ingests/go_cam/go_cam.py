import json
import re
import tarfile
import tempfile
from collections import Counter
from pathlib import Path
from typing import Optional, Any, Iterable

import koza

from translator_ingest.ingests.go_cam.ro_predicate_mapping import map_causal_predicate
from translator_ingest.util.http_utils import get_geneontology_release_version
from translator_ingest.util.transform_utils import entity_id
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Gene,
    KnowledgeLevelEnum,
    GeneToGeneAssociation,
    RetrievalSource,
    ResourceRoleEnum,
)
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.logging_utils import get_logger


# Constants
INFORES_GO_CAM = "infores:go-cam"
INFORES_REACTOME = "infores:reactome"

# GO-CAM packs several references into one string with a pipe, e.g.
# "MGI:MGI:4834177 | GO_REF:0000096", so every raw value is split before parsing.
REFERENCE_SEPARATOR = "|"

# Accepts the PMID spellings actually present in the source: stray leading/trailing
# whitespace and tabs, lowercase "pmid", and runs of spaces after the colon.
PMID_PATTERN = re.compile(r"^PMID\s*:\s*(\d+)$", re.IGNORECASE)

# A reference that is nothing but digits is a bare PubMed id (e.g. "34782749").
BARE_PMID_PATTERN = re.compile(r"^\d+$")

# Accepts "GO_REF", plus the "GOREF" and "GO:REF" misspellings found in the source.
GO_REF_PATTERN = re.compile(r"^GO[_:]?REF\s*:\s*(\d+)$", re.IGNORECASE)

# Contributors are mostly ORCID URLs, but GO-CAM also uses opaque curator-group ids
# such as "GOC:reactome_curators", which are not publications.
ORCID_PATTERN = re.compile(r"^https?://orcid\.org/(\d{4}-\d{4}-\d{4}-\d{3}[\dX])$", re.IGNORECASE)

# ECO CURIEs are exactly seven digits; anything else in an "assessed_by" field is junk.
ECO_PATTERN = re.compile(r"^ECO:\d{7}$")

# The ECO codes in this release that sit in ECO's "evidence used in automatic assertion"
# branch (ECO:0000501). They are the only signal in the data separating bulk-imported
# content from curator-authored content, and in practice they mark the Reactome-derived
# models. Every other code in the data is in the manual-assertion branch (ECO:0000352).
AUTOMATIC_ASSERTION_ECO_TERMS = frozenset({
    "ECO:0000313",  # imported information used in automatic assertion
    "ECO:0000363",  # computational inference used in automatic assertion
    "ECO:0000501",  # evidence used in automatic assertion
})

# Reactome-derived edges cite a Reactome pathway record as their reference. Those are
# not literature, so they belong on the retrieval source rather than in publications.
REACTOME_REFERENCE_PATTERN = re.compile(r"^Reactome:(R-[A-Z]{3}-\d+(?:\.\d+)?)$", re.IGNORECASE)
REACTOME_RECORD_URL = "https://reactome.org/content/detail/{}"

# The addressable GO-CAM record for a model, as documented in the RIG's
# data_access_locations. model_info.id is prefixed ("gomodel:0000000300000001").
GO_CAM_RECORD_URL = "https://live-go-cam.geneontology.io/product/json/low-level/{}.json"

logger = get_logger(__name__)


def get_latest_version() -> str:
    """Fetch the current GO release version from the public metadata endpoint."""
    return get_geneontology_release_version()


def extract_value(value) -> Optional[str]:
    """Extract a single value from either a string or a list containing one string."""
    if not value:
        return None
    if isinstance(value, list):
        return value[0] if value else None
    return value


def normalize_id(node_id: str) -> str:
    """
    Remove duplicate prefixes from node IDs
    (e.g., MGI:MGI:1921700 -> MGI:1921700) and convert URIs to CURIEs.
    """
    # Handle REACTO URIs - convert to Biolink-compliant reactome: CURIEs
    if node_id.startswith("obo:go/extensions/reacto.owl#REACTO_"):
        # Convert obo:go/extensions/reacto.owl#REACTO_R-HSA-12345 to reactome:R-HSA-12345
        reacto_id = node_id.split("#REACTO_")[-1]
        return f"reactome:{reacto_id}"

    # Handle other OBO URIs if present
    if node_id.startswith("obo:"):
        # Handle OBO URIs with '#' delimiter
        if "#" in node_id:
            parts = node_id.split("#")
            if len(parts) == 2:
                # Extract the ID part after #
                return parts[1]
        else:
            # Handle OBO URIs without '#' delimiter, e.g., obo:GO:12345
            parts = node_id.split(":")
            if len(parts) == 3:
                # Extract the CURIE part after 'obo:'
                return f"{parts[1]}:{parts[2]}"

    # Handle http URIs
    if node_id.startswith("http://identifiers.org/"):
        # Convert http://identifiers.org/PomBase:SPCC1183.03c to PomBase:SPCC1183.03c
        return node_id.replace("http://identifiers.org/", "")

    if node_id.startswith("http://www.ebi.ac.uk/"):
        # Extract ID from EBI URIs
        # e.g., http://www.ebi.ac.uk/intact/complex/details/EBI-767671 to ComplexPortal:EBI-767671
        if "/intact/complex/details/" in node_id:
            complex_id = node_id.split("/")[-1]
            return f"ComplexPortal:{complex_id}"

    # Handle duplicate prefixes (original functionality)
    if ":" in node_id:
        # Split on the first colon to get prefix and remainder
        parts = node_id.split(":", 1)
        if len(parts) == 2:
            prefix, remainder = parts

            # Check if remainder starts with the same prefix followed by colon
            duplicate_prefix = f"{prefix}:"
            if remainder.startswith(duplicate_prefix):
                # Remove the duplicate prefix
                return f"{prefix}:{remainder[len(duplicate_prefix):]}"

    return node_id


def as_list(value: Any) -> list[str]:
    """
    Coerce a GO-CAM field that may be a bare string, a list, or absent into a list.

    Duplicates are dropped while preserving order: the source repeats a value once per
    underlying evidence instance, so ``["ECO:0000318", "ECO:0000318"]`` is one code
    asserted twice, not two codes.

    >>> as_list(None)
    []
    >>> as_list("PMID:12345")
    ['PMID:12345']
    >>> as_list(["ECO:0000318", "ECO:0000318"])
    ['ECO:0000318']
    """
    if not value:
        return []
    values = value if isinstance(value, list) else [value]
    return list(dict.fromkeys(v for v in values if isinstance(v, str)))


def normalize_reference(reference: str) -> Optional[str]:
    """
    Normalize one GO-CAM reference token to a PMID or GO_REF CURIE.

    GO-CAM reference strings are hand-entered and inconsistent, so matching on a literal
    ``"PMID:"`` prefix silently discards a large share of real references. This repairs
    the spellings observed in the source and returns ``None`` for anything that is not a
    publication, leaving the caller to count and report the drops.

    Well-formed values pass through untouched:

    >>> normalize_reference("PMID:12345678")
    'PMID:12345678'
    >>> normalize_reference("GO_REF:0000024")
    'GO_REF:0000024'

    Whitespace, tabs, case, and spacing after the colon are all repaired:

    >>> normalize_reference(" PMID:33331896")
    'PMID:33331896'
    >>> normalize_reference("PMID:18224415\t")
    'PMID:18224415'
    >>> normalize_reference("PMID:     24755855")
    'PMID:24755855'
    >>> normalize_reference("pmid:26030875")
    'PMID:26030875'

    A bare number is a PubMed id that lost its prefix:

    >>> normalize_reference("34782749")
    'PMID:34782749'

    The two GO_REF misspellings present in the source are accepted:

    >>> normalize_reference("GOREF:0000033")
    'GO_REF:0000033'
    >>> normalize_reference("GO:REF:0000008")
    'GO_REF:0000008'

    Everything else is not a publication and is dropped. Reactome values are pathway
    records rather than literature, ``PAINT_REF`` is a separate namespace from
    ``GO_REF``, and ISBN and MGI reference ids have no Biolink publication CURIE here:

    >>> normalize_reference("Reactome:R-HSA-201451") is None
    True
    >>> normalize_reference("PAINT_REF:12107") is None
    True
    >>> normalize_reference("ISBN:0-87901-047-9") is None
    True
    >>> normalize_reference("MGI:MGI:4417868") is None
    True
    """
    token = reference.strip()
    if BARE_PMID_PATTERN.match(token):
        return f"PMID:{token}"
    pmid_match = PMID_PATTERN.match(token)
    if pmid_match:
        return f"PMID:{pmid_match.group(1)}"
    go_ref_match = GO_REF_PATTERN.match(token)
    if go_ref_match:
        return f"GO_REF:{go_ref_match.group(1)}"
    return None


def extract_references(raw_references: Any) -> tuple[list[str], list[str]]:
    """
    Turn a GO-CAM ``*_has_reference`` field into publication CURIEs plus the rejects.

    Some references pack several identifiers into one pipe-delimited string, so each
    value is split before normalization - otherwise the embedded PMID in
    ``"MGI:MGI:5005039 | PMID:21459323"`` is lost along with the MGI id.

    Returns ``(publications, unrecognized)`` so the caller can report what it dropped
    instead of discarding it silently.

    >>> extract_references(["PMID:12345678", "GO_REF:0000024"])
    (['PMID:12345678', 'GO_REF:0000024'], [])
    >>> extract_references("MGI:MGI:5005039 | PMID:21459323")
    (['PMID:21459323'], ['MGI:MGI:5005039'])
    >>> extract_references(["Reactome:R-HSA-201451"])
    ([], ['Reactome:R-HSA-201451'])
    >>> extract_references(None)
    ([], [])
    """
    publications: list[str] = []
    unrecognized: list[str] = []
    for raw_reference in as_list(raw_references):
        for token in raw_reference.split(REFERENCE_SEPARATOR):
            if not token.strip():
                continue
            normalized = normalize_reference(token)
            if normalized:
                publications.append(normalized)
            else:
                unrecognized.append(token.strip())
    return list(dict.fromkeys(publications)), list(dict.fromkeys(unrecognized))


def agent_type_for_evidence(eco_terms: Iterable[str]) -> AgentTypeEnum:
    """
    Resolve the Biolink agent type from the ECO codes backing a causal statement.

    An assertion is only automated when *every* code behind it is an automatic-assertion
    code; a single manual code means a curator was in the loop. Codes ECO leaves
    unqualified by assertion method, and evidence-free edges, fall back to manual, since
    GO-CAM models are manually authored by construction.

    >>> agent_type_for_evidence(["ECO:0000313"])
    <AgentTypeEnum.automated_agent: 'automated_agent'>
    >>> agent_type_for_evidence(["ECO:0000313", "ECO:0000363"])
    <AgentTypeEnum.automated_agent: 'automated_agent'>
    >>> agent_type_for_evidence(["ECO:0000314"])
    <AgentTypeEnum.manual_agent: 'manual_agent'>
    >>> agent_type_for_evidence(["ECO:0000313", "ECO:0000314"])
    <AgentTypeEnum.manual_agent: 'manual_agent'>
    >>> agent_type_for_evidence([])
    <AgentTypeEnum.manual_agent: 'manual_agent'>
    """
    terms = list(eco_terms)
    if terms and all(term in AUTOMATIC_ASSERTION_ECO_TERMS for term in terms):
        return AgentTypeEnum.automated_agent
    return AgentTypeEnum.manual_agent


def extract_reactome_record_urls(raw_references: Any) -> list[str]:
    """
    Pull Reactome pathway records out of a GO-CAM reference field as resolvable URLs.

    These are the only reference the Reactome-derived edges carry, but a Reactome
    pathway is a database record rather than literature, so it is not a publication.
    Recording it as a ``source_record_urls`` entry on the Reactome retrieval source keeps
    the provenance without overstating it as a citation.

    >>> extract_reactome_record_urls(["Reactome:R-HSA-201451"])
    ['https://reactome.org/content/detail/R-HSA-201451']
    >>> extract_reactome_record_urls("Reactome:R-MMU-8964026 | PMID:12345678")
    ['https://reactome.org/content/detail/R-MMU-8964026']
    >>> extract_reactome_record_urls(["PMID:12345678"])
    []
    >>> extract_reactome_record_urls(None)
    []
    """
    urls = []
    for raw_reference in as_list(raw_references):
        for token in raw_reference.split(REFERENCE_SEPARATOR):
            match = REACTOME_REFERENCE_PATTERN.match(token.strip())
            if match:
                urls.append(REACTOME_RECORD_URL.format(match.group(1)))
    return list(dict.fromkeys(urls))


def build_sources(model_id: str, reactome_record_urls: list[str]) -> list[RetrievalSource]:
    """
    Build the retrieval sources for one edge, carrying the records it came from.

    Reactome-derived models are identified by an ``R-HSA`` pattern in the model id; those
    edges get ``infores:reactome`` as primary knowledge source with ``infores:go-cam``
    aggregating. Everything else is GO-CAM-native with a single primary source.

    Sources are built per-edge rather than per-model because the Reactome pathway record
    varies edge to edge within a model.

    >>> sources = build_sources("gomodel:R-HSA-201451", ["https://reactome.org/content/detail/R-HSA-201451"])
    >>> [(s.resource_id, str(s.resource_role)) for s in sources]
    [('infores:reactome', 'primary_knowledge_source'), ('infores:go-cam', 'aggregator_knowledge_source')]
    >>> sources[0].source_record_urls
    ['https://reactome.org/content/detail/R-HSA-201451']

    A GO-CAM-native model has one source, pointing at the model record:

    >>> sources = build_sources("gomodel:0000000300000001", [])
    >>> [(s.resource_id, str(s.resource_role)) for s in sources]
    [('infores:go-cam', 'primary_knowledge_source')]
    >>> sources[0].source_record_urls
    ['https://live-go-cam.geneontology.io/product/json/low-level/0000000300000001.json']
    """
    model_record_urls = (
        [GO_CAM_RECORD_URL.format(model_id.split(":", 1)[-1])] if model_id else None
    )
    if model_id and "R-HSA-" in model_id:
        return [
            RetrievalSource(
                id=INFORES_REACTOME,
                resource_id=INFORES_REACTOME,
                resource_role=ResourceRoleEnum.primary_knowledge_source,
                source_record_urls=reactome_record_urls or None,
            ),
            RetrievalSource(
                id=INFORES_GO_CAM,
                resource_id=INFORES_GO_CAM,
                resource_role=ResourceRoleEnum.aggregator_knowledge_source,
                upstream_resource_ids=[INFORES_REACTOME],
                source_record_urls=model_record_urls,
            ),
        ]
    return [
        RetrievalSource(
            id=INFORES_GO_CAM,
            resource_id=INFORES_GO_CAM,
            resource_role=ResourceRoleEnum.primary_knowledge_source,
            source_record_urls=model_record_urls,
        )
    ]


def extract_curator_orcids(raw_contributors: Any) -> list[str]:
    """
    Turn a GO-CAM ``*_contributors`` field into ORCID CURIEs.

    Contributors are recorded as ORCID URLs, but GO-CAM also uses opaque curator-group
    identifiers - every Reactome-derived edge is attributed to ``GOC:reactome_curators``
    rather than to a person. Those are not publications and are dropped.

    >>> extract_curator_orcids(["https://orcid.org/0000-0001-6330-7526"])
    ['ORCID:0000-0001-6330-7526']
    >>> extract_curator_orcids("http://orcid.org/0000-0002-1825-0097")
    ['ORCID:0000-0002-1825-0097']
    >>> extract_curator_orcids(["GOC:reactome_curators"])
    []
    >>> extract_curator_orcids(None)
    []
    """
    orcids = []
    for contributor in as_list(raw_contributors):
        match = ORCID_PATTERN.match(contributor.strip())
        if match:
            orcids.append(f"ORCID:{match.group(1).upper()}")
    return list(dict.fromkeys(orcids))


def extract_evidence_codes(raw_evidence: Any) -> tuple[list[str], list[str]]:
    """
    Turn a GO-CAM ``*_assessed_by`` field into ECO CURIEs plus the rejects.

    Returns ``(eco_terms, unrecognized)``. The source is nearly clean here, but not
    entirely - at least one MGI reference id has been observed in an evidence field, and
    emitting it as an evidence code would produce a malformed edge.

    >>> extract_evidence_codes(["ECO:0000314"])
    (['ECO:0000314'], [])
    >>> extract_evidence_codes(["ECO:0000318", "ECO:0000318"])
    (['ECO:0000318'], [])
    >>> extract_evidence_codes(["MGI:MGI:5490144"])
    ([], ['MGI:MGI:5490144'])
    >>> extract_evidence_codes(None)
    ([], [])
    """
    eco_terms = []
    unrecognized = []
    for value in as_list(raw_evidence):
        token = value.strip()
        (eco_terms if ECO_PATTERN.match(token) else unrecognized).append(token)
    return eco_terms, unrecognized


def extract_tar_gz(tar_path: str) -> str:
    """Extract tar.gz file to a temporary directory and return the path."""
    extract_dir = tempfile.mkdtemp(prefix="go_cam_extract_")

    logger.info(f"Extracting {tar_path} to {extract_dir}")
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(extract_dir)

    return extract_dir


@koza.prepare_data()
def prepare_go_cam_data(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Extract tar.gz and yield JSON model data, filtering by taxon from configuration."""
    logger.info("Preparing GO-CAM data: extracting tar.gz and finding all JSON files...")


    # Path to the downloaded tar.gz file (from kghub-downloader)
    tar_path = f"{koza.input_files_dir}/go-cam-networkx.tar.gz"

    # Extract the tar.gz file
    extracted_path = extract_tar_gz(str(tar_path))

    # Find all JSON files
    json_files = list(Path(extracted_path).glob("**/*_networkx.json"))
    logger.info(f"Found {len(json_files)} networkx JSON files to process")

    # Get filter configuration from Koza's extra_fields (from YAML transform.filters)
    filters = koza.extra_fields.get("filters", [])
    target_taxa = set()

    # Extract target taxa from filter configuration
    for filter_config in filters:
        if (
            filter_config.get("column") == "taxon"
            and filter_config.get("filter_code") == "in_exact"
            and filter_config.get("inclusion") == "include"
        ):
            target_taxa = set(filter_config.get("value", []))
            logger.info(f"Configured to include taxa: {target_taxa}")
            break

    if not target_taxa:
        logger.error("No target taxa included in the Koza GO-CAM ingest configuration? No data to process!")
        return []

    logger.info(f"Filtering for taxa: {target_taxa}")

    models_processed = 0
    models_filtered = 0

    # Yield the content of each JSON file, filtering by species from config
    for json_file in json_files:
        try:
            with open(json_file, "r") as f:
                model_data = json.load(f)

            models_processed += 1

            # Extract taxon from nested structure
            taxon = model_data.get("graph", {}).get("model_info", {}).get("taxon", "")

            # Apply filtering based on configuration
            if taxon in target_taxa:
                model_data["taxon"] = taxon  # Expose for consistency
                model_data["_file_path"] = str(json_file)
                yield model_data
                models_filtered += 1
            else:
                # Skip models that don't match filter
                logger.debug(f"Skipping model {Path(json_file).name} with taxon: {taxon}")

        except Exception as e:
            logger.error(f"Error reading JSON file {json_file}: {e}")

    logger.info(f"Filtered {models_filtered} models out of {models_processed} total models")

    if models_filtered == 0:
        logger.warning(f"No models matched the filter criteria. Target taxa: {target_taxa}")

    return []  # empty run or just the end of yielded data...


@koza.transform()
def transform_go_cam_models(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    """Process all GO-CAM model data with linked node/edge validation."""
    dropped_predicates: Counter[str] = Counter()  # Causal predicates with no sound biolink target
    dropped_references: Counter[str] = Counter()  # Reference values that are not publications
    dropped_evidence: Counter[str] = Counter()  # "assessed_by" values that are not ECO CURIEs
    agent_types_assigned: Counter[str] = Counter()  # Agent type resolved from ECO, for reporting
    model_statuses: Counter[str] = Counter()  # GO-CAM model status, reported but not filtered on

    nodes_created = dict()

    for model_data in data:

        # Get model info (filtering is now handled by Koza filters in YAML)
        model_id = model_data.get("graph", {}).get("model_info", {}).get("id", "")
        taxon = model_data.get("graph", {}).get("model_info", {}).get("taxon", "")

        # Model status is reported but deliberately not filtered on: every
        # Reactome-derived model is flagged "development", so filtering to production
        # would silently drop the whole infores:reactome knowledge source.
        model_statuses[str(model_data.get("graph", {}).get("model_info", {}).get("status"))] += 1

        # Build lookup of nodes for label/name resolution
        node_lookup = {}

        for node in model_data.get("nodes", []):
            node_id = node.get("id")
            if node_id:
                normalized_id = normalize_id(node_id)

                # Store both original and normalized for edge lookup
                node_lookup[node_id] = {"id": normalized_id, "name": node.get("label"), "taxon": taxon}
                if normalized_id != node_id:
                    node_lookup[normalized_id] = node_lookup[node_id]

        # Determine knowledge sources based on model_id
        # Track nodes and edges for this model
        nodes = dict()
        edges = []

        # Process edges with linked validation
        edge: dict
        for edge in model_data.get("edges", []):
            # Extract values that might be strings or lists
            source_id = extract_value(edge.get("source"))
            target_id = extract_value(edge.get("target"))
            causal_predicate = extract_value(edge.get("causal_predicate"))

            # Skip edge if missing required data
            if not all([source_id, target_id, causal_predicate]):
                continue

            # Skip edge if either node is not in our node lookup
            if source_id not in node_lookup or target_id not in node_lookup:
                logger.debug(f"Skipping edge {source_id}->{target_id}: node(s) not found in model")
                continue

            # Create the Gene nodes for this edge
            for gene_id in [source_id, target_id]:
                gene_info = node_lookup[gene_id]
                if gene_info["id"] not in nodes_created:
                    # Only create a node once the first time it is
                    # encountered within at least one edge in any model_data
                    gene_node = Gene(
                        id=gene_info["id"],
                        name=gene_info["name"],
                        category=["biolink:Gene"],
                        in_taxon=[gene_info["taxon"]] if gene_info["taxon"] else None,
                    )
                    nodes_created[gene_info["id"]] = gene_node

                # Only add a node to the 'nodes' set once
                # for a given collection of model_data edges
                if gene_info["id"] not in nodes:
                    nodes[gene_info["id"]] = nodes_created[gene_info["id"]]

            # Since we know here that they exist, get the normalized IDs for the association
            normalized_source_id = node_lookup[source_id]["id"]
            normalized_target_id = node_lookup[target_id]["id"]

            # Resolve the causal predicate against the generated RO/biolink table.
            # An edge with no sound Biolink target is dropped rather than emitted as
            # biolink:related_to: a related_to edge carries no reasoning signal, and it
            # is worse than an absent edge because absence is measurable.
            predicate_mapping = map_causal_predicate(causal_predicate)
            if predicate_mapping is None:
                dropped_predicates[f"{causal_predicate} (not in mapping table)"] += 1
                continue
            if predicate_mapping.predicate is None:
                dropped_predicates[
                    f"{causal_predicate} ({predicate_mapping.ro_label}): {predicate_mapping.provenance}"
                ] += 1
                continue
            biolink_predicate = predicate_mapping.predicate

            # Extract publications: literature references (PMID, GO_REF) plus the
            # ORCIDs of the curators who asserted this causal statement.
            references, unrecognized_references = extract_references(
                edge.get("causal_predicate_has_reference")
            )
            dropped_references.update(unrecognized_references)
            publications = references + extract_curator_orcids(
                edge.get("causal_predicate_contributors")
            )

            # Reactome pathway records are provenance, not citations, so they ride on
            # the retrieval source instead of in publications.
            reactome_record_urls = extract_reactome_record_urls(
                edge.get("causal_predicate_has_reference")
            )
            sources = build_sources(model_id, reactome_record_urls)

            # Evidence backing the causal statement itself. The sibling "*_assessed_by"
            # fields on this edge evidence the qualifier annotations rather than the
            # causal claim, so they are deliberately not merged in here.
            eco_terms, unrecognized_evidence = extract_evidence_codes(
                edge.get("causal_predicate_assessed_by")
            )
            dropped_evidence.update(unrecognized_evidence)

            # ECO distinguishes evidence used in manual assertion from evidence used in
            # automatic assertion, which is the only signal in the data that separates
            # curator-authored edges from bulk-imported ones.
            agent_type = agent_type_for_evidence(eco_terms)
            agent_types_assigned[agent_type.value] += 1

            # Capture GO terms for statement subject and object Gene nodes
            # molecular activity, biological process and cellular compartmentalization
            source_gene_molecular_function = extract_value(edge.get("source_gene_molecular_function"))
            source_gene_biological_process = extract_value(edge.get("source_gene_biological_process"))
            source_gene_occurs_in = extract_value(edge.get("source_gene_occurs_in"))
            target_gene_molecular_function = extract_value(edge.get("target_gene_molecular_function"))
            target_gene_biological_process = extract_value(edge.get("target_gene_biological_process"))
            target_gene_occurs_in = extract_value(edge.get("target_gene_occurs_in"))

            # Create the gene-to-gene association
            association = GeneToGeneAssociation(
                id=entity_id(),
                subject=normalized_source_id,
                subject_activity_qualifier=source_gene_molecular_function,
                subject_process_qualifier=source_gene_biological_process,
                subject_context_qualifier=source_gene_occurs_in,
                predicate=biolink_predicate,
                object=normalized_target_id,
                object_activity_qualifier=target_gene_molecular_function,
                object_process_qualifier=target_gene_biological_process,
                object_context_qualifier=target_gene_occurs_in,
                # These three read as one sentence - "<subject> causes increased
                # activity of <object>" - and are set together or not at all.
                qualified_predicate=predicate_mapping.qualified_predicate,
                object_aspect_qualifier=predicate_mapping.object_aspect,
                object_direction_qualifier=predicate_mapping.direction,
                original_subject=source_id,
                original_predicate=causal_predicate,
                original_object=target_id,
                publications=publications if publications else None,
                has_evidence_of_type=eco_terms if eco_terms else None,
                sources=sources,
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=agent_type,
            )

            edges.append(association)

        # Yield a KnowledgeGraph for this model if there are any edges
        if edges:
            yield KnowledgeGraph(nodes=list(nodes.values()), edges=edges)

    if dropped_predicates:
        logger.warning(
            f"Dropped {sum(dropped_predicates.values())} edges across "
            f"{len(dropped_predicates)} causal predicates with no sound Biolink target: "
            f"{dropped_predicates.most_common()}"
        )

    logger.info(f"Agent type resolved from ECO evidence: {dict(agent_types_assigned)}")
    logger.info(f"GO-CAM model status distribution (not filtered): {dict(model_statuses)}")

    if dropped_references:
        logger.warning(
            f"Dropped {sum(dropped_references.values())} reference values across "
            f"{len(dropped_references)} distinct forms that are not PMID or GO_REF "
            f"publications: {dropped_references.most_common(10)}"
        )
    if dropped_evidence:
        logger.warning(
            f"Dropped {sum(dropped_evidence.values())} non-ECO values across "
            f"{len(dropped_evidence)} distinct forms found in evidence fields: "
            f"{dropped_evidence.most_common(10)}"
        )
