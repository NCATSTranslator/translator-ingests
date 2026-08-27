# Ontology Utilities

This subpackage of the translator_ingest project contains utilities and caching mechanisms 
for working with common public ontologies.

Ontology lookup methods are in the root package module. Three levels of operations facilitate
the lookup of the 'best match' ontology term corresponding to a concept name:

1. In-memory cache (Python @lru_cache on top-level access methods)
2. File-based cache (with ontology mapping files stored in the 'cache' subpackage)
3. Retrieval from the Translator (Phase 2 SRI RENCI team-built) Name Resolution Service endpoint

# Name Resolution Service

The Translator Phase 2 SRI RENCI team built Name Resolution Service (NRS) web service endpoint
is documented at https://name-resolution-sri.renci.org/docs#. Given a concept name as a query 'string'
against a given ontology namespace, a structured JSON response is returned.

For example, given 'placenta' searched in UBERON, something like the following JSON response is returned (below).

Note that the response is scored by the NRS, and the Biolink Model category list is returned. For Translator Ingest
operational purposes, we are only interested in the highest scoring ontology term and only that term is returned 
as the 'best match' for the given concept name, cached locally within the ontology package for future use.
In cases where no such term is found, then the cache tags the concept as 'unresolved' against the target ontology.

```json
    [
      {
        "curie": "UBERON:0001987",
        "label": "placenta",
        "highlighting": {},
        "synonyms": [
          "PLACENTA",
          "Placenta",
          "placenta",
          "placentas",
          "Placentas",
          "Placental",
          "placentome",
          "Placentome",
          "Placentomes",
          "Placenta, NOS",
          "eutherian placenta",
          "allantoic placenta",
          "Placental structure",
          "Placentomes (body structure)",
          "Placental structure (body structure)"
        ],
        "taxa": [],
        "types": [
          "biolink:GrossAnatomicalStructure",
          "biolink:AnatomicalEntity",
          "biolink:PhysicalEssence",
          "biolink:OrganismalEntity",
          "biolink:SubjectOfInvestigation",
          "biolink:BiologicalEntity",
          "biolink:ThingWithTaxon",
          "biolink:NamedThing",
          "biolink:Entity",
          "biolink:PhysicalEssenceOrOccurrent"
        ],
        "score": 1720.6982,
        "clique_identifier_count": 4
      },
      {
        "curie": "UBERON:0009002",
        "label": "placental membrane",
        "highlighting": {},
        "synonyms": [
          "placental barrier",
          "placental membrane",
          "Placental membrane",
          "Placental Membrane",
          "barriers placental",
          "Structure of placental membrane",
          "Structure of placental membrane (body structure)"
        ],
        "taxa": [],
        "types": [
          "biolink:GrossAnatomicalStructure",
          "biolink:AnatomicalEntity",
          "biolink:PhysicalEssence",
          "biolink:OrganismalEntity",
          "biolink:SubjectOfInvestigation",
          "biolink:BiologicalEntity",
          "biolink:ThingWithTaxon",
          "biolink:NamedThing",
          "biolink:Entity",
          "biolink:PhysicalEssenceOrOccurrent"
        ],
        "score": 150.515,
        "clique_identifier_count": 3
      },
      {
        "curie": "UBERON:0003946",
        "label": "placenta labyrinth",
        "highlighting": {},
        "synonyms": [
          "labyrinthine layer",
          "placenta labyrinth",
          "placental labyrinth",
          "labyrinthine layer of placenta"
        ],
        "taxa": [],
        "types": [
          "biolink:GrossAnatomicalStructure",
          "biolink:AnatomicalEntity",
          "biolink:PhysicalEssence",
          "biolink:OrganismalEntity",
          "biolink:SubjectOfInvestigation",
          "biolink:BiologicalEntity",
          "biolink:ThingWithTaxon",
          "biolink:NamedThing",
          "biolink:Entity",
          "biolink:PhysicalEssenceOrOccurrent"
        ],
        "score": 134.73756,
        "clique_identifier_count": 1
      },
      {
        "curie": "UBERON:0002450",
        "label": "decidua",
        "highlighting": {},
        "synonyms": [
          "Decidua",
          "decidua",
          "Deciduas",
          "Decidual",
          "Deciduum",
          "endometrium",
          "Decidua, NOS",
          "uterine decidua",
          "decidua basalis",
          "Decidua structure",
          "Decidous membrane",
          "maternal placenta",
          "decidous membrane",
          "Decidua Graviditas",
          "Endometrial decidua",
          "extraembryonic placenta",
          "maternal decidual layer",
          "maternal part of placenta",
          "extraembryonic part of placenta",
          "placenta maternal decidual layer",
          "Decidua structure (body structure)"
        ],
        "taxa": [],
        "types": [
          "biolink:AnatomicalEntity",
          "biolink:PhysicalEssence",
          "biolink:OrganismalEntity",
          "biolink:SubjectOfInvestigation",
          "biolink:BiologicalEntity",
          "biolink:ThingWithTaxon",
          "biolink:NamedThing",
          "biolink:Entity",
          "biolink:PhysicalEssenceOrOccurrent"
        ],
        "score": 132.3551,
        "clique_identifier_count": 6
      },
      {
        "curie": "UBERON:0003972",
        "label": "placenta junctional zone",
        "highlighting": {},
        "synonyms": [
          "placenta junctional zone"
        ],
        "taxa": [],
        "types": [
          "biolink:GrossAnatomicalStructure",
          "biolink:AnatomicalEntity",
          "biolink:PhysicalEssence",
          "biolink:OrganismalEntity",
          "biolink:SubjectOfInvestigation",
          "biolink:BiologicalEntity",
          "biolink:ThingWithTaxon",
          "biolink:NamedThing",
          "biolink:Entity",
          "biolink:PhysicalEssenceOrOccurrent"
        ],
        "score": 130.59557,
        "clique_identifier_count": 1
      },
      {
        "curie": "UBERON:0008855",
        "label": "placenta metrial gland",
        "highlighting": {},
        "synonyms": [
          "placenta metrial gland"
        ],
        "taxa": [],
        "types": [
          "biolink:GrossAnatomicalStructure",
          "biolink:AnatomicalEntity",
          "biolink:PhysicalEssence",
          "biolink:OrganismalEntity",
          "biolink:SubjectOfInvestigation",
          "biolink:BiologicalEntity",
          "biolink:ThingWithTaxon",
          "biolink:NamedThing",
          "biolink:Entity",
          "biolink:PhysicalEssenceOrOccurrent"
        ],
        "score": 130.59557,
        "clique_identifier_count": 1
      },
      {
        "curie": "UBERON:0022358",
        "label": "placenta blood vessel",
        "highlighting": {},
        "synonyms": [
          "placental vessel",
          "placenta blood vessel"
        ],
        "taxa": [],
        "types": [
          "biolink:GrossAnatomicalStructure",
          "biolink:AnatomicalEntity",
          "biolink:PhysicalEssence",
          "biolink:OrganismalEntity",
          "biolink:SubjectOfInvestigation",
          "biolink:BiologicalEntity",
          "biolink:ThingWithTaxon",
          "biolink:NamedThing",
          "biolink:Entity",
          "biolink:PhysicalEssenceOrOccurrent"
        ],
        "score": 130.59557,
        "clique_identifier_count": 1
      },
      {
        "curie": "UBERON:0010006",
        "label": "placenta intervillous maternal lacunae",
        "highlighting": {},
        "synonyms": [
          "placenta intervillous maternal lacunae"
        ],
        "taxa": [],
        "types": [
          "biolink:AnatomicalEntity",
          "biolink:PhysicalEssence",
          "biolink:OrganismalEntity",
          "biolink:SubjectOfInvestigation",
          "biolink:BiologicalEntity",
          "biolink:ThingWithTaxon",
          "biolink:NamedThing",
          "biolink:Entity",
          "biolink:PhysicalEssenceOrOccurrent"
        ],
        "score": 126.9929,
        "clique_identifier_count": 1
      },
      {
        "curie": "UBERON:0010007",
        "label": "placenta fetal blood space",
        "highlighting": {},
        "synonyms": [
          "placenta fetal blood space"
        ],
        "taxa": [],
        "types": [
          "biolink:AnatomicalEntity",
          "biolink:PhysicalEssence",
          "biolink:OrganismalEntity",
          "biolink:SubjectOfInvestigation",
          "biolink:BiologicalEntity",
          "biolink:ThingWithTaxon",
          "biolink:NamedThing",
          "biolink:Entity",
          "biolink:PhysicalEssenceOrOccurrent"
        ],
        "score": 126.9929,
        "clique_identifier_count": 1
      },
      {
        "curie": "UBERON:0007106",
        "label": "Chorionic villi",
        "highlighting": {},
        "synonyms": [
          "fetal placenta",
          "chorionic villi",
          "Chorionic villi",
          "Chorionic Villi",
          "placental villi",
          "Placental Villi",
          "villous chorion",
          "Chorionic villus",
          "Villi, Placental",
          "Villi, Chorionic",
          "Chorionic Villus",
          "Placental Villus",
          "placental villus",
          "Placental villus",
          "chorionic villus",
          "placental villous",
          "Villus, Placental",
          "chorionic villous",
          "Villus, Chorionic",
          "embryonic placenta",
          "villous of placenta",
          "Chorionic villi, NOS",
          "fetal part of placenta",
          "Chorionic villi structure",
          "embryonic part of placenta",
          "Chorionic villi structure (body structure)"
        ],
        "taxa": [],
        "types": [
          "biolink:AnatomicalEntity",
          "biolink:PhysicalEssence",
          "biolink:OrganismalEntity",
          "biolink:SubjectOfInvestigation",
          "biolink:BiologicalEntity",
          "biolink:ThingWithTaxon",
          "biolink:NamedThing",
          "biolink:Entity",
          "biolink:PhysicalEssenceOrOccurrent"
        ],
        "score": 115.57341,
        "clique_identifier_count": 5
      }
    ]

```