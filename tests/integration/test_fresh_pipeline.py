"""Opt-in full pipeline acceptance with real HTTP downloads and Node Normalizer."""

import functools
import json
import os
import shutil
import sys
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import requests
import yaml

import translator_ingest.ingests
from translator_ingest import pipeline
from translator_ingest.util import metadata
from translator_ingest.util.storage import local


@pytest.mark.skipif(
    os.environ.get("RUN_LIVE_PIPELINE_TESTS") != "1",
    reason="Set RUN_LIVE_PIPELINE_TESTS=1 to exercise live normalization and schema services.",
)
def test_full_pipeline_on_fresh_storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed source publishes nothing; a healthy fixture completes every build stage."""
    downloads = tmp_path / "http"
    downloads.mkdir()
    (downloads / "version.txt").write_text("fixture-1")
    # Duplicate evidence exercises actual merge deduplication as well as normalization.
    (downloads / "input.tsv").write_text(
        "subject\tobject\nNCBIGene:1956\tNCBIGene:7157\nNCBIGene:1956\tNCBIGene:7157\n"
    )
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), functools.partial(SimpleHTTPRequestHandler, directory=str(downloads))
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    ingests = tmp_path / "ingests"
    ingests.mkdir()
    data = tmp_path / "data"
    monkeypatch.setattr(translator_ingest.ingests, "__path__", [str(ingests)])
    monkeypatch.setattr(pipeline, "INGESTS_PARSER_PATH", ingests)
    monkeypatch.setattr(metadata, "INGESTS_PARSER_PATH", ingests)
    monkeypatch.setattr(local, "INGESTS_DATA_PATH", data)
    sources = ("fresh_broken_fixture", "fresh_healthy_fixture")
    base_url = f"http://127.0.0.1:{server.server_port}"
    for source in sources:
        directory = ingests / source
        directory.mkdir()
        (directory / "__init__.py").write_text("")
        endpoint = "missing.txt" if "broken" in source else "version.txt"
        code = f'''import koza
import requests
from koza.model.graphs import KnowledgeGraph
from biolink_model.datamodel.pydanticmodel_v2 import Gene, GeneToGeneAssociation, RetrievalSource


def get_latest_version() -> str:
    """Read the fixture version from a real HTTP endpoint."""
    response = requests.get("{base_url}/{endpoint}", timeout=10)
    response.raise_for_status()
    return response.text.strip()


@koza.transform_record()
def transform_record(koza: koza.KozaTransform, record: dict) -> KnowledgeGraph:
    """Transform a fixture row using real Koza and Biolink models."""
    return KnowledgeGraph(
        nodes=[Gene(id=record["subject"]), Gene(id=record["object"])],
        edges=[GeneToGeneAssociation(
            id="fixture-edge", subject=record["subject"], object=record["object"],
            predicate="biolink:related_to", knowledge_level="knowledge_assertion",
            agent_type="manual_agent",
            sources=[RetrievalSource(id="infores:translator-testing", resource_id="infores:translator-testing",
                                     resource_role="primary_knowledge_source")],
        )],
    )
'''
        (directory / f"{source}.py").write_text(code)
        (directory / f"{source}.yaml").write_text(yaml.safe_dump({
            "name": source,
            "reader": {"format": "csv", "delimiter": "\t", "files": ["input.tsv"]},
            "transform": {"code": f"{source}.py"},
        }))
        (directory / "download.yaml").write_text(yaml.safe_dump([
            {"url": f"{base_url}/input.tsv", "local_name": "input.tsv"}
        ]))
        (directory / f"{source}_rig.yaml").write_text(yaml.safe_dump({
            "name": "Fresh pipeline acceptance fixture",
            "source_info": {"description": "Synthetic test data", "data_access_locations": base_url},
        }))
    try:
        assert not data.exists()
        with pytest.raises(requests.HTTPError, match="404"):
            pipeline.run_pipeline(sources[0])
        assert not (data / sources[0]).exists()
        pipeline.run_pipeline(sources[1])
        build_path = data / sources[1] / "latest-build.json"
        assert build_path.exists(), "Pipeline did not publish a successful build"
        build = json.loads(build_path.read_text())
        assert build["source_version"] == "fixture-1"
        assert build["build_version"]
        assert build["source_download_date"]
        run_metadata = metadata.PipelineMetadata.from_dict(build)
        assert pipeline.get_validation_result(run_metadata)
        nodes_file, edges_file = local.get_versioned_file_paths(local.IngestFileType.MERGED_KGX_FILES, run_metadata)
        nodes = [json.loads(line) for line in nodes_file.read_text().splitlines()]
        edges = [json.loads(line) for line in edges_file.read_text().splitlines()]
        assert {node["id"] for node in nodes} == {"NCBIGene:1956", "NCBIGene:7157"}
        assert len(edges) == 1
        assert edges[0]["subject"] == "NCBIGene:1956"
        assert edges[0]["object"] == "NCBIGene:7157"
        assert pipeline.is_graph_metadata_complete(run_metadata)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        # Preserve optional diagnostic artifacts for manual acceptance runs.
        if artifact_dir := os.environ.get("PIPELINE_ACCEPTANCE_ARTIFACTS"):
            shutil.copytree(tmp_path, Path(artifact_dir), dirs_exist_ok=True)
        for source in sources:
            sys.modules.pop(source, None)
            sys.modules.pop(f"translator_ingest.ingests.{source}.{source}", None)
            sys.modules.pop(f"translator_ingest.ingests.{source}", None)
