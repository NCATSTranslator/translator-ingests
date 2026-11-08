import pytest
from sqlalchemy import create_engine, text

from translator_ingest.ingests.drugcentral.drugcentral import (
    get_server_url,
    DIALECT,
    DRIVER,
    USER,
    PASSWORD,
    HOST,
    PORT,
    DBNAME,
)
from translator_ingest.ingests.drugcentral.mappings import ACTION_TYPE_MAPPING


@pytest.mark.integration
def test_every_live_action_type_is_mapped():
    """New action_type values in the public DrugCentral database must be added to ACTION_TYPE_MAPPING,
    or bioactivity_transform fails with a KeyError."""
    engine = create_engine(get_server_url(DIALECT, DRIVER, USER, PASSWORD, HOST, PORT, DBNAME))
    with engine.connect() as connection:
        action_types = connection.execute(
            text("SELECT DISTINCT action_type FROM act_table_full WHERE action_type IS NOT NULL")
        ).scalars().all()
    assert set(action_types) - set(ACTION_TYPE_MAPPING) == set()
