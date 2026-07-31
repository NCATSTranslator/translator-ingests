from uuid import uuid4

def entity_id() -> str:
    """
    Generate a unique edge identifier for translator KGs.

    Returns
    -------
    str
        unique uuid identifier
    """
    return str(uuid4())
