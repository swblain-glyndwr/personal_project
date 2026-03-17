from typing import Optional


def map_tbl(
    template: str,
    schema: Optional[str] = None,
    client: Optional[str] = None,
    catalog: Optional[str] = None,
    **kwargs
) -> str:
    """
    String formatting wrapper for mapping `schema` and `client` to
    parameterised table name, in accordance with the dev/prod schema
    DevOps pattern for 'write' tables.
    """
    if not template:
        raise ValueError("Template cannot be empty")
    
    # Build format map from all provided arguments
    format_map = {
        'catalog': catalog,
        'schema': schema,
        'client': client,
        **kwargs
    }
    
    # Remove None values so they don't cause issues
    format_map = {k: v for k, v in format_map.items() if v is not None}
    
    # Format and return
    try:
        return template.format_map(format_map)
    except KeyError as e:
        available = list(format_map.keys())
        raise KeyError(
            f"Missing required placeholder {e} in template: {template}\n"
            f"Available: {available}"
        ) from e
