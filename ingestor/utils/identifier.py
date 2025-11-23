import json
from typing import Any, Optional
from ingestor.utils.hashing import sha256_hex

def extract_identifier_field(data: Any, field_name: str) -> Optional[str]:
    """
    Extrae un campo en cualquier parte del JSON y devuelve DIRECTAMENTE
    el hash sha256 del valor. Si el valor existe, siempre retorna el hash.
    Si el JSON es inválido o el campo no existe → retorna None.
    """

    # 1. Si viene como string, intentar parsear JSON
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return None

    # 2. Validar estructura
    if not isinstance(data, (dict, list)):
        return None

    def deep_search(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k.lower() == field_name.lower():
                    # Convertir cualquier valor a string
                    value_str = str(v).strip().lower()
                    # Hashear directamente
                    return sha256_hex(value_str)

                res = deep_search(v)
                if res is not None:
                    return res

        elif isinstance(obj, list):
            for item in obj:
                res = deep_search(item)
                if res is not None:
                    return res

        return None

    return deep_search(data)
