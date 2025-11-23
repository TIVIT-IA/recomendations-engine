# ingestor/utils/identifier.py
"""
Utility para extraer un único identificador desde un JSON arbitrario.
Puede ser email, dni u otro campo único, configurable mediante IDENTIFIER_KEY.
"""

import re
from typing import Any

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
DNI_REGEX = re.compile(r"\b\d{8}\b")

# Cambiar entre "email", "dni" o cualquier clave específica
IDENTIFIER_KEY = "email"


def extract_identifier(value: Any) -> str | None:
    """
    Busca un único identificador en estructuras dict/list/strings.
    - Si IDENTIFIER_KEY = "email": detecta emails (regex).
    - Si IDENTIFIER_KEY = "dni": detecta DNIs (regex).
    - Si IDENTIFIER_KEY es otra cadena: busca esa clave exacta.
    Retorna solo **un valor**, no una lista.
    """

    target = IDENTIFIER_KEY.lower()

    # Caso string
    if isinstance(value, str):
        if target == "email":
            m = EMAIL_REGEX.search(value)
            return m.group(0) if m else None
        if target == "dni":
            m = DNI_REGEX.search(value)
            return m.group(0) if m else None
        return None

    # Caso dict
    if isinstance(value, dict):
        # Buscar clave exacta
        for key, v in value.items():
            if key.lower() == target:
                if isinstance(v, str):
                    return v.strip()
                return extract_identifier(v)
        # Buscar recursivamente
        for v in value.values():
            result = extract_identifier(v)
            if result:
                return result
        return None

    # Caso lista
    if isinstance(value, list):
        for item in value:
            result = extract_identifier(item)
            if result:
                return result
        return None

    return None
