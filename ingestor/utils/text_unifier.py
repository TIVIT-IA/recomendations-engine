# ingestor/src/text_unifier.py
def build_texto_unificado(record: dict) -> str:
    """
    Convierte cualquier JSON (sin importar estructura) en texto unificado.
    Extrae recursivamente todos los valores relevantes.
    Version universal funcional.
    """

    def recorrer(obj):
        partes = []

        if isinstance(obj, dict):
            for k, v in obj.items():
                # no agregamos claves que sean ruido
                if k.lower() not in ("id", "uuid", "_id"):
                    partes.append(str(k))
                partes.extend(recorrer(v))

        elif isinstance(obj, list):
            for item in obj:
                partes.extend(recorrer(item))

        # valores atómicos
        else:
            # ignoramos valores vacíos
            if obj not in (None, "", " ", "null"):
                partes.append(str(obj))

        return partes

    try:
        texto = " ".join(recorrer(record))
        texto = " ".join(texto.split())  # normalizar espacios
        return texto.strip()

    except Exception:
        return ""
