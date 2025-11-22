# ingestor/src/text_unifier.py
def build_texto_unificado(record: dict) -> str:
    """
    Normaliza varios esquemas: intenta extraer nombre, experiencia, skills y resumen.
    record es el dict real del trabajador (no el wrapper).
    """
    parts = []

    # nombre
    nombre = record.get('nombres') or record.get('nombre') or record.get('fullName') or record.get('documento')
    if nombre:
        parts.append(str(nombre))

    # experiencia (puede venir como 'experiencia', 'jobs', etc)
    experiencias = record.get('experiencia') or record.get('jobs') or record.get('positions') or []
    if isinstance(experiencias, list):
        for e in experiencias:
            # intenta varios campos
            puesto = e.get('puesto') or e.get('title') or e.get('role') or ""
            empresa = e.get('empresa') or e.get('company') or ""
            anos = e.get('anos') or e.get('years') or e.get('duration') or ""
            seg = " ".join([str(x) for x in (puesto, empresa, anos) if x])
            if seg:
                parts.append(seg)

    # skills
    skills = record.get('skills') or record.get('competencias') or record.get('tags') or []
    if isinstance(skills, list):
        parts.append(", ".join([str(s) for s in skills if s]))

    # resumen / bio / contenido (para drive docs)
    resumen = record.get('resumen') or record.get('bio') or record.get('contenido') or record.get('perfil') or ""
    if resumen:
        parts.append(str(resumen))

    texto = ' . '.join([p for p in parts if p])
    return texto.strip()
