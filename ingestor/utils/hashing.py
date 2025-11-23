import hashlib
import json

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def compute_hash_estable(record: dict) -> str:
    dni = str(record.get('dni','')).strip().lower()
    correo = str(record.get('correo','')).strip().lower()
    return sha256_hex(f"{dni}|{correo}")

def compute_hash_completo(record: dict) -> str:
    t = json.dumps(record, sort_keys=True, ensure_ascii=False)
    return sha256_hex(t)
