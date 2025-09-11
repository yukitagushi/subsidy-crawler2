import hashlib, unicodedata, re

def norm_ws(s: str | None) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", str(s))
    return re.sub(r"\s+"," ", s).strip()

def clip(s: str | None, limit=800) -> str | None:
    if not s: return s
    return s if len(s) <= limit else s[:limit]

def content_hash(row: dict) -> str:
    basis = "||".join([row.get(k) or "" for k in (
        "title","summary","rate","cap","target","cost_items","deadline"
    )])
    return hashlib.md5(basis.encode("utf-8")).hexdigest()