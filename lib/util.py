import unicodedata, re, hashlib

def norm_ws(s: str | None) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", str(s))
    return re.sub(r"\s+", " ", s).strip()

def clip(s: str | None, limit=800) -> str | None:
    if s is None: return None
    return s if len(s) <= limit else s[:limit]

def content_hash(row: dict) -> str:
    basis = "||".join([
        row.get("title") or "",
        row.get("summary") or "",
        row.get("rate") or "",
        row.get("cap") or "",
        row.get("target") or "",
        row.get("cost_items") or "",
        row.get("deadline") or "",
    ])
    return hashlib.md5(basis.encode("utf-8")).hexdigest()
