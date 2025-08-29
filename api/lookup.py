# api/lookup.py
from __future__ import annotations
import os, re, json
from typing import Optional, Tuple, List, Dict, Any
import pandas as pd
from urllib.parse import unquote_plus

# ---------------------------
# 설정/경로
# ---------------------------
CSV_PATH = os.path.join(os.path.dirname(__file__), "pnu10.csv")

# CORS/응답 헤더
JSON_HEADERS = {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, X-API-Key",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
}

# ---------------------------
# 유틸리티
# ---------------------------
_DASHES = "－–—"

def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _fix_input_text(raw: str) -> str:
    """브라우저/클라이언트에서 넘어온 한글/퍼센트/깨짐 등을 최대한 복원"""
    if not raw:
        return ""
    text = unquote_plus(raw)
    # 이중 인코딩 방지용 재시도
    try:
        if "%" in text:
            t2 = unquote_plus(text)
            if t2 != text:
                text = t2
    except Exception:
        pass
    # �(U+FFFD) 있을 때 CP949 복구 시도
    if "\ufffd" in text:
        try:
            text = text.encode("latin-1", "ignore").decode("cp949")
        except Exception:
            pass
    return text.strip()

def normalize_address(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("서울시", "서울특별시").replace("서울 특별시", "서울특별시")
    s = s.replace("광역 시", "광역시")
    for ch in _DASHES:
        s = s.replace(ch, "-")
    return _norm_spaces(s)

# bun/ji (지번) 파싱
_BUN_JI_RE = re.compile(
    r"""
    (?:^|[\s,()])           # 경계
    (?:산\s*)?              # '산' 여부는 별도 플래그로만 사용
    (?P<bun>\d{1,6})        # 본번
    (?:\s*-\s*(?P<ji>\d{1,6}))?  # 부번
    (?!\d)
    """,
    re.VERBOSE,
)
def parse_bunjib(addr: str):
    mt = 1 if re.search(r"\b산\s*\d", addr or "") else 0
    matches = list(_BUN_JI_RE.finditer(addr or ""))
    if not matches:
        return mt, None, None
    m = matches[-1]
    bun = int(m.group("bun"))
    ji = int(m.group("ji")) if m.group("ji") else 0
    return mt, bun, ji

def _strip_bunjib(addr: str) -> str:
    return _BUN_JI_RE.sub(" ", addr or "")

# 시/도 통일
_SI_SYNONYMS = {
    "서울": "서울특별시", "서울시": "서울특별시",
    "부산": "부산광역시", "부산시": "부산광역시",
    "인천": "인천광역시", "인천시": "인천광역시",
    "대구": "대구광역시", "대구시": "대구광역시",
    "대전": "대전광역시", "대전시": "대전광역시",
    "광주": "광주광역시", "광주시": "광주광역시",
    "울산": "울산광역시", "울산시": "울산광역시",
    "세종": "세종특별자치시", "세종시": "세종특별자치시",
    "제주": "제주특별자치도", "제주시": "제주특별자치도",
    "경기": "경기도",
    "강원": "강원특별자치도", "강원도": "강원특별자치도",
    "충북": "충청북도", "충남": "충청남도",
    "전북": "전북특별자치도", "전라북도": "전북특별자치도",
    "전남": "전라남도",
    "경북": "경상북도", "경남": "경상남도",
}
def _canonical_si(token: str) -> str:
    t = (token or "").strip()
    return _SI_SYNONYMS.get(t, t)

def _split_parts(name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    parts = _norm_spaces(name).split(" ") if name else []
    if not parts:
        return None, None, None
    if len(parts) == 1:
        return parts[0], None, None
    if len(parts) == 2:
        return parts[0], None, parts[1]
    return parts[0], parts[1], parts[-1]

# ---------------------------
# PNU 인덱스
# ---------------------------
class PNUIndex:
    def __init__(self, csv_path: str, encoding: str = "utf-8"):
        self.ok = False
        self.rows: List[Dict[str, str]] = []
        self.by_full: Dict[str, str] = {}
        self.by_sigu_emd: Dict[Tuple[str, str], List[Tuple[str, str, str]]] = {}
        self.by_emd: Dict[str, List[Tuple[str, str, str, str]]] = {}
        try:
            df = pd.read_csv(csv_path, sep=",", encoding=encoding, low_memory=False)
            if "법정동" not in df.columns or "pnu" not in df.columns:
                raise RuntimeError("CSV에는 '법정동', 'pnu' 컬럼이 필요합니다.")
            df["법정동"] = df["법정동"].astype(str).str.strip()
            df["pnu10"] = df["pnu"].astype(str).str.zfill(10)
            for name, code in zip(df["법정동"], df["pnu10"]):
                full = _norm_spaces(name)
                self.rows.append({"법정동": full, "pnu": code})
            for r in self.rows:
                full = r["법정동"]; code = r["pnu"]
                si, sigu, emd = _split_parts(full)
                self.by_full[full] = code
                if emd:
                    if sigu:
                        self.by_sigu_emd.setdefault((sigu, emd), []).append((full, code, si))
                    self.by_emd.setdefault(emd, []).append((full, code, si, sigu))
            self.ok = True
        except Exception as e:
            print(f"[PNUIndex] load error: {e}")
            self.ok = False

    @staticmethod
    def build_pnu19(code10: str, mt: int, bun: int, ji: int) -> str:
        return f"{code10}{mt}{bun:04d}{ji:04d}"

    def _lookup_pnu10_from_name(self, name: str) -> Dict[str, Any]:
        q = _norm_spaces(name)
        if not q:
            return {"ok": False, "error": "질의가 비어 있습니다.", "query": name}

        parts = q.split(" ")
        if parts:
            parts[0] = _canonical_si(parts[0])

        full = " ".join(parts)
        if full in self.by_full:
            return {"ok": True, "admCd10": self.by_full[full], "matched": full}

        if len(parts) >= 3:
            cand = " ".join([_canonical_si(parts[-3]), parts[-2], parts[-1]])
            if cand in self.by_full:
                return {"ok": True, "admCd10": self.by_full[cand], "matched": cand}

        if len(parts) == 2:
            a, b = parts
            # '서초구 양재동'
            if a.endswith(("구", "군", "시")):
                key = (a, b)
                if key in self.by_sigu_emd:
                    hits = self.by_sigu_emd[key]
                    if len(hits) == 1:
                        full2, code2, _si = hits[0]
                        return {"ok": True, "admCd10": code2, "matched": full2}
                    else:
                        return {
                            "ok": False,
                            "error": "여러 시/도에서 동일한 시군구·법정동 조합이 있습니다. 시도까지 포함해 주세요.",
                            "query": name,
                            "candidates": [h[0] for h in hits],
                        }
            # '서울특별시 양재동'
            key_si = _canonical_si(a)
            cands = []
            for full2, code2 in self.by_full.items():
                si, sigu, emd = _split_parts(full2)
                if si == key_si and emd == b:
                    cands.append((full2, code2))
            if len(cands) == 1:
                full2, code2 = cands[0]
                return {"ok": True, "admCd10": code2, "matched": full2}
            elif len(cands) > 1:
                return {
                    "ok": False,
                    "error": "여러 지역에서 일치합니다. 시군구를 포함해 주세요.",
                    "query": name,
                    "candidates": [c[0] for c in cands],
                }

        if len(parts) == 1:
            emd = parts[0]
            hits = self.by_emd.get(emd, [])
            if not hits:
                return {"ok": False, "error": "법정동을 찾지 못했습니다.", "query": name}
            if len(hits) == 1:
                full2, code2, _si, _sigu = hits[0]
                return {"ok": True, "admCd10": code2, "matched": full2}
            return {
                "ok": False,
                "error": "여러 지역에서 일치합니다. '서초구 양재동'처럼 시군구를 포함해 주세요.",
                "query": name,
                "candidates": [h[0] for h in hits],
            }

        # Fallback: 끝부분 일치
        tail2 = " ".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
        cands2 = [full2 for full2 in self.by_full.keys() if full2.endswith(tail2)]
        if len(cands2) == 1:
            full2 = cands2[0]
            return {"ok": True, "admCd10": self.by_full[full2], "matched": full2}
        elif len(cands2) > 1:
            return {
                "ok": False,
                "error": "여러 지역에서 일치합니다. 시군구를 포함해 주세요.",
                "query": name,
                "candidates": cands2,
            }

        return {"ok": False, "error": "법정동을 찾지 못했습니다.", "query": name}

    def lookup_from_address(self, address: str) -> Dict[str, Any]:
        cleaned = normalize_address(address)
        name_part = _norm_spaces(_strip_bunjib(cleaned))

        # 1) 정확도 높은 매칭
        res = self._lookup_pnu10_from_name(name_part)
        if res.get("ok") or res.get("candidates"):
            return res

        # 2) 주소에 포함된 풀네임 서브스트링 탐색
        hits = [full for full in self.by_full.keys() if full in cleaned]
        if len(hits) == 1:
            full = hits[0]
            return {"ok": True, "admCd10": self.by_full[full], "matched": full}
        elif len(hits) > 1:
            return {
                "ok": False,
                "error": "여러 지역에서 일치합니다. 시군구를 포함해 주세요.",
                "query": address,
                "candidates": hits,
            }
        return {"ok": False, "error": "법정동을 찾지 못했습니다.", "query": address}

# 인덱스 로드(콜드스타트 시 1회)
_INDEX = PNUIndex(CSV_PATH)

# ---------------------------
# Vercel 서버리스 핸들러
# ---------------------------
def handler(request):
    # Preflight
    if getattr(request, "method", "GET").upper() == "OPTIONS":
        return {"statusCode": 204, "headers": JSON_HEADERS, "body": ""}

    try:
        # 1) 입력값 가져오기 (GET: text/query, POST: JSON)
        q = None
        if getattr(request, "method", "GET").upper() == "GET":
            q = request.query.get("text") or request.query.get("query")
        else:
            try:
                data = request.json() or {}
                q = data.get("text") or data.get("query")
            except Exception:
                q = None

        if not q:
            return {"statusCode": 400, "headers": JSON_HEADERS, "body": "Missing 'text' (or 'query') parameter"}

        raw = _fix_input_text(q)
        addr = normalize_address(raw)
        mt, bun, ji = parse_bunjib(addr)

        if not _INDEX.ok:
            return {"statusCode": 500, "headers": JSON_HEADERS, "body": "PNU10 CSV not loaded"}

        res10 = _INDEX.lookup_from_address(addr)

        base = {
            "ok": False,
            "input": q,
            "normalized": addr,
            "full": res10.get("matched"),
            "admCd10": res10.get("admCd10"),
            "bun": f"{bun:04d}" if isinstance(bun, int) else None,
            "ji": f"{ji:04d}" if isinstance(ji, int) else None,
            "mtYn": str(mt) if isinstance(mt, int) else None,
            "pnu": None,
            "source": "csv",
            "candidates": res10.get("candidates"),
        }

        if not res10.get("ok"):
            body = json.dumps(base, ensure_ascii=False)
            return {"statusCode": 200, "headers": JSON_HEADERS, "body": body}

        # bun/ji 없으면 10자리만 반환
        if bun is None:
            base.update({"ok": True})
            body = json.dumps(base, ensure_ascii=False)
            return {"statusCode": 200, "headers": JSON_HEADERS, "body": body}

        # 19자리 생성
        pnu19 = _INDEX.build_pnu19(res10["admCd10"], mt, bun, ji if ji is not None else 0)
        base.update({"ok": True, "pnu": pnu19})
        body = json.dumps(base, ensure_ascii=False)
        return {"statusCode": 200, "headers": JSON_HEADERS, "body": body}

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "text/plain; charset=utf-8", "Access-Control-Allow-Origin": "*"},
            "body": f"ERROR: {type(e).__name__}: {e}",
        }
