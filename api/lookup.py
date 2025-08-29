import os, re, json
import pandas as pd

CSV_PATH = os.path.join(os.path.dirname(__file__), "pnu10.csv")
_df = None

def _load_df():
    global _df
    if _df is not None:
        return _df
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")
    _df = pd.read_csv(CSV_PATH)
    # 컬럼 검증
    need = {"법정동", "pnu"}
    if not need.issubset(set(_df.columns)):
        raise ValueError(f"CSV columns missing. need={need}, got={set(_df.columns)}")
    return _df

def handler(request):
    try:
        q = (request.query.get("query") or "").strip()
        if not q:
            return {"statusCode": 400, "body": "Missing 'query' parameter"}

        df = _load_df()
        q = re.sub(r"\s+", " ", q)
        matches = df[df["법정동"].str.contains(q, na=False)]

        if matches.empty:
            return {"statusCode": 404, "body": f"No match found for '{q}'"}

        results = [{"법정동": r["법정동"], "pnu": str(r["pnu"])} for _, r in matches.iterrows()]
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json; charset=utf-8"},
            "body": json.dumps(results, ensure_ascii=False)
        }
    except Exception as e:
        # 에러 내용을 그대로 돌려줘서 원인 파악
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "text/plain; charset=utf-8"},
            "body": f"ERROR: {type(e).__name__}: {e}"
        }
