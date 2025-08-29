import os
import re
import pandas as pd

# Vercel 서버에서 작동할 경로
CSV_PATH = os.path.join(os.path.dirname(__file__), "pnu10.csv")
df = pd.read_csv(CSV_PATH)

def handler(request):
    query = request.query.get("query", "").strip()
    if not query:
        return {
            "statusCode": 400,
            "body": "Missing 'query' parameter"
        }

    query = re.sub(r"\s+", " ", query)
    matches = df[df["법정동"].str.contains(query, na=False)]

    if matches.empty:
        return {
            "statusCode": 404,
            "body": f"No match found for '{query}'"
        }

    results = [
        {"법정동": row["법정동"], "pnu": str(row["pnu"])}
        for _, row in matches.iterrows()
    ]

    import json
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(results, ensure_ascii=False)
    }
