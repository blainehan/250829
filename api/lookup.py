import argparse
import pandas as pd
import sys
import re

# CSV 경로 (같은 디렉토리에 pnu10.csv 파일이 있어야 함)
CSV_PATH = "pnu10.csv"

# 유사한 동명을 포함하여 일치 항목 찾기
def search_pnu(query: str, df: pd.DataFrame) -> None:
    query = re.sub(r"\s+", " ", query.strip())

    # 입력 예시: '서울 서초구 양재동', '양재동'
    # 데이터에서 '법정동'이 query를 포함하는 행 검색
    matches = df[df["법정동"].str.contains(query)]

    if matches.empty:
        print(f"[!] '{query}'에 해당하는 법정동을 찾을 수 없습니다.")
    elif len(matches) == 1:
        row = matches.iloc[0]
        print(f"[✓] 일치 항목: {row['법정동']} → PNU: {row['pnu']}")
    else:
        print(f"[i] 여러 항목이 일치합니다 (총 {len(matches)}건):\n")
        for i, row in matches.iterrows():
            print(f"- {row['법정동']} → PNU: {row['pnu']}")

# 명령행 인자 파서 설정
def main():
    parser = argparse.ArgumentParser(description="법정동 주소로 PNU 찾기")
    parser.add_argument("--query", "-q", required=True, help="법정동 주소 입력 (예: '서초구 양재동')")
    args = parser.parse_args()

    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"[X] CSV 파일을 불러올 수 없습니다: {e}")
        sys.exit(1)

    search_pnu(args.query, df)

if __name__ == "__main__":
    main()
