import re, time, argparse
import requests, pandas as pd
from bs4 import BeautifulSoup
from tqdm.auto import tqdm


def get_soup(url: str, headers=None) -> BeautifulSoup:
    resp = requests.get(url, headers=headers or {}, timeout=10)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def parse_race_results(html, year, 회차, 일차, 날짜):
    """
    html: BeautifulSoup 객체 – <tbody> 혹은 <table> 태그에서 파싱
    """
    import re
    import pandas as pd

    # td 안에 .name 이 여러 개 있을 때 모두 뽑아내고,
    # 없으면 빈 문자열 반환
    def parse_player_td(td):
        divs = td.select('.name')
        nums, names = [], []
        for div in divs:
            no_tag  = div.select_one('.sign')
            nm_tag  = div.select_one('.player')
            if no_tag and nm_tag:
                nums.append(no_tag.get_text(strip=True))
                names.append(nm_tag.get_text(strip=True))
        return '/'.join(nums), '/'.join(names)

    get_raw = lambda td: td.get_text(separator="|", strip=True)

    records = []
    for row in html.select('tr'):
        tds = row.find_all(['th','td'])
        # 0번 th 에서 경주 아이디(예: "광명01") 뽑기
        race_label = row.select_one('th span.mark').get_text(strip=True)

        # 1~3번 td: 입상 선수
        p1_no, p1_nm = parse_player_td(tds[1])
        p2_no, p2_nm = parse_player_td(tds[2])
        p3_no, p3_nm = parse_player_td(tds[3])

        # 4~6: 배당
        ys  = get_raw(tds[4])  # 연승식
        ss  = get_raw(tds[5])  # 쌍승식
        bs  = get_raw(tds[6])  # 복승식
        sb  = get_raw(tds[7])  # 삼복승식
        ps  = get_raw(tds[8])  # 쌍복승식
        ts  = get_raw(tds[9])  # 삼쌍승식

        records.append({
            '연도':       year,
            '회차':       회차,
            '일차':       일차,
            '경주':       race_label,
            '1착 번호':   p1_no,
            '1착 이름':   p1_nm,
            '2착 번호':   p2_no,
            '2착 이름':   p2_nm,
            '3착 번호':   p3_no,
            '3착 이름':   p3_nm,
            '연승식':     ys,
            '쌍승식':     ss,
            '복승식':     bs,
            '삼복승식':   sb,
            '쌍복승식':   ps,
            '삼쌍승식':   ts,
        })

    return pd.DataFrame(records)


def crawl_yearly_results(year: int, pause: float = 0.5) -> pd.DataFrame:
    """
    주어진 연도의 모든 회차·일차에 대해 경주 결과를 크롤링하여
    하나의 DataFrame으로 반환합니다.
    """
    # 1) 해당 year의 (회차, 일차, 날짜) 리스트 가져오기
    soup = get_soup(f"https://www.kcycle.or.kr/race/result/general/{year}/01/1")
    opts = soup.select('select[name="tmsDayOrd"] option')
    days = []
    for op in opts:
        m = re.search(r"\((\d+)회 (\d+)일\)\s+(\d{1,2})월\s*(\d{1,2})일", op.text)
        if not m:
            continue
        c, d = m.group(1), m.group(2)
        mon, day = int(m.group(3)), int(m.group(4))
        ymd = f"{year}{mon:02d}{day:02d}"
        days.append((c, d, ymd))
    days.reverse()

    all_results = []
    for 회차, 일차, 날짜 in tqdm(days, desc=f"{year} 결과 크롤링", unit="일차"):
        try:
            # 2) 페이지 요청
            url = f"https://www.kcycle.or.kr/race/result/general/{year}/{int(회차):02d}/{일차}"
            day_soup = get_soup(url, headers={"User-Agent":"Mozilla/5.0"})

            # 3) tbody 추출
            tbody = day_soup.select_one("div.comDataTable table.excel_table tbody")
            if not tbody:
                tqdm.write(f"⚠️ tbody 없음: {year}-{회차}-{일차}")
                continue

            # 4) 파싱
            df = parse_race_results(
                html=tbody,
                year=year,
                회차=회차,
                일차=일차,
                날짜=날짜
            )
            if not df.empty:
                all_results.append(df)

        except Exception as e:
            tqdm.write(f"❌ 오류: {year}-{회차}-{일차}: {e}")

        time.sleep(pause)

    if all_results:
        return pd.concat(all_results, ignore_index=True)
    else:
        return pd.DataFrame()


def parse_years_arg(s: str):
    """ "2016-2020" 또는 "2016,2018,2020" 형식 """
    if '-' in s:
        a, b = s.split('-', 1)
        return list(range(int(a), int(b) + 1))
    return [int(x) for x in s.split(',')]


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="경주 결과 크롤러")
    p.add_argument(
        "--years", required=True,
        help="크롤링할 연도 범위. 예: 2016-2025 또는 2016,2018,2020"
    )
    p.add_argument(
        "--pause", type=float, default=0.5,
        help="각 페이지 요청 사이 대기 시간(초)"
    )
    p.add_argument(
        "--output", default="./data/race_results.csv",
        help="저장할 CSV 파일명"
    )
    args = p.parse_args()

    years = parse_years_arg(args.years)
    result_dfs = []
    for y in years:
        df_y = crawl_yearly_results(y, pause=args.pause)
        if not df_y.empty:
            result_dfs.append(df_y)

    if result_dfs:
        df_all = pd.concat(result_dfs, ignore_index=True)
        df_all.to_csv(args.output, index=False, encoding="utf-8-sig")
        print(f"✅ 저장 완료: {args.output} ({len(df_all)} rows)")
    else:
        print("⚠️ 수집된 데이터가 없습니다.")

    # 예시
    # python kcycle_result_crawler.py --years 2017-2025 --pause 0.3