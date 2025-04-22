# race_crawler.py

import re
import time
import requests
import pandas as pd
import argparse
from bs4 import BeautifulSoup
from tqdm.auto import tqdm

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36")
}

def get_soup(url: str) -> BeautifulSoup:
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def get_race_day_list(year: int):
    soup = get_soup(f"https://www.kcycle.or.kr/race/card/decision/{year}/01/1")
    opts   = soup.select('select[name="tmsDayOrd"] option')
    result = []
    for op in opts:
        m = re.search(
            r"\((\d+)회 (\d+)일\)\s+(\d{1,2})월\s*(\d{1,2})일",
            op.text
        )
        if not m:
            continue
        회차, 일차 = m.group(1), m.group(2)
        mon, day  = int(m.group(3)), int(m.group(4))
        yyyymmdd = f"{year}{mon:02d}{day:02d}"
        result.append((회차, 일차, yyyymmdd))
    return list(reversed(result))

def parse_one_race(
    year: int,
    회차: str,
    일차: str,
    날짜: str,
    region: str = "광명",
    race_no: str = None
) -> pd.DataFrame:
    """
    year, 회차, 일차, 날짜 로 페이지를 열고,
    region (e.g. "광명") 과 race_no (e.g. "01") 조합에 해당하는 경주를 파싱합니다.
    race_no 가 None 이면 해당 day 의 첫 번째 region 경주를 가져옵니다.
    """
    url  = f"https://www.kcycle.or.kr/race/card/decision/{year}/{회차}/{일차}"
    soup = get_soup(url)

    # 1) swiper 버튼 중에서 region 과 (race_no 일치시) 선택
    btns = soup.select("div.swiper-slide button")
    target_onclick = None
    for btn in btns:
        reg = btn.select_one(".region").text.strip()
        num = btn.select_one(".date").text.strip()
        if reg == region and (race_no is None or num == race_no):
            target_onclick = btn["onclick"]
            break
    if not target_onclick:
        raise ValueError(f"{region}{race_no or ''} 경주 버튼을 찾을 수 없습니다.")

    race_id = re.search(r"scrlMoveTo\(['\"](.*?)['\"]", target_onclick).group(1)
    race    = soup.find("div", id=race_id)

    # 2) h2 제목에서 지역·번호·종류·시간 추출
    title = race.select_one("h2").get_text(strip=True)
    m = re.match(r"(.+?)\s+(\d+)경주\s*\(\s*(\S+)\s+([\d:]+)\s*\)", title)
    if m:
        경주지역, 경주번호, 경주종류, 경주시간 = m.groups()
    else:
        경주지역 = 경주번호 = 경주종류 = 경주시간 = ""

    # 3) 표0: 기본 선수 정보 (기수/나이, 7명만)
    base = []
    for r in race.select("table.excel_table")[0].select("tbody tr"):
        name_a = r.select_one(".name a")
        if not name_a:
            continue
        num = r.select_one(".sign").text.strip()
        oth = r.select_one(".other").text.strip()
        mm = re.match(r"(\d+)기/(\d+)세", oth)
        기수, 나이 = (mm.group(1), mm.group(2)) if mm else ("", "")
        tds = [td.get_text(strip=True) for td in r.select("td")]
        if len(tds) < 14:
            continue  # 결장 처리
        base.append({
            "이름":        name_a.text.strip(),
            "번호":        num,
            "기수":        기수,
            "나이":        나이,
            "기어배수":    tds[0],
            "200m":       tds[1],
            "훈련지":      tds[2],
            "승률":        tds[3],
            "연대율":      tds[4],
            "삼연대율":    tds[5],
            "입상/출전":   tds[6],
            "선행":        tds[7],
            "젖히기":      tds[8],
            "추입":        tds[9],
            "마크":        tds[10],
            "등급조정":    tds[11],
            "최근3득점":   tds[12],
            "최근3순위":   tds[13],
        })
    df_base = pd.DataFrame(base)
    if len(df_base) != 7:
        # 7명 아니면 빈 데이터프레임 리턴
        return pd.DataFrame()

    # 4) 표1: 훈련 일지
    train = []
    for r in race.select("table.excel_table")[1].select("tbody tr"):
        name_a = r.select_one(".name a")
        if not name_a:
            continue
        tds = [td.get_text(strip=True) for td in r.select("td")]
        train.append({
            "이름":        name_a.text.strip(),
            "훈련일수":    tds[0],
            "훈련동참자":  tds[1],
            "훈련내용":    tds[2],
        })
    df_train = pd.DataFrame(train)

    # 5) 표2: 최근 성적
    rec = []
    for r in race.select("table.excel_table")[2].select("tbody tr"):
        name_a = r.select_one(".name a")
        if not name_a:
            continue
        tds = [td.get_text(strip=True) for td in r.select("td")]
        rec.append({
            "이름":          name_a.text.strip(),
            "최근3_장소일자": tds[0], "최근3_1일": tds[1], "최근3_2일": tds[2], "최근3_3일": tds[3],
            "최근2_장소일자": tds[4], "최근2_1일": tds[5], "최근2_2일": tds[6], "최근2_3일": tds[7],
            "최근1_장소일자": tds[8], "최근1_1일": tds[9], "최근1_2일": tds[10], "최근1_3일": tds[11],
            "금회_1일":      tds[12], "금회_2일": tds[13],
            "금회_3일":      tds[14] if len(tds) > 14 else "",
        })
    df_rec = pd.DataFrame(rec)

    # 6) 병합
    df = (
        df_base
        .merge(df_train, on="이름", how="left")
        .merge(df_rec,   on="이름", how="left")
    )

    # 7) 앞에 메타컬럼 삽입
    df.insert(0, '경주시간', 경주시간)
    df.insert(0, '경주종류', 경주종류)
    df.insert(0, '경주번호', 경주번호)
    df.insert(0, '경주지역', 경주지역)
    df.insert(0, '일차',   일차)
    df.insert(0, '회차',   회차)
    df.insert(0, '연도',   year)
    df.insert(0, '날짜',   날짜)

    return df

def parse_all_races(year: int, 회차: str, 일차: str, 날짜: str):
    url  = f"https://www.kcycle.or.kr/race/card/decision/{year}/{회차}/{일차}"
    soup = get_soup(url)

    # 1) swiper 에 있는 모든 버튼에서 race_id 추출
    race_ids = []
    for btn in soup.select("div.swiper-slide button"):
        onclick = btn["onclick"]
        m = re.search(r'scrlMoveTo\(["\']([^"\']+)["\']', onclick)
        if m:
            race_ids.append(m.group(1))

    all_dfs = []
    for rid in race_ids:
        race = soup.find("div", id=rid)
        if not race:
            continue

        title = race.select_one("h2").get_text(strip=True)
        m = re.match(
            r"(.+?)\s+(\d+)경주\s*\(\s*(\S+)\s+([\d:]+)\s*\)",
            title
        )
        if m:
            경주지역, 경주번호, 경주종류, 경주시간 = m.groups()
        else:
            경주지역 = 경주번호 = 경주종류 = 경주시간 = ""

        # print(경주지역, 경주번호, 경주종류, 경주시간)
        # (가) 기본 선수 정보
        base = []
        for r in race.select("table.excel_table")[0].select("tbody tr"):
            a = r.select_one('.name a')
            num = r.select_one('.sign')
            other_tag = r.select_one('.other')
            if not a:
                continue

            # "01기/51세" → 기수, 나이 분리
            oth = other_tag.get_text(strip=True)
            m   = re.match(r"(\d+)기/(\d+)세", oth)
            기수 = m.group(1) if m else ""
            나이 = m.group(2) if m else ""

            t = [td.get_text(strip=True) for td in r.select("td")]
            # 결장 선수가 있으면 생략
            if len(t) < 14:
                continue

            base.append({
                "이름": a.text.strip(),   "번호": num.text.strip(),
                "기수": 기수,             "나이": 나이,
                "기어배수": t[0],         "200m": t[1],
                "훈련지": t[2],           "승률": t[3],
                "연대율": t[4],           "삼연대율": t[5],
                "입상/출전": t[6],        "선행": t[7],
                "젖히기": t[8],           "추입": t[9],
                "마크": t[10],            "등급조정": t[11],
                "최근3득점": t[12],       "최근3순위": t[13],
            })
        df_base = pd.DataFrame(base)

        # 참가 선수가 7명이 아니면 생략
        if len(df_base) != 7:
            continue

        # (나) 훈련 상담
        train = []
        for r in race.select("table.excel_table")[1].select("tbody tr"):
            a = r.select_one('.name a')
            if not a:
                continue
            t = [td.get_text(strip=True) for td in r.select("td")]
            train.append({
                "이름": a.text.strip(),
                "훈련일수": t[0],
                "훈련동참자": t[1],
                "훈련내용": t[2],
            })
        df_train = pd.DataFrame(train)

        # (다) 최근 성적
        rec = []
        for r in race.select("table.excel_table")[2].select("tbody tr"):
            a = r.select_one('.name a')
            if not a:
                continue
            t = [td.get_text(strip=True) for td in r.select("td")]
            rec.append({
                "이름":        a.text.strip(),
                "최근3_장소일자": t[0],  "최근3_1일":  t[1],  "최근3_2일":  t[2],  "최근3_3일":  t[3],
                "최근2_장소일자": t[4],  "최근2_1일":  t[5],  "최근2_2일":  t[6],  "최근2_3일":  t[7],
                "최근1_장소일자": t[8],  "최근1_1일":  t[9],  "최근1_2일": t[10], "최근1_3일": t[11],
                "금회_1일":      t[12], "금회_2일":    t[13],
                "금회_3일":      t[14] if len(t) > 14 else ""
            })
        df_rec = pd.DataFrame(rec)

        # 병합
        df = (
            df_base
            .merge(df_train, on="이름", how="left")
            .merge(df_rec,   on="이름", how="left")
        )

        # h2에서 “XX경주” 번호 추출
        title = race.select_one("h2").get_text(strip=True)
        no = re.search(r"(\d+)경주", title)
        경주번호 = no.group(1) if no else ""

        # 맨 앞에 공통 컬럼 추가
        df.insert(0, '경주시간',  경주시간)
        df.insert(0, '경주종류',  경주종류)
        df.insert(0, '경주번호',  경주번호)
        df.insert(0, '경주지역',  경주지역)
        df.insert(0, '일차',    일차)
        df.insert(0, '회차',    회차)
        df.insert(0, '연도',    year)
        df.insert(0, '날짜',    날짜)

        all_dfs.append(df)

    return pd.concat(all_dfs, ignore_index=True)


def crawl_year(year: int, pause: float = 1.0) -> pd.DataFrame:
    days = get_race_day_list(year)
    year_dfs = []
    for 회차, 일차, 날짜 in tqdm(days, desc=f"{year}년 크롤링", unit="일차"):
        try:
            df = parse_all_races(year, 회차, 일차, 날짜)
            if not df.empty:
                year_dfs.append(df)
            # tqdm.write(f"[{year}] {회차}회차 {일차}일차 → {len(df)}건")
        except Exception as e:
            tqdm.write(f"⚠️ 실패: {year}-{회차}-{일차} ({e})")
        time.sleep(pause)
    return pd.concat(year_dfs, ignore_index=True) if year_dfs else pd.DataFrame()

# ───── 커맨드라인 인자 처리 ────────────────────────────────────────────────

def parse_years_arg(s: str):
    """ "2017-2020" 이나 "2017,2019,2021" 형식 파싱 """
    if '-' in s:
        start, end = s.split('-', 1)
        return list(range(int(start), int(end) + 1))
    else:
        return [int(y) for y in s.split(',')]

if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="경륜 출주표 크롤러: --years 2016-2025 형식으로 지정"
    )
    p.add_argument(
        "--years", required=True,
        help="크롤링할 연도. 예: 2016-2020 또는 2017,2019,2021"
    )
    p.add_argument(
        "--pause", type=float, default=0.5,
        help="요청 사이 대기 시간(초)"
    )
    p.add_argument(
        "--output", default="./data/kcycle_race_inputs.csv",
        help="결과를 저장할 CSV 파일명"
    )
    args = p.parse_args()

    years = parse_years_arg(args.years)
    all_dfs = []
    for y in years:
        df_y = crawl_year(y, pause=args.pause)
        if not df_y.empty:
            all_dfs.append(df_y)

    if all_dfs:
        df_all = pd.concat(all_dfs, ignore_index=True)
        df_all.to_csv(args.output, index=False, encoding="utf-8-sig")
        print(f"✅ 저장 완료: {args.output} ({len(df_all)} rows)")
    else:
        print("⚠️ 수집된 데이터가 없습니다.")

    # 예시
    # python kcycle_race_crawler.py --years 2017-2025 --pause 0.5 --output ./data/train.csv
