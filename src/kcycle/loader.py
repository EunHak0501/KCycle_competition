import re
import pandas as pd

def load_data():

    # 1) CSV 읽기
    info_data   = pd.read_csv('./data/race_info.csv',   low_memory=False)
    result_data = pd.read_csv('./data/race_results.csv', low_memory=False)

    # 2) result_data를 long 포맷으로 변환하며 rank 부여
    records = []
    for _, row in result_data.iterrows():
        연도 = int(row['연도'])
        회차 = str(row['회차']).zfill(2)
        일차 = int(row['일차'])
        경주 = row['경주']
        for rank in (1, 2, 3):
            num_col = f"{rank}착 번호"
            num = row[num_col]
            if pd.isna(num) or str(num).strip() in ('', '-'):
                continue
            records.append({
                '연도': 연도,
                '회차': 회차,
                '일차': 일차,
                '경주': 경주,
                '번호': str(num).strip(),
                'rank': rank
            })

    res_long = pd.DataFrame(records)

    # 3) '경주' 컬럼에서 지역/번호 분리 (정수형 인덱스로 접근)
    extracted = res_long['경주'].str.extract(r'(.+?)(\d+)$')
    res_long['경주지역'] = extracted[0]
    res_long['경주번호'] = extracted[1].str.zfill(2)

    # 4) info_data key 컬럼 타입 통일
    info_data['연도']     = info_data['연도'].astype(int)
    info_data['회차']     = info_data['회차'].astype(int).astype(str).str.zfill(2)
    info_data['일차']     = info_data['일차'].astype(int)
    info_data['경주번호'] = info_data['경주번호'].astype(str).str.zfill(2)
    info_data['번호']     = info_data['번호'].astype(str)

    # 5) merge 로 rank 붙이기
    info_with_rank = info_data.merge(
        res_long[[
            '연도','회차','일차',
            '경주지역','경주번호','번호','rank'
        ]],
        on=['연도','회차','일차','경주지역','경주번호','번호'],
        how='left'
    )

    return info_with_rank
