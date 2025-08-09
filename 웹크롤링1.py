import streamlit as st
import requests
from bs4 import BeautifulSoup

# ————————————————————————————————————————————
# 1단계: ISBN으로 출판사명 추출
def get_publisher_name_from_isbn(isbn):
    search_url = "https://bnk.kpipa.or.kr/home/v3/addition/search"
    params = {
        "ST": isbn,
        "PG": 1,
        "PG2": 1,
        "DSF": "Y",
        "SO": "weight",
        "DT": "A"
    }
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        res = requests.get(search_url, params=params, headers=headers)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        first_result_link = soup.select_one("a.book-grid-item")
        if not first_result_link:
            return None, "❌ 검색 결과 없음"

        detail_href = first_result_link["href"]
        detail_url = f"https://bnk.kpipa.or.kr{detail_href}"
        detail_res = requests.get(detail_url, headers=headers)
        detail_res.raise_for_status()
        detail_soup = BeautifulSoup(detail_res.text, "html.parser")

        pub_info_tag = detail_soup.find("dt", string="출판사 / 임프린트")
        if not pub_info_tag:
            return None, "❌ '출판사 / 임프린트' 항목을 찾을 수 없습니다."

        dd_tag = pub_info_tag.find_next_sibling("dd")
        if dd_tag:
            full_text = dd_tag.get_text(strip=True)
            publisher_name = full_text.split("/")[0].strip()
            return publisher_name, None

        return None, "❌ 'dd' 태그에서 텍스트를 추출할 수 없습니다."
    except Exception as e:
        return None, f"❌ 예외 발생: {e}"

# ————————————————————————————————————————————
# 2단계: 출판사명으로 지역 정보 검색 (쿠키 + CSRF 포함)
def fetch_publisher_region(publisher_name):
    url = "https://bnk.kpipa.or.kr/home/v3/addition/adiPblshrInfoList/search"

    # TODO: 실제 브라우저에서 복사한 최신 쿠키, CSRF 토큰 넣으세요
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": "session_id = y8s7sUUBInxudrRrAYiWPM7tZx7CrT4ESkG6ITNRlgZWLBvpfbIl4RpVkmExKhhLg8se7UAiWUfCBfimLELDRA==",
        "Host": "bnk.kpipa.or.kr",
        "Origin": "https://bnk.kpipa.or.kr",
        "Referer": "https://bnk.kpipa.or.kr/home/v3/addition/adiPblshrInfoList",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "X-CSRF-TOKEN": "6f6c3b15-ee08-4bc8-9803-1dee123c958f",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"'
    }

    payload = {
        "pageIndex": "1",
        "searchCondition": "pblshrNm",
        "searchKeyword": publisher_name,
        "searchType": "",
        "searchValue": ""
    }

    try:
        res = requests.post(url, headers=headers, data=payload)
        if res.status_code != 200:
            return f"❌ 요청 실패 (HTTP {res.status_code})"

        content_type = res.headers.get("Content-Type", "")
        if "application/json" not in content_type:
            st.error("❌ JSON 응답이 아닙니다. (HTML 등)")
            st.code(res.text[:1000], language="html")
            return "❌ JSON 형식이 아님"

        json_data = res.json()
        result_list = json_data.get("resultList", [])
        if not result_list:
            return "❌ 검색 결과 없음"

        region = result_list[0].get("region", "❓ 지역 정보 없음")
        return region

    except Exception as e:
        return f"❌ 예외 발생: {e}"

# ————————————————————————————————————————————
# Streamlit UI
st.title("📚 ISBN → 출판사 → 지역 정보 조회 (KPIPA)")

isbn_input = st.text_input("ISBN을 입력하세요 (예: 9791130649672)")

if st.button("검색"):
    if not isbn_input.strip():
        st.warning("ISBN을 입력해주세요.")
    else:
        with st.spinner("🔍 ISBN으로 출판사 조회 중..."):
            publisher, error_msg = get_publisher_name_from_isbn(isbn_input.strip())

        if error_msg:
            st.error(error_msg)
        elif publisher:
            st.success(f"✅ 출판사명: {publisher}")

            with st.spinner("🌐 출판사 지역 조회 중..."):
                region_info = fetch_publisher_region(publisher)

            if "❌" in region_info:
                st.error(region_info)
            else:
                st.success(f"🏙️ 지역: {region_info}")
