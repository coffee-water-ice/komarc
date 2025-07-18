import streamlit as st
import requests
from bs4 import BeautifulSoup

# ✅ 1단계: ISBN으로 출판사명 추출
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
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

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

# ✅ 2단계: 출판사명으로 지역 정보 검색 (API)
def fetch_publisher_region(publisher_name):
    api_url = "https://bnk.kpipa.or.kr/home/v3/addition/adiPblshrInfoList"
    session_id = st.secrets.get("kpipa", {}).get("session_id")

    if not session_id:
        return "❌ JSESSIONID가 설정되지 않았습니다 (st.secrets 확인 필요)"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Cookie": f"JSESSIONID={session_id}"
    }
    params = {"ST": publisher_name}

    try:
        res = requests.get(api_url, headers=headers, params=params)

        content_type = res.headers.get("Content-Type", "")
        if "application/json" not in content_type:
            return f"❌ 예상과 다른 응답 (Content-Type: {content_type})\n응답 예시: {res.text[:500]}"

        json_data = res.json()
        if "list" in json_data and len(json_data["list"]) > 0:
            region = json_data["list"][0].get("region", "지역 정보 없음")
            return region
        else:
            return f"❌ API 검색 결과 없음\nJSON 원문: {json_data}"

    except Exception as e:
        return f"❌ JSON 파싱 오류: {e}\n\n응답 예시: {res.text[:500]}"

# ✅ Streamlit 인터페이스
st.title("📚 ISBN → 출판사 → 지역 정보 조회")

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
