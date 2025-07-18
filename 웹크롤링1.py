import streamlit as st
import requests
from bs4 import BeautifulSoup

# ✅ 출판사/임프린트 추출 (1차)
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

    res = requests.get(search_url, params=params, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")
    first_result_link = soup.select_one("a.book-grid-item")

    if not first_result_link:
        return None

    detail_href = first_result_link["href"]
    detail_url = f"https://bnk.kpipa.or.kr{detail_href}"
    detail_res = requests.get(detail_url, headers=headers)
    detail_soup = BeautifulSoup(detail_res.text, "html.parser")

    # "출판사 / 임프린트" 영역 추출
    pub_info_tag = detail_soup.find("dt", string="출판사 / 임프린트")
    if not pub_info_tag:
        return None

    dd_tag = pub_info_tag.find_next_sibling("dd")
    if dd_tag:
        full_text = dd_tag.get_text(strip=True)
        publisher_name = full_text.split("/")[0].strip()
        return publisher_name

    return None

# ✅ API로 지역 정보 추출 (2차)
def fetch_publisher_region(publisher_name):
    api_url = "https://bnk.kpipa.or.kr/home/v3/addition/adiPblshrInfoList"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Cookie": f"JSESSIONID={st.secrets['kpipa']['session_id']}"
    }
    params = {"ST": publisher_name}
    res = requests.get(api_url, headers=headers, params=params)
    res.raise_for_status()

    try:
        json_data = res.json()
        if "list" in json_data and len(json_data["list"]) > 0:
            first_entry = json_data["list"][0]
            return first_entry.get("region", "지역 정보 없음")
        else:
            return "검색 결과 없음"
    except Exception as e:
        return f"JSON 파싱 오류: {e}"

# ✅ Streamlit UI
st.title("ISBN → 출판사 지역 조회")

isbn_input = st.text_input("ISBN을 입력하세요:")

if st.button("검색"):
    if not isbn_input.strip():
        st.warning("ISBN을 입력해주세요.")
    else:
        st.info("🔍 ISBN으로 출판사명을 조회 중...")
        publisher = get_publisher_name_from_isbn(isbn_input.strip())

        if not publisher:
            st.error("❌ 출판사명을 찾을 수 없습니다.")
        else:
            st.success(f"📘 출판사명: {publisher}")
            st.info("🌐 출판사 지역 정보를 조회 중...")
            region = fetch_publisher_region(publisher)
            st.success(f"🏙️ 지역: {region}")
