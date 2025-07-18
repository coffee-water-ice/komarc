import streamlit as st
import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 📌 Google Sheets 연결
def connect_to_sheet():
    json_key = dict(st.secrets["gspread"])
    json_key["private_key"] = json_key["private_key"].replace('\\n', '\n')
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key, scope)
    client = gspread.authorize(creds)
    sheet = client.open("출판사 DB").worksheet("시트3")
    return sheet

# 🔍 BNK API를 이용한 출판사/임프린트 정보 추출
def get_publisher_from_kpipa(isbn):
    search_url = "https://bnk.kpipa.or.kr/home/v3/addition/search"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://bnk.kpipa.or.kr/",
        "Cookie": "JSESSIONID=y8s7sUUBInxudrRrAYiWPM7tZx7CrT4ESkG6ITNRlgZWLBvpfbIl4RpVkmExKhhLg8se7UAiWUfCBfimLELDRA=="
    }
    params = {
        "TB": "",
        "PG": 1,
        "PG2": 1,
        "ST": isbn,
        "DO": "",
        "DSF": "Y",
        "DST": "",
        "SR": "",
        "SO": "weight",
        "DT": "A",
        "DTS": "",
        "DTE": "",
        "PT": "",
        "KD": "",
        "SB": ""
    }

    try:
        response = requests.get(search_url, headers=headers, params=params, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        first_result = soup.select_one("li.list > a")
        if not first_result or not first_result.get("href"):
            return "검색 결과 없음"

        detail_url = "https://bnk.kpipa.or.kr" + first_result["href"]
        detail_res = requests.get(detail_url, headers=headers, timeout=10)
        detail_soup = BeautifulSoup(detail_res.text, "html.parser")

        th_tag = detail_soup.find("th", string="출판사/인프린트")
        if not th_tag:
            return "출판사 정보 없음"

        publisher = th_tag.find_next_sibling("td").get_text(strip=True)
        return publisher
    except Exception as e:
        return f"에러 발생: {e}"

# 📝 시트 업데이트 함수
def update_sheet_with_publisher(isbn):
    sheet = connect_to_sheet()
    isbn_list = sheet.col_values(1)  # A열: ISBN

    for idx, val in enumerate(isbn_list[1:], start=2):  # 2행부터
        if val.strip() == isbn.strip():
            publisher = get_publisher_from_kpipa(isbn)
            sheet.update_cell(idx, 3, publisher)  # C열 = 3
            return f"✅ ISBN {isbn} → 출판사/인프린트: {publisher}"
    return f"❌ ISBN {isbn} 이(가) 시트에서 발견되지 않음"

# ▶️ Streamlit UI
st.title("📚 KPIPA 출판사/인프린트 추출기")

isbn_input = st.text_input("🔍 ISBN을 입력하세요")

if st.button("출판사 정보 추출 및 시트에 반영"):
    if isbn_input.strip():
        with st.spinner("검색 중입니다..."):
            result = update_sheet_with_publisher(isbn_input.strip())
        st.success(result)
    else:
        st.warning("ISBN을 입력해주세요.")
