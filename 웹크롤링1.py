import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import streamlit as st
import copy

# ✅ Google Sheets 연결 함수
def connect_to_sheet():
    json_key = dict(st.secrets["gspread"])  # ✅ dict()로 복사
    json_key["private_key"] = json_key["private_key"].replace('\\n', '\n')

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key, scope)
    client = gspread.authorize(creds)
    sheet = client.open("출판사 DB").worksheet("시트3")
    return sheet


# 🔍 BNK 검색 결과 → 출판사/인프린트 정보 추출
def get_publisher_from_kpipa(isbn):
    search_url = "https://bnk.kpipa.or.kr/front/search/bookSearchListAjax.do"
    detail_url_base = "https://bnk.kpipa.or.kr/front/search/bookDetailView.do?book_seq="

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "https://bnk.kpipa.or.kr/html/searchList.php",
        "User-Agent": "Mozilla/5.0"
    }

    data = {
        "searchKeyword": isbn,
        "searchType": "isbn",
        "page": "1"
    }

    response = requests.post(search_url, headers=headers, data=data)
    soup = BeautifulSoup(response.text, "html.parser")

    first_result = soup.select_one("#ST")
    if not first_result:
        return "검색 결과 없음"

    href = first_result["href"]
    # href = "/front/search/bookDetailView.do?book_seq=123456"
    if "book_seq=" not in href:
        return "상세페이지 링크 없음"

    book_seq = href.split("book_seq=")[-1]
    detail_url = detail_url_base + book_seq

    # 상세 페이지 접근
    detail_response = requests.get(detail_url)
    detail_soup = BeautifulSoup(detail_response.text, "html.parser")

    th = detail_soup.find("th", string="출판사/인프린트")
    if not th:
        return "출판사 정보 없음"

    publisher = th.find_next_sibling("td").get_text(strip=True)
    return publisher

# 📝 Google Sheet 업데이트 (C열: 출판사명)
def update_sheet_with_publisher(isbn):
    sheet = connect_to_sheet()
    isbn_list = sheet.col_values(1)  # A열: ISBN 리스트

    for idx, val in enumerate(isbn_list[1:], start=2):  # 첫 행 제외
        if val == isbn:
            publisher = get_publisher_from_kpipa(isbn)
            sheet.update_cell(idx, 3, publisher)  # C열 = 3번째 열
            return f"✅ ISBN {isbn} → 출판사명: {publisher}"
    return f"❌ ISBN {isbn} 이(가) 시트에서 발견되지 않음"
#
import streamlit as st

st.title("📚 ISBN으로 출판사명 추출 (BNK + Google Sheets)")

isbn_input = st.text_input("ISBN 입력")

if st.button("출판사명 추출 및 시트 반영"):
    if isbn_input:
        result = update_sheet_with_publisher(isbn_input.strip())
        st.success(result)
    else:
        st.warning("ISBN을 입력해주세요.")
