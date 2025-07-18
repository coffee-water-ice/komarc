import streamlit as st
import requests
from bs4 import BeautifulSoup

# 🔍 KPIPA API를 통한 출판사 정보 추출 (임프린트 포함)
def get_publisher_from_kpipa(isbn, show_html=False):
    try:
        search_url = "https://bnk.kpipa.or.kr/home/v3/addition/search"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://bnk.kpipa.or.kr/",
            "Cookie": "JSESSIONID=y8s7sUUBInxudrRrAYiWPM7tZx7CrT4ESkG6ITNRlgZWLBvpfbIl4RpVkmExKhhLg8se7UAiWUfCBfimLELDRA=="
        }
        params = {
            "TB": "", "PG": 1, "PG2": 1, "ST": isbn, "DO": "",
            "DSF": "Y", "DST": "", "SR": "", "SO": "weight",
            "DT": "A", "DTS": "", "DTE": "", "PT": "", "KD": "", "SB": ""
        }

        response = requests.get(search_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        if show_html:
            st.subheader("🔍 검색 결과 HTML 구조")
            st.code(soup.prettify(), language="html")

        first_result = soup.select_one("a.book-grid-item")
        if not first_result:
            st.warning("⚠️ 검색 결과 없음 - 해당 ISBN에 대한 도서를 찾지 못했습니다.")
            return None

        if not first_result.get("href"):
            st.warning("⚠️ 상세 링크 없음 - 결과는 있지만 <a href> 태그가 누락되었습니다.")
            return None

        detail_url = "https://bnk.kpipa.or.kr" + first_result["href"]
        detail_res = requests.get(detail_url, headers=headers, timeout=10)
        detail_res.raise_for_status()
        detail_soup = BeautifulSoup(detail_res.text, "html.parser")

        if show_html:
            st.subheader("🔍 상세 페이지 HTML 구조")
            st.code(detail_soup.prettify(), language="html")

        # ⛳ 출판사 / 임프린트 항목 찾기
        dt_tag = detail_soup.find("dt", string=lambda t: t and "출판사" in t)
        if not dt_tag:
            st.warning("⚠️ 상세페이지에서 '출판사 / 임프린트' 항목을 찾지 못했습니다.")
            return None

        dd_tag = dt_tag.find_next_sibling("dd")
        if not dd_tag:
            st.warning("⚠️ 출판사 정보를 담고 있는 <dd> 태그를 찾지 못했습니다.")
            return None

        full_text = dd_tag.get_text(strip=True)
        publisher_main = full_text.split(" / ")[0]  # 앞부분만 출력
        return publisher_main

    except Exception as e:
        st.error("❌ 오류 발생:")
        st.exception(e)
        return None

# ▶️ Streamlit UI
st.title("📚 KPIPA 출판사 추출기 (임프린트 제외)")

isbn_input = st.text_input("🔍 ISBN을 입력하세요")
show_html = st.checkbox("📄 HTML 구조 보기 (디버깅용)")

if st.button("출판사 정보 추출"):
    if isbn_input.strip():
        with st.spinner("검색 중입니다..."):
            publisher = get_publisher_from_kpipa(isbn_input.strip(), show_html)
        if publisher:
            st.success(f"✅ 출판사: {publisher}")
        else:
            st.error("❌ 출판사 정보를 찾을 수 없습니다.")
    else:
        st.warning("⚠️ ISBN을 입력해주세요.")
