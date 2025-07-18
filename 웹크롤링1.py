import streamlit as st
import requests
from bs4 import BeautifulSoup

# 1️⃣ ISBN → 출판사명 추출
def get_publisher_name_from_isbn(isbn):
    try:
        search_url = "https://bnk.kpipa.or.kr/home/v3/addition/search"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://bnk.kpipa.or.kr/",
            "Cookie": "JSESSIONID=y8s7sUUBInxudrRrAYiWPM7tZx7CrT4ESkG6ITNRlgZWLBvpfbIl4RpVkmExKhhLg8se7UAiWUfCBfimLELDRA=="
        }
        params = {
            "ST": isbn,
            "DSF": "Y"
        }

        response = requests.get(search_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        first_result = soup.select_one("a.book-grid-item")
        if not first_result or not first_result.get("href"):
            return None

        detail_url = "https://bnk.kpipa.or.kr" + first_result["href"]
        detail_res = requests.get(detail_url, headers=headers, timeout=10)
        detail_res.raise_for_status()
        detail_soup = BeautifulSoup(detail_res.text, "html.parser")

        dt_tag = detail_soup.find("dt", string=lambda t: t and "출판사" in t)
        if not dt_tag:
            return None

        dd_tag = dt_tag.find_next_sibling("dd")
        if not dd_tag:
            return None

        full_text = dd_tag.get_text(strip=True)
        publisher_main = full_text.split(" / ")[0]  # ㈜다산북스
        return publisher_main

    except Exception as e:
        st.error("❌ [ISBN 검색 오류]")
        st.exception(e)
        return None

# 2️⃣ 출판사명 → 출판사명 및 지역 정보 추출 (업데이트된 선택자 반영)
def get_publisher_location(publisher_name):
    try:
        search_url = "https://bnk.kpipa.or.kr/home/v3/addition/adiPblshrInfoList"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://bnk.kpipa.or.kr/"
        }
        params = {
            "ST": publisher_name
        }

        response = requests.get(search_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # 테이블에서 첫 번째 결과 행 선택
        row = soup.select_one("table.table.srch tbody tr")
        if not row:
            return None, None

        td_list = row.find_all("td")
        if len(td_list) < 3:
            return None, None

        publisher = td_list[1].get_text(strip=True)  # 출판사명
        location = td_list[2].get_text(strip=True)   # 지역

        return publisher, location

    except Exception as e:
        st.error("❌ [출판사 지역 검색 오류]")
        st.exception(e)
        return None, None

# ▶️ Streamlit UI
st.title("📚 ISBN → 출판사 및 지역 정보 추출기")

isbn_input = st.text_input("🔍 ISBN을 입력하세요")

if st.button("정보 추출하기"):
    if isbn_input.strip():
        with st.spinner("1️⃣ ISBN으로 출판사명 검색 중..."):
            publisher = get_publisher_name_from_isbn(isbn_input.strip())

        if publisher:
            st.success(f"📘 1차 결과 - 출판사명: {publisher}")

            with st.spinner("2️⃣ 출판사명으로 출판사명 및 지역 검색 중..."):
                pub_name, location = get_publisher_location(publisher)

            if pub_name and location:
                st.success(f"📚 2차 결과 - 출판사명: {pub_name}")
                st.success(f"📍 지역: {location}")
            else:
                st.warning("⚠️ 출판사 지역 정보를 찾을 수 없습니다.")
        else:
            st.warning("⚠️ ISBN으로부터 출판사 정보를 찾을 수 없습니다.")
    else:
        st.warning("⚠️ ISBN을 입력해주세요.")
