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
            "Referer": "https://bnk.kpipa.or.kr"
        }
        params = {
            "ST": publisher_name
        }

        response = requests.get(search_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # 전체 HTML 크기 로그
        st.write(f"🔎 2차 검색 응답 HTML 크기: {len(response.text)} bytes")

        # 테이블 선택 여부 확인
        table = soup.select_one("table.table.srch")
        if not table:
            st.error("❌ 'table.table.srch' 요소를 찾지 못했습니다.")
            st.code(response.text[:2000], language="html")  # 최대 2000자만 출력
            return None, None

        row = table.select_one("tbody tr")
        if not row:
            st.error("❌ 결과 테이블에 <tbody><tr> 행이 없습니다.")
            st.code(table.prettify()[:2000], language="html")
            return None, None

        td_list = row.find_all("td")
        st.write(f"🔎 행 내 td 개수: {len(td_list)}")
        if len(td_list) < 3:
            st.error("❌ td 태그가 3개 미만입니다.")
            st.code(str(row), language="html")
            return None, None

        publisher = td_list[1].get_text(strip=True)
        location = td_list[2].get_text(strip=True)
        st.write(f"🔎 추출된 출판사명: {publisher}")
        st.write(f"🔎 추출된 지역: {location}")

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
