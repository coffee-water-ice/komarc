import streamlit as st
import requests
from bs4 import BeautifulSoup

st.title("문화체육관광부 도서정보 검색")

# 검색어 입력
query = st.text_input("검색어를 입력하세요:", "그린애플")

if st.button("검색하기"):
    # 🔹 검색 URL 구성
    url = "https://book.mcst.go.kr/html/searchList.php"
    params = {
        "search_area": "전체",
        "search_state": "1",
        "search_kind": "1",
        "search_type": "1",
        "search_word": query
    }

    try:
        # 🔹 GET 요청
        response = requests.get(url, params=params)
        response.raise_for_status()

        # 🔹 BeautifulSoup 파싱
        soup = BeautifulSoup(response.text, "html.parser")

        # 🔹 검색 결과 추출 (예: 책 제목, 저자, 출판사)
        results = []
        for row in soup.select(".searchList tr")[1:]:  # 첫 행은 헤더라서 제외
            cols = row.find_all("td")
            if len(cols) >= 4:
                title = cols[1].get_text(strip=True)
                author = cols[2].get_text(strip=True)
                publisher = cols[3].get_text(strip=True)
                results.append((title, author, publisher))

        # 🔹 출력
        if results:
            st.write("### 검색 결과")
            for title, author, publisher in results:
                st.write(f"📖 **{title}** — {author} / {publisher}")
        else:
            st.warning("검색 결과가 없습니다.")

    except Exception as e:
        st.error(f"오류 발생: {e}")
