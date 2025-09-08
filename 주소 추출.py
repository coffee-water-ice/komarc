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

       # 🔹 검색 결과 추출 (예: 등록구분, 상호, 주소, 영업구분)
results = []
for row in soup.select("table.board tbody tr"):
    cols = row.find_all("td")
    if len(cols) >= 4:
        reg_type = cols[0].get_text(strip=True)   # 등록구분
        name = cols[1].get_text(strip=True)       # 상호
        address = cols[2].get_text(strip=True)    # 주소
        status = cols[3].get_text(strip=True)     # 영업구분
        results.append((reg_type, name, address, status))

# 🔹 출력
if results:
    st.write("### 검색 결과")
    for reg_type, name, address, status in results:
        st.write(f"🏷️ {reg_type} | 📖 **{name}** | 📍 {address} | 🔹 {status}")
else:
    st.warning("검색 결과가 없습니다.")


        # 🔹 출력
        if results:
            st.write("### 검색 결과")
            for title, author, publisher in results:
                st.write(f"📖 **{title}** — {author} / {publisher}")
        else:
            st.warning("검색 결과가 없습니다.")

    except Exception as e:
        st.error(f"오류 발생: {e}")
