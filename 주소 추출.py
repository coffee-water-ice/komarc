import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd

st.title("문화체육관광부 도서정보 검색")

# 여러 검색어 입력 (줄바꿈으로 구분)
query_text = st.text_area("검색어를 입력하세요 (여러 개는 줄바꿈으로 구분):", "그린애플\n시공주니어")

if st.button("검색하기"):
    queries = [q.strip() for q in query_text.split("\n") if q.strip()]
    all_results = []

    for query in queries:
        st.subheader(f"🔎 검색어: {query}")

        url = "https://book.mcst.go.kr/html/searchList.php"
        params = {
            "search_area": "전체",
            "search_state": "1",
            "search_kind": "1",
            "search_type": "1",
            "search_word": query
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # 결과 추출
            results = []
            for row in soup.select("table.board tbody tr"):
                cols = row.find_all("td")
                if len(cols) >= 4:
                    reg_type = cols[0].get_text(strip=True)   # 등록구분
                    name = cols[1].get_text(strip=True)       # 상호
                    address = cols[2].get_text(strip=True)    # 주소
                    status = cols[3].get_text(strip=True)     # 영업구분
                    results.append((reg_type, name, address, status))
                    all_results.append((query, reg_type, name, address, status))

            # 출력
            if results:
                df = pd.DataFrame(results, columns=["등록구분", "상호", "주소", "영업구분"])
                st.dataframe(df, use_container_width=True)
            else:
                st.warning("검색 결과가 없습니다.")

        except Exception as e:
            st.error(f"오류 발생: {e}")

    # 전체 결과 모아서 출력
    if all_results:
        st.write("### 📊 전체 검색 결과 통합")
        df_all = pd.DataFrame(all_results, columns=["검색어", "등록구분", "상호", "주소", "영업구분"])
        st.dataframe(df_all, use_container_width=True)
