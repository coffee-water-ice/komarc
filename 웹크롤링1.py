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
