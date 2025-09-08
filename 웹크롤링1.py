# =========================
# --- Streamlit UI ---
# =========================
st.title("📚 ISBN → KORMARC 변환기 (정규화 순서 개선 + KPIPA + 문체부)")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력하세요:")

if isbn_input:
    isbn_list = [re.sub(r"[^\d]", "", s) for s in isbn_input.split("/") if s.strip()]
    publisher_data, region_data = load_publisher_db()

    for idx, isbn in enumerate(isbn_list, start=1):
        st.markdown(f"---\n### 📘 {idx}. ISBN: `{isbn}`")
        debug_messages = []

        # 1) Aladin API
        result, error = search_aladin_by_isbn(isbn)
        if error: debug_messages.append(f"❌ Aladin API 오류: {error}")

        # 2) 형태사항
        field_300, err_300 = extract_physical_description_by_crawling(isbn)
        if err_300: debug_messages.append(f"⚠️ 형태사항 크롤링 경고: {err_300}")

        if result:
            publisher = result["publisher"]
            pubyear = result["pubyear"]
            location_raw = "출판지 미상"

            # --------------------
            # 검색 순서
            # --------------------
            # 1차 정규화 완전일치
            loc1, debug1, _ = search_publisher_location_with_alias(publisher, publisher_data, stage2=False)
            debug_messages.extend(debug1)
            if loc1 not in ["출판지 미상", "부분일치 후보 다수"]:
                location_raw = loc1

            # 2차 정규화 완전일치
            if location_raw == "출판지 미상":
                loc2, debug2, _ = search_publisher_location_with_alias(publisher, publisher_data, stage2=True)
                debug_messages.extend(debug2)
                if loc2 not in ["출판지 미상", "부분일치 후보 다수"]:
                    location_raw = loc2

            # 2차 정규화 부분일치
            if location_raw == "출판지 미상":
                loc3, debug3, candidates = search_publisher_location_with_alias(publisher, publisher_data, stage2=True)
                debug_messages.extend(debug3)
                if loc3 == "부분일치 후보 다수":
                    with st.expander("⚠️ 부분일치 후보가 여러 개 발견됨 (2차 정규화)"):
                        for i, (name, region) in enumerate(candidates, start=1):
                            st.write(f"{i}. 출판사명: {name}, 지역: {region}")
                elif loc3 not in ["출판지 미상", "부분일치 후보 다수"]:
                    location_raw = loc3

            # KPIPA 검색
            if location_raw == "출판지 미상":
                pub_full, pub_norm, kpipa_err = get_publisher_name_from_isbn_kpipa(isbn)
                if kpipa_err: debug_messages.append(f"❌ KPIPA 검색 실패: {kpipa_err}")
                else:
                    debug_messages.append(f"🔍 KPIPA 원문: {pub_full}")
                    debug_messages.append(f"🧪 KPIPA 정규화: {pub_norm}")
                    kpipa_location, kpipa_candidates = get_publisher_location(pub_norm, publisher_data)
                    if kpipa_location not in ["출판지 미상", "부분일치 후보 다수"]:
                        location_raw = kpipa_location
                        debug_messages.append(f"🏙️ KPIPA 기반 재검색 결과: {location_raw}")

            # 문체부 검색 1차 정규화
            if location_raw == "출판지 미상":
                addr, mcst_results = get_mcst_address(publisher)
                debug_messages.append(f"🏛️ 문체부 1차 검색 결과: {addr}")
                if addr != "미확인":
                    location_raw = addr

            # 문체부 검색 2차 정규화
            if location_raw == "출판지 미상":
                # 2차 정규화 후 검색
                addr2, mcst_results2 = get_mcst_address(normalize_stage2(publisher))
                debug_messages.append(f"🏛️ 문체부 2차 검색 결과: {addr2}")
                if addr2 != "미확인":
                    location_raw = addr2

            # --------------------
            # 최종 처리
            # --------------------
            location_display = normalize_publisher_location_for_display(location_raw)
            country_code = get_country_code_by_region(location_raw, region_data)

            # ▶ KORMARC 출력
            st.code(f"=008  \\$a{country_code}", language="text")
            st.code(result["245"], language="text")
            st.code(f"=260  \\$a{location_display} :$b{publisher},$c{pubyear}.", language="text")
            st.code(field_300, language="text")

            # ▶ 문체부 검색 결과 별도 확인
            if 'mcst_results' in locals() and mcst_results:
                with st.expander(f"🏛️ 문체부 검색 상세 ({publisher})"):
                    df_mcst = pd.DataFrame(mcst_results, columns=["등록구분", "상호", "주소", "영업구분"])
                    st.dataframe(df_mcst, use_container_width=True)

        # ▶ 디버깅 메시지
        if debug_messages:
            with st.expander("🛠️ 디버깅 및 경고 메시지"):
                for m in debug_messages:
                    st.write(m)
