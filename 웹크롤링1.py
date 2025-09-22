import streamlit as st
import requests
import re
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# =========================
# --- 구글 시트 로드 ---
# =========================
@st.cache_data(ttl=3600)
def load_publisher_db():
    json_key = dict(st.secrets["gspread"])
    json_key["private_key"] = json_key["private_key"].replace("\\n", "\n")
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key, scope)
    client = gspread.authorize(creds)
    sh = client.open("출판사 DB")

    # 출판사-지역 매칭
    publisher_data = pd.DataFrame(sh.worksheet("KPIPA_PUB_REG").get_all_values()[1:], 
                                  columns=["출판사명", "지역"])
    # 발행국 코드
    region_data = pd.DataFrame(sh.worksheet("008").get_all_values()[1:], 
                               columns=["지역", "발행국", "발행국코드"])

    # IM_* 시트 합치기
    imprint_frames = []
    for ws in sh.worksheets():
        if ws.title.startswith("IM_"):
            df = pd.DataFrame(ws.get_all_values()[1:], columns=["출판사", "임프린트"])
            imprint_frames.append(df)
    imprint_data = pd.concat(imprint_frames, ignore_index=True) if imprint_frames else pd.DataFrame(columns=["출판사","임프린트"])

    return publisher_data, region_data, imprint_data


# =========================
# --- 보조 함수 ---
# =========================
def normalize(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사|프레스", "", name).lower()


def search_publisher_location_with_alias(pub_name, publisher_df):
    """KPIPA DB에서 출판사 정확 검색"""
    debug = []
    matches = publisher_df[publisher_df["출판사명"].apply(normalize) == pub_name]
    if not matches.empty:
        region = matches.iloc[0]["지역"]
        debug.append(f"✅ DB 정확검색 성공: {pub_name} → {region}")
        return region, debug
    debug.append(f"❌ DB에서 '{pub_name}' 미발견")
    return "출판지 미상", debug


def search_publisher_location_stage2_contains(pub_name, publisher_df):
    """부분일치 검색 (2차 정규화)"""
    debug = []
    matches = publisher_df[publisher_df["출판사명"].str.contains(pub_name, case=False, na=False)]
    if not matches.empty:
        debug.append(f"🔎 부분일치 검색 후보 {len(matches)}건 발견")
        return matches[["출판사명","지역"]].values.tolist(), debug
    debug.append(f"❌ 부분일치에서도 '{pub_name}' 발견 실패")
    return [], debug


def find_main_publisher_from_imprints(imprint_name, imprint_df):
    """IM_* 시트에서 임프린트 뒷부분으로 출판사 찾기"""
    matches = imprint_df[imprint_df["임프린트"].apply(normalize) == normalize(imprint_name)]
    if not matches.empty:
        return matches.iloc[0]["출판사"]
    return None


def get_country_code_by_region(region, region_df):
    row = region_df[region_df["지역"] == region]
    if not row.empty:
        return row.iloc[0]["발행국코드"]
    return "xx "


# =========================
# --- 외부 검색 함수 ---
# =========================
def search_aladin_by_isbn(isbn):
    """알라딘 API (출판사, 발행연도 등 가져오기)"""
    # TODO: 실제 API 연동 (여기선 더미)
    return {"publisher": "민음사", "pubyear": "2020", "245": "=245  10$a더미제목"}, None


def get_publisher_name_from_isbn_kpipa(isbn):
    """KPIPA 페이지 ISBN 검색"""
    search_url = "https://bnk.kpipa.or.kr/home/v3/addition/search"
    params = {"ST": isbn, "PG": 1, "PG2": 1, "DSF": "Y", "SO": "weight", "DT": "A"}
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        res = requests.get(search_url, params=params, headers=headers, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        first_result_link = soup.select_one("a.book-grid-item")
        if not first_result_link:
            return None, None, "❌ KPIPA ISBN 검색 결과 없음"

        detail_href = first_result_link.get("href")
        detail_url = f"https://bnk.kpipa.or.kr{detail_href}"
        detail_res = requests.get(detail_url, headers=headers, timeout=15)
        detail_res.raise_for_status()
        detail_soup = BeautifulSoup(detail_res.text, "html.parser")

        pub_info_tag = detail_soup.find("dt", string="출판사 / 임프린트")
        if not pub_info_tag:
            return None, None, "❌ '출판사 / 임프린트' 항목 없음"

        dd_tag = pub_info_tag.find_next_sibling("dd")
        if dd_tag:
            full_text = dd_tag.get_text(strip=True)
            publisher_name_part = full_text.split("/")[0].strip()
            return full_text, normalize(publisher_name_part), None

        return None, None, "❌ KPIPA 상세에서 추출 실패"
    except Exception as e:
        return None, None, f"KPIPA 예외: {e}"


def get_mcst_address(pub_name):
    """문체부 검색 (더미 구현)"""
    return "서울특별시 종로구 세종대로", [{"출판사": pub_name, "주소": "서울특별시 종로구 세종대로"}]


# =========================
# --- Streamlit 메인 ---
# =========================
st.title("📚 ISBN → KORMARC 변환기 (KPIPA·IM·정규화·문체부 통합)")

if st.button("🔄 구글시트 새로고침"):
    st.cache_data.clear()
    st.success("캐시 초기화 완료!")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력:")

records = []
all_mcst_results = []

if isbn_input:
    isbn_list = [re.sub(r"[^\d]", "", s) for s in isbn_input.split("/") if s.strip()]
    publisher_data, region_data, imprint_data = load_publisher_db()

    for idx, isbn in enumerate(isbn_list, start=1):
        st.markdown(f"---\n### 📘 {idx}. ISBN: `{isbn}`")
        debug_messages = []

        # 1) Aladin API
        result, error = search_aladin_by_isbn(isbn)
        if error:
            st.warning(error)
            continue
        publisher_api = result["publisher"]
        pubyear = result["pubyear"]

        # 2) KPIPA ISBN 검색
        publisher_full, publisher_norm, kpipa_error = get_publisher_name_from_isbn_kpipa(isbn)
        if publisher_norm:
            debug_messages.append(f"✅ KPIPA ISBN 검색 성공: {publisher_full}")
            location_raw, debug_db = search_publisher_location_with_alias(publisher_norm, publisher_data)
            debug_messages.extend(debug_db)
        else:
            debug_messages.append(kpipa_error)
            publisher_norm = normalize(publisher_api)
            debug_messages.append(f"➡️ KPIPA 실패 → Aladin API 사용: {publisher_norm}")
            location_raw, debug_db = search_publisher_location_with_alias(publisher_norm, publisher_data)
            debug_messages.extend(debug_db)

        # 3) 1차 정규화 DB 검색 실패
        if location_raw == "출판지 미상":
            main_pub = find_main_publisher_from_imprints(publisher_norm, imprint_data)
            if main_pub:
                debug_messages.append(f"✅ IM 시트 매칭 성공: {publisher_norm} → {main_pub}")
                location_raw, debug_db = search_publisher_location_with_alias(normalize(main_pub), publisher_data)
                debug_messages.extend(debug_db)

        # 4) 2차 정규화 (부분일치)
        two_stage_matches = []
        if location_raw == "출판지 미상":
            matches, debug_stage2 = search_publisher_location_stage2_contains(publisher_norm, publisher_data)
            debug_messages.extend(debug_stage2)
            if matches:
                two_stage_matches = matches
                location_raw = matches[0][1]  # 첫 후보지역

        # 5) 문체부 검색
        if location_raw == "출판지 미상":
            addr, mcst_results = get_mcst_address(publisher_norm)
            if mcst_results:
                all_mcst_results.extend(mcst_results)
                location_raw = "문체부 확인 필요"

        # 6) 최종 발행국 코드
        location_display = location_raw if location_raw != "출판지 미상" else "[발행지불명]"
        country_code = get_country_code_by_region(location_raw, region_data)

        # KORMARC 출력
        field_008 = f"=008  \\\\$a{country_code}"
        field_245 = result["245"]
        field_260 = f"=260  \\\\$a{location_display} :$b{publisher_api},$c{pubyear}."

        st.code(field_008, language="text")
        st.code(field_245, language="text")
        st.code(field_260, language="text")

        records.append({"ISBN": isbn, "008": field_008, "245": field_245, "260": field_260})

        # 디버그 메시지
        if debug_messages:
            st.markdown("### 🛠️ 검색 디버그")
            for msg in debug_messages:
                st.text(msg)

        # 2차 정규화 후보 출력
        if two_stage_matches:
            st.markdown("### 🔎 2차 정규화 후보")
            df_stage2 = pd.DataFrame(two_stage_matches, columns=["출판사명","지역"])
            st.dataframe(df_stage2, use_container_width=True)

    # 문체부 결과는 마지막에 출력
    if all_mcst_results:
        st.markdown("---\n## 🏛️ 문체부 검색 결과")
        df_mcst = pd.DataFrame(all_mcst_results)
        st.dataframe(df_mcst, use_container_width=True)
