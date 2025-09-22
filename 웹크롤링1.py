import streamlit as st
import requests
import re
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# =========================
# --- 구글시트 로드 & 캐시 관리 ---
# =========================
@st.cache_data(ttl=3600)
def load_publisher_db():
    json_key = dict(st.secrets["gspread"])
    json_key["private_key"] = json_key["private_key"].replace('\\n', '\n')
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key, scope)
    client = gspread.authorize(creds)

    sh = client.open("출판사 DB")

    # KPIPA_PUB_REG: 번호, 출판사명, 주소, 전화번호 → 출판사명, 주소만 사용
    pub_rows = sh.worksheet("KPIPA_PUB_REG").get_all_values()[1:]
    publisher_data = pd.DataFrame(pub_rows).iloc[:, 1:3]
    publisher_data.columns = ["출판사명", "주소"]

    # 008: 발행국 코드
    region_rows = sh.worksheet("008").get_all_values()[1:]
    region_data = pd.DataFrame(region_rows)
    region_data.columns = ["발행국", "코드"]

    # IM_* 시트: 출판사/임프린트
    imprint_frames = []
    for ws in sh.worksheets():
        if ws.title.startswith("IM_"):
            rows = ws.get_all_values()[1:]
            df = pd.DataFrame(rows, columns=["출판사/임프린트"])
            imprint_frames.append(df)
    imprint_data = pd.concat(imprint_frames, ignore_index=True) if imprint_frames else pd.DataFrame(columns=["출판사/임프린트"])

    return publisher_data, region_data, imprint_data

# =========================
# --- 정규화 함수 ---
# =========================
def normalize_publisher_name(name):
    return re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사", "", name).lower()

def normalize_stage2(name):
    name = re.sub(r"(주니어|JUNIOR|어린이|키즈|북스|아이세움|프레스)", "", name, flags=re.IGNORECASE)
    eng_to_kor = {"springer": "스프링거", "cambridge": "케임브리지", "oxford": "옥스포드"}
    for eng, kor in eng_to_kor.items():
        name = re.sub(eng, kor, name, flags=re.IGNORECASE)
    return name.strip().lower()

def split_publisher_aliases(name):
    aliases = []
    bracket_contents = re.findall(r"\((.*?)\)", name)
    for content in bracket_contents:
        parts = re.split(r"[,/]", content)
        parts = [p.strip() for p in parts if p.strip()]
        aliases.extend(parts)
    name_no_brackets = re.sub(r"\(.*?\)", "", name).strip()
    if "/" in name_no_brackets:
        parts = [p.strip() for p in name_no_brackets.split("/") if p.strip()]
        rep_name = parts[0]
        aliases.extend(parts[1:])
    else:
        rep_name = name_no_brackets
    return rep_name, aliases

def normalize_publisher_location_for_display(location_name):
    if not location_name or location_name in ("출판지 미상", "예외 발생"):
        return location_name
    location_name = location_name.strip()
    major_cities = ["서울", "인천", "대전", "광주", "울산", "대구", "부산", "세종"]
    for city in major_cities:
        if city in location_name:
            return location_name[:2]
    parts = location_name.split()
    loc = parts[1] if len(parts) > 1 else parts[0]
    if loc.endswith("시"):
        loc = loc[:-1]
    return loc

# =========================
# --- Aladin API 검색 ---
# =========================
def search_aladin_by_isbn(isbn):
    try:
        ttbkey = st.secrets["aladin"]["ttbkey"]
        url = "https://www.aladin.co.kr/ttb/api/ItemLookUp.aspx"
        params = {"ttbkey": ttbkey, "itemIdType": "ISBN", "ItemId": isbn, "output": "js", "Version": "20131101"}
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()
        data = res.json()
        if "item" not in data or not data["item"]:
            return None, f"도서 정보를 찾을 수 없습니다. [응답: {data}]"
        book = data["item"][0]
        title = book.get("title", "제목 없음")
        author = book.get("author", "")
        publisher = book.get("publisher", "출판사 정보 없음")
        pubdate = book.get("pubDate", "")
        pubyear = pubdate[:4] if len(pubdate) >= 4 else "발행년도 없음"
        authors = [a.strip() for a in author.split(",")] if author else []
        creator_str = " ; ".join(authors) if authors else "저자 정보 없음"
        field_245 = f"=245  10$a{title} /$c{creator_str}"
        return {"title": title, "creator": creator_str, "publisher": publisher, "pubyear": pubyear, "245": field_245}, None
    except Exception as e:
        return None, f"Aladin API 예외: {e}"

# =========================
# --- KPIPA ISBN 검색 ---
# =========================
def get_publisher_name_from_isbn_kpipa(isbn):
    search_url = "https://bnk.kpipa.or.kr/home/v3/addition/search"
    params = {"ST": isbn, "PG": 1, "PG2": 1, "DSF": "Y", "SO": "weight", "DT": "A"}
    headers = {"User-Agent": "Mozilla/5.0"}

    def normalize(name):
        return re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사|프레스", "", name).lower()

    try:
        res = requests.get(search_url, params=params, headers=headers, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        first_result_link = soup.select_one("a.book-grid-item")
        if not first_result_link:
            return None, None, "❌ 검색 결과 없음 (KPIPA)"
        detail_href = first_result_link.get("href")
        detail_url = f"https://bnk.kpipa.or.kr{detail_href}"
        detail_res = requests.get(detail_url, headers=headers, timeout=15)
        detail_res.raise_for_status()
        detail_soup = BeautifulSoup(detail_res.text, "html.parser")
        pub_info_tag = detail_soup.find("dt", string="출판사 / 임프린트")
        if not pub_info_tag:
            return None, None, "❌ '출판사 / 임프린트' 항목을 찾을 수 없습니다. (KPIPA)"
        dd_tag = pub_info_tag.find_next_sibling("dd")
        if dd_tag:
            full_text = dd_tag.get_text(strip=True)
            publisher_name_full = full_text
            publisher_name_part = publisher_name_full.split("/")[0].strip()
            publisher_name_norm = normalize(publisher_name_part)
            return publisher_name_full, publisher_name_norm, None
        return None, None, "❌ 'dd' 태그에서 텍스트를 추출할 수 없습니다. (KPIPA)"
    except Exception as e:
        return None, None, f"KPIPA 예외: {e}"

# =========================
# --- KPIPA DB 검색 ---
# =========================
def search_publisher_location_with_alias(publisher_norm, publisher_data):
    debug = []
    match = publisher_data[publisher_data["출판사명"].str.lower() == publisher_norm.lower()]
    if not match.empty:
        debug.append(f"✅ KPIPA DB 검색 성공: {match.iloc[0]['출판사명']}")
        return match.iloc[0]["주소"], debug
    debug.append("❌ KPIPA DB 검색 실패")
    return "출판지 미상", debug

# =========================
# --- IM DB 검색 ---
# =========================
def search_im_db(publisher_name, imprint_data):
    debug = []
    for idx, row in imprint_data.iterrows():
        if "/" in row["출판사/임프린트"]:
            parts = [p.strip() for p in row["출판사/임프린트"].split("/")]
            if publisher_name in [normalize_stage2(p) for p in parts]:
                debug.append(f"✅ IM DB 검색 성공: {row['출판사/임프린트']}")
                return parts[0], debug
    debug.append("❌ IM DB 검색 실패")
    return "출판지 미상", debug

# =========================
# --- 문체부 ---
# =========================
def get_mcst_address(publisher_name):
    url = "https://book.mcst.go.kr/html/searchList.php"
    params = {"search_area": "전체", "search_state": "1", "search_kind": "1", "search_type": "1", "search_word": publisher_name}
    try:
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        results = []
        for row in soup.select("table.board tbody tr"):
            cols = row.find_all("td")
            if len(cols) >= 4:
                reg_type = cols[0].get_text(strip=True)
                name = cols[1].get_text(strip=True)
                address = cols[2].get_text(strip=True)
                status = cols[3].get_text(strip=True)
                if status == "영업":
                    results.append((reg_type, name, address, status))
        if results:
            return results[0][2], results
        else:
            return "미확인", []
    except Exception as e:
        return f"오류: {e}", []

# =========================
# --- 국가 코드 ---
# =========================
def get_country_code_by_region(region_name, region_data):
    match = region_data[region_data["발행국"] == region_name]
    if not match.empty:
        return match.iloc[0]["코드"]
    return "  "

# =========================
# --- Streamlit UI ---
# =========================
st.title("📚 ISBN → KORMARC 변환기 (KPIPA·IM·2차정규화·문체부 통합)")

if st.button("🔄 구글시트 새로고침"):
    st.cache_data.clear()
    st.success("캐시 초기화 완료! 다음 호출 시 최신 데이터 반영됩니다.")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력:")

records = []

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

        # 2) KPIPA ISBN 페이지 검색
        publisher_full, publisher_norm, kpipa_error = get_publisher_name_from_isbn_kpipa(isbn)
        if publisher_norm:
            debug_messages.append(f"✅ KPIPA 페이지 검색 성공: {publisher_full}")
            location_raw, debug_kpipa_db = search_publisher_location_with_alias(publisher_norm, publisher_data)
            debug_messages.extend(debug_kpipa_db)
        else:
            debug_messages.append(kpipa_error)
            # 1차 정규화 후 KPIPA DB
            publisher_norm = normalize_publisher_name(publisher_api)
            location_raw, debug_stage1 = search_publisher_location_with_alias(publisher_norm, publisher_data)
            debug_messages.extend(debug_stage1)

        # 3) IM DB 검색
        if location_raw == "출판지 미상":
            publisher_stage2 = normalize_stage2(publisher_norm)
            location_raw, debug_im = search_im_db(publisher_stage2, imprint_data)
            debug_messages.extend(debug_im)

        # 4) 문체부 검색
        if location_raw == "출판지 미상":
            location_raw, debug_mcst = get_mcst_address(publisher_api)
            debug_messages.extend([f"📌 {msg}" for msg in debug_mcst])

        location_display = normalize_publisher_location_for_display(location_raw)
        country_code = get_country_code_by_region(location_display, region_data)

        # KORMARC 출력
        field_008 = f"=008  \\\\$a{country_code}"
        field_245 = result["245"]
        field_260 = f"=260  \\\\$a{location_display} :$b{publisher_api},$c{result['pubyear']}."

        st.code(field_008, language="text")
        st.code(field_245, language="text")
        st.code(field_260, language="text")

        if debug_messages:
            st.markdown("### 🛠️ 검색 디버그")
            for msg in debug_messages:
                st.text(msg)
