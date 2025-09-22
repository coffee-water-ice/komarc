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
    creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gspread"], 
                                                            ["https://spreadsheets.google.com/feeds",
                                                             "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    sh = client.open("출판사 DB")
    
    # KPIPA_PUB_REG: 번호, 출판사명, 주소, 전화번호 → 출판사명, 주소만 사용
    pub_rows = sh.worksheet("KPIPA_PUB_REG").get_all_values()[1:]
    pub_rows_filtered = [row[1:3] for row in pub_rows]  # 출판사명, 주소
    publisher_data = pd.DataFrame(pub_rows_filtered, columns=["출판사명", "주소"])
    
    # 008: 발행국 발행국 부호 → 첫 2열만
    region_rows = sh.worksheet("008").get_all_values()[1:]
    region_rows_filtered = [row[:2] for row in region_rows]
    region_data = pd.DataFrame(region_rows_filtered, columns=["발행국", "발행국 부호"])
    
    # IM_* 시트: 출판사/임프린트 하나의 칼럼
    imprint_frames = []
    for ws in sh.worksheets():
        if ws.title.startswith("IM_"):
            data = ws.get_all_values()[1:]
            imprint_frames.extend([row[0] for row in data if row])
    imprint_data = pd.DataFrame(imprint_frames, columns=["임프린트"])
    
    return publisher_data, region_data, imprint_data

# =========================
# --- 알라딘 API ---
# =========================
def search_aladin_by_isbn(isbn):
    try:
        ttbkey = st.secrets["aladin"]["ttbkey"]
        url = "https://www.aladin.co.kr/ttb/api/ItemLookUp.aspx"
        params = {"ttbkey": ttbkey, "itemIdType": "ISBN", "ItemId": isbn, 
                  "output": "js", "Version": "20131101"}
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
# --- KPIPA DB 검색 보조 함수 ---
# =========================
def search_publisher_location_with_alias(name, publisher_data):
    debug_msgs = []
    if not name:
        return "출판지 미상", ["❌ 검색 실패: 입력된 출판사명이 없음"]
    norm_name = normalize_publisher_name(name)
    candidates = publisher_data[publisher_data["출판사명"].apply(lambda x: normalize_publisher_name(x)) == norm_name]
    if not candidates.empty:
        address = candidates.iloc[0]["주소"]
        debug_msgs.append(f"✅ KPIPA DB 매칭 성공: {name} → {address}")
        return address, debug_msgs
    else:
        debug_msgs.append(f"❌ KPIPA DB 매칭 실패: {name}")
        return "출판지 미상", debug_msgs

# =========================
# --- IM 임프린트 보조 함수 ---
# =========================
def find_main_publisher_from_imprints(rep_name, imprint_data, publisher_data):
    """
    IM_* 시트에서 임프린트명을 검색하고, KPIPA DB에서 해당 출판사명으로 주소를 반환
    """
    norm_rep = normalize_publisher_name(rep_name)
    for full_text in imprint_data["임프린트"]:
        if "/" in full_text:
            pub_part, imprint_part = [p.strip() for p in full_text.split("/", 1)]
        else:
            pub_part, imprint_part = full_text.strip(), None

        if imprint_part:
            norm_imprint = normalize_publisher_name(imprint_part)
            if norm_imprint == norm_rep:
                # KPIPA DB에서 pub_part를 검색
                location, debug_msgs = search_publisher_location_with_alias(pub_part, publisher_data)
                return location, debug_msgs
    return None, ["❌ IM DB 검색 실패: 매칭되는 임프린트 없음"]

# =========================
# --- KPIPA 페이지 검색 ---
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
# --- 문체부 검색 ---
# =========================
def get_mcst_address(publisher_name):
    url = "https://book.mcst.go.kr/html/searchList.php"
    params = {"search_area": "전체", "search_state": "1", "search_kind": "1", 
              "search_type": "1", "search_word": publisher_name}
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
# --- Streamlit UI ---
# =========================
st.title("📚 ISBN → KORMARC 변환기 (KPIPA·IM·2차정규화·문체부 통합)")

if st.button("🔄 구글시트 새로고침"):
    st.cache_data.clear()
    st.success("캐시 초기화 완료! 다음 호출 시 최신 데이터 반영됩니다.")

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
            st.warning(f"[Aladin API] {error}")
            continue
        publisher_api = result["publisher"]
        pubyear = result["pubyear"]

        # 2) KPIPA 페이지 검색
        publisher_full, publisher_norm, kpipa_error = get_publisher_name_from_isbn_kpipa(isbn)
        location_raw = "출판지 미상"
        if publisher_norm:
            debug_messages.append(f"✅ KPIPA 페이지 검색 성공: {publisher_full}")
            location_raw, debug_kpipa_db = search_publisher_location_with_alias(publisher_norm, publisher_data)
            debug_messages.extend([f"[KPIPA DB] {msg}" for msg in debug_kpipa_db])
        else:
            debug_messages.append(f"[KPIPA 페이지] {kpipa_error}")
            publisher_norm = publisher_api

        # 3) 1차 정규화 후 KPIPA DB
        if location_raw == "출판지 미상":
            rep_name, aliases = split_publisher_aliases(publisher_norm)
            location_raw, debug_stage1 = search_publisher_location_with_alias(rep_name, publisher_data)
            debug_messages.extend([f"[1차 정규화 KPIPA DB] {msg}" for msg in debug_stage1])

        # 4) IM 검색
        if location_raw == "출판지 미상":
            main_pub = find_main_publisher_from_imprints(rep_name, imprint_data)
            if main_pub:
                location_raw, debug_im = search_publisher_location_with_alias(main_pub, publisher_data)
                debug_messages.extend([f"[IM DB] {msg}" for msg in debug_im])
            else:
                debug_messages.append(f"[IM DB] 매칭 실패: {rep_name}")

        # 5) 2차 정규화 KPIPA DB
        if location_raw == "출판지 미상":
            stage2_name = normalize_stage2(publisher_norm)
            location_raw, debug_stage2 = search_publisher_location_with_alias(stage2_name, publisher_data)
            debug_messages.extend([f"[2차 정규화 KPIPA DB] {msg}" for msg in debug_stage2])

            # ✅ 2차 정규화 후 IM DB 검색
            if location_raw == "출판지 미상":
                main_pub_stage2 = find_main_publisher_from_imprints(stage2_name, imprint_data)
                if main_pub_stage2:
                    location_raw, debug_im_stage2 = search_publisher_location_with_alias(main_pub_stage2, publisher_data)
                    debug_messages.extend([f"[IM DB 2차 정규화 후] {msg}" for msg in debug_im_stage2])
                else:
                    debug_messages.append(f"[IM DB 2차 정규화 후] 매칭 실패: {stage2_name}")


        # 6) 문체부 검색
        mcst_address, mcst_results = get_mcst_address(publisher_norm)
        if mcst_address != "미확인":
            location_raw = mcst_address
            debug_messages.append(f"[문체부] 매칭 성공: {mcst_address}")
        else:
            debug_messages.append(f"[문체부] 매칭 실패")
        all_mcst_results.append(mcst_results)

        # 7) 발행국 표시용 정규화
        location_display = normalize_publisher_location_for_display(location_raw)

        # 8) MARC 008 발행국 발행국 부호
        code_row = region_data[region_data["발행국"] == location_display]
        code = code_row["발행국 부호"].values[0] if not code_row.empty else "??"

        # 9) 최종 출력
        st.write(f"출판사명: {publisher_api}")
        st.write(f"출판지(raw): {location_raw}")
        st.write(f"출판지(표시용): {location_display}")
        st.write(f"발행국 발행국 부호: {code}")
        st.write(f"MARC 245: {result['245']}")
        st.write("🔹 Debug / 후보 메시지")
        for msg in debug_messages:
            st.write(msg)
