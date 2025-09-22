import streamlit as st
import requests
import re
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

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
    publisher_data = pd.DataFrame(sh.worksheet("KPIPA_PUB_REG").get_all_values()[1:], columns=["출판사명", "주소", "전화번호"])
    region_data = pd.DataFrame(sh.worksheet("008").get_all_values()[1:], columns=["발행국", "발행국코드"])

    imprint_frames = []
    for ws in sh.worksheets():
        if ws.title.startswith("IM_"):
            df_im = pd.DataFrame(ws.get_all_values()[1:], columns=["출판사/임프린트"])
            imprint_frames.append(df_im)
    imprint_data = pd.concat(imprint_frames, ignore_index=True) if imprint_frames else pd.DataFrame(columns=["출판사/임프린트"])

    return publisher_data, region_data, imprint_data

# =========================
# --- Aladin API ---
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
            publisher_name_norm = normalize_publisher_name(publisher_name_part)
            return publisher_name_full, publisher_name_norm, None

        return None, None, "❌ 'dd' 태그에서 텍스트를 추출할 수 없습니다. (KPIPA)"
    except Exception as e:
        return None, None, f"KPIPA 예외: {e}"

# =========================
# --- KPIPA DB 검색 ---
# =========================
def search_publisher_location_with_alias(publisher_norm, publisher_data):
    for idx, row in publisher_data.iterrows():
        db_norm = normalize_publisher_name(str(row["출판사명"]))
        if publisher_norm == db_norm:
            return row["주소"], f"💙 KPIPA DB 검색 성공: {row['출판사명']}"
    return "출판지 미상", f"❌ KPIPA DB 검색 실패: {publisher_norm}"

# =========================
# --- IM DB 검색 ---
# =========================
def find_main_publisher_from_imprints(publisher_name, imprint_data):
    publisher_name_norm = normalize_publisher_name(publisher_name)
    for idx, row in imprint_data.iterrows():
        try:
            im_str = row["출판사/임프린트"]
            imprint_part = im_str.split("/")[-1].strip().lower()
            if publisher_name_norm == imprint_part:
                main_pub = im_str.split("/")[0].strip()
                return main_pub, f"🟠 IM DB 검색 성공: {main_pub}"
        except:
            continue
    return None, f"❌ IM DB 검색 실패: {publisher_name_norm}"

# =========================
# --- 문체부 검색 ---
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
# --- Streamlit UI ---
# =========================
st.title("📚 ISBN → KORMARC 변환기 (KPIPA·IM·2차정규화·문체부 통합)")

if st.button("🔄 구글시트 새로고침"):
    st.cache_data.clear()
    st.success("캐시 초기화 완료! 다음 호출 시 최신 데이터 반영됩니다.")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력:")

all_mcst_results = []

if isbn_input:
    isbn_list = [re.sub(r"[^\d]", "", s) for s in isbn_input.split("/") if s.strip()]
    publisher_data, region_data, imprint_data = load_publisher_db()

    for idx, isbn in enumerate(isbn_list, start=1):
        st.markdown(f"---\n### 📘 {idx}. ISBN: `{isbn}`")
        debug_messages = []

        result_api, error_api = search_aladin_by_isbn(isbn)
        if error_api:
            st.warning(error_api)
            continue

        publisher_api = result_api["publisher"]
        pubyear = result_api["pubyear"]
        field_245 = result_api["245"]

        publisher_full, publisher_norm, kpipa_error = get_publisher_name_from_isbn_kpipa(isbn)
        if publisher_norm:
            debug_messages.append(f"<span style='color:green'>✅ KPIPA 페이지 검색 성공: {publisher_full}</span>")
            location_raw, debug_kpipa_db = search_publisher_location_with_alias(publisher_norm, publisher_data)
            debug_messages.append(f"<span style='color:blue'>{debug_kpipa_db}</span>")
        else:
            debug_messages.append(f"<span style='color:red'>{kpipa_error}</span>")
            publisher_norm = publisher_api
            location_raw, debug_stage1 = search_publisher_location_with_alias(publisher_norm, publisher_data)
            debug_messages.append(f"<span style='color:blue'>{debug_stage1}</span>")

        if location_raw == "출판지 미상":
            main_pub, debug_im = find_main_publisher_from_imprints(publisher_norm, imprint_data)
            if main_pub:
                publisher_norm = main_pub
                location_raw, debug_kpipa_db2 = search_publisher_location_with_alias(publisher_norm, publisher_data)
                debug_messages.append(f"<span style='color:orange'>{debug_im}</span>")
                debug_messages.append(f"<span style='color:blue'>{debug_kpipa_db2}</span>")

        if location_raw == "출판지 미상":
            publisher_norm_stage2 = normalize_stage2(publisher_norm)
            matches = []
            for idx2, row in publisher_data.iterrows():
                db_norm = normalize_stage2(str(row["출판사명"]))
                if publisher_norm_stage2 in db_norm or db_norm in publisher_norm_stage2:
                    matches.append((row["출판사명"], row["주소"]))
            if matches:
                publisher_norm, location_raw = matches[0]
                st.markdown("### 🔎 2차 정규화 후보")
                df_stage2 = pd.DataFrame(matches, columns=["출판사명", "주소"])
                st.dataframe(df_stage2, use_container_width=True)
                debug_messages.append(f"<span style='color:purple'>🟣 2차 정규화 후보: {len(matches)}건</span>")

        if location_raw == "출판지 미상":
            addr_mcst, mcst_results = get_mcst_address(publisher_norm)
            if mcst_results:
                all_mcst_results.extend(mcst_results)
                location_raw = addr_mcst
                debug_messages.append(f"<span style='color:red'>🔴 문체부 검색 후보: {len(mcst_results)}건</span>")

        country_code_row = region_data[region_data["발행국"].str.contains("한국", na=False)]
        country_code = country_code_row["발행국코드"].values[0] if not country_code_row.empty else "--"

        field_008 = f"=008  \\\\$a{country_code}"
        field_260 = f"=260  \\\\$a{normalize_publisher_location_for_display(location_raw)} :$b{publisher_norm},$c{pubyear}."

        st.code(field_008, language="text")
        st.code(field_245, language="text")
        st.code(field_260, language="text")

        if debug_messages:
            st.markdown("### 🛠️ 검색 단계별 결과")
            for msg in debug_messages:
                st.markdown(msg, unsafe_allow_html=True)

if all_mcst_results:
    st.markdown("### 📌 문체부 검색 후보")
    df_mcst = pd.DataFrame(all_mcst_results, columns=["등록유형", "출판사명", "주소", "상태"])
    st.dataframe(df_mcst, use_container_width=True)
