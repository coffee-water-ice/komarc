import streamlit as st
import requests
import re
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import io

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

    # KPIPA_PUB_REG
    publisher_sheet = client.open("출판사 DB").worksheet("KPIPA_PUB_REG")
    publisher_data = publisher_sheet.get_all_values()[1:]

    # 008 (발행국 코드)
    region_sheet = client.open("출판사 DB").worksheet("008")
    region_data = region_sheet.get_all_values()[1:]

    # IM_* 시트 모두 합치기
    imprint_data = []
    for ws in client.open("출판사 DB").worksheets():
        if ws.title.startswith("IM_"):
            imprint_data.extend(ws.get_all_values()[1:])  # header 제외

    return publisher_data, region_data, imprint_data

# =========================
# --- 정규화 함수 ---
# =========================
def normalize_publisher_name(name):
    return re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사|프레스", "", name).lower()

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
# --- 구글시트 검색 ---
# =========================
def get_publisher_location(publisher_name, publisher_data):
    target = normalize_publisher_name(publisher_name)
    for row in publisher_data:
        if len(row) < 3:
            continue
        sheet_name, region = row[1], row[2]
        if normalize_publisher_name(sheet_name) == target:
            return region.strip() or "출판지 미상"
    return "출판지 미상"

def search_publisher_location_with_alias(publisher_name, publisher_data):
    rep_name, aliases = split_publisher_aliases(publisher_name)
    debug = [f"KPIPA 검색 대표명: `{rep_name}`"]
    location = get_publisher_location(rep_name, publisher_data)
    if location != "출판지 미상":
        return location, debug
    for alias in aliases:
        debug.append(f"별칭 검색: `{alias}`")
        location = get_publisher_location(alias, publisher_data)
        if location != "출판지 미상":
            return location, debug
    return "출판지 미상", debug

def search_publisher_location_stage2_contains(publisher_name, publisher_data):
    rep_name, aliases = split_publisher_aliases(publisher_name)
    rep_name_norm = normalize_stage2(rep_name)
    matches = []
    for row in publisher_data:
        if len(row) < 3:
            continue
        sheet_name, region = row[1], row[2]
        if rep_name_norm in normalize_stage2(sheet_name):
            matches.append((sheet_name, region))
    debug = [f"2차 정규화 부분일치 검색: `{rep_name_norm}` → {len(matches)}건"]
    return matches, debug

def find_main_publisher_from_imprints(publisher_name, imprint_data):
    name_norm = normalize_publisher_name(publisher_name)
    for row in imprint_data:
        if len(row) < 2:
            continue
        sheet_pub, imprint = row[0], row[1]
        if normalize_publisher_name(imprint) == name_norm:
            return sheet_pub
    return None

def get_country_code_by_region(region_name, region_data):
    normalized_input = (region_name or "")[:2]
    for row in region_data:
        if len(row) < 2:
            continue
        sheet_region, country_code = row[0], row[1]
        if (sheet_region or "")[:2] == normalized_input:
            return country_code.strip() or "xxu"
    return "xxu"

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
# --- Streamlit UI ---
# =========================
st.title("📚 ISBN → KORMARC 변환기 (KPIPA·IM·2차정규화·문체부 통합)")

if st.button("🔄 구글시트 새로고침"):
    st.cache_data.clear()
    st.success("캐시 초기화 완료! 다음 호출 시 최신 데이터 반영됩니다.")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력:")

records = []
all_mcst_results = []  # 문체부 결과 통합

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
        field_245 = result["245"]

        # --- KPIPA 페이지 ISBN 검색 ---
        location_raw, debug_kpipa = search_publisher_location_with_alias(publisher_api, publisher_data)
        debug_messages.extend(debug_kpipa)

        # --- 1차 정규화 후 KPIPA 검색 실패 시 ---
        if location_raw == "출판지 미상":
            # 1차 정규화 KPIPA
            rep_name, _ = split_publisher_aliases(publisher_api)
            location_raw, debug_stage1 = search_publisher_location_with_alias(rep_name, publisher_data)
            debug_messages.extend(debug_stage1)

        # --- IM DB 검색 ---
        if location_raw == "출판지 미상":
            main_pub = find_main_publisher_from_imprints(publisher_api, imprint_data)
            if main_pub:
                publisher_api = main_pub
                location_raw, debug_im = search_publisher_location_with_alias(main_pub, publisher_data)
                debug_messages.extend(debug_im)

        # --- 2차 정규화 ---
        if location_raw == "출판지 미상":
            matches, debug_stage2 = search_publisher_location_stage2_contains(publisher_api, publisher_data)
            debug_messages.extend(debug_stage2)
            if matches:
                publisher_api, location_raw = matches[0]

        # --- 2차 정규화 후 IM DB 검색 ---
        if location_raw == "출판지 미상":
            main_pub = find_main_publisher_from_imprints(publisher_api, imprint_data)
            if main_pub:
                publisher_api = main_pub
                location_raw, debug_im2 = search_publisher_location_with_alias(main_pub, publisher_data)
                debug_messages.extend(debug_im2)

        # --- 문체부 ---
        if location_raw == "출판지 미상":
            addr, mcst_results = get_mcst_address(publisher_api)
            if mcst_results:
                all_mcst_results.extend(mcst_results)
                location_raw = addr

        # --- 최종 발행지 불명 처리 ---
        if location_raw == "출판지 미상":
            location_raw = "[발행지불명]"

        location_display = normalize_publisher_location_for_display(location_raw)
        country_code = get_country_code_by_region(location_raw, region_data)

        # KORMARC 출력
        field_008 = f"=008  \\\\$a{country_code}"
        field_260 = f"=260  \\\\$a{location_display} :$b{publisher_api},$c{pubyear}."

        st.code(field_008, language="text")
        st.code(field_245, language="text")
        st.code(field_260, language="text")

        # Debug 메시지 출력
        if debug_messages:
            st.markdown("### 🛠️ 검색 경로/Debug")
            for msg in debug_messages:
                st.text(msg)

        records.append({"ISBN": isbn, "008": field_008, "245": field_245, "260": field_260})

# 문체부 통합 출력
if all_mcst_results:
    st.markdown("---\n### 🏛️ 문체부 통합 검색 결과")
    df_mcst = pd.DataFrame(all_mcst_results, columns=["등록 구분", "출판사명", "주소", "상태"])
    st.dataframe(df_mcst, use_container_width=True)

# 엑셀 다운로드
if records:
    def clean_marc_field(value: str) -> str:
        if not isinstance(value, str):
            return value
        cleaned = value.replace("=008", "").replace("=245", "").replace("=260", "").replace("10$a", "").replace("\\", "").replace("$a", "").replace("$b", "").replace("$c", "").replace("$", "").strip()
        return cleaned

    cleaned_records = []
    for rec in records:
        cleaned_records.append({
            "ISBN": rec["ISBN"],
            "008": clean_marc_field(rec["008"]),
            "245": clean_marc_field(rec["245"]),
            "260": clean_marc_field(rec["260"]),
        })

    df_out = pd.DataFrame(cleaned_records)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df_out.to_excel(writer, index=False, sheet_name="KORMARC 결과")
    buffer.seek(0)
    st.download_button(
        label="📥 변환 결과 엑셀 다운로드 (순수 텍스트)",
        data=buffer.getvalue(),
        file_name="kormarc_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
