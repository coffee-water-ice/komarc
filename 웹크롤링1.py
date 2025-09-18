import streamlit as st
import requests
import re
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import io   # ✅ 추가

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
    publisher_sheet = client.open("출판사 DB").worksheet("KPIPA_PUB_REG")
    region_sheet = client.open("출판사 DB").worksheet("008")
    publisher_data = publisher_sheet.get_all_values()[1:]
    region_data = region_sheet.get_all_values()[1:]
    return publisher_data, region_data

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
# --- 구글시트 검색 ---
# =========================
def get_publisher_location(publisher_name, publisher_data):
    try:
        target = normalize_publisher_name(publisher_name)
        for row in publisher_data:
            if len(row) < 3:
                continue
            sheet_name, region = row[1], row[2]
            if normalize_publisher_name(sheet_name) == target:
                return region.strip() or "출판지 미상"
        for row in publisher_data:  # fallback
            if len(row) < 3:
                continue
            sheet_name, region = row[1], row[2]
            if sheet_name.strip() == publisher_name.strip():
                return region.strip() or "출판지 미상"
        return "출판지 미상"
    except:
        return "예외 발생"

def search_publisher_location_with_alias(publisher_name, publisher_data):
    rep_name, aliases = split_publisher_aliases(publisher_name)
    debug = []
    rep_name_norm = normalize_publisher_name(rep_name)
    debug.append(f"1차 정규화 대표명: `{rep_name_norm}`")

    location = get_publisher_location(rep_name_norm, publisher_data)
    if location != "출판지 미상":
        return location, debug

    for alias in aliases:
        alias_norm = normalize_publisher_name(alias)
        debug.append(f"별칭 검색: `{alias_norm}`")
        location = get_publisher_location(alias_norm, publisher_data)
        if location != "출판지 미상":
            return location, debug
    return "출판지 미상", debug

def search_publisher_location_stage2_contains(publisher_name, publisher_data):
    """2차 정규화된 값 포함검색"""
    rep_name, aliases = split_publisher_aliases(publisher_name)
    rep_name_norm = normalize_stage2(rep_name)

    matches = []
    for row in publisher_data:
        if len(row) < 3:
            continue
        sheet_name, region = row[1], row[2]
        sheet_norm = normalize_stage2(sheet_name)
        if rep_name_norm in sheet_norm:
            matches.append((sheet_name, region))

    debug = [f"부분일치 검색 대표명: `{rep_name_norm}`, 결과 {len(matches)}건"]
    return matches, debug

# =========================
# --- 지역 코드 변환 ---
# =========================
def get_country_code_by_region(region_name, region_data):
    def normalize_region_for_code(region):
        region = (region or "").strip()
        if region.startswith(("전라", "충청", "경상")):
            if len(region) >= 3:
                return region[0] + region[2]
        return region[:2]

    normalized_input = normalize_region_for_code(region_name)
    for row in region_data:
        if len(row) < 2:
            continue
        sheet_region, country_code = row[0], row[1]
        if normalize_region_for_code(sheet_region) == normalized_input:
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
# --- KPIPA ---
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
st.title("📚 ISBN → KORMARC 변환기 (KPIPA·문체부 통합)")

if st.button("🔄 구글시트 새로고침"):
    st.cache_data.clear()
    st.success("캐시 초기화 완료! 다음 호출 시 최신 데이터 반영됩니다.")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력:")

records = []

if isbn_input:
    isbn_list = [re.sub(r"[^\d]", "", s) for s in isbn_input.split("/") if s.strip()]
    publisher_data, region_data = load_publisher_db()

    for idx, isbn in enumerate(isbn_list, start=1):
        st.markdown(f"---\n### 📘 {idx}. ISBN: `{isbn}`")
        debug_messages = []

        # 1) Aladin API
        result, error = search_aladin_by_isbn(isbn)
        if error:
            debug_messages.append(f"❌ Aladin API 오류: {error}")

        if result:
            publisher = result["publisher"]
            pubyear = result["pubyear"]

            # 2) KPIPA
            pub_full, pub_norm, kpipa_err = get_publisher_name_from_isbn_kpipa(isbn)
            if kpipa_err:
                st.warning(kpipa_err)
            else:
                st.markdown(f"**🔎 KPIPA 출판사/임프린트:** {pub_full}")

            # 3) 문체부
            addr, mcst_results = get_mcst_address(publisher)
            if addr != "미확인":
                st.markdown(f"**🏛️ 문체부 주소:** {addr}")
            if mcst_results:
                st.markdown("### 📑 문체부 검색 결과")
                df_mcst = pd.DataFrame(mcst_results, columns=["등록 구분", "출판사명", "주소", "상태"])
                st.dataframe(df_mcst, use_container_width=True)

            # 4) 지역 코드
            location_raw, _ = search_publisher_location_with_alias(publisher, publisher_data)
            location_display = normalize_publisher_location_for_display(location_raw)
            country_code = get_country_code_by_region(location_raw, region_data)

            # 5) KORMARC 출력
            field_008 = f"=008  \\\\$a{country_code}"
            field_245 = result["245"]
            field_260 = f"=260  \\\\$a{location_display} :$b{publisher},$c{pubyear}."

            st.code(field_008, language="text")
            st.code(field_245, language="text")
            st.code(field_260, language="text")

            records.append({
                "ISBN": isbn,
                "008": field_008,
                "245": field_245,
                "260": field_260
            })

# =========================
# --- 📥 엑셀 다운로드 ---
# =========================
if records:
    def clean_marc_field(value: str) -> str:
        if not isinstance(value, str):
            return value
        cleaned = (
            value.replace("=008", "")
            .replace("=245", "")
            .replace("=260", "")
            .replace("10$a", "")
            .replace("\\", "")
            .replace("$a", "")
            .replace("$b", "")
            .replace("$c", "")
            .replace("$", "")
            .strip()
        )
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
