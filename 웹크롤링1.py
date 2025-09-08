import streamlit as st
import requests
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import pandas as pd

# =========================
# --- 구글 시트 로드 ---
# =========================
@st.cache_data(ttl=3600)
def load_publisher_db():
    json_key = dict(st.secrets["gspread"])
    json_key["private_key"] = json_key["private_key"].replace('\\n', '\n')
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key, scope)
    client = gspread.authorize(creds)
    publisher_sheet = client.open("출판사 DB").worksheet("시트3")
    region_sheet = client.open("출판사 DB").worksheet("Sheet2")
    publisher_data = publisher_sheet.get_all_values()[1:]
    region_data = region_sheet.get_all_values()[1:]
    return publisher_data, region_data

# =========================
# --- 정규화 및 별칭/괄호 처리 ---
# =========================
def normalize_publisher_name(name):
    return re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사|프레스", "", name).lower()

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

def search_publisher_location_with_alias(publisher_name, publisher_data):
    rep_name, aliases = split_publisher_aliases(publisher_name)
    location = get_publisher_location(rep_name, publisher_data)
    if location != "출판지 미상":
        return location
    for alias in aliases:
        location = get_publisher_location(alias, publisher_data)
        if location != "출판지 미상":
            return location
    return "출판지 미상"

def get_publisher_location(publisher_name, publisher_data):
    target = normalize_publisher_name(publisher_name)
    for row in publisher_data:
        if len(row) < 3: continue
        sheet_name, region = row[1], row[2]
        if normalize_publisher_name(sheet_name) == target:
            return region.strip() or "출판지 미상"
    for row in publisher_data:
        if len(row) < 3: continue
        sheet_name, region = row[1], row[2]
        if sheet_name.strip() == publisher_name.strip():
            return region.strip() or "출판지 미상"
    return "출판지 미상"

def get_country_code_by_region(region_name, region_data):
    def normalize_region_for_code(region):
        region = (region or "").strip()
        if region.startswith(("전라", "충청", "경상")):
            if len(region) >= 3: return region[0] + region[2]
            return region[:2]
        return region[:2]
    normalized_input = normalize_region_for_code(region_name)
    for row in region_data:
        if len(row) < 2: continue
        sheet_region, country_code = row[0], row[1]
        if normalize_region_for_code(sheet_region) == normalized_input:
            return country_code.strip() or "xxu"
    return "xxu"

# =========================
# --- Aladin API / 245, 크롤링 300 ---
# =========================
def search_aladin_by_isbn(isbn):
    try:
        ttbkey = st.secrets["aladin"]["ttbkey"]
        url = "https://www.aladin.co.kr/ttb/api/ItemLookUp.aspx"
        params = {
            "ttbkey": ttbkey,
            "itemIdType": "ISBN",
            "ItemId": isbn,
            "output": "js",
            "Version": "20131101"
        }
        res = requests.get(url, params=params, timeout=15)
        if res.status_code != 200: return None, f"API 요청 실패 (status: {res.status_code})"
        data = res.json()
        if "item" not in data or not data["item"]: return None, "도서 정보를 찾을 수 없습니다."
        book = data["item"][0]
        title = book.get("title", "제목 없음")
        author = book.get("author", "")
        authors = [a.strip() for a in author.split(",")] if author else []
        creator_str = " ; ".join(authors) if authors else "저자 정보 없음"
        pubdate = book.get("pubDate", "")
        pubyear = pubdate[:4] if len(pubdate)>=4 else "발행년도 없음"
        field_245 = f"=245  10$a{title} /$c{creator_str}"
        publisher = book.get("publisher", "출판사 정보 없음")
        return {"title": title, "creator": creator_str, "publisher": publisher,
                "pubyear": pubyear, "245": field_245}, None
    except Exception as e:
        return None, f"Aladin API 예외: {e}"

def extract_physical_description_by_crawling(isbn):
    try:
        search_url = f"https://www.aladin.co.kr/search/wsearchresult.aspx?SearchWord={isbn}"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(search_url, headers=headers, timeout=15)
        if res.status_code != 200: return "=300  \\$a1책.", f"검색 실패 (status {res.status_code})"
        soup = BeautifulSoup(res.text, "html.parser")
        link_tag = soup.select_one("div.ss_book_box a.bo3")
        if not link_tag or not link_tag.get("href"): return "=300  \\$a1책.", "도서 링크를 찾을 수 없습니다."
        detail_url = link_tag["href"]
        detail_res = requests.get(detail_url, headers=headers, timeout=15)
        if detail_res.status_code != 200: return "=300  \\$a1책.", f"상세페이지 요청 실패 (status {detail_res.status_code})"
        detail_soup = BeautifulSoup(detail_res.text, "html.parser")
        form_wrap = detail_soup.select_one("div.conts_info_list1")
        a_part = ""
        c_part = ""
        if form_wrap:
            items = [s.strip() for s in form_wrap.stripped_strings]
            for item in items:
                if re.search(r"(쪽|p)\s*$", item):
                    m = re.search(r"(\d+)\s*(쪽|p)?$", item)
                    if m: a_part = f"{m.group(1)} p."
                elif "mm" in item:
                    size_match = re.search(r"(\d+)\s*[\*x×X]\s*(\d+)\s*mm", item)
                    if size_match:
                        width, height = int(size_match.group(1)), int(size_match.group(2))
                        w_cm, h_cm = round(width/10), round(height/10)
                        c_part = f"{w_cm}x{h_cm} cm"
        if a_part or c_part:
            field_300 = "=300  \\\\$a"
            if a_part: field_300 += a_part
            if c_part:
                field_300 += f" ;$c{c_part}." if a_part else f"$c{c_part}."
        else:
            field_300 = "=300  \\$a1책."
        return field_300, None
    except Exception as e:
        return "=300  \\$a1책.", f"크롤링 예외: {e}"

# =========================
# --- KPIPA 검색 ---
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
        if not first_result_link: return None, None, "❌ KPIPA 검색 결과 없음"
        detail_url = f"https://bnk.kpipa.or.kr{first_result_link.get('href')}"
        detail_res = requests.get(detail_url, headers=headers, timeout=15)
        detail_res.raise_for_status()
        detail_soup = BeautifulSoup(detail_res.text, "html.parser")
        pub_info_tag = detail_soup.find("dt", string="출판사 / 임프린트")
        if not pub_info_tag: return None, None, "❌ '출판사 / 임프린트' 항목 없음 (KPIPA)"
        dd_tag = pub_info_tag.find_next_sibling("dd")
        if dd_tag:
            full_text = dd_tag.get_text(strip=True)
            publisher_name_full = full_text
            publisher_name_part = publisher_name_full.split("/")[0].strip()
            publisher_name_norm = normalize(publisher_name_part)
            return publisher_name_full, publisher_name_norm, None
        return None, None, "❌ dd 태그에서 텍스트 추출 불가 (KPIPA)"
    except Exception as e:
        return None, None, f"KPIPA 예외: {e}"

# =========================
# --- Streamlit UI ---
# =========================
st.title("📚 ISBN → Aladin + KPIPA + 구글시트 + 문체부 통합 조회")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력:")

if isbn_input:
    isbn_list = [re.sub(r"[^\d]", "", s) for s in isbn_input.split("/") if s.strip()]
    publisher_data, region_data = load_publisher_db()
    for idx, isbn in enumerate(isbn_list, start=1):
        st.markdown(f"---\n### 📘 {idx}. ISBN: `{isbn}`")
        debug_msgs = []

        # 1️⃣ Aladin API
        aladin_info, aladin_err = search_aladin_by_isbn(isbn)
        if aladin_err: debug_msgs.append(aladin_err)
        field_245 = aladin_info["245"] if aladin_info else "=245  10$a제목 없음 /$c저자 없음"
        pub_name_aladin = aladin_info["publisher"] if aladin_info else None
        pub_year = aladin_info["pubyear"] if aladin_info else "발행년도 없음"

        # 2️⃣ Aladin 300 필드
        field_300, crawl_err = extract_physical_description_by_crawling(isbn)
        if crawl_err: debug_msgs.append(crawl_err)

        # 3️⃣ KPIPA 출판사명 + 구글시트 / 문체부 출판지
        pub_full, pub_norm, kpipa_err = get_publisher_name_from_isbn_kpipa(isbn)
        if kpipa_err: debug_msgs.append(kpipa_err)
        location_raw = "출판지 미상"
        if pub_norm:
            location_raw = search_publisher_location_with_alias(pub_norm, publisher_data)
        else:
            # 문체부 검색 fallback
            try:
                mcst_url = "https://book.mcst.go.kr/html/searchList.php"
                params = {"search_area":"전체","search_state":"1","search_kind":"1","search_type":"1","search_word":pub_full or ""}
                res = requests.get(mcst_url, params=params, timeout=15)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                for row in soup.select("table.board tbody tr"):
                    cols = row.find_all("td")
                    if len(cols)>=4:
                        name = cols[1].get_text(strip=True)
                        address = cols[2].get_text(strip=True)
                        location_raw = search_publisher_location_with_alias(name, publisher_data)
                        break
            except Exception as e:
                debug_msgs.append(f"문체부 예외: {e}")

        country_code = get_country_code_by_region(location_raw, region_data)
        location_display = location_raw

        # --- 출력 ---
        st.code(f"=008  \\$a{country_code}", language="text")
        st.code(f"=260  \\$a{location_display} :$b{pub_name_aladin or pub_full},$c{pub_year}.", language="text")
        st.code(field_245, language="text")
        st.code(field_300, language="text")

        if debug_msgs:
            with st.expander("🛠️ 디버깅/경고 메시지"):
                for msg in debug_msgs: st.write(msg)
