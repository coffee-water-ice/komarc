import streamlit as st
import requests
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import pandas as pd

# =========================
# --- 구글 시트 관련 함수 ---
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
    publisher_data = publisher_sheet.get_all_values()[1:]  # 헤더 제외
    region_data = region_sheet.get_all_values()[1:]        # 헤더 제외
    return publisher_data, region_data

def normalize_publisher_name(name):
    return re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사|프레스", "", name).lower()

def normalize_publisher_location_for_display(location_name):
    if not location_name or location_name in ("출판지 미상", "예외 발생"):
        return location_name
    location_name = location_name.strip()
    major_cities = ["서울", "인천", "대전", "광주", "울산", "대구", "부산"]
    for city in major_cities:
        if city in location_name:
            return location_name[:2]
    parts = location_name.split()
    loc = parts[1] if len(parts) > 1 else parts[0]
    if loc.endswith("시") or loc.endswith("군"):
        loc = loc[:-1]
    return loc

def get_publisher_location(publisher_name, publisher_data):
    try:
        target = normalize_publisher_name(publisher_name)
        for row in publisher_data:
            if len(row) < 3:
                continue
            sheet_name, region = row[1], row[2]
            if normalize_publisher_name(sheet_name) == target:
                return region.strip() or "출판지 미상"
        for row in publisher_data:
            if len(row) < 3:
                continue
            sheet_name, region = row[1], row[2]
            if sheet_name.strip() == publisher_name.strip():
                return region.strip() or "출판지 미상"
        return "출판지 미상"
    except:
        return "예외 발생"

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

def get_country_code_by_region(region_name, region_data):
    try:
        def normalize_region_for_code(region):
            region = (region or "").strip()
            if region.startswith(("전라", "충청", "경상")):
                if len(region) >= 3:
                    return region[0] + region[2]
                return region[:2]
            return region[:2]
        normalized_input = normalize_region_for_code(region_name)
        for row in region_data:
            if len(row) < 2:
                continue
            sheet_region, country_code = row[0], row[1]
            if normalize_region_for_code(sheet_region) == normalized_input:
                return country_code.strip() or "xxu"
        return "xxu"
    except:
        return "xxu"

# =========================
# --- Aladin 관련 함수 ---
# =========================

def search_aladin_by_isbn(isbn):
    try:
        ttbkey = st.secrets["aladin"]["ttbkey"]
        url = "https://www.aladin.co.kr/ttb/api/ItemLookUp.aspx"
        params = {"ttbkey": ttbkey,"itemIdType":"ISBN","ItemId":isbn,"output":"js","Version":"20131101"}
        res = requests.get(url, params=params, timeout=15)
        if res.status_code != 200: return None, f"API 요청 실패 (status: {res.status_code})"
        data = res.json()
        if "item" not in data or not data["item"]: return None, f"도서 정보를 찾을 수 없습니다. [응답: {data}]"
        book = data["item"][0]
        title = book.get("title","제목 없음")
        author = book.get("author","")
        publisher = book.get("publisher","출판사 정보 없음")
        pubdate = book.get("pubDate","")
        pubyear = pubdate[:4] if len(pubdate)>=4 else "발행년도 없음"
        authors = [a.strip() for a in author.split(",")] if author else []
        creator_str = " ; ".join(authors) if authors else "저자 정보 없음"
        field_245 = f"=245  10$a{title} /$c{creator_str}"
        return {"title":title,"creator":creator_str,"publisher":publisher,"pubyear":pubyear,"245":field_245}, None
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
                        width = int(size_match.group(1))
                        height = int(size_match.group(2))
                        w_cm = round(width/10)
                        h_cm = round(height/10)
                        c_part = f"{w_cm}x{h_cm} cm"
        if a_part or c_part:
            field_300 = "=300  \\\\$a"
            if a_part: field_300+=a_part
            if c_part:
                field_300 += f" ;$c{c_part}." if a_part else f"$c{c_part}."
        else:
            field_300 = "=300  \\$a1책."
        return field_300, None
    except Exception as e:
        return "=300  \\$a1책.", f"크롤링 예외: {e}"

# =========================
# --- KPIPA + 문체부 주소 크롤링 ---
# =========================

def get_publisher_address_from_kpipa(isbn):
    search_url = "https://bnk.kpipa.or.kr/home/v3/addition/search"
    params = {"ST": isbn, "PG": 1, "PG2": 1, "DSF": "Y", "SO": "weight", "DT": "A"}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(search_url, params=params, headers=headers, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        first_result_link = soup.select_one("a.book-grid-item")
        if not first_result_link: return None, "❌ KPIPA 검색 결과 없음"
        detail_url = f"https://bnk.kpipa.or.kr{first_result_link.get('href')}"
        detail_res = requests.get(detail_url, headers=headers, timeout=15)
        detail_res.raise_for_status()
        detail_soup = BeautifulSoup(detail_res.text, "html.parser")
        addr_tag = detail_soup.find("dt", string="주소")
        if addr_tag:
            dd_tag = addr_tag.find_next_sibling("dd")
            if dd_tag: return dd_tag.get_text(strip=True), None
        return None, "❌ KPIPA에서 주소 항목을 찾을 수 없음"
    except Exception as e:
        return None, f"KPIPA 주소 예외: {e}"

def get_publisher_address_from_mcst(publisher_name):
    try:
        url = "https://book.mcst.go.kr/html/searchList.php"
        params = {"search_area": "전체","search_state":1,"search_kind":1,"search_type":
