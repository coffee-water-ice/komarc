import streamlit as st
import requests
import re
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# =========================
# --- 구글시트 로드 & 출판사/지역 조회 ---
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

def normalize_stage1(name):
    return re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사|프레스", "", name).lower()

def normalize_stage2(name):
    name = re.sub(r"(주니어|어린이)", "", name, flags=re.IGNORECASE)
    eng_to_kor = {"springer":"스프링거","cambridge":"케임브리지","oxford":"옥스포드"}
    for eng, kor in eng_to_kor.items():
        name = re.sub(eng, kor, name, flags=re.IGNORECASE)
    return name.strip().lower()

def normalize_location_for_display(location_name):
    if not location_name or location_name in ("출판지 미상", "예외 발생"):
        return location_name
    location_name = location_name.strip()
    major_cities = ["서울", "인천", "대전", "광주", "울산", "대구", "부산"]
    for city in major_cities:
        if city in location_name: return location_name[:2]
    parts = location_name.split()
    loc = parts[1] if len(parts) > 1 else parts[0]
    if loc.endswith("시") or loc.endswith("군"): loc = loc[:-1]
    return loc

def get_publisher_location(publisher_name, publisher_data):
    target = normalize_stage1(publisher_name)
    for row in publisher_data:
        if len(row)<3: continue
        sheet_name, region = row[1], row[2]
        if normalize_stage1(sheet_name) == target:
            return region.strip() or "출판지 미상"
    for row in publisher_data:
        if len(row)<3: continue
        sheet_name, region = row[1], row[2]
        if sheet_name.strip() == publisher_name.strip():
            return region.strip() or "출판지 미상"
    return "출판지 미상"

def get_publisher_location_stage2(publisher_name, publisher_data):
    target = normalize_stage2(publisher_name)
    for row in publisher_data:
        if len(row)<3: continue
        sheet_name, region = row[1], row[2]
        if normalize_stage2(sheet_name) == target:
            return region.strip() or "출판지 미상"
    return "출판지 미상"

def split_publisher_aliases(name):
    aliases = []
    bracket_contents = re.findall(r"\((.*?)\)", name)
    for content in bracket_contents:
        parts = re.split(r"[,/]", content)
        aliases.extend([p.strip() for p in parts if p.strip()])
    name_no_brackets = re.sub(r"\(.*?\)","",name).strip()
    if "/" in name_no_brackets:
        parts = [p.strip() for p in name_no_brackets.split("/") if p.strip()]
        rep_name = parts[0]; aliases.extend(parts[1:])
    else: rep_name = name_no_brackets
    return rep_name, aliases

def get_country_code_by_region(region_name, region_data):
    try:
        def normalize_region(region):
            region = (region or "").strip()
            if region.startswith(("전라","충청","경상")):
                return region[0]+region[2] if len(region)>=3 else region[:2]
            return region[:2]
        normalized_input = normalize_region(region_name)
        for row in region_data:
            if len(row)<2: continue
            sheet_region, country_code = row[0], row[1]
            if normalize_region(sheet_region) == normalized_input:
                return country_code.strip() or "xxu"
        return "xxu"
    except: return "xxu"

# =========================
# --- Aladin API ---
# =========================
def search_aladin_by_isbn(isbn):
    try:
        ttbkey = st.secrets["aladin"]["ttbkey"]
        url = "https://www.aladin.co.kr/ttb/api/ItemLookUp.aspx"
        params = {"ttbkey":ttbkey,"itemIdType":"ISBN","ItemId":isbn,"output":"js","Version":"20131101"}
        res = requests.get(url, params=params, timeout=15)
        if res.status_code != 200: return None, f"API 요청 실패 (status: {res.status_code})"
        data = res.json()
        if "item" not in data or not data["item"]:
            return None, f"도서 정보를 찾을 수 없습니다. [응답: {data}]"
        book = data["item"][0]
        title = book.get("title","제목 없음")
        author = book.get("author","")
        publisher = book.get("publisher","출판사 정보 없음")
        pubdate = book.get("pubDate",""); pubyear = pubdate[:4] if len(pubdate)>=4 else "발행년도 없음"
        authors = [a.strip() for a in author.split(",")] if author else []
        creator_str = " ; ".join(authors) if authors else "저자 정보 없음"
        field_245 = f"=245  10$a{title} /$c{creator_str}"
        return {"title":title,"creator":creator_str,"publisher":publisher,"pubyear":pubyear,"245":field_245}, None
    except Exception as e:
        return None, f"Aladin API 예외: {e}"

# =========================
# --- 300 크롤링 ---
# =========================
def extract_physical_description_by_crawling(isbn):
    try:
        search_url = f"https://www.aladin.co.kr/search/wsearchresult.aspx?SearchWord={isbn}"
        headers = {"User-Agent":"Mozilla/5.0"}
        res = requests.get(search_url, headers=headers, timeout=15)
        if res.status_code !=200: return "=300  \\$a1책.", f"검색 실패 (status {res.status_code})"
        soup = BeautifulSoup(res.text,"html.parser")
        link_tag = soup.select_one("div.ss_book_box a.bo3")
        if not link_tag or not link_tag.get("href"): return "=300  \\$a1책.","도서 링크를 찾을 수 없습니다."
        detail_url = link_tag["href"]
        detail_res = requests.get(detail_url, headers=headers, timeout=15)
        if detail_res.status_code !=200: return "=300  \\$a1책.", f"상세페이지 요청 실패 (status {detail_res.status_code})"
        detail_soup = BeautifulSoup(detail_res.text,"html.parser")
        form_wrap = detail_soup.select_one("div.conts_info_list1")
        a_part, c_part = "", ""
        if form_wrap:
            items = [s.strip() for s in form_wrap.stripped_strings]
            for item in items:
                if re.search(r"(쪽|p)\s*$", item):
                    m = re.search(r"(\d+)\s*(쪽|p)?$", item)
                    if m: a_part = f"{m.group(1)} p."
                elif "mm" in item:
                    m = re.search(r"(\d+)\s*[\*x×X]\s*(\d+)\s*mm", item)
                    if m: c_part = f"{round(int(m.group(1))/10)}x{round(int(m.group(2))/10)} cm"
        field_300 = "=300  \\\\$a"
        if a_part: field_300+=a_part
        if c_part: field_300+=f" ;$c{c_part}." if a_part else f"$c{c_part}."
        if not(a_part or c_part): field_300="=300  \\$a1책."
        return field_300, None
    except Exception as e:
        return "=300  \\$a1책.", f"크롤링 예외: {e}"

# =========================
# --- KPIPA 크롤링 ---
# =========================
def get_publisher_name_from_isbn_kpipa(isbn):
    search_url = "https://bnk.kpipa.or.kr/home/v3/addition/search"
    params = {"ST":isbn,"PG":1,"PG2":1,"DSF":"Y","SO":"weight","DT":"A"}
    headers = {"User-Agent":"Mozilla/5.0"}
    try:
        res = requests.get(search_url, params=params, headers=headers, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text,"html.parser")
        first_result_link = soup.select_one("a.book-grid-item")
        if not first_result_link: return None,None,"❌ 검색 결과 없음 (KPIPA)"
        detail_href = first_result_link.get("href")
        detail_url = f"https://bnk.kpipa.or.kr{detail_href}"
        detail_res = requests.get(detail_url, headers=headers, timeout=15)
        detail_res.raise_for_status()
        detail_soup = BeautifulSoup(detail_res.text,"html.parser")
        pub_info_tag = detail_soup.find("dt", string="출판사 / 임프린트")
        if not pub_info_tag: return None,None,"❌ '출판사 / 임프린트' 항목을 찾을 수 없습니다. (KPIPA)"
        dd_tag = pub_info_tag.find_next_sibling("dd")
        if dd_tag:
            full_text = dd_tag.get_text(strip=True)
            publisher_name_full = full_text
            publisher_name_part = publisher_name_full.split("/")[0].strip()
            publisher_name_norm = re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사|프레스","",publisher_name_part).lower()
            return publisher_name_full, publisher_name_norm, None
        return None,None,"❌ 'dd' 태그에서 텍스트를 추출할 수 없습니다. (KPIPA)"
    except Exception as e:
        return None,None,f"KPIPA 예외: {e}"

# =========================
# --- 문체부 검색 ---
# =========================
def get_mcst_address(publisher_name):
    url = "https://book.mcst.go.kr/html/searchList.php"
    params = {"search_area":"전체","search_state":"1","search_kind":"1","search_type":"1","search_word":publisher_name}
    try:
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text,"html.parser")
        results=[]
        for row in soup.select("table.board tbody tr"):
            cols = row.find_all("td")
            if len(cols)>=4:
                reg_type = cols[0].get_text(strip=True)
                name = cols[1].get_text(strip=True)
                address = cols[2].get_text(strip=True)
                status = cols[3].get_text(strip=True)
                if status=="영업": results.append((reg_type,name,address,status))
        if results: return results[0][2],results
        return "미확인",[]
    except Exception as e: return f"오류: {e}",[]

def get_mcst_address_with_normalization(publisher_name):
    addr,results = get_mcst_address(publisher_name)
    if results: return addr,results
    name_stage2 = normalize_stage2(publisher_name)
    if name_stage2 != publisher_name.lower():
        addr2,results2 = get_mcst_address(name_stage2)
        if results2: return addr2,results2
    return "미확인",results2 if 'results2' in locals() else []

# =========================
# --- Streamlit UI ---
# =========================
st.title("📚 ISBN → KORMARC 변환기 (검색 순서 변경)")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력하세요:")

if isbn_input:
    isbn_list = [re.sub(r"[^\d]","",s) for s in isbn_input.split("/") if s.strip()]
    publisher_data, region_data = load_publisher_db()

    for idx, isbn in enumerate(isbn_list, start=1):
        st.markdown(f"---\n### 📘 {idx}. ISBN: `{isbn}`")
        debug_messages=[]

        # 1) Aladin API
        result, error = search_aladin_by_isbn(isbn)
        if error: debug_messages.append(f"❌ Aladin API 오류: {error}")

        # 2) 형태사항(300)
        field_300, err_300 = extract_physical_description_by_crawling(isbn)
        if err_300: debug_messages.append(f"⚠️ 형태사항 크롤링 경고: {err_300}")

        if result:
            publisher = result["publisher"]; pubyear = result["pubyear"]

            # --- 1차 정규화 구글시트 ---
            location_raw = get_publisher_location(publisher, publisher_data)
            location_display = normalize_location_for_display(location_raw)

            # --- 2차 정규화 구글시트 ---
            if location_raw=="출판지 미상":
                location_raw = get_publisher_location_stage2(publisher, publisher_data)
                location_display = normalize_location_for_display(location_raw)
                if location_raw!="출판지 미상":
                    debug_messages.append("✅ 2차 정규화로 구글시트 검색 성공")

            # --- KPIPA 검색 ---
            if location_raw=="출판지 미상":
                pub_full, pub_norm, crawl_err = get_publisher_name_from_isbn_kpipa(isbn)
                if crawl_err: debug_messages.append(f"❌ KPIPA 크롤링 실패: {crawl_err}")
                else:
                    new_location = get_publisher_location(pub_norm, publisher_data)
                    if new_location != "출판지 미상":
                        location_raw=new_location
                        location_display = normalize_location_for_display(new_location)
                        debug_messages.append(f"🏙️ KPIPA 기반 재검색 결과: {new_location}")

            # --- 문체부 검색 ---
            if location_raw=="출판지 미상":
                mcst_address, mcst_results = get_mcst_address_with_normalization(publisher)
                if mcst_results:
                    location_display = normalize_location_for_display(mcst_results[0][2])
                    location_raw = location_display
                    debug_messages.append(f"🏛️ 문체부 주소 검색 결과 사용: {location_display}")
                else:
                    location_display = "미확인"

            # --- 발행국 부호 ---
            country_code = get_country_code_by_region(location_raw, region_data)

            # ▶ KORMARC 출력
            st.code(f"=008  \\$a{country_code}",language="text")
            st.code(result["245"],language="text")
            st.code(f"=260  \\$a{location_display} :$b{publisher},$c{pubyear}.",language="text")
            st.code(field_300,language="text")

        if debug_messages:
            with st.expander("🛠️ 디버깅/경고 메시지"):
                for m in debug_messages: st.write(m)
