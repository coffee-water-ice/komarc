import streamlit as st
import requests
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup

# =========================
# --- 구글시트 캐싱 ---
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
    region_data = region_sheet.get_all_values()[1:]      # 헤더 제외

    return publisher_data, region_data

# =========================
# --- 출판사명/주소 정규화 ---
# =========================
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

# =========================
# --- 구글시트 기반 출판사 → 지역 조회 ---
# =========================
def get_publisher_location(publisher_name, publisher_data):
    try:
        target = normalize_publisher_name(publisher_name)
        for row in publisher_data:
            if len(row) < 3: continue
            sheet_name, region = row[1], row[2]
            if normalize_publisher_name(sheet_name) == target:
                return region.strip() or "출판지 미상"
        return "출판지 미상"
    except Exception:
        return "예외 발생"

# =========================
# --- 발행국 부호 조회 ---
# =========================
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
            if len(row) < 2: continue
            sheet_region, country_code = row[0], row[1]
            if normalize_region_for_code(sheet_region) == normalized_input:
                return country_code.strip() or "xxu"
        return "xxu"
    except Exception:
        return "xxu"

# =========================
# --- Aladin API ---
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
        if "item" not in data or not data["item"]: return None, f"도서 정보 없음 [응답: {data}]"

        book = data["item"][0]
        title = book.get("title", "제목 없음")
        author = book.get("author", "")
        publisher = book.get("publisher", "출판사 정보 없음")
        pubdate = book.get("pubDate", "")
        pubyear = pubdate[:4] if len(pubdate) >= 4 else "발행년도 없음"

        authors = [a.strip() for a in author.split(",")] if author else []
        creator_str = " ; ".join(authors) if authors else "저자 정보 없음"
        field_245 = f"=245  10$a{title} /$c{creator_str}"

        return {
            "title": title,
            "creator": creator_str,
            "publisher": publisher,
            "pubyear": pubyear,
            "245": field_245
        }, None
    except Exception as e:
        return None, f"Aladin API 예외: {e}"

# =========================
# --- Aladin 형태사항(300) 크롤링 ---
# =========================
def extract_physical_description_by_crawling(isbn):
    try:
        search_url = f"https://www.aladin.co.kr/search/wsearchresult.aspx?SearchWord={isbn}"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(search_url, headers=headers, timeout=15)
        if res.status_code != 200: return "=300  \\$a1책.", f"검색 실패 (status {res.status_code})"

        soup = BeautifulSoup(res.text, "html.parser")
        link_tag = soup.select_one("div.ss_book_box a.bo3")
        if not link_tag or not link_tag.get("href"):
            return "=300  \\$a1책.", "도서 링크를 찾을 수 없습니다."

        detail_url = link_tag["href"]
        detail_res = requests.get(detail_url, headers=headers, timeout=15)
        detail_soup = BeautifulSoup(detail_res.text, "html.parser")
        form_wrap = detail_soup.select_one("div.conts_info_list1")
        a_part, c_part = "", ""
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
                        w_cm = round(width / 10)
                        h_cm = round(height / 10)
                        c_part = f"{w_cm}x{h_cm} cm"
        field_300 = "=300  \\\\$a"
        if a_part: field_300 += a_part
        if c_part: field_300 += f" ;$c{c_part}." if a_part else f"$c{c_part}."
        if not (a_part or c_part): field_300 = "=300  \\$a1책."
        return field_300, None
    except Exception as e:
        return "=300  \\$a1책.", f"크롤링 예외: {e}"

# =========================
# --- KPIPA 검색 ---
# =========================
def get_publisher_info_from_kpipa(isbn):
    results = []
    headers = {"User-Agent": "Mozilla/5.0"}
    kpipa_url = "https://bnk.kpipa.or.kr/home/v3/addition/search"
    params = {"ST": isbn, "PG": 1, "PG2": 1, "DSF": "Y", "SO": "weight", "DT": "A"}
    try:
        res = requests.get(kpipa_url, params=params, headers=headers, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        for link in soup.select("a.book-grid-item"):
            detail_url = f"https://bnk.kpipa.or.kr{link.get('href')}"
            dres = requests.get(detail_url, headers=headers, timeout=15)
            dres.raise_for_status()
            dsoup = BeautifulSoup(dres.text, "html.parser")
            name_tag = dsoup.find("dt", string="출판사 / 임프린트")
            pub_name = name_tag.find_next_sibling("dd").get_text(strip=True) if name_tag else None
            addr_tag = dsoup.find("dt", string="주소")
            addr = addr_tag.find_next_sibling("dd").get_text(strip=True) if addr_tag else None
            results.append({"publisher": pub_name, "address": addr})
        return results if results else None, None
    except Exception as e:
        return None, f"KPIPA 예외: {e}"

# =========================
# --- 문체부 검색 ---
# =========================
def get_publisher_address_from_mcst(publisher_name):
    headers = {"User-Agent": "Mozilla/5.0"}
    search_url = "https://book.mcst.go.kr/html/searchList.php"
    params = {
        "search_area": "전체",
        "search_state": "1",
        "search_kind": "1",
        "search_type": "1",
        "search_word": publisher_name
    }
    try:
        response = requests.get(search_url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        results = []
        for row in soup.select("table.board tbody tr"):
            cols = row.find_all("td")
            if len(cols) >= 4:
                name = cols[1].get_text(strip=True)
                addr = cols[2].get_text(strip=True)
                results.append({
                    "name": name,
                    "address": addr,
                    "address_norm": normalize_publisher_location_for_display(addr)
                })
        if results: return results, None
        else: return None, "검색 결과 없음 (문체부)"
    except Exception as e:
        return None, f"문체부 검색 예외: {e}"

# =========================
# --- Streamlit UI ---
# =========================
st.title("📚 ISBN → KPIPA + 문체부 → KORMARC 변환기")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력하세요:")

if isbn_input:
    isbn_list = [re.sub(r"[^\d]", "", s) for s in isbn_input.split("/") if s.strip()]
    publisher_data, region_data = load_publisher_db()

    for idx, isbn in enumerate(isbn_list, start=1):
        st.markdown(f"---\n### 📘 {idx}. ISBN: `{isbn}`")
        debug_messages = []

        # 1) Aladin API
        result, error = search_aladin_by_isbn(isbn)
        if error: debug_messages.append(error)

        # 2) 형태사항
        field_300, err_300 = extract_physical_description_by_crawling(isbn)
        if err_300: debug_messages.append(err_300)

        # 3) KPIPA 검색
        kpipa_results, kpipa_err = get_publisher_info_from_kpipa(isbn)
        if kpipa_err: debug_messages.append(kpipa_err)

        final_addresses = []
        if kpipa_results:
            for entry in kpipa_results:
                addr = entry["address"] if entry["address"] else "출판지 미상"
                addr_norm = normalize_publisher_location_for_display(addr)
                country_code = get_country_code_by_region(addr_norm, region_data)
                final_addresses.append({
                    "publisher": entry["publisher"],
                    "address": addr_norm,
                    "country_code": country_code
                })
        else:
            # KPIPA 실패 시 문체부 검색
            if result:
                pub_name = result["publisher"]
                mcst_results, mcst_err = get_publisher_address_from_mcst(pub_name)
                if mcst_err: debug_messages.append(mcst_err)
                if mcst_results:
                    for entry in mcst_results:
                        country_code = get_country_code_by_region(entry["address_norm"], region_data)
                        final_addresses.append({
                            "publisher": entry["name"],
                            "address": entry["address_norm"],
                            "country_code": country_code
                        })
                else:
                    final_addresses.append({
                        "publisher": pub_name,
                        "address": "출판지 미상",
                        "country_code": "xxu"
                    })
            else:
                final_addresses.append({
                    "publisher": "정보 없음",
                    "address": "출판지 미상",
                    "country_code": "xxu"
                })

        # 4) Streamlit 출력
        with st.expander("📖 KORMARC 변환 결과"):
            if result: st.code(result["245"], language="text")
            for dr in final_addresses:
                st.code(f"=008  \\$a{dr['country_code']}", language="text")
                st.code(f"=260  \\$a{dr['address']} :$b{dr['publisher']},$c{result['pubyear'] if result else '발행년도 없음'}.", language="text")
                st.code(field_300, language="text")

        if debug_messages:
            with st.expander("🛠️ 디버깅 메시지"):
                for m in debug_messages: st.write(m)
