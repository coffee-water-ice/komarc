import streamlit as st
import requests
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup

# --- 구글 시트 데이터 한번만 읽기 및 캐싱 ---
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

# --- 출판사 지역명 정규화 함수 ---
def normalize_publisher_location(location_name):
    location_name = location_name.strip()

    major_cities = ["서울", "인천", "대전", "광주", "울산", "대구", "부산"]

    for city in major_cities:
        if city in location_name:
            return location_name[:2]

    parts = location_name.split()
    if len(parts) > 1:
        loc = parts[1]
    else:
        loc = parts[0]

    if loc.endswith("시") or loc.endswith("군"):
        loc = loc[:-1]

    return loc

# --- 발행국 부호 구하기 (region_data 활용) ---
def get_country_code_by_region(region_name, region_data):
    try:
        st.write(f"🌍 발행국 부호 찾는 중... 참조 지역: `{region_name}`")

        def normalize_region(region):
            region = region.strip()
            if region.startswith(("전라", "충청", "경상")):
                if len(region) >= 3:
                    return region[0] + region[2]
                else:
                    return region[:2]
            else:
                return region[:2]

        normalized_input = normalize_region(region_name)
        st.write(f"🧪 정규화된 참조지역: `{normalized_input}`")

        for row in region_data:
            if len(row) < 2:
                continue
            sheet_region, country_code = row[0], row[1]
            if normalize_region(sheet_region) == normalized_input:
                return country_code.strip() or "xxu"

        return "xxu"

    except Exception as e:
        st.write(f"⚠️ 오류 발생: {e}")
        return "xxu"

# --- 출판사 지역명 추출 (publisher_data 활용) ---
def get_publisher_location(publisher_name, publisher_data):
    try:
        st.write(f"📥 출판사 지역을 구글 시트에서 찾는 중입니다... `{publisher_name}`")

        def normalize(name):
            return re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사", "", name).lower()

        target = normalize(publisher_name)
        st.write(f"🧪 정규화된 입력값: `{target}`")

        for row in publisher_data:
            if len(row) < 3:
                continue
            sheet_name, region = row[1], row[2]
            if normalize(sheet_name) == target:
                return region.strip() or "출판지 미상"

        for row in publisher_data:
            if len(row) < 3:
                continue
            sheet_name, region = row[1], row[2]
            if sheet_name.strip() == publisher_name.strip():
                return region.strip() or "출판지 미상"

        return "출판지 미상"

    except Exception as e:
        st.write(f"⚠️ 오류 발생: {e}")
        return "예외 발생"

# --- ISBN으로 출판사명 추가 크롤링 ---
def get_publisher_name_from_isbn(isbn):
    search_url = "https://bnk.kpipa.or.kr/home/v3/addition/search"
    params = {
        "ST": isbn,
        "PG": 1,
        "PG2": 1,
        "DSF": "Y",
        "SO": "weight",
        "DT": "A"
    }
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        res = requests.get(search_url, params=params, headers=headers)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        first_result_link = soup.select_one("a.book-grid-item")
        if not first_result_link:
            return None, None, "❌ 검색 결과 없음"

        detail_href = first_result_link["href"]
        detail_url = f"https://bnk.kpipa.or.kr{detail_href}"
        detail_res = requests.get(detail_url, headers=headers)
        detail_res.raise_for_status()
        detail_soup = BeautifulSoup(detail_res.text, "html.parser")

        pub_info_tag = detail_soup.find("dt", string="출판사 / 임프린트")
        if not pub_info_tag:
            return None, None, "❌ '출판사 / 임프린트' 항목을 찾을 수 없습니다."

        dd_tag = pub_info_tag.find_next_sibling("dd")
        if dd_tag:
            full_text = dd_tag.get_text(strip=True)
            # '/' 앞부분(출판사명)만 추출 및 정규화
            publisher_name_full = full_text
            def normalize(name):
                return re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사", "", name).lower()
            publisher_name_part = publisher_name_full.split("/")[0].strip()
            publisher_name_norm = normalize(publisher_name_part)
            return publisher_name_full, publisher_name_norm, None

        return None, None, "❌ 'dd' 태그에서 텍스트를 추출할 수 없습니다."
    except Exception as e:
        return None, None, f"❌ 예외 발생: {e}"

# --- API 기반 도서정보 가져오기 ---
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

        res = requests.get(url, params=params)
        if res.status_code != 200:
            return None, f"API 요청 실패 (status: {res.status_code})"

        data = res.json()
        if "item" not in data or not data["item"]:
            return None, f"도서 정보를 찾을 수 없습니다. [응답 내용: {data}]"

        book = data["item"][0]

        title = book.get("title", "제목 없음")
        author = book.get("author", "")
        publisher = book.get("publisher", "출판사 정보 없음")
        pubdate = book.get("pubDate", "")
        pubyear = pubdate[:4] if len(pubdate) >= 4 else "발행년도 없음"

        authors = [a.strip() for a in author.split(",")]
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
        return None, f"API 예외 발생: {str(e)}"

# --- 형태사항 크롤링 추출 ---
def extract_physical_description_by_crawling(isbn):
    try:
        search_url = f"https://www.aladin.co.kr/search/wsearchresult.aspx?SearchWord={isbn}"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(search_url, headers=headers)
        if res.status_code != 200:
            return "=300  \\$a1책.", f"검색 실패 (status {res.status_code})"

        soup = BeautifulSoup(res.text, "html.parser")
        link_tag = soup.select_one("div.ss_book_box a.bo3")
        if not link_tag or not link_tag.get("href"):
            return "=300  \\$a1책.", "도서 링크를 찾을 수 없습니다."

        detail_url = link_tag["href"]
        detail_res = requests.get(detail_url, headers=headers)
        if detail_res.status_code != 200:
            return "=300  \\$a1책.", f"상세페이지 요청 실패 (status {detail_res.status_code})"

        detail_soup = BeautifulSoup(detail_res.text, "html.parser")
        form_wrap = detail_soup.select_one("div.conts_info_list1")
        a_part = ""
        c_part = ""

        if form_wrap:
            form_items = [item.strip() for item in form_wrap.stripped_strings]
            for item in form_items:
                if re.search(r"(쪽|p)\s*$", item):
                    page_match = re.search(r"\d+", item)
                    if page_match:
                        a_part = f"{page_match.group()} p."
                elif "mm" in item:
                    size_match = re.search(r"(\d+)\s*[\*x×X]\s*(\d+)", item)
                    if size_match:
                        width = int(size_match.group(1))
                        height = int(size_match.group(2))
                        if width == height or width > height or width < height / 2:
                            w_cm = round(width / 10)
                            h_cm = round(height / 10)
                            c_part = f"{w_cm}x{h_cm} cm"
                        else:
                            h_cm = round(height / 10)
                            c_part = f"{h_cm} cm"

        if a_part or c_part:
            field_300 = "=300  \\\\$a"
            if a_part:
                field_300 += a_part
            if c_part:
                field_300 += f" ;$c{c_part}."
        else:
            field_300 = "=300  \\$a1책."

        return field_300, None

    except Exception as e:
        return "=300  \\$a1책.", f"예외 발생: {str(e)}"


# --- Streamlit UI ---
st.title("📚 ISBN → API + 크롤링 → KORMARC 변환기")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력하세요:")

if isbn_input:
    isbn_list = [re.sub(r"[^\d]", "", isbn) for isbn in isbn_input.split("/") if isbn.strip()]

    # 구글 시트 데이터 한번만 로드
    publisher_data, region_data = load_publisher_db()

    for idx, isbn in enumerate(isbn_list, 1):
        st.markdown(f"---\n### 📘 {idx}. ISBN: `{isbn}`")

        debug_messages = []

        with st.spinner("🔍 도서 정보 검색 중..."):
            result, error = search_aladin_by_isbn(isbn)
        if error:
            debug_messages.append(f"❌ 오류: {error}")

        with st.spinner("📐 형태사항 크롤링 중..."):
            field_300, err_300 = extract_physical_description_by_crawling(isbn)
        if err_300:
            debug_messages.append(f"⚠️ 형태사항 크롤링 경고: {err_300}")

        if result:
            publisher = result["publisher"]
            pubyear = result["pubyear"]

            if publisher == "출판사 정보 없음":
                location_raw = "[출판지 미상]"
                location_norm = location_raw

                with st.spinner("🔎 추가 출판사명 검색 중..."):
                    pub_name_full, pub_name_norm, crawl_err = get_publisher_name_from_isbn(isbn)
                    if pub_name_full:
                        debug_messages.append("🔔 출판사 지명 미상으로 추가 검색 진행됨")
                        debug_messages.append(f"🔍 크롤링된 '출판사 / 임프린트' 전체: {pub_name_full}")
                        debug_messages.append(f"🔍 '/' 앞부분 출판사명 정규화: {pub_name_norm}")

                        location_raw = get_publisher_location(pub_name_norm, publisher_data)
                        location_norm = normalize_publisher_location(location_raw)
                        debug_messages.append(f"🏙️ 출판사 지역 (추가 검색): {location_raw} / 정규화: {location_norm}")
                    else:
                        debug_messages.append(f"❌ 추가 검색 실패: {crawl_err}")

            else:
                with st.spinner(f"📍 '{publisher}'의 지역정보 검색 중..."):
                    location_raw = get_publisher_location(publisher, publisher_data)
                    location_norm = normalize_publisher_location(location_raw)

            if publisher != "출판사 정보 없음":
                debug_messages.append(f"🏙️ 출판사 지역 (원본): {location_raw}")
                debug_messages.append(f"🏙️ 출판사 지역 (정규화): {location_norm}")

            country_code = get_country_code_by_region(location_raw, region_data)

            with st.container():
                st.code(f"=008  \\$a{country_code}", language="text")
                st.code(result["245"], language="text")
                st.code(f"=260  \\$a{location_norm} :$b{publisher},$c{pubyear}.", language="text")
                st.code(field_300, language="text")

        else:
            debug_messages.append("⚠️ 결과 없음")

        if debug_messages:
            with st.expander("🛠️ 디버깅 및 경고 메시지 보기"):
                for msg in debug_messages:
                    st.write(msg)
