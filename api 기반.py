import streamlit as st
import requests
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import copy

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


# --- 출판사명 정규화(구글시트 대조용) ---
def normalize_publisher_name(name):
    return re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사|프레스", "", name).lower()


# --- 출판사 지역명 표시용 정규화 (UI/260에 쓸 이름) ---
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
    if loc.endswith("시") or loc.endswith("군"):
        loc = loc[:-1]
    return loc


# --- 구글시트(publisher_data)에서 출판사 → 지역 조회 (캐시된 데이터 사용) ---
def get_publisher_location(publisher_name, publisher_data):
    try:
        st.write(f"📥 출판사 지역을 구글 시트에서 찾는 중입니다... `{publisher_name}`")
        target = normalize_publisher_name(publisher_name)
        st.write(f"🧪 정규화된 입력값: `{target}`")

        for row in publisher_data:
            if len(row) < 3:
                continue
            sheet_name, region = row[1], row[2]
            if normalize_publisher_name(sheet_name) == target:
                return region.strip() or "출판지 미상"

        # fallback: 원본 문자열 일치
        for row in publisher_data:
            if len(row) < 3:
                continue
            sheet_name, region = row[1], row[2]
            if sheet_name.strip() == publisher_name.strip():
                return region.strip() or "출판지 미상"

        return "출판지 미상"
    except Exception as e:
        st.write(f"⚠️ get_publisher_location 예외: {e}")
        return "예외 발생"


# --- 출판사명에서 대표명과 별칭(괄호/슬래시 분리) 추출 ---
def split_publisher_aliases(name):
    aliases = []

    # 괄호 안 내용 추출, 쉼표나 슬래시로 나누기
    bracket_contents = re.findall(r"\((.*?)\)", name)
    for content in bracket_contents:
        parts = re.split(r"[,/]", content)
        parts = [p.strip() for p in parts if p.strip()]
        aliases.extend(parts)

    # 괄호 제거
    name_no_brackets = re.sub(r"\(.*?\)", "", name).strip()

    # 슬래시 분리
    if "/" in name_no_brackets:
        parts = [p.strip() for p in name_no_brackets.split("/") if p.strip()]
        rep_name = parts[0]
        aliases.extend(parts[1:])
    else:
        rep_name = name_no_brackets

    return rep_name, aliases


# --- 괄호/별칭 분리 후 두번 검색 적용한 출판지 조회 ---
def search_publisher_location_with_alias(publisher_name, publisher_data):
    rep_name, aliases = split_publisher_aliases(publisher_name)

    st.write(f"🔍 대표명으로 1차 검색: `{rep_name}`")
    location = get_publisher_location(rep_name, publisher_data)
    if location != "출판지 미상":
        return location

    # 1차에서 미상일 경우 별칭으로 2차 검색
    for alias in aliases:
        st.write(f"🔍 별칭으로 2차 검색 시도: `{alias}`")
        location = get_publisher_location(alias, publisher_data)
        if location != "출판지 미상":
            return location

    return "출판지 미상"


# --- 구글시트(region_data)로 발행국 부호 조회 (캐시된 데이터 사용) ---
def get_country_code_by_region(region_name, region_data):
    try:
        st.write(f"🌍 발행국 부호 찾는 중... 참조 지역: `{region_name}`")

        def normalize_region_for_code(region):
            region = (region or "").strip()
            if region.startswith(("전라", "충청", "경상")):
                if len(region) >= 3:
                    return region[0] + region[2]
                return region[:2]
            return region[:2]

        normalized_input = normalize_region_for_code(region_name)
        st.write(f"🧪 정규화된 참조지역(코드대조용): `{normalized_input}`")

        for row in region_data:
            if len(row) < 2:
                continue
            sheet_region, country_code = row[0], row[1]
            if normalize_region_for_code(sheet_region) == normalized_input:
                return country_code.strip() or "xxu"

        return "xxu"
    except Exception as e:
        st.write(f"⚠️ get_country_code_by_region 예외: {e}")
        return "xxu"


# --- Aladin API: ISBN으로 도서 정보 조회 (title, author, publisher, pubyear, 245 필드) ---
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
        if res.status_code != 200:
            return None, f"API 요청 실패 (status: {res.status_code})"

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

        return {
            "title": title,
            "creator": creator_str,
            "publisher": publisher,
            "pubyear": pubyear,
            "245": field_245
        }, None

    except Exception as e:
        return None, f"Aladin API 예외: {e}"


# --- Aladin 크롤링: 형태사항(쪽수/크기) 추출 (300 필드 생성) ---
def extract_physical_description_by_crawling(isbn):
    try:
        search_url = f"https://www.aladin.co.kr/search/wsearchresult.aspx?SearchWord={isbn}"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(search_url, headers=headers, timeout=15)
        if res.status_code != 200:
            return "=300  \\$a1책.", f"검색 실패 (status {res.status_code})"

        soup = BeautifulSoup(res.text, "html.parser")
        link_tag = soup.select_one("div.ss_book_box a.bo3")
        if not link_tag or not link_tag.get("href"):
            return "=300  \\$a1책.", "도서 링크를 찾을 수 없습니다."

        detail_url = link_tag["href"]
        detail_res = requests.get(detail_url, headers=headers, timeout=15)
        if detail_res.status_code != 200:
            return "=300  \\$a1책.", f"상세페이지 요청 실패 (status {detail_res.status_code})"

        detail_soup = BeautifulSoup(detail_res.text, "html.parser")
        form_wrap = detail_soup.select_one("div.conts_info_list1")
        a_part = ""
        c_part = ""

        if form_wrap:
            items = [s.strip() for s in form_wrap.stripped_strings]
            for item in items:
                # 쪽수 (~쪽, ~p)
                if re.search(r"(쪽|p)\s*$", item):
                    m = re.search(r"(\d+)\s*(쪽|p)?$", item)
                    if m:
                        a_part = f"{m.group(1)} p."
                # 크기 (mm 포함, ex. 148*210mm)
                elif "mm" in item:
                    size_match = re.search(r"(\d+)\s*[\*x×X]\s*(\d+)\s*mm", item)
                    if size_match:
                        width = int(size_match.group(1))
                        height = int(size_match.group(2))
                        w_cm = round(width / 10)
                        h_cm = round(height / 10)
                        c_part = f"{w_cm}x{h_cm} cm"

        if a_part or c_part:
            field_300 = "=300  \\\\$a"
            if a_part:
                field_300 += a_part
            if c_part:
                if a_part:
                    field_300 += f" ;$c{c_part}."
                else:
                    field_300 += f"$c{c_part}."
        else:
            field_300 = "=300  \\$a1책."

        return field_300, None

    except Exception as e:
        return "=300  \\$a1책.", f"크롤링 예외: {e}"


# --- KPIPA에서 ISBN으로 출판사 / 임프린트 크롤링 (원문 + 정규화) ---
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
# --- Streamlit UI 부분 ---
# =========================
st.title("📚 ISBN → API + 크롤링 → KORMARC 변환기")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력하세요:")

if isbn_input:
    isbn_list = [re.sub(r"[^\d]", "", s) for s in isbn_input.split("/") if s.strip()]

    # 구글 시트 데이터 한번만 로드 (캐시)
    publisher_data, region_data = load_publisher_db()

    for idx, isbn in enumerate(isbn_list, start=1):
        st.markdown(f"---\n### 📘 {idx}. ISBN: `{isbn}`")
        debug_messages = []

        # 1) Aladin API로 도서 정보 조회
        with st.spinner("🔍 도서 정보 검색 중..."):
            result, error = search_aladin_by_isbn(isbn)
        if error:
            debug_messages.append(f"❌ Aladin API 오류: {error}")

        # 2) 형태사항(300) 크롤링
        with st.spinner("📐 형태사항 크롤링 중..."):
            field_300, err_300 = extract_physical_description_by_crawling(isbn)
        if err_300:
            debug_messages.append(f"⚠️ 형태사항 크롤링 경고: {err_300}")

        if result:
            publisher = result["publisher"]
            pubyear = result["pubyear"]

            # 3) 출판사명 괄호/슬래시 분리 후 두 번 검색 적용하여 출판지 조회
            location_raw = search_publisher_location_with_alias(publisher, publisher_data)
            location_norm_for_display = normalize_publisher_location_for_display(location_raw)

            # 4) 추가 크롤링: **출판지 미상인 경우에만** KPIPA에서 출판사명 크롤링 시도
            if location_raw == "출판지 미상":
                debug_messages.append("🔔 출판지 미상 — KPIPA 추가 검색 실행")
                pub_full, pub_norm, crawl_err = get_publisher_name_from_isbn_kpipa(isbn)
                if crawl_err:
                    debug_messages.append(f"❌ KPIPA 크롤링 실패: {crawl_err}")
                else:
                    debug_messages.append(f"🔍 KPIPA 크롤링 원문('출판사 / 임프린트'): {pub_full}")
                    debug_messages.append(f"🧪 KPIPA에서 추출한 정규화된 출판사명: {pub_norm}")

                    # KPIPA에서 정규화한 출판사명으로 재검색 (publisher_data 사용)
                    new_location = get_publisher_location(pub_norm, publisher_data)
                    new_location_norm_display = normalize_publisher_location_for_display(new_location)
                    debug_messages.append(f"🏙️ KPIPA 기반 재검색 결과: {new_location} / 정규화: {new_location_norm_display}")

                    if new_location and new_location not in ("출판지 미상", "예외 발생"):
                        location_raw = new_location
                        location_norm_for_display = new_location_norm_display

            # 5) 발행국 부호 조회 (region_data 사용)
            country_code = get_country_code_by_region(location_raw, region_data)

            # ▶ 출력: 008, 245, 260, 300
            with st.container():
                st.code(f"=008  \\$a{country_code}", language="text")
                st.code(result["245"], language="text")
                st.code(f"=260  \\$a{location_norm_for_display} :$b{publisher},$c{pubyear}.", language="text")
                st.code(field_300, language="text")

        else:
            debug_messages.append("⚠️ Aladin에서 도서 정보를 가져오지 못했습니다.")

        # ▶ 디버깅 메시지 출력
        if debug_messages:
            with st.expander("🛠️ 디버깅 및 경고 메시지"):
                for m in debug_messages:
                    st.write(m)
