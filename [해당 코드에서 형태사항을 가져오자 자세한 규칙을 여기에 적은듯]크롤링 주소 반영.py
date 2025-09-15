import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import copy
import traceback

# 🔹 발행국 부호 구하기 (구글 시트 Sheet2 활용)
def get_country_code_by_region(region_name):
    try:
        st.write(f"🌍 발행국 부호 찾는 중... 참조 지역: `{region_name}`")

        json_key = dict(st.secrets["gspread"])
        json_key["private_key"] = json_key["private_key"].replace('\\n', '\n')

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key, scope)
        client = gspread.authorize(creds)
        sheet = client.open("출판사 DB").worksheet("Sheet2")

        region_col = sheet.col_values(1)[1:]  # A열: 지역명 (기준)
        code_col = sheet.col_values(2)[1:]    # B열: 발행국 부호

        # ✅ 정규화 함수
        def normalize_region(region):
            region = region.strip()
            original = region

            # 1. 특별자치도 제거 (단, 따로 표시해 기억)
            was_teukbyeol = "특별자치도" in region
            region = re.sub(r"(광역시|특별시|특별자치도)", "", region)

            # 2. 예외 처리
            if region in ["강원도", "제주도", "경기도"]:
                return region.replace("도", "")

            # 3. ~도 처리 (특별자치도였던 항목은 여기서 제외)
            if region.endswith("도") and len(region) >= 4 and not was_teukbyeol:
                return region[0] + region[2]

            # 4. ~시 처리
            if region.endswith("시"):
                return region[:-1]

            return region

            

        normalized_input = normalize_region(region_name)
        st.write(f"🧪 정규화된 참조지역: `{normalized_input}`")

        for sheet_region, country_code in zip(region_col, code_col):
            if normalize_region(sheet_region) == normalized_input:
                return country_code.strip() or "xxu"

        return "xxu"  # 미상

    except Exception as e:
        return "xxu"


# 🔹 Google Sheets에서 지역명 추출 (디버깅 포함)
def get_publisher_location(publisher_name):
    try:
        st.write(f"📥 출판사 지역을 구글 시트에서 찾는 중입니다...")
        st.write(f"🔍 입력된 출판사명: `{publisher_name}`")

        # ✅ st.secrets는 dict로 변환 (deepcopy 금지)
        json_key = dict(st.secrets["gspread"])
        json_key["private_key"] = json_key["private_key"].replace('\\n', '\n')

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key, scope)
        client = gspread.authorize(creds)
        sheet = client.open("출판사 DB").worksheet("Sheet1")

        publisher_names = sheet.col_values(2)[1:]  # B열
        regions = sheet.col_values(3)[1:]          # C열

        def normalize(name):
            return re.sub(r"\s|\(.*?\)|주식회사|㈜|도서출판|출판사", "", name).lower()

        target = normalize(publisher_name)
        st.write(f"🧪 정규화된 입력값: `{target}`")

        preview_names = [normalize(name) for name in publisher_names[:10]]
        st.write(f"📋 구글 시트 내 출판사 정규화 리스트 (상위 10개): `{preview_names}`")

        for sheet_name, region in zip(publisher_names, regions):
            if normalize(sheet_name) == target:
                return region.strip() or "출판지 미상"

        for sheet_name, region in zip(publisher_names, regions):
            if sheet_name.strip() == publisher_name.strip():
                return region.strip() or "출판지 미상"

        return "출판지 미상"

    except Exception as e:
        return f"예외 발생: {str(e)}"


# 🔹 알라딘 상세 페이지 파싱 (형태사항 포함)
def parse_aladin_detail_page(html):
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.select_one("span.Ere_bo_title")
    title = title_tag.text.strip() if title_tag else "제목 없음"

    li_tag = soup.select_one("li.Ere_sub2_title")
    author_list = []
    publisher = ""
    pubyear = ""

    if li_tag:
        children = li_tag.contents
        last_a_before_date = None

        for i, node in enumerate(children):
            if getattr(node, "name", None) == "a":
                name = node.text.strip()
                next_text = children[i+1].strip() if i+1 < len(children) and isinstance(children[i+1], str) else ""
                if "지은이" in next_text:
                    author_list.append(f"{name} 지음")
                elif "옮긴이" in next_text:
                    author_list.append(f"{name} 옮김")
                else:
                    last_a_before_date = name
            elif isinstance(node, str):
                date_match = re.search(r"\d{4}-\d{2}-\d{2}", node)
                if date_match:
                    pubyear = date_match.group().split("-")[0]
                    if last_a_before_date:
                        publisher = last_a_before_date

    # ✅ 형태사항 정보 추출
    form_wrap = soup.select_one("div.conts_info_list1")
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

    creator_str = " ; ".join(author_list) if author_list else "저자 정보 없음"
    publisher = publisher if publisher else "출판사 정보 없음"
    pubyear = pubyear if pubyear else "발행연도 없음"

    return {
        "title": title,
        "creator": creator_str,
        "publisher": publisher,
        "pubyear": pubyear,
        "245": f"=245  10$a{title} /$c{creator_str}",
        "300": field_300
    }

# 🔹 알라딘 ISBN 검색
def search_aladin_by_isbn(isbn):
    search_url = f"https://www.aladin.co.kr/search/wsearchresult.aspx?SearchWord={isbn}"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        res = requests.get(search_url, headers=headers)
        if res.status_code != 200:
            return None, f"검색 실패 (status {res.status_code})"

        soup = BeautifulSoup(res.text, "html.parser")
        link_tag = soup.select_one("div.ss_book_box a.bo3")
        if not link_tag or not link_tag.get("href"):
            return None, "도서 링크를 찾을 수 없습니다."

        detail_url = link_tag["href"]
        detail_res = requests.get(detail_url, headers=headers)
        if detail_res.status_code != 200:
            return None, f"상세페이지 요청 실패 (status {detail_res.status_code})"

        result = parse_aladin_detail_page(detail_res.text)
        return result, None

    except Exception as e:
        return None, f"예외 발생: {str(e)}"

# 🔹 Streamlit UI
st.title("📚 ISBN → 크롤링 → KORMARC 변환기 😂")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력하세요:")

if isbn_input:
    isbn_list = [
        re.sub(r"[^\d]", "", isbn)  # ✅ 숫자만 남김: 979-11-94244-18-9 → 9791194244189
        for isbn in isbn_input.split("/")
        if isbn.strip()
    ]

    for idx, isbn in enumerate(isbn_list, 1):
        st.markdown(f"---\n### 📘 {idx}. ISBN: `{isbn}`")
        with st.spinner("🔍 도서 정보 검색 중..."):
            result, error = search_aladin_by_isbn(isbn)

        if error:
            st.error(f"❌ 오류: {error}")
            continue

        if result:
            publisher = result["publisher"]
            pubyear = result["pubyear"]

            # 245 필드 먼저 출력
            st.code(result["245"], language="text")

            # 260 필드 구성
            if publisher == "출판사 정보 없음":
                location = "[출판지 미상]"
            else:
                with st.spinner(f"📍 '{publisher}'의 지역정보 검색 중..."):
                    location = get_publisher_location(publisher)

            # 디버깅 or 지역정보 메시지 (가장 마지막)
            if publisher != "출판사 정보 없음":
                st.info(f"🏙️ 지역정보 결과: **{location}**")

            # 260 필드 출력
            updated_260 = f"=260  \\$a{location} :$b{publisher},$c{pubyear}."
            st.code(updated_260, language="text")  

            # 300 필드 출력
            st.code(result["300"], language="text")
            
            # 008 필드 출력 (발행국 부호)
            country_code = get_country_code_by_region(location)
            field_008 = f"=008  \\\\$a{country_code}"
            st.code(field_008, language="text")


        else:
            st.warning("결과 없음")
