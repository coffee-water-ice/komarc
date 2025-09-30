import streamlit as st
import requests
import re
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import io
from pymarc import Record, Field, MARCWriter, Subfield   # ✅ Subfield 추가


# =========================
# --- 알라딘 상세 페이지 파싱 (형태사항) ---
# =========================
def detect_illustrations(text: str):
    if not text:
        return False, None

    keyword_groups = {
        "천연색삽화": ["삽화", "일러스트", "일러스트레이션", "illustration", "그림"],
        "삽화": ["흑백 삽화", "흑백 일러스트", "흑백 일러스트레이션", "흑백 그림"],
        "사진": ["사진", "포토", "photo", "화보"],
        "도표": ["도표", "차트", "그래프"],
        "지도": ["지도", "지도책"],
    }

    found_labels = set()

    for label, keywords in keyword_groups.items():
        if any(kw in text for kw in keywords):
            found_labels.add(label)

    if found_labels:
        return True, ", ".join(sorted(found_labels))
    else:
        return False, None

def parse_aladin_physical_book_info(html):
    """
    알라딘 상세 페이지 HTML에서 300 필드 파싱
    """
    soup = BeautifulSoup(html, "html.parser")

    # -------------------------------
    # 제목, 부제, 책소개
    # -------------------------------
    title = soup.select_one("span.Ere_bo_title")
    subtitle = soup.select_one("span.Ere_sub1_title")
    title_text = title.get_text(strip=True) if title else ""
    subtitle_text = subtitle.get_text(strip=True) if subtitle else ""

    description = None
    desc_tag = soup.select_one("div.Ere_prod_mconts_R")
    if desc_tag:
        description = desc_tag.get_text(" ", strip=True)

    # -------------------------------
    # 형태사항
    # -------------------------------
    form_wrap = soup.select_one("div.conts_info_list1")
    a_part = ""
    b_part = ""
    c_part = ""
    page_value = None
    size_value = None

    if form_wrap:
        form_items = [item.strip() for item in form_wrap.stripped_strings if item.strip()]
        for item in form_items:
            if re.search(r"(쪽|p)\s*$", item):
                page_match = re.search(r"\d+", item)
                if page_match:
                    page_value = int(page_match.group())
                    a_part = f"{page_match.group()} p."
            elif "mm" in item:
                size_match = re.search(r"(\d+)\s*[\*x×X]\s*(\d+)", item)
                if size_match:
                    width = int(size_match.group(1))
                    height = int(size_match.group(2))
                    size_value = f"{width}x{height}mm"
                    if width == height or width > height or width < height / 2:
                        w_cm = round(width / 10)
                        h_cm = round(height / 10)
                        c_part = f"{w_cm}x{h_cm} cm"
                    else:
                        h_cm = round(height / 10)
                        c_part = f"{h_cm} cm"

    # -------------------------------
    # 삽화 감지 (제목 + 부제 + 책소개 전체)
    # -------------------------------
    combined_text = " ".join(filter(None, [title_text, subtitle_text, description]))
    has_illus, illus_label = detect_illustrations(combined_text)
    if has_illus:
        b_part = f" :$b{illus_label}"

    # -------------------------------
    # 300 필드 조합
    # -------------------------------
    if a_part or b_part or c_part:
        field_300 = "=300  \\$a"
        if a_part:
            field_300 += a_part
        if b_part:
            field_300 += b_part
        if c_part:
            field_300 += f" ;$c{c_part}."
        else:
            field_300 += "."
    else:
        field_300 = "=300  \\$a1책."

    return {
        "300": field_300,
        "page_value": page_value,
        "size_value": size_value,
        "illustration_possibility": illus_label if illus_label else "없음"
    }


def search_aladin_detail_page(link):
    try:
        res = requests.get(link, timeout=15)
        res.raise_for_status()
        return parse_aladin_physical_book_info(res.text), None
    except Exception as e:
        return {
            "300": "=300  \\$a1책. [상세 페이지 파싱 오류]",
            "page_value": None,
            "size_value": None,
            "illustration_possibility": "정보 없음"
        }, f"Aladin 상세 페이지 크롤링 예외: {e}"


# =========================
# --- 구글시트 로드 & 캐시 관리 ---
# =========================
@st.cache_data(ttl=3600)
def load_publisher_db():
    creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gspread"], 
                                                             ["https://spreadsheets.google.com/feeds",
                                                              "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    sh = client.open("출판사 DB")
    
    # KPIPA_PUB_REG: 번호, 출판사명, 주소, 전화번호 → 출판사명, 주소만 사용
    pub_rows = sh.worksheet("KPIPA_PUB_REG").get_all_values()[1:]
    pub_rows_filtered = [row[1:3] for row in pub_rows]  # 출판사명, 주소
    publisher_data = pd.DataFrame(pub_rows_filtered, columns=["출판사명", "주소"])
    
    # 008: 발행국 발행국 부호 → 첫 2열만
    region_rows = sh.worksheet("008").get_all_values()[1:]
    region_rows_filtered = [row[:2] for row in region_rows]
    region_data = pd.DataFrame(region_rows_filtered, columns=["발행국", "발행국 부호"])
    
    # IM_* 시트: 출판사/임프린트 하나의 칼럼
    imprint_frames = []
    for ws in sh.worksheets():
        if ws.title.startswith("IM_"):
            data = ws.get_all_values()[1:]
            imprint_frames.extend([row[0] for row in data if row])
    imprint_data = pd.DataFrame(imprint_frames, columns=["임프린트"])
    
    return publisher_data, region_data, imprint_data

# =========================
# --- 알라딘 API ---
# =========================
def search_aladin_by_isbn(isbn):
    try:
        ttbkey = st.secrets["aladin"]["ttbkey"]
        url = "https://www.aladin.co.kr/ttb/api/ItemLookUp.aspx"
        params = {"ttbkey": ttbkey, "itemIdType": "ISBN", "ItemId": isbn, 
                  "output": "js", "Version": "20131101"}
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()
        data = res.json()
        if "item" not in data or not data["item"]:
            return None, None, f"도서 정보를 찾을 수 없습니다. [응답: {data}]"
        book = data["item"][0]
        title = book.get("title", "제목 없음")
        author = book.get("author", "")
        publisher = book.get("publisher", "출판사 정보 없음")
        pubdate = book.get("pubDate", "")
        pubyear = pubdate[:4] if len(pubdate) >= 4 else "발행년도 없음"
        authors = [a.strip() for a in author.split(",")] if author else []
        creator_str = " ; ".join(authors) if authors else "저자 정보 없음"
        field_245 = f"=245  10$a{title} /$c{creator_str}"
        link = book.get("link")  # 상세 페이지 링크 추출
        
        return {"title": title, "creator": creator_str, "publisher": publisher, "pubyear": pubyear, "245": field_245}, link, None
    except Exception as e:
        return None, None, f"Aladin API 예외: {e}"

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
    if not location_name or location_name in ("출판지 미상", "[예외] 발행지미상"):
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
# --- KPIPA DB 검색 보조 함수 ---
# =========================
def search_publisher_location_with_alias(name, publisher_data):
    debug_msgs = []
    if not name:
        return "출판지 미상", ["❌ 검색 실패: 입력된 출판사명이 없음"]
    norm_name = normalize_publisher_name(name)
    candidates = publisher_data[publisher_data["출판사명"].apply(lambda x: normalize_publisher_name(x)) == norm_name]
    if not candidates.empty:
        address = candidates.iloc[0]["주소"]
        debug_msgs.append(f"✅ KPIPA DB 매칭 성공: {name} → {address}")
        return address, debug_msgs
    else:
        debug_msgs.append(f"❌ KPIPA DB 매칭 실패: {name}")
    return "출판지 미상", debug_msgs

# =========================
# --- IM 임프린트 보조 함수 ---
# =========================
def find_main_publisher_from_imprints(rep_name, imprint_data, publisher_data):
    """
    IM_* 시트에서 임프린트명을 검색하고, KPIPA DB에서 해당 출판사명으로 주소를 반환
    """
    norm_rep = normalize_publisher_name(rep_name)
    for full_text in imprint_data["임프린트"]:
        if "/" in full_text:
            pub_part, imprint_part = [p.strip() for p in full_text.split("/", 1)]
        else:
            pub_part, imprint_part = full_text.strip(), None

        if imprint_part:
            norm_imprint = normalize_publisher_name(imprint_part)
            if norm_imprint == norm_rep:
                # KPIPA DB에서 pub_part를 검색
                location, debug_msgs = search_publisher_location_with_alias(pub_part, publisher_data)
                return location, debug_msgs
    return None, [f"❌ IM DB 검색 실패: 매칭되는 임프린트 없음 ({rep_name})"]

    
# =========================
# --- KPIPA 페이지 검색 ---
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
# ----발행국 부호 찾기-----
# =========================

def get_country_code_by_region(region_name, region_data):
    """
    지역명을 기반으로 008 발행국 부호를 찾음.
    region_data: DataFrame, columns=["발행국", "발행국 부호"]
    """
    try:
        def normalize_region_for_code(region):
            region = (region or "").strip()
            if region.startswith(("전라", "충청", "경상")):
                return region[0] + (region[2] if len(region) > 2 else "")
            return region[:2]
        normalized_input = normalize_region_for_code(region_name)
        for idx, row in region_data.iterrows():
            sheet_region, country_code = row["발행국"], row["발행국 부호"]
            if normalize_region_for_code(sheet_region) == normalized_input:
                return country_code.strip() or "xxu"

        return "xxu"
    except Exception as e:
        st.write(f"⚠️ get_country_code_by_region 예외: {e}")
        return "xxu"

# =========================
# --- 문체부 검색 ---
# =========================
def get_mcst_address(publisher_name):
    url = "https://book.mcst.go.kr/html/searchList.php"
    params = {"search_area": "전체", "search_state": "1", "search_kind": "1", 
              "search_type": "1", "search_word": publisher_name}
    debug_msgs = []
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
            debug_msgs.append(f"[문체부] 검색 성공: {len(results)}건")
            return results[0][2], results, debug_msgs
        else:
            debug_msgs.append("[문체부] 검색 결과 없음")
            return "[문체부] [발행지미상]", [], debug_msgs
    except Exception as e:
        debug_msgs.append(f"[문체부] 예외 발생: {e}")
        return "발생 [오류]", [], debug_msgs
        
# =========================
# --- MRC 변환 함수 추가 ---
# =========================
def export_to_mrc(records):
    output = io.BytesIO()
    writer = MARCWriter(output)
    for rec in records:
        record = Record(force_utf8=True)
        # 008 (발행국 부호만 예시로 기록)
        record.add_field(Field(tag="008", data=rec["발행국 부호"]))
        # 245
        record.add_field(Field(
            tag="245", indicators=["1", "0"],
            subfields=[Subfield("a", rec["제목"]), Subfield("c", rec["저자"])]   
        ))
        # 260
        record.add_field(Field(
            tag="260", indicators=[" ", " "],
            subfields=[Subfield("a", rec["출판지"]), Subfield("b", rec["출판사"]), Subfield("c", rec["발행년도"])]
        ))
        # 300
        field_300 = rec["MARC 300"].replace("=300  ", "").strip()
        record.add_field(Field(tag="300", indicators=[" ", " "], subfields=[Subfield("a", field_300)]))
        writer.write(record)

    output.seek(0)
    return output
        
# =========================
# --- Streamlit UI ---
# =========================
st.title("📚 ISBN → KORMARC 변환기")

if st.button("🔄 구글시트 새로고침"):
    st.cache_data.clear()
    st.success("캐시 초기화 완료! 다음 호출 시 최신 데이터 반영됩니다.")

isbn_input = st.text_area("ISBN을 '/'로 구분하여 입력:")

records = []
all_mcst_results = []

if isbn_input:
    isbn_list = [re.sub(r"[^\d]", "", s) for s in isbn_input.split("/") if s.strip()]
    publisher_data, region_data, imprint_data = load_publisher_db()

    for idx, isbn in enumerate(isbn_list, start=1):
        st.markdown(f"---\n### 📘 {idx}. ISBN: `{isbn}`")
        debug_messages = []

        # 1) Aladin API (기본 정보 + 상세 페이지 링크)
        result, link, error = search_aladin_by_isbn(isbn)
        if error:
            st.warning(f"[Aladin API] {error}")
            continue
        publisher_api = result["publisher"]
        pubyear = result["pubyear"]
        
        # 1-1) Aladin 상세 페이지 크롤링 (300 필드)
        physical_data, detail_error = search_aladin_detail_page(link)
        field_300 = physical_data.get("300", "=300  \\$a1책. [파싱 실패]") 
       
        if detail_error:
            debug_messages.append(f"[Aladin 상세] {detail_error}")
        else:
            page_val = physical_data.get('page_value', 'N/A')
            size_val = physical_data.get('size_value', 'N/A')
            illus_val = physical_data.get('illustration_possibility', '없음')
            debug_messages.append(
                f"✅ Aladin 상세 페이지 파싱 성공 "
                f"(페이지: {page_val}, 크기: {size_val}, 삽화감지: {illus_val})"
            )

        # 2) KPIPA 페이지 검색
        publisher_full, publisher_norm, kpipa_error = get_publisher_name_from_isbn_kpipa(isbn)
        location_raw = "출판지 미상"
        if publisher_norm:
            debug_messages.append(f"✅ KPIPA 페이지 검색 성공: {publisher_full}")
            location_raw, debug_kpipa_db = search_publisher_location_with_alias(publisher_norm, publisher_data)
            debug_messages.extend([f"[KPIPA DB] {msg}" for msg in debug_kpipa_db])
        else:
            debug_messages.append(f"[KPIPA 페이지] {kpipa_error}")
            publisher_norm = publisher_api

        # 3) 1차 정규화 후 KPIPA DB
        if location_raw == "출판지 미상":
            rep_name, aliases = split_publisher_aliases(publisher_norm)
            location_raw, debug_stage1 = search_publisher_location_with_alias(rep_name, publisher_data)
            debug_messages.extend([f"[1차 정규화 KPIPA DB] {msg}" for msg in debug_stage1])
            if location_raw == "출판지 미상":
                for alias in aliases:
                    location_raw, debug_alias = search_publisher_location_with_alias(alias, publisher_data)
                    if location_raw != "출판지 미상":
                        debug_messages.append(f"✅ 별칭 '{alias}' 매칭 성공! ({location_raw})")
                        break          

        # 4) IM 검색
        if location_raw == "출판지 미상":
            main_pub, debug_im = find_main_publisher_from_imprints(rep_name, imprint_data, publisher_data)
            if main_pub:
                location_raw = main_pub
            debug_messages.extend([f"[IM DB] {msg}" for msg in debug_im])

        # 5) 2차 정규화 KPIPA DB
        if location_raw == "출판지 미상":
            stage2_name = normalize_stage2(publisher_norm)
            location_raw, debug_stage2 = search_publisher_location_with_alias(stage2_name, publisher_data)
            debug_messages.extend([f"[2차 정규화 KPIPA DB] {msg}" for msg in debug_stage2])

            # ✅ 2차 정규화 후 IM DB 검색
            if location_raw == "출판지 미상":
                main_pub_stage2, debug_im_stage2 = find_main_publisher_from_imprints(stage2_name, imprint_data, publisher_data)
                if main_pub_stage2:
                    location_raw = main_pub_stage2
                debug_messages.extend([f"[IM DB 2차 정규화 후] {msg}" for msg in debug_im_stage2])


        # 6) 문체부 검색
        mcst_address, mcst_results, debug_mcst = get_mcst_address(publisher_norm)
        debug_messages.extend(debug_mcst)
        if location_raw == "출판지 미상":
            if mcst_results:
                location_raw = mcst_results[0][2]
                debug_messages.append(f"[문체부] 매칭 성공: {mcst_results}")
            else:
                location_raw = mcst_address
                debug_messages.append(f"[문체부] 매칭 실패")

        # 7) 발행국 표시용 정규화
        location_display = normalize_publisher_location_for_display(location_raw)

        # 8) MARC 008 발행국 발행국 부호
        code = get_country_code_by_region(location_raw, region_data)

        # 9) 최종 출력
        with st.container():
            marc_text = (
                f"=008  \\$a{code}\n"
                f"{result['245']}\n"
                f"=260  \\$a{location_display} :$b{publisher_api},$c{pubyear}\n"
                f"{field_300}"
            )
            st.code(marc_text, language="text")
        with st.expander("🔹 Debug / 후보 메시지"):
            for msg in debug_messages:
                st.write(msg)
        with st.expander("🔹 문체부 등록 출판사 결과 확인"):
            if mcst_results:
                st.table(pd.DataFrame(mcst_results, columns=["등록구분", "출판사명", "주소", "상태"]))
            else:
                st.write("❌ 문체부 결과 없음")
        # 결과를 딕셔너리로 저장
        record = {
            "ISBN": isbn,
            "제목": result['title'],
            "저자": result['creator'],
            "출판사": publisher_api,
            "발행년도": pubyear,
            "출판지": location_raw,
            "발행국 부호": code,
            "MARC 245": result['245'],
            "MARC 260": f"=260  \\$a{location_display} :$b{publisher_api},$c{pubyear}",
            "MARC 300": field_300
        }
        records.append(record)

    # 모든 ISBN 처리 후 엑셀 다운로드 버튼 표시
    if records:
        df = pd.DataFrame(records)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='MARC_Results')
        output.seek(0)
        
        st.markdown("---")
        st.subheader("🎉 모든 ISBN 처리 완료!")
        st.success("아래 버튼을 눌러 결과를 엑셀 파일로 다운로드하세요.")
        st.download_button(
            label="📥 결과 엑셀 파일 다운로드",
            data=output,
            file_name="kormarc_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        # ✅ MRC 다운로드
        mrc_data = export_to_mrc(records)
        st.download_button(
            label="📥 결과 MRC 파일 다운로드",
            data=mrc_data,
            file_name="kormarc_results.mrc",
            mime="application/marc"
        )




