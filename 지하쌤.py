import streamlit as st
import requests
import pandas as pd
import openai
import xml.etree.ElementTree as ET
import re
import io
import xml.etree.ElementTree as ET
import re, datetime
from collections import Counter
from bs4 import BeautifulSoup
from openai import OpenAI
from requests.adapters import HTTPAdapter, Retry
from concurrent.futures import ThreadPoolExecutor

# ── 한 번만 생성: 국중API용 세션 & 재시도 설정
_nlk_session = requests.Session()
_nlk_session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=1,                # 재시도 1회
            backoff_factor=0.5,     # 0.5초 간격
            status_forcelist=[429,500,502,503,504]
        )
    )
)

# ✅ API 키 (secrets.toml에서 불러오기)
openai_key = st.secrets["api_keys"]["openai_key"]
aladin_key = st.secrets["api_keys"]["aladin_key"]
nlk_key = st.secrets["api_keys"]["nlk_key"]

gpt_client = OpenAI(api_key=openai_key)

# 008 본문(40자) 조립기: 단행본 기준, type_of_date 기본 's'
def build_008_kormarc_bk(
    date_entered,          # 00-05 YYMMDD
    date1,                 # 07-10 4자리(예: '2025' / '19uu' 허용)
    country3,              # 15-17 3자리 (지금은 'HST' 고정)
    lang3,                 # 35-37 3자리 (지금은 'MRT' 고정)
    date2="",              # 11-14
    illus4="",             # 18-21 최대 4자 (예: 'a', 'ad', 'ado'…)
    has_index="0",         # 31 '0' 없음 / '1' 있음
    lit_form=" ",          # 33 문학형식 (p 시, f 소설, e 수필, i 서간문학, m 기행/일기/수기)
    bio=" ",               # 34 전기 (a 자서전, b 전기, d 부분적 전기)
    type_of_date="s",      # 06 기본 's'
    modified_record=" ",   # 28 기본 공백
    cataloging_src=" ",    # 32 기본 공백
):
    def pad(s, n, fill=" "):
        s = "" if s is None else str(s)
        return (s[:n] + fill * n)[:n]

    if len(date_entered) != 6 or not date_entered.isdigit():
        raise ValueError("date_entered는 YYMMDD 6자리 숫자여야 합니다.")
    if len(date1) != 4:
        raise ValueError("date1은 4자리여야 합니다. 예: '2025', '19uu'")

    body = "".join([
        date_entered,               # 00-05
        pad(type_of_date,1),        # 06
        date1,                      # 07-10
        pad(date2,4),               # 11-14
        pad(country3,3),            # 15-17
        pad(illus4,4),              # 18-21
        " " * 4,                    # 22-25 (이용대상/자료형태/내용형식) 공백
        " " * 2,                    # 26-27 공백
        pad(modified_record,1),     # 28 공백
        " ",                        # 29 회의간행물 공백
        " ",                        # 30 기념논문집 공백
        has_index if has_index in ("0","1") else "0", # 31 색인
        pad(cataloging_src,1),      # 32 공백
        pad(lit_form,1),            # 33 문학형식
        pad(bio,1),                 # 34 전기
        pad(lang3,3),               # 35-37 언어
        " " * 2                     # 38-39 공백
    ])
    if len(body) != 40:
        raise AssertionError(f"008 length != 40: {len(body)}")
    return body

# 알라딘 pubDate 문자열에서 연도만 추출
def extract_year_from_aladin_pubdate(pubdate_str: str) -> str:
    m = re.search(r"(19|20)\d{2}", pubdate_str or "")
    return m.group(0) if m else "19uu"

# 삽화 감지: a(삽화/일러스트), d(도표/그래프), o(사진/화보)
def detect_illus4(text: str) -> str:
    keys = []
    if re.search(r"삽화|일러스트|일러스트레이션|그림|illustration", text, re.I): keys.append("a")
    if re.search(r"도표|차트|그래프", text, re.I):                              keys.append("d")
    if re.search(r"사진|포토|화보|photo", text, re.I):                           keys.append("o")
    out=[]; [out.append(k) for k in keys if k not in out]
    return "".join(out)[:4]

# 색인 감지: '색인', '찾아보기', 'index'
def detect_index(text: str) -> str:
    return "1" if re.search(r"색인|찾아보기|index", text, re.I) else "0"

# 문학형식 감지: p 시 / f 소설 / e 수필 / i 서간문학 / m 기행·일기·수기
def detect_lit_form(title: str, category: str, kdc: str = None) -> str:
    blob = f"{title} {category}"
    if re.search(r"서간집|편지|서간문|letters?", blob, re.I): return "i"
    if re.search(r"기행|여행기|일기|수기|diary|travel", blob, re.I): return "m"
    if re.search(r"시집|poem|poetry", blob, re.I): return "p"
    if re.search(r"소설|novel|fiction", blob, re.I): return "f"
    if re.search(r"에세이|수필|essay", blob, re.I): return "e"
    return " "  # 비문학 또는 미분류

# 전기 감지: a 자서전 / b 전기·평전(타인) / d 부분적 전기(회고/일기 등 암시)
def detect_bio(text: str) -> str:
    t = text or ""
    if re.search(r"자서전|autobiograph", t, re.I): return "a"
    if re.search(r"전기|평전|biograph", t, re.I):  return "b"
    if re.search(r"전기적|회고|회상", t):         return "d"
    return " "

# ISBN 하나로 008 생성 (요청사항 반영: country/lang 임시 고정값)
COUNTRY_FIXED = "HST"  # TODO: 300 모듈 완성 후 override
LANG_FIXED    = "MRT"  # TODO: 041 모듈 완성 후 override

def build_008_from_isbn(
    isbn: str,
    *,
    aladin_pubdate: str = "",
    aladin_title: str = "",
    aladin_category: str = "",
    aladin_desc: str = "",
    override_country3: str = None,  # 나중에 300에서 채워 넣기
    override_lang3: str = None,     # 나중에 041에서 채워 넣기
):
    today  = datetime.datetime.now().strftime("%y%m%d")  # YYMMDD
    date1  = extract_year_from_aladin_pubdate(aladin_pubdate)
    country3 = (override_country3 or COUNTRY_FIXED)
    lang3    = (override_lang3    or LANG_FIXED)

    bigtext   = " ".join([aladin_title or "", aladin_desc or ""])
    illus4    = detect_illus4(bigtext)
    has_index = detect_index(bigtext)
    lit_form  = detect_lit_form(aladin_title or "", aladin_category or "")
    bio       = detect_bio(bigtext)

    return build_008_kormarc_bk(
        date_entered=today,
        date1=date1,
        country3=country3,
        lang3=lang3,
        illus4=illus4,
        has_index=has_index,
        lit_form=lit_form,
        bio=bio
    )
# ========= 008 생성 블록: 붙여넣기 끝 =========

# 🔍 키워드 추출 (konlpy 없이)
def extract_keywords_from_text(text, top_n=7):
    words = re.findall(r'\b[\w가-힣]{2,}\b', text)
    filtered = [w for w in words if len(w) > 1]
    freq = Counter(filtered)
    return [kw for kw, _ in freq.most_common(top_n)]

def clean_keywords(words):
    stopwords = {"아주", "가지", "필요한", "등", "위해", "것", "수", "더", "이런", "있다", "된다", "한다"}
    return [w for w in words if w not in stopwords and len(w) > 1]

# 📚 카테고리 키워드 추출
def extract_category_keywords(category_str):
    keywords = set()
    lines = category_str.strip().splitlines()
    for line in lines:
        parts = [x.strip() for x in line.split('>') if x.strip()]
        if parts:
            keywords.add(parts[-1])
    return list(keywords)

# 🔧 GPT 기반 KDC 추천
# 🔧 GPT 기반 KDC 추천 (OpenAI 1.6.0+ 방식으로 리팩토링)
def recommend_kdc(title, author, api_key):
    try:
        # 🔑 비밀의 열쇠로 클라이언트를 깨웁니다
        client = OpenAI(api_key=api_key)

        # 📜 주문문을 준비하고
        prompt = (
            f"도서 제목: {title}\n"
            f"저자: {author}\n"
            "이 책의 주제를 고려하여 한국십진분류(KDC) 번호 하나를 추천해 주세요.\n"
            "정확한 숫자만 아래 형식으로 간단히 응답해 주세요:\n"
            "KDC: 813.7"
        )

        # 🧠 GPT의 지혜를 소환
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )

        # ✂️ “KDC:” 뒤의 숫자만 꺼내서 돌려드립니다
        for line in response.choices[0].message.content.splitlines():
            if "KDC:" in line:
                return line.split("KDC:")[1].strip()

    except Exception as e:
        st.warning(f"🧠 GPT 오류: {e}")

    # 🛡️ 만약 실패하면 디폴트 “000”
    return "000"


# 📡 부가기호 추출 (국립중앙도서관)
@st.cache_data(ttl=24*3600)
def fetch_additional_code_from_nlk(isbn: str) -> str:
    url = (
        f"https://www.nl.go.kr/seoji/SearchApi.do?"
        f"cert_key={nlk_key}&result_style=xml"
        f"&page_no=1&page_size=1&isbn={isbn}"
    )
    try:
        res = _nlk_session.get(url, timeout=3)  # 3초만 기다리고
        res.raise_for_status()
        root = ET.fromstring(res.text)
        doc  = root.find('.//docs/e')
        return (doc.findtext('EA_ADD_CODE') or "").strip() if doc is not None else ""
    except Exception:
        st.warning("⚠️ 국중API 지연, 부가기호는 생략합니다.")
        return ""


# 🔤 언어 감지 및 041, 546 생성
ISDS_LANGUAGE_CODES = {
    'kor': '한국어', 'eng': '영어', 'jpn': '일본어', 'chi': '중국어', 'rus': '러시아어',
    'ara': '아랍어', 'fre': '프랑스어', 'ger': '독일어', 'ita': '이탈리아어', 'spa': '스페인어',
    'und': '알 수 없음'
}

def detect_language(text):
    text = re.sub(r'[\s\W_]+', '', text)
    if not text:
        return 'und'
    first_char = text[0]
    if '\uac00' <= first_char <= '\ud7a3':
        return 'kor'
    elif '\u3040' <= first_char <= '\u30ff':
        return 'jpn'
    elif '\u4e00' <= first_char <= '\u9fff':
        return 'chi'
    elif '\u0400' <= first_char <= '\u04FF':
        return 'rus'
    elif 'a' <= first_char.lower() <= 'z':
        return 'eng'
    else:
        return 'und'

def generate_546_from_041_kormarc(marc_041: str) -> str:
    a_codes, h_code = [], None
    for part in marc_041.split():
        if part.startswith("$a"):
            a_codes.append(part[2:])
        elif part.startswith("$h"):
            h_code = part[2:]
    if len(a_codes) == 1:
        a_lang = ISDS_LANGUAGE_CODES.get(a_codes[0], "알 수 없음")
        if h_code:
            h_lang = ISDS_LANGUAGE_CODES.get(h_code, "알 수 없음")
            return f"{a_lang}로 씀, 원저는 {h_lang}임"
        else:
            return f"{a_lang}로 씀"
    elif len(a_codes) > 1:
        langs = [ISDS_LANGUAGE_CODES.get(code, "알 수 없음") for code in a_codes]
        return f"{'、'.join(langs)} 병기"
    return "언어 정보 없음"

def crawl_aladin_original_and_price(isbn13):
    url = f"https://www.aladin.co.kr/shop/wproduct.aspx?ISBN={isbn13}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        original = soup.select_one("div.info_original")
        price = soup.select_one("span.price2")
        return {
            "original_title": original.text.strip() if original else "",
            "price": price.text.strip().replace("정가 : ", "").replace("원", "").replace(",", "").strip() if price else ""
        }
    except:
        return {}

# 📄 653 필드 키워드 생성
# ② 알라딘 메타데이터 호출 함수
def fetch_aladin_metadata(isbn):
    url = (
        "http://www.aladin.co.kr/ttb/api/ItemLookUp.aspx"
        f"?ttbkey={aladin_key}"
        "&ItemIdType=ISBN"
        f"&ItemId={isbn}"
        "&output=js"
        "&Version=20131101"
        "&OptResult=Toc" 
    )
    data = requests.get(url).json()
    item = data["item"][0]
    return {
        "category": item.get("categoryName", ""),
        "title": item.get("title", ""),
        "description": item.get("description", ""),
        "toc": item.get("toc", ""),
    }


# ③ GPT-4 기반 653 생성 함수
def generate_653_with_gpt(category, title, description, toc, max_keywords=7):
    parts = [p.strip() for p in category.split(">") if p.strip()]
    cat_kw = parts[-1] if parts else ""
    system_msg = {
        "role": "system",
        "content": (
            "당신은 도서관 메타데이터 전문가입니다. "
            "책의 분류, 제목, 설명, 목차 정보를 바탕으로 "
            "MARC 653 필드용 주제어를 추출하세요."
        )
    }
    user_msg = {
        "role": "user",
        "content": (
            f"다음 입력으로 최대 {max_keywords}개의 MARC 653 주제어를 한 줄로 출력해 주세요:\n\n"
            f"- 분류: \"{cat_kw}\"\n"
            f"- 제목: \"{title}\"\n"
            f"- 설명: \"{description}\"\n"
            f"- 목차: \"{toc}\"\n\n"
             "※ “제목”에 사용된 단어는 제외하고, 순수하게 분류·설명·목차에서 추출된 주제어만 뽑아주세요.\n"
            "출력 형식: $a키워드1 $a키워드2 …"
        )
    }
    try:
        resp = gpt_client.chat.completions.create(
            model="gpt-4",
            messages=[system_msg, user_msg],
            temperature=0.2,
            max_tokens=150,
        )
        # 1) 원본 응답을 raw에 담습니다
        raw = resp.choices[0].message.content.strip()

        # 2) $a … 다음 $a 또는 끝까지 캡처 (non-greedy)
        pattern = re.compile(r"\$a(.*?)(?=(?:\$a|$))", re.DOTALL)
        kws = [m.group(1).strip() for m in pattern.finditer(raw)]

        # 3) 각 키워드 내부 공백 제거
        kws = [kw.replace(" ", "") for kw in kws]

        # 4) 다시 "$a키워드" 형태로 조립
        return "".join(f"$a{kw}" for kw in kws)

    except Exception as e:
        st.warning(f"⚠️ 653 주제어 생성 실패: {e}")
        return None
    


# 📚 MARC 생성
@st.cache_data(show_spinner=False)
def fetch_book_data_from_aladin(isbn, reg_mark="", reg_no="", copy_symbol=""):
    import re
    from concurrent.futures import ThreadPoolExecutor

    # 1) 알라딘 + (옵션) 국중 부가기호 동시 요청
    url = (
        f"https://www.aladin.co.kr/ttb/api/ItemLookUp.aspx?"
        f"ttbkey={aladin_key}&itemIdType=ISBN&ItemId={isbn}"
        f"&output=js&Version=20131101"
    )
    with ThreadPoolExecutor(max_workers=2) as ex:
        future_aladin = ex.submit(lambda: requests.get(url, verify=False, timeout=5))
        future_nlk    = ex.submit(fetch_additional_code_from_nlk, isbn)

        try:
            resp = future_aladin.result()
            resp.raise_for_status()
            data = resp.json().get("item", [{}])[0]
        except Exception as e:
            st.error(f"🚨 알라딘API 오류: {e}")
            return ""

        add_code = future_nlk.result()  # 실패 시 빈 문자열

    # 2) 메타데이터 (알라딘)
    title       = data.get("title",       "제목없음")
    author      = data.get("author",      "저자미상")
    publisher   = data.get("publisher",   "출판사미상")
    pubdate     = data.get("pubDate",     "2025")  # 'YYYY' 또는 'YYYY-MM-DD'
    category    = data.get("categoryName", "")
    description = data.get("description", "")
    toc         = data.get("subInfo", {}).get("toc", "")
    price       = str(data.get("priceStandard", ""))  # 020/950 용

    # 3) =008 생성 (ISBN만으로 자동, country/lang은 임시 고정값 → 추후 override)
    tag_008 = "=008  " + build_008_from_isbn(
        isbn,
        aladin_pubdate=pubdate,
        aladin_title=title,
        aladin_category=category,
        aladin_desc=description,
        # override_country3="ulk",  # 300 모듈 완성 시 사용
        # override_lang3="kor",     # 041 모듈 완성 시 사용
    )

    # 4) 041/546 (간이 감지: 기존 로직 유지)
    lang_a  = detect_language(title)
    lang_h  = detect_language(data.get("title", ""))
    tag_041 = f"=041  \\$a{lang_a}" + (f"$h{lang_h}" if lang_h != "und" else "")
    tag_546 = f"=546  \\$a{generate_546_from_041_kormarc(tag_041)}"

    # 5) 020 (부가기호 있으면 $g 추가)
    tag_020 = f"=020  \\$a{isbn}:$c{price}"
    if add_code:
        tag_020 += f"$g{add_code}"

    # 6) 653/KDC — ✅ 여기서만 생성 (GPTAPI 최신 함수로 통일)
    kdc     = recommend_kdc(title, author, api_key=openai_key)
    gpt_653 = generate_653_with_gpt(category, title, description, toc, max_keywords=7)
    tag_653 = f"=653  \\{gpt_653.replace(' ', '')}" if gpt_653 else ""

    # 7) 기본 MARC 라인
    marc_lines = [
        tag_008,
        "=007  ta",
        f"=245  00$a{title} /$c{author}",
        f"=260  \\$a서울 :$b{publisher},$c{pubdate[:4]}.",
    ]

    # 8) 490·830 (총서)
    series = data.get("seriesInfo", {})
    name = (series.get("seriesName") or "").strip()
    vol  = (series.get("volume")    or "").strip()
    if name:
        marc_lines.append(f"=490  \\$a{name};$v{vol}")
        marc_lines.append(f"=830  \\$a{name};$v{vol}")

    # 9) 기타 필드
    marc_lines.append(tag_020)
    marc_lines.append(tag_041)
    marc_lines.append(tag_546)
    if kdc and kdc != "000":
        marc_lines.append(f"=056  \\$a{kdc}$26")
    if tag_653:
        marc_lines.append(tag_653)
    marc_lines.append(f"=950  0\\$b{price}")

    # 10) 049: 소장기호(입력된 경우만)
    if reg_mark or reg_no or copy_symbol:
        line = f"=049  0\\$I{reg_mark}{reg_no}"
        if copy_symbol:
            line += f"$f{copy_symbol}"
        marc_lines.append(line)

    # 11) 번호 오름차순 정렬 후 출력
    marc_lines.sort(key=lambda L: int(re.match(r"=(\d+)", L).group(1)))
    return "\n".join(marc_lines)




# 🎛️ Streamlit UI
st.title("📚 ISBN to MARC 변환기 (통합버전)")

isbn_list = []
single_isbn = st.text_input("🔹 단일 ISBN 입력", placeholder="예: 9788936434267")
if single_isbn.strip():
    isbn_list = [[single_isbn.strip(), "", "", ""]]

uploaded_file = st.file_uploader("📁 CSV 업로드 (ISBN, 등록기호, 등록번호, 별치기호)", type="csv")
if uploaded_file:
    df = pd.read_csv(uploaded_file)
    if {'ISBN', '등록기호', '등록번호', '별치기호'}.issubset(df.columns):
        isbn_list = df[['ISBN', '등록기호', '등록번호', '별치기호']].dropna(subset=['ISBN']).values.tolist()
    else:
        st.error("❌ 필요한 열이 없습니다: ISBN, 등록기호, 등록번호, 별치기호")

if isbn_list:
    st.subheader("📄 MARC 출력")
    marc_results = []
    for row in isbn_list:
        isbn, reg_mark, reg_no, copy_symbol = row
        marc = fetch_book_data_from_aladin(isbn, reg_mark, reg_no, copy_symbol)
        if marc:
            st.code(marc, language="text")
            marc_results.append(marc)

    full_text = "\n\n".join(marc_results)
    st.download_button("📦 모든 MARC 다운로드", data=full_text, file_name="marc_output.txt", mime="text/plain")

# 📄 템플릿 예시 다운로드
example_csv = "ISBN,등록기호,등록번호,별치기호\n9791173473968,JUT,12345,TCH\n"
buffer = io.BytesIO()
buffer.write(example_csv.encode("utf-8-sig"))
buffer.seek(0)
st.download_button("📄 서식 파일 다운로드", data=buffer, file_name="isbn_template.csv", mime="text/csv")

# ⬇️ 하단 마크
st.markdown("""
<div style='text-align: center; font-size: 14px; color: gray;'>
📚 <strong>도서 DB 제공</strong> : <a href='https://www.aladin.co.kr' target='_blank'>알라딘 인터넷서점(www.aladin.co.kr)</a>
</div>
""", unsafe_allow_html=True)
