# 필요 패키지 설치
# pip install selenium webdriver-manager pandas

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

# =========================
# ChromeDriver 세팅 (백그라운드 실행)
# =========================
def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # 백그라운드 실행
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    return driver

# =========================
# 순번 기반 출판사 ID 추출 (다음 페이지 탐색 포함)
# =========================
def get_publisher_id_by_serial(driver, serial):
    url = "https://bnk.kpipa.or.kr/home/v3/addition/adiPblshrInfoList"
    driver.get(url)
    time.sleep(2)

    while True:
        try:
            rows = WebDriverWait(driver, 5).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.table tbody tr"))
            )
            for row in rows:
                try:
                    serial_cell = row.find_element(By.CSS_SELECTOR, "td.text-center")
                    row_serial = int(serial_cell.text.strip())
                    if row_serial == serial:
                        onclick_attr = row.find_element(By.CSS_SELECTOR, "td.title").get_attribute("onclick")
                        publisher_id = onclick_attr.split("'")[1]
                        return publisher_id
                except:
                    continue

            # 다음 페이지 클릭
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, "a[title='다음 페이지로 이동']")
                if "disabled" in next_button.get_attribute("class"):
                    break
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(2)
            except:
                break
        except:
            break
    return None

# =========================
# 출판사 페이지 크롤링
# =========================
def crawl_publisher_by_id(driver, publisher_id):
    url = f"https://bnk.kpipa.or.kr/home/v3/addition/adiPblshrInfoDetailView/seq_{publisher_id}"
    driver.get(url)
    time.sleep(2)

    # '도서 전체보기' 클릭
    try:
        all_books_button = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, f"a.goto-all-books[onclick*='{publisher_id}']"))
        )
        driver.execute_script("arguments[0].click();", all_books_button)
        time.sleep(3)
    except:
        print(f"출판사 {publisher_id} 도서 전체보기 버튼 없음")
        return [], 0

    # 리스트 보기 강제 전환
    try:
        list_view_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a[onclick*=\"fnPageView('searchTypeView','list'\"]"))
        )
        driver.execute_script("arguments[0].click();", list_view_btn)
        time.sleep(3)
    except Exception as e:
        print(f"리스트 보기 전환 실패: {e}")

    # 도서 테이블에서 출판사/임프린트 수집 (순번 기준)
    imprints_set = set()
    book_count = 0
    page_num = 1
    end_seq = 1  # 순번 1까지 탐색

    while True:
        print(f"출판사 {publisher_id}: {page_num} 페이지 크롤링 중...")
        rows = driver.find_elements(By.CSS_SELECTOR, "table.table tbody tr")
        stop_flag = False

        for row in rows:
            try:
                seq_cell = row.find_element(By.CSS_SELECTOR, "td.text-center:first-child")
                book_seq = int(seq_cell.text.strip())

                # 도서 정보 수집
                imprint_cell = row.find_element(By.CSS_SELECTOR, "td:nth-child(5)")
                text = imprint_cell.text.strip()
                if text:
                    imprints_set.add(text)
                book_count += 1

                # 종료 조건 확인 (순번 1 이하)
                if book_seq <= end_seq:
                    stop_flag = True
            except:
                continue

        if stop_flag:
            break

        # 다음 페이지 클릭
        try:
            next_buttons = driver.find_elements(By.CSS_SELECTOR, "a[title='다음 페이지로 이동']")
            if next_buttons and "disabled" not in next_buttons[0].get_attribute("class"):
                driver.execute_script("arguments[0].click();", next_buttons[0])
                time.sleep(2)
                page_num += 1
            else:
                break
        except:
            break

    return list(imprints_set), book_count

# =========================
# 순번 범위 기반 전체 크롤링
# =========================
def crawl_publishers(start_serial, end_serial):
    driver = create_driver()
    all_imprints = []
    total_books = 0
    found_serials = 0

    for serial in range(start_serial, end_serial - 1, -1):
        try:
            print(f"순번 {serial} 크롤링 중...")
            publisher_id = get_publisher_id_by_serial(driver, serial)
            if not publisher_id:
                print(f"순번 {serial} ID 없음")
                continue
            found_serials += 1
            imprints, book_count = crawl_publisher_by_id(driver, publisher_id)
            print(f"순번 {serial} 완료, 수집 개수: {len(imprints)}, 도서 수: {book_count}")
            total_books += book_count
            all_imprints.extend(imprints)
        except Exception as e:
            print(f"순번 {serial} 크롤링 중 오류: {e}")

    driver.quit()
    # 중복 제거 후 CSV 저장
    all_imprints = list(set(all_imprints))
    df = pd.DataFrame(all_imprints, columns=["출판사/임프린트"])
    df.to_csv("imprints.csv", index=False, encoding="utf-8-sig")
    print("CSV 저장 완료")
    print(f"총 검색된 순번: {found_serials}, 총 도서 수: {total_books}")

# =========================
# 실행 예시
# =========================
if __name__ == "__main__":
    start_serial = 3777
    end_serial = 2580
    crawl_publishers(start_serial, end_serial)
