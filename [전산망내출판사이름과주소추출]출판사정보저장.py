import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Headless Chrome 설정
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 10)

driver.get("https://bnk.kpipa.or.kr/home/v3/addition/adiPblshrInfoList")

all_data = []

def parse_table():
    """현재 페이지 테이블의 모든 행 데이터를 리스트로 반환"""
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="ajaxDiv_get"]/div[2]/div/div/table/tbody/tr')))
    rows = driver.find_elements(By.XPATH, '//*[@id="ajaxDiv_get"]/div[2]/div/div/table/tbody/tr')
    page_data = []
    for row in rows:
        cols = row.find_elements(By.TAG_NAME, 'td')
        if len(cols) < 4:
            continue
        # 예: 1열=번호, 2열=출판사명, 3열=주소, 4열=전화번호 (페이지 구조에 맞게 조정)
        num = cols[0].text.strip()
        publisher = cols[1].text.strip()
        address = cols[2].text.strip()
        phone = cols[3].text.strip()
        page_data.append([num, publisher, address, phone])
    return page_data

for page_num in range(1, 11):
    print(f"{page_num} 페이지 수집 중...")

    # 테이블 데이터 수집
    data = parse_table()
    all_data.extend(data)

    if page_num < 10:
        try:
            # 다음 버튼 클릭 전 다시 찾아서 클릭 (StaleElementReference 방지)
            next_btn = wait.until(EC.element_to_be_clickable((By.XPATH, '//a[@title="다음 페이지로 이동"]')))
            driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
            time.sleep(0.5)
            next_btn.click()
            time.sleep(2)  # 페이지 로딩 대기
        except Exception as e:
            print(f"다음 버튼 클릭 실패: {e}")
            break

driver.quit()

# 수집한 데이터를 DataFrame으로 저장 후 엑셀로 출력
df = pd.DataFrame(all_data, columns=["번호", "출판사명", "주소", "전화번호"])
output_file = "출판사정보_크롤링결과.xlsx"
df.to_excel(output_file, index=False)
print(f"크롤링 완료, 파일 저장: {output_file}")
