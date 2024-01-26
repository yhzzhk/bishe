from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
import time

options = Options()
options.page_load_strategy = 'eager'
# options.headless = True
# 实例化 Chrome WebDriver
driver = webdriver.Chrome(options=options)

# 设置一个较长的隐式等待时间
driver.implicitly_wait(20)

# 打开网页
driver.get('http://ethernodes.org/nodes')

try:
    # 等待页面加载
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, "table_paginate"))
    )

    with open('results0126.txt', 'w', encoding='utf-8') as file:
        while True:
            # 提取表格数据的逻辑
            tbody = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, 'tbody'))
            )
            rows = tbody.find_elements(By.TAG_NAME, 'tr')
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                row_data = [cell.text for cell in cells]
                file.write('|'.join(row_data) + '\n')  # 将数据写入文件
                print(row_data)  # 同时在控制台输出
            
            # 寻找“下一页”按钮，并检查是否可点击
            next_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, 'table_next'))
            )
            if 'disabled' in next_btn.get_attribute('class'):
                # 如果“下一页”按钮被禁用，则停止翻页
                break
            
            # 点击“下一页”按钮
            next_btn.click()

            # 等待随机时间
            time.sleep(5)  # 根据实际情况您可能需要调整这个时间

except NoSuchElementException:
    print("Some navigation elements were not found on the page.")
except TimeoutException:
    print("Loading took too much time - perhaps the elements have changed.")
finally:
    # 完成后关闭浏览器
    driver.quit()


    
# from selenium import webdriver
# driver = webdriver.Chrome()
# if __name__=='__main__':
#     driver.get('http://www.baidu.com')
