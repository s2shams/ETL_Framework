import pandas as pd
import time
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

def append_data_to_df(df, dict):
    df = pd.concat([df, pd.DataFrame.from_records([dict])])
    return df

# The courses keep loading as you scroll down, so we need to keep scrolling down until we hit the bottom of the page
def scroll_to_bottom(driver):
    last_height = driver.execute_script('return document.body.scrollHeight')
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        # Check if height has changed
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break ## if height didn't change, we've hit the bottom of the page
        last_height = new_height

def wait_for_element(driver, xpath):
    try:
        element = EC.presence_of_element_located((By.XPATH, xpath))
        WebDriverWait(driver, 10).until(element)
    except TimeoutException:
        print("Timed out waiting for page to load")

def check_exists_by_xpath(driver, xpath):
    try:
        driver.find_elements(By.XPATH, xpath)
    except NoSuchElementException:
        return False
    return True