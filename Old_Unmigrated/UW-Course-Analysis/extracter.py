from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from Helper_functions import *
import pandas as pd
import pdb
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

options = webdriver.ChromeOptions()
#options.add_argument('--headless')
options.binary_location = './chrome-win64/chrome.exe'
driver = webdriver.Chrome(options=options)
driver.get("https://uwflow.com/explore")
wait_for_element(driver, '(//div[@role="rowgroup"]//div[@role="row"])[1]')
actions = ActionChains(driver)

# Scroll to bottom of page to load all the courses.
# NOT NEEDED
# scroll_to_bottom(driver)

# Create empty dataframes to store the data
df = pd.DataFrame(columns = ['Course_Code', 'Course_Name', 'Number_of_Ratings', 'Number_of_Comments', 'Useful', 'Easy', 'Liked', 'Course_Reviews', 'Course_Enrollment'])
df2 = pd.DataFrame(columns = ['Professor_Name', 'Course', 'Liked_%', 'Professor_Reviews'])

# Access each course tab and push the data to the dataframe
course_tabs = driver.find_elements(By.XPATH, '//div[@role="rowgroup"]//div[@role="row"]')
copy_tabs = course_tabs.copy()
course_index = 0
more_courses = True

while more_courses:
    try:
        tab = copy_tabs[course_index]
        # Retrieve relevant information from the summary
        course_code = tab.find_element(By.XPATH,".//div//a").text
        course_name = tab.find_element(By.XPATH,".//div[2]").text
        number_of_ratings = tab.find_element(By.XPATH,".//div[3]").text
        course_useful = tab.find_element(By.XPATH,".//div[4]").text
        course_easy = tab.find_element(By.XPATH,".//div[5]").text
        course_liked = tab.find_element(By.XPATH,".//div[6]").text

        # if no ratings, then move to the next course
        if int(number_of_ratings) > 0:
            # Click into the course to obtain the recent comments
            course_link = tab.find_element(By.XPATH,".//div//a").get_attribute('href')
            driver.execute_script(f'window.open("{course_link}", "_blank");')
            driver.switch_to.window(driver.window_handles[1])
            # wait_for_element(driver, "//div[@class='sc-pKMan dgjbLL']")
            wait_for_element(driver, f"//*[contains(text(), '{course_code}')]")

            # number_of_comments = driver.find_element(By.XPATH, "//a[@class='sc-qPwPv gjSZrg']").text
            if driver.find_elements(By.XPATH, "//a[contains(normalize-space(),'comments')]"):
                number_of_comments = driver.find_elements(By.XPATH, "//a[contains(normalize-space(),'comments')]")[0].text
            else:
                number_of_comments = 0

            show_course_reviews = driver.find_elements(By.XPATH, "//button//div[contains(text(), 'reviews') and contains(text(), 'Show all')]")
            if show_course_reviews:
                driver.execute_script("arguments[0].click();", show_course_reviews[0])

            reviews_raw = driver.find_elements(By.XPATH, "//div[@class= 'sc-pcZJD cnFfnA']//div[normalize-space(text()) and not(translate(text(), '0123456789', '') = '')][1]")
            if not reviews_raw:
                reviews = ['No reviews']
            else:
                reviews = [review.text for review in reviews_raw]
            
            # add number of students enrolled if available
            wait_for_element(driver, "//button[contains(text(), 'Professor reviews')]")

            enrollment = []
            if driver.find_elements(By.XPATH, "//div[@class='sc-oTaid gAHERc']"):
                terms = driver.find_elements(By.XPATH, "//div[@class='sc-oTaid gAHERc']//div//button[normalize-space(text())]")
                for term in terms:
                    actions.click(term).perform()
                    lectures = driver.find_elements(By.XPATH, "//div[@role='rowgroup']//div[@role='row']//div[@role='cell'][3]//div//div//div")
                    if lectures:
                        for lecture in lectures:
                            enrollment.append(lecture.text)
                            #print(lecture.text)
            else:
                enrollment.append("no data")

            driver.find_element(By.XPATH, "//button[contains(text(), 'Professor reviews')]").click()

            toggle_reviews = driver.find_elements(By.XPATH, "//div[contains(text(), 'Show all')]")
            if toggle_reviews:
                for toggle in toggle_reviews: 
                    toggle.click()
            # pdb.set_trace()
            professor_tabs = driver.find_elements(By.XPATH, '//div[@class="sc-psDhf jiTPGb"]')
            if professor_tabs:
                for professor in professor_tabs:
                    professor_name = professor.find_element(By.XPATH, ".//div//div//div//a[@class='sc-qYUWV cNsHwk']").text
                    professor_ratings = professor.find_element(By.XPATH, ".//following::div[@class='sc-pZdvY kzhDZP']").text
                    #time.sleep(1)
                    
                    # print(professor_name)
                    # print(professor_ratings)

                    prof_reviews_raw = professor.find_elements(By.XPATH, './/div[@class="sc-pLwIe kqSAIH"]')
                    if not prof_reviews_raw:
                        prof_reviews = ['No reviews']
                    else:
                        prof_reviews = [review.text for review in prof_reviews_raw]
                    
                    # push to dataframe
                    df2 = append_data_to_df(df2, {'Professor_Name':professor_name, 'Course': course_code, 'Liked_%':professor_ratings, "Professor_Reviews":prof_reviews})
                    # incase code runs into an exception, I want to be adding every row as the code runs
                    df2.to_csv('Prof_data.csv', index=False)

            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        else:
            number_of_comments = 0
            enrollment = ['no data']
            course_easy = 0
            course_liked = 0
            course_useful = 0

        time.sleep(1)
        course_index += 1

        ## Push to dataframe
        df = append_data_to_df(df, {'Course_Code':course_code, 'Course_Name':course_name, 'Number_of_Ratings':number_of_ratings, 'Number_of_Comments':number_of_comments,
                            'Useful':course_useful, 'Easy':course_easy, 'Liked':course_liked, 'Course_Reviews': reviews, 'Course_Enrollment':enrollment})
        
        # incase code runs into an exception, I want to be adding every row as the code runs
        df.to_csv('Course_data.csv', index= False)
        
        if tab == course_tabs[-1]:
            # Make the driver scroll down to load more courses when needed
            actions.move_to_element(tab).perform()
            #center it (seems like it's needed, to make selenium load the new courses)
            desired_y = (tab.size['height'] / 2) + tab.location['y']
            window_h = driver.execute_script('return window.innerHeight')
            window_y = driver.execute_script('return window.pageYOffset')
            current_y = (window_h / 2) + window_y
            scroll_y_by = desired_y - current_y
            driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_y_by)

            # check if there are more courses that we can add
            all_tabs = driver.find_elements(By.XPATH, '//div[@role="rowgroup"]//div[@role="row"]')
            copy_tabs = [item for item in all_tabs if item not in course_tabs]
            course_tabs.extend(copy_tabs)
            # reset index if copy_tabs is changed
            course_index = 0
        
        # pdb.set_trace()
        if not copy_tabs:
            more_courses = False # breaks when copy tabs is empty, i.e all_tabs == course_tabs

    except Exception as e:
        print("Exception caught: " + str(e))
        print(course_code + "--------------------------------") # debugging purposes
        sys.exit(1) # debugging purposes

# save as csv
df.to_csv('Course_data.csv', index= False)
df2.to_csv('Prof_data.csv', index=False)
driver.quit()
