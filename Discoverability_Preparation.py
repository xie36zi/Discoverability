#!/usr/bin/env python
# coding: utf-8

# # Discoverability Inefficient GK&Missing GK Volume Preparation 自动化工具

# In[1]:


from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import time
import urllib.parse
import logging
import re
import retrying
import pyperclip
import os
import math
import numpy as np
import pandas as pd
import csv
import os, sys
import re


# In[2]:


logger_selenium_factory = logging.getLogger("Entry.SeleniumFactory")


def download_data_from_sc_product (dir_path, asin_list):
    driver = SeleniumFactory(dir_path, 30)
    count = len(asin_list)
    v = 100000
    h = math.ceil(len(asin_list) / v)
    asin_list = np.append(asin_list, np.array([0]*(h*100000 - count)))
    asin_list_split = asin_list.reshape(h, v)
    for _ in asin_list_split:
        driver.search_sc(_, "savedTaskItem_6161261", level='product')  #这个地方的task_id, level应该根据需要调整 
    download_wait(dir_path, 600)
    driver.close()
    
def download_data_from_sc_vendor (dir_path, asin_list):
    driver = SeleniumFactory(dir_path, 30)
    count = len(asin_list)
    v = 100000
    h = math.ceil(len(asin_list) / v)
    asin_list = np.append(asin_list, np.array([0]*(h*100000 - count)))
    asin_list_split = asin_list.reshape(h, v)
    for _ in asin_list_split:
        driver.search_sc(_, "savedTaskItem_4775143", level='vendor')  #这个地方的task_id, level应该根据需要调整 
    download_wait(dir_path, 600)
    driver.close()
    
def change_sc_file_name ():
    file_list = os.listdir(os.getcwd())
    all_file_name = [] 
    for i in file_list:
        all_file_name.append(i)              
    all_file_name_str = ' '.join(all_file_name)
    filename = re.findall(r'\w*export.*?xlsx\w*', all_file_name_str)
    return filename


def find_SC_file(Advanced_Basic):
    file_list = os.listdir(os.getcwd())
    all_file_name = []
    for i in file_list:
        all_file_name.append(i)
    all_file_name_str = ' '.join(all_file_name)

    if Advanced_Basic == 'Advanced':
        filename = re.findall(r'\w*SC_Advanced_Raw_Data.*?xlsx\w*', all_file_name_str)
    elif Advanced_Basic == 'Basic':
        filename = re.findall(r'\w*SC_Basic_Raw_Data.*?xlsx\w*', all_file_name_str)
    elif Advanced_Basic == 'Advanced_LL':
        filename = re.findall(r'\w*SC_Advanced_LL_Raw_Data.*?xlsx\w*', all_file_name_str)
    else:
        filename = re.findall(r'\w*SC_Basic_LL_Raw_Data.*?xlsx\w*', all_file_name_str)

    return filename


def download_wait(directory, timeout, nfiles=None):
    """
    Wait for downloads to finish with a specified timeout.

    Args
    ----
    directory : str
        The path to the folder where the files will be downloaded.
    timeout : int
        How many seconds to wait until timing out.
    nfiles : int, defaults to None
        If provided, also wait for the expected number of files.

    """
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < timeout:
        time.sleep(1)
        dl_wait = False
        files = os.listdir(directory)
        if nfiles and len(files) != nfiles:
            dl_wait = True
        for fname in files:
            if fname.endswith('.crdownload'):
                dl_wait = True

        seconds += 1
    return


class SeleniumFactory:

    def __init__(self, dir_path: str, INTERVAL: int):
        self.INTERVAL = INTERVAL

        # 额外设置主要是为了设置下载路径
        options = webdriver.ChromeOptions()
        prefs = {'download.default_directory': dir_path,
                 "download.directory_upgrade": True,
                 "download.prompt_for_download": False,
                 "profile.default_content_setting_values.automatic_downloads": 1}
        options.add_experimental_option('prefs', prefs)
        options.add_experimental_option('w3c', False)
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(20)
        self.driver.maximize_window()

    @retrying.retry(stop_max_attempt_number=1)
    def search_sc(self, asin_list: list, query_task_id: str, level: str='product'):
        """

        :param asin_list: 要下载的asin的列表
        :param query_task_id: 类似'savedTaskItem_4671391'的字符串，是SC页面中的task在页面上的id
        :return:
        """
        main_page = self.driver.current_window_handle
        try:
            _ = list(filter(lambda x: x != 0, asin_list))
            open_new_page_js = 'window.open("");'
            self.driver.execute_script(open_new_page_js)
            handles = self.driver.window_handles
            self.driver.switch_to.window(handles[-1])
            self.driver.get("https://selection-ned.corp.amazon.com/?selectedTab=RCSTab")

            # 等待手动输入验证信息
            while True:
                if urllib.parse.urlparse(self.driver.current_url).netloc == "midway-auth.amazon.com":
                    continue
                break

            # 点掉提示框
            if self.driver.find_elements_by_xpath("//awsui-button[@class='awsui-modal-dismiss-control']/button"):
                self.driver.find_element_by_xpath("//awsui-button[@class='awsui-modal-dismiss-control']/button").click()
            query_ids = '\n'.join(_)
            pyperclip.copy(query_ids)

            # 等待页面加载并切换到正确的frame中
            WebDriverWait(self.driver, 30).until(
                EC.frame_to_be_available_and_switch_to_it(self.driver.find_element_by_tag_name('iframe')))
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.ID, 'SearchPageTopLoadingOverlay')))
            logger_selenium_factory.info("page loaded.")
            WebDriverWait(self.driver, 300).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="taskDiv"]')))
            logger_selenium_factory.info("tasks loaded.")

            # 选择task
            # savedTaskItem_4671391
            ActionChains(self.driver).double_click(self.driver.find_element_by_id(query_task_id)).perform()
            ActionChains(self.driver).double_click(self.driver.find_element_by_id("queryIdsTextArea")).perform()
            time.sleep(2)

            # 清空输入asin用的文本框，并输入asin
            self.driver.find_element_by_id("queryIdsTextArea").clear()
            self.driver.find_element_by_id("queryIdsTextArea").send_keys(Keys.CONTROL, "v")
            time.sleep(1)
            self.driver.find_element_by_id("queryIdsTextArea").send_keys("\n")
            time.sleep(1)
            self.driver.execute_script(
                "document.getElementById(\"uniqueQueryIdsTextArea\").value=\"" + query_ids.replace('\n', '\\n') + "\";")
            time.sleep(1)
            self.driver.find_element_by_id("RCSTab").click()
            time.sleep(1)

            # 提交
            self.driver.execute_script("$(arguments[0]).click();",
                                       self.driver.find_element_by_id("submitSearchButton"))
            time.sleep(3)

            # 等待页面加载
            WebDriverWait(self.driver, 30).until_not(
                EC.presence_of_element_located((By.XPATH, "div.loader-wrapper.loader-wrapper--in-page")))
            WebDriverWait(self.driver, 30).until(
                EC.frame_to_be_available_and_switch_to_it(self.driver.find_element_by_tag_name('iframe')))

            WebDriverWait(self.driver, 3000).until_not(
                EC.presence_of_element_located((By.XPATH, '//div[@id="vendorContributionsTab"]//div[@class="spinner-foreground"]/div[@class="spinner"]')))
            # WebDriverWait(self.driver, 3000).until(
            #     EC.visibility_of_any_elements_located((By.CLASS_NAME, 'sc-asin-image')))
            # WebDriverWait(self.driver, 300).until(
            #     EC.visibility_of_element_located((By.XPATH,
            #                                     '//div[@id="vendorContributionsTab-loading"]//p[contains(@class, "sc-export-asin-count")]')))
            WebDriverWait(self.driver, 30).until(
                EC.element_to_be_clickable((By.ID, 'productInformationTab')))
            logger_selenium_factory.info("search result loaded.")

            if level == 'product':
                # 选择product level， 并export
                self.driver.find_element_by_xpath("//*[@id='productInformationTab']").click()
                logger_selenium_factory.info("search result loaded.")
                self.driver.execute_script("$(arguments[0]).click();",
                                           self.driver.find_element_by_xpath(
                                               "//button[@id='productInformationTab-export']"))
            else:
                # 选择vendor level， 并export
                self.driver.execute_script("$(arguments[0]).click();",
                                           self.driver.find_element_by_xpath(
                                               "//button[@id='vendorContributionsTab-export']"))

            # 点击export后，页面会弹出一个小提示，里面包含着导出/下载页面的url，拿到它并进去这个页面
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.message > a')))
            task_url = self.driver.find_element_by_css_selector('div.message > a').get_attribute("href")
            # driver.execute_script("alert(\"" + task_url + "\");")
            task_id = re.search(r'exportId=(\d*)', task_url).group(1)
            logger_selenium_factory.info("task id: " + task_id)
            self.driver.get(task_url)

            # 根据页面上的元素变化，判断导出状态，等待导出完成
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.CLASS_NAME, "ExportStatusIndicator"))
            )

            flag = True
            while flag:
                end_time = time.time() + self.INTERVAL
                while True:
                    if time.time() < end_time:
                        # logger_selenium_factory.debug(
                        #     task_id + ": " + self.driver.find_element_by_xpath('//*[@id="exportStatusValue"]')
                        #     .text)
                        if self.driver.find_element_by_class_name("ExportStatusIndicator").text == "Completed":
                            flag = False
                            break
                    else:
                        self.driver.refresh()
                        WebDriverWait(self.driver, 300).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "ExportStatusIndicator")))
                        break
            logger_selenium_factory.info(task_id + ": " + self.driver.find_element_by_class_name('ExportStatusIndicator').text)

            # 导出完成，可以下载了
            self.driver.get("https://selection.amazon.com/downloadExportResult?taskId=" + task_id)
            logger_selenium_factory.info(task_id + ": Downloaded.")
        finally:
            if self.driver.current_window_handle == main_page:
                pass
            else:
                self.driver.close()
                self.driver.switch_to.window(main_page)

    def close(self):
        self.driver.close()


#导入文件
file_data_path = input('Upload Advancabsed/Basic volume: ').strip()
GL_PL_data_path = input('Upload gl-pl&NG_brand.xlsx: ').strip()
LL_Vendor_data_path = input('Upload JP Retail L&L untouchable Vendor Code: ').strip()
Advanced_touched_volume_data_path = input('Upload Advanced_touched_volume: ').strip()
Basic_touched_volume_data_path = input('Upload Basic_touched_volume: ').strip()


# 将drive或doc得到一个月的Advancabsed/Basic volume放进工具
Advanced_data = pd.DataFrame(pd.read_excel(file_data_path,sheet_name = 'Inefficent GK'))
Basic_data = pd.DataFrame(pd.read_excel(file_data_path,sheet_name = 'Missing GK'))



# 将Advanced以及Basic task已经touch过的asin放入工具，作为筛选重复ASIN的库
Advanced_touched_volume_data = pd.DataFrame(pd.read_excel(Advanced_touched_volume_data_path))
Basic_touched_volume_data = pd.DataFrame(pd.read_excel(Basic_touched_volume_data_path))
Advanced_touched_volume_data['touched']='Y'
Basic_touched_volume_data['touched']='Y'



# Advanced/Basic volume ASIN列去重
Advanced_data_drop_duplicates = Advanced_data.drop_duplicates(subset=['ASIN'],keep='first',inplace=False)
Basic_data_drop_duplicates = Basic_data.drop_duplicates(subset=['ASIN'],keep='first',inplace=False)



# 与Advanced以及Basic task已经touch过的asin去重
Advanced_data_drop_duplicates_touched_volume = pd.merge(Advanced_data_drop_duplicates, Advanced_touched_volume_data , left_on='ASIN', right_on='ASIN', how='left')
Basic_data_drop_duplicates_touched_volume = pd.merge(Basic_data_drop_duplicates, Basic_touched_volume_data , left_on='ASIN', right_on='ASIN', how='left')
Advanced_data_drop_duplicates_untouched_volume = Advanced_data_drop_duplicates_touched_volume[Advanced_data_drop_duplicates_touched_volume.touched != 'Y']
Basic_data_drop_duplicates_untouched_volume = Basic_data_drop_duplicates_touched_volume[Basic_data_drop_duplicates_touched_volume.touched != 'Y']



# 加入GL L&L/TCEK/SL 列 并筛选L&L/TCEK/SL的ASIN
GL_PL_data = pd.DataFrame(pd.read_excel(GL_PL_data_path,sheet_name = 'gl_pl'))
Advanced_data_drop_duplicates_untouched_volume_GL = pd.merge(Advanced_data_drop_duplicates_untouched_volume, GL_PL_data, left_on='Product Group Description', right_on='GL', how='left')
Basic_data_drop_duplicates_untouched_volume_GL = pd.merge(Basic_data_drop_duplicates_untouched_volume, GL_PL_data, left_on='Product Group Description', right_on='GL', how='left')
Advanced_data_drop_duplicates_untouched_volume_GL_PL = Advanced_data_drop_duplicates_untouched_volume_GL[(Advanced_data_drop_duplicates_untouched_volume_GL.PL_Select == 'SL') | (Advanced_data_drop_duplicates_untouched_volume_GL.PL_Select == 'TCEK') | (Advanced_data_drop_duplicates_untouched_volume_GL.PL_Select == 'L&L')]
Basic_data_drop_duplicates_untouched_volume_GL_PL = Basic_data_drop_duplicates_untouched_volume_GL[(Basic_data_drop_duplicates_untouched_volume_GL.PL_Select == 'SL') | (Basic_data_drop_duplicates_untouched_volume_GL.PL_Select == 'TCEK') | (Basic_data_drop_duplicates_untouched_volume_GL.PL_Select == 'L&L')]




# 删除L&L/ TCEK ASINs gk_byte_size＜500、SL ASINs gk_byte_size＜250的ASIN 此步骤为Advanced ASIN限定
Advanced_data_drop_duplicates_untouched_volume_GL_PL_GK = Advanced_data_drop_duplicates_untouched_volume_GL_PL[((Advanced_data_drop_duplicates_untouched_volume_GL_PL.PL_Select == 'L&L') & (Advanced_data_drop_duplicates_untouched_volume_GL_PL.gk_bite_size <= 500)) | ((Advanced_data_drop_duplicates_untouched_volume_GL_PL.PL_Select == 'TCEK') & (Advanced_data_drop_duplicates_untouched_volume_GL_PL.gk_bite_size <= 500)) |((Advanced_data_drop_duplicates_untouched_volume_GL_PL.PL_Select == 'SL') & (Advanced_data_drop_duplicates_untouched_volume_GL_PL.gk_bite_size <= 250))]


# ASIN导出为excel
Advanced_title = time.strftime('Advanced_Raw_Data_%m.xlsx')
Basic_title = time.strftime('Basic_Raw_Data_%m.xlsx')
Advanced_data_drop_duplicates_untouched_volume_GL_PL_GK.ASIN.to_excel(Advanced_title, index = False)
Basic_data_drop_duplicates_untouched_volume_GL_PL.ASIN.to_excel(Basic_title, index = False)



# 从SC下载 SC_Advanced_Raw_Data_月份 
Advanced_SC_path = os.getcwd()
Advanced_sc_title = time.strftime('SC_Advanced_Raw_Data_%mmonth')
download_data_from_sc_product (Advanced_SC_path, Advanced_data_drop_duplicates_untouched_volume_GL_PL_GK.ASIN.astype('str'))
for i, file in enumerate(change_sc_file_name ()):
    os.rename(file, (Advanced_sc_title +'_'+ str(i) + '.xlsx'))




# 从SC下载 SC_Basic_Raw_Data_月份 
Basic_SC_path = os.getcwd()
Basic_sc_title = time.strftime('SC_Basic_Raw_Data_%mmonth')
download_data_from_sc_product (Basic_SC_path, Basic_data_drop_duplicates_untouched_volume_GL_PL.ASIN.astype('str'))
for i, file in enumerate(change_sc_file_name ()):
    os.rename(file, (Basic_sc_title +'_'+ str(i) + '.xlsx'))



# 导入SC的ASIN和attribute

Advanced_SC_data = pd.DataFrame()
Basic_SC_data = pd.DataFrame()

for file in (find_SC_file('Advanced')):
    Advanced_SC_data_path = Advanced_SC_path + '\\' + file
    Advanced_SC_data = Advanced_SC_data.append(pd.DataFrame(pd.read_excel(Advanced_SC_data_path,header =1)),ignore_index=True)

for file in (find_SC_file('Basic')):
    Basic_SC_data_path = Basic_SC_path + '\\' + file
    Basic_SC_data = Basic_SC_data.append(pd.DataFrame(pd.read_excel(Basic_SC_data_path,header =1)),ignore_index=True)



# ASIN merchant_name.value为空的话则删除
Advanced_SC_data_MP = Advanced_SC_data[Advanced_SC_data['$merchant_name'].notnull()]
Basic_SC_data_MP = Basic_SC_data[Basic_SC_data['$merchant_name'].notnull()]

# 只输出无issue或者只有warning state的ASIN
Advanced_SC_data_MP_noissue = Advanced_SC_data_MP[Advanced_SC_data_MP['(issue.severity)'] !='error']
Basic_SC_data_MP_noissue = Basic_SC_data_MP[Basic_SC_data_MP['(issue.severity)'] !='error']

#将按照上述两条规则删除后剩下的ASIN 匹配item_name以及brand
Advanced_SC_data_item_brand = Advanced_SC_data_MP_noissue[['asin', 'item_name.value', 'brand.value']]
Basic_SC_data_item_brand = Basic_SC_data_MP_noissue[['asin', 'item_name.value', 'brand.value']]



# 导入NG Brand list
NG_Brand_data = pd.DataFrame(pd.read_excel(GL_PL_data_path,sheet_name = 'NG_brand'))
NG_Brand_data_Distinct_submissions = pd.concat([NG_Brand_data, NG_Brand_data['Distinct_ submissions'].str.split('/', expand=True)], axis=1)
NG_Brand_data_NBbrand = NG_Brand_data_Distinct_submissions.drop(['Distinct_ submissions'],axis=1)

# 根据Appendix的NG Brand list删除这些brand的ASIN
NG_Brand_data_NBbrand_list = NG_Brand_data_NBbrand.values.tolist()
NG_Brand_data_NBbrand_list_all = []
for line in NG_Brand_data_NBbrand_list:
    NG_Brand_data_NBbrand_list_all.extend(line)
Advanced_SC_data_item_noNBbrand = Advanced_SC_data_item_brand[~Advanced_SC_data_item_brand['brand.value'].isin(NG_Brand_data_NBbrand_list_all)]
Basic_SC_data_item_noNBbrand = Basic_SC_data_item_brand[~Basic_SC_data_item_brand['brand.value'].isin(NG_Brand_data_NBbrand_list_all)] 



# 将ASIN 分为L&L/TCEK/SL 三个excel，对L&L.xlsx 进行进一步的除外。将L&L的ASIN在SC导出相应属性，下载vendor level的结果文件

# TCEK/SL 输出最终的结果文件
Basic_data_TCEK = pd.merge(Basic_SC_data_item_noNBbrand, Basic_data_drop_duplicates_untouched_volume_GL_PL[Basic_data_drop_duplicates_untouched_volume_GL_PL.PL_Select == 'TCEK'], left_on='asin', right_on='ASIN')
Basic_data_SL = pd.merge(Basic_SC_data_item_noNBbrand, Basic_data_drop_duplicates_untouched_volume_GL_PL[Basic_data_drop_duplicates_untouched_volume_GL_PL.PL_Select == 'SL'], left_on='asin', right_on='ASIN')
Advanced_data_TCEK = pd.merge(Advanced_SC_data_item_noNBbrand, Advanced_data_drop_duplicates_untouched_volume_GL_PL_GK[Advanced_data_drop_duplicates_untouched_volume_GL_PL_GK.PL_Select == 'TCEK'], left_on='asin', right_on='ASIN')
Advanced_data_SL = pd.merge(Advanced_SC_data_item_noNBbrand, Advanced_data_drop_duplicates_untouched_volume_GL_PL_GK[Advanced_data_drop_duplicates_untouched_volume_GL_PL_GK.PL_Select == 'SL'], left_on='asin', right_on='ASIN')

Basic_data_TCEK_output = Basic_data_TCEK[['ASIN', 'MarketPlace Id', 'Product Group Code', 'Product Group Description', 'PL_Select', 'item_name','brand_name','idq_grade','classification', 'offertype']]
Basic_data_TCEK_output.rename(columns={'PL_Select':'PL'},inplace=True)
Basic_data_SL_output = Basic_data_SL[['ASIN', 'MarketPlace Id', 'Product Group Code', 'Product Group Description', 'PL_Select', 'item_name','brand_name','idq_grade','classification', 'offertype']]
Basic_data_SL_output.rename(columns={'PL_Select':'PL'},inplace=True)

Advanced_data_TCEK_output = Advanced_data_TCEK[['ASIN', 'MarketPlace Id', 'Product Group Description', 'Product Family', 'PL_Select', 'item_name.value','brand.value','generic_keywords','bytes', 'gk_bite_size', 'idq_grade']]
Advanced_data_TCEK_output.rename(columns={'PL_Select':'PL'},inplace=True)
Advanced_data_SL_output = Advanced_data_SL[['ASIN', 'MarketPlace Id', 'Product Group Description', 'Product Family', 'PL_Select', 'item_name.value','brand.value','generic_keywords','bytes', 'gk_bite_size', 'idq_grade']]
Advanced_data_SL_output.rename(columns={'PL_Select':'PL'},inplace=True)


Basic_TCEK_title = time.strftime('Basic_TCEK_Volume_Allocation_%m.xlsx')
Basic_SL_title = time.strftime('Basic_SL_Volume_Allocation_%m.xlsx')
Advanced_TCEK_title = time.strftime('Advanced_TCEK_Volume_Allocation_%m.xlsx')
Advanced_SL_title = time.strftime('Advanced_SL_Volume_Allocation_%m.xlsx')
Basic_data_TCEK_output.to_excel(Basic_TCEK_title,index = False)
Basic_data_SL_output.to_excel(Basic_SL_title,index = False)
Advanced_data_TCEK_output.to_excel(Advanced_TCEK_title,index = False)
Advanced_data_SL_output.to_excel(Advanced_SL_title,index = False)




# 将L&L的ASIN在SC导出相应属性，下载vendor level的结果文件
Advanced_data_LL = pd.merge(Advanced_SC_data_item_noNBbrand, Advanced_data_drop_duplicates_untouched_volume_GL_PL_GK[Advanced_data_drop_duplicates_untouched_volume_GL_PL_GK.PL_Select == 'L&L'], left_on='asin', right_on='ASIN')
Basic_data_LL = pd.merge(Basic_SC_data_item_noNBbrand, Basic_data_drop_duplicates_untouched_volume_GL_PL[Basic_data_drop_duplicates_untouched_volume_GL_PL.PL_Select == 'L&L'], left_on='asin', right_on='ASIN')



# 从SC下载 SC_Advanced_Raw_L$L_Data_月份 
Advanced_SC_path_LL = os.getcwd()
Advanced_sc_title_LL = time.strftime('SC_Advanced_LL_Raw_Data_%mmonth')
download_data_from_sc_vendor (Advanced_SC_path_LL, Advanced_data_LL.ASIN.astype('str'))
for i, file in enumerate(change_sc_file_name ()):
    os.rename(file, (Advanced_sc_title_LL +'_'+ str(i) + '.xlsx'))



# 从SC下载 SC_Basic_Raw_L$L_Data_月份
Basic_SC_path_LL = os.getcwd()
Basic_sc_title_LL = time.strftime('SC_Basic_LL_Raw_Data_%mmonth')
download_data_from_sc_vendor (Basic_SC_path_LL, Basic_data_LL.ASIN.astype('str'))
for i, file in enumerate(change_sc_file_name ()):
    os.rename(file, (Basic_sc_title_LL +'_'+ str(i) + '.xlsx'))



# 导入SC的ASIN和attribute 将vendor level的结果文件 放入工具

Advanced_SC_LL_data = pd.DataFrame()
Basic_SC_LL_data = pd.DataFrame()

for i, file in enumerate(find_SC_file('Advanced_LL')):
    Advanced_SC_LL_data_path =  Advanced_SC_path_LL + '\\' + file
    Advanced_SC_LL_data = Advanced_SC_LL_data.append(pd.DataFrame(pd.read_excel(Advanced_SC_LL_data_path,header =1)))
    
for i, file in enumerate(find_SC_file('Basic_LL')):
    Basic_SC_data_LL_path =  Basic_SC_path_LL + '\\' + file
    Basic_SC_LL_data = Basic_SC_LL_data.append(pd.DataFrame(pd.read_excel(Basic_SC_data_LL_path,header =1)))



# *将JP Retail 提供的L&L untouchable Vendor Code 放入工具
LL_Vendor_data = pd.DataFrame(pd.read_excel(LL_Vendor_data_path,sheet_name = 'Sheet1'))
LL_Vendor_data_dropna = LL_Vendor_data.dropna(subset = ['Manufacturer Vendor Code'])

#将sc 导出数据的manufacturer/ brand 两项属性删除，vendor_name一列只保留AmazonJp/XXXXX形式的vendor name
Advanced_SC_LL_data.drop(['manufacturer.value', 'brand.value', 'vendor_name.value','brand_code.value'], axis=1, inplace=True)
Basic_SC_LL_data.drop(['manufacturer.value', 'brand.value', 'vendor_name.value','brand_code.value'], axis=1, inplace=True)



# 清理sc_vendor_name
Advanced_SC_LL_data_vendor = pd.concat([Advanced_SC_LL_data, Advanced_SC_LL_data['sc_vendor_name'].str.split('/', expand=True)], axis=1)
Basic_SC_LL_data_vendor = pd.concat([Basic_SC_LL_data, Basic_SC_LL_data['sc_vendor_name'].str.split('/', expand=True)], axis=1)

Advanced_SC_LL_data_vendor.drop(['sc_vendor_name'], axis=1, inplace=True)
Basic_SC_LL_data_vendor.drop(['sc_vendor_name'], axis=1, inplace=True)
Advanced_SC_LL_data_vendor.rename(columns={0: 'AmazonJp_OrNot', 1: 'vendor_code'}, inplace=True)
Basic_SC_LL_data_vendor.rename(columns={0: 'AmazonJp_OrNot', 1: 'vendor_code'}, inplace=True)

# 排除非AmazonJp的值
Advanced_SC_LL_data_vendor_AmazonJp = Advanced_SC_LL_data_vendor[Advanced_SC_LL_data_vendor['AmazonJp_OrNot'] == 'AmazonJp']
Basic_SC_LL_data_vendor_AmazonJp = Basic_SC_LL_data_vendor[Basic_SC_LL_data_vendor['AmazonJp_OrNot'] == 'AmazonJp']




# 填充manufacturer_vendor_code一列的空值 
Advanced_SC_LL_manufacturer_vendor_manufacturer_vendor_code = Advanced_SC_LL_data_vendor[['asin','manufacturer_vendor_code.value']]
Basic_SC_LL_manufacturer_vendor_manufacturer_vendor_code = Basic_SC_LL_data_vendor[['asin','manufacturer_vendor_code.value']]

Advanced_SC_LL_manufacturer_vendor_NaN = Advanced_SC_LL_manufacturer_vendor_manufacturer_vendor_code[Advanced_SC_LL_manufacturer_vendor_manufacturer_vendor_code['manufacturer_vendor_code.value'].isnull()]
Basic_SC_LL_manufacturer_vendor_NaN = Basic_SC_LL_manufacturer_vendor_manufacturer_vendor_code[Basic_SC_LL_manufacturer_vendor_manufacturer_vendor_code['manufacturer_vendor_code.value'].isnull()]
Advanced_SC_LL_manufacturer_vendor_NotNan = Advanced_SC_LL_manufacturer_vendor_manufacturer_vendor_code[Advanced_SC_LL_manufacturer_vendor_manufacturer_vendor_code['manufacturer_vendor_code.value'].notnull()]
Basic_SC_LL_manufacturer_vendor_NotNan = Basic_SC_LL_manufacturer_vendor_manufacturer_vendor_code[Basic_SC_LL_manufacturer_vendor_manufacturer_vendor_code['manufacturer_vendor_code.value'].notnull()]

Advanced_data_LL_manufacturer_vendor_all = pd.merge(Advanced_SC_LL_manufacturer_vendor_NaN, Advanced_SC_LL_manufacturer_vendor_NotNan, left_on='asin', right_on='asin', how = 'outer')
Basic_data_LL_manufacturer_vendor_all = pd.merge(Basic_SC_LL_manufacturer_vendor_NaN, Basic_SC_LL_manufacturer_vendor_NotNan, left_on='asin', right_on='asin', how = 'outer')

Advanced_data_LL_manufacturer_vendor_all.drop(['manufacturer_vendor_code.value_x'], axis=1, inplace=True)
Basic_data_LL_manufacturer_vendor_all.drop(['manufacturer_vendor_code.value_x'], axis=1, inplace=True)



# sc_vendor_name为准进行vlookup,筛选出untouchable ASINs
Advanced_SC_LL_AmazonJp_vendor_code = pd.merge(Advanced_SC_LL_data_vendor_AmazonJp, LL_Vendor_data_dropna, left_on='vendor_code', right_on='Manufacturer Vendor Code', how = 'left')
Basic_SC_LL_AmazonJp_vendor_code = pd.merge(Basic_SC_LL_data_vendor_AmazonJp, LL_Vendor_data_dropna, left_on='vendor_code', right_on='Manufacturer Vendor Code', how = 'left')
Advanced_SC_LL_AmazonJp_vendor_code_untouchable = Advanced_SC_LL_AmazonJp_vendor_code[Advanced_SC_LL_AmazonJp_vendor_code['Manufacturer Vendor Code'].notnull()]
Basic_SC_LL_AmazonJp_vendor_code_untouchable = Basic_SC_LL_AmazonJp_vendor_code[Basic_SC_LL_AmazonJp_vendor_code['Manufacturer Vendor Code'].notnull()]

Advanced_SC_LL_AmazonJp_vendor_code_untouchable_drop_duplicates = Advanced_SC_LL_AmazonJp_vendor_code_untouchable.drop_duplicates(subset=['asin'],keep='first',inplace=False)
Basic_SC_LL_AmazonJp_vendor_code_untouchable_drop_duplicates = Basic_SC_LL_AmazonJp_vendor_code_untouchable.drop_duplicates(subset=['asin'],keep='first',inplace=False)

# manufacturer_vendor_code.value为准进行vlookup,筛选出untouchable ASINs
Advanced_data_LL_manufacturer_vendor_touchable = pd.merge(Advanced_data_LL_manufacturer_vendor_all, LL_Vendor_data_dropna, left_on='manufacturer_vendor_code.value_y', right_on='Manufacturer Vendor Code', how = 'left')
Basic_data_LL_manufacturer_vendor_touchable = pd.merge(Basic_data_LL_manufacturer_vendor_all, LL_Vendor_data_dropna, left_on='manufacturer_vendor_code.value_y', right_on='Manufacturer Vendor Code', how = 'left')

Advanced_data_LL_manufacturer_vendor_untouchable_all = Advanced_data_LL_manufacturer_vendor_touchable[Advanced_data_LL_manufacturer_vendor_touchable['Manufacturer Vendor Code'].notnull()]
Basic_data_LL_manufacturer_vendor_untouchable_all = Basic_data_LL_manufacturer_vendor_touchable[Basic_data_LL_manufacturer_vendor_touchable['Manufacturer Vendor Code'].notnull()]

Advanced_data_LL_manufacturer_vendor_untouchable_all_drop_duplicates = Advanced_data_LL_manufacturer_vendor_untouchable_all.drop_duplicates(subset=['asin'],keep='first',inplace=False)
Basic_data_LL_manufacturer_vendor_untouchable_all_drop_duplicates = Basic_data_LL_manufacturer_vendor_untouchable_all.drop_duplicates(subset=['asin'],keep='first',inplace=False)

# sc_vendor_name和manufacturer_vendor_code.value为准进行vlookup,筛选出touchable ASINs
Advanced_SC_LL_data_vendor_AmazonJp_touchable_1 = pd.merge(Advanced_SC_LL_data_vendor_AmazonJp.asin, Advanced_SC_LL_AmazonJp_vendor_code_untouchable_drop_duplicates.asin, left_on = 'asin', right_on = 'asin', how = 'left', indicator = True)
Advanced_SC_LL_data_vendor_AmazonJp_touchable_2 = pd.merge(Advanced_SC_LL_data_vendor_AmazonJp_touchable_1, Advanced_data_LL_manufacturer_vendor_untouchable_all_drop_duplicates, left_on = 'asin', right_on = 'asin', how = 'left')
Basic_SC_LL_data_vendor_AmazonJp_touchable_1 = pd.merge(Basic_SC_LL_data_vendor_AmazonJp.asin, Basic_SC_LL_AmazonJp_vendor_code_untouchable_drop_duplicates.asin, left_on = 'asin', right_on = 'asin', how = 'left', indicator = True)
Basic_SC_LL_data_vendor_AmazonJp_touchable_2 = pd.merge(Basic_SC_LL_data_vendor_AmazonJp_touchable_1, Basic_data_LL_manufacturer_vendor_untouchable_all_drop_duplicates, left_on = 'asin', right_on = 'asin', how = 'left')

Advanced_SC_LL_data_vendor_AmazonJp_touchable_all = Advanced_SC_LL_data_vendor_AmazonJp_touchable_2[(Advanced_SC_LL_data_vendor_AmazonJp_touchable_2._merge == 'left_only') & (Advanced_SC_LL_data_vendor_AmazonJp_touchable_2['manufacturer_vendor_code.value_y'].isnull())]
Basic_SC_LL_data_vendor_AmazonJp_touchable_all = Basic_SC_LL_data_vendor_AmazonJp_touchable_2[(Basic_SC_LL_data_vendor_AmazonJp_touchable_2._merge == 'left_only') & (Basic_SC_LL_data_vendor_AmazonJp_touchable_2['manufacturer_vendor_code.value_y'].isnull())]



# LL 输出最终的结果文件
Basic_data_LL_final = pd.merge(Basic_SC_LL_data_vendor_AmazonJp_touchable_all, Basic_data_drop_duplicates_untouched_volume_GL_PL[Basic_data_drop_duplicates_untouched_volume_GL_PL.PL_Select == 'L&L'], left_on='asin', right_on='ASIN')
Advanced_data_LL_final = pd.merge(Advanced_SC_LL_data_vendor_AmazonJp_touchable_all, Advanced_data_LL, left_on='asin', right_on='ASIN', how = 'left')

Basic_data_LL_final_output = Basic_data_LL_final[['ASIN', 'MarketPlace Id', 'Product Group Code', 'Product Group Description', 'PL_Select', 'item_name','brand_name','idq_grade','classification', 'offertype']]
Basic_data_LL_final_output.rename(columns={'PL_Select':'PL'},inplace=True)

Advanced_data_LL_final_output = Advanced_data_LL_final[['ASIN', 'MarketPlace Id', 'Product Group Description', 'Product Family', 'PL_Select', 'item_name.value','brand.value','generic_keywords','bytes', 'gk_bite_size', 'idq_grade']]
Advanced_data_LL_final_output.rename(columns={'PL_Select':'PL'},inplace=True)

Advanced_data_LL_final_output_drop_duplicates = Advanced_data_LL_final_output.drop_duplicates(subset=['ASIN'],keep='first',inplace=False)
Basic_data_LL_final_output_drop_duplicates = Basic_data_LL_final_output.drop_duplicates(subset=['ASIN'],keep='first',inplace=False)


Basic_LL_title = time.strftime('Basic_L&L_Volume_Allocation_%m.xlsx')
Advanced_LL_title = time.strftime('Advanced_L&L_Volume_Allocation_%m.xlsx')
Basic_data_LL_final_output_drop_duplicates.to_excel(Basic_LL_title,index = False)
Advanced_data_LL_final_output_drop_duplicates.to_excel(Advanced_LL_title,index = False)

