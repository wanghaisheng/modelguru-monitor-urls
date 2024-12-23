from getbrowser import setup_chrome
print('start')
browser = setup_chrome()
print('borw',browser)
tab=browser.new_tab()
url ="https://search.ccgp.gov.cn/bxsearch?searchtype=2&page_index=1&bidSort=0&buyerName=&projectId=&pinMu=0&bidType=7&dbselect=bidx&kw=CT&start_time=2024%3A06%3A25&end_time=2024%3A12%3A24&timeType=5&displayZone=&zoneId=&pppStatus=0&agentName="

tab.get(url)
print('sssss')
counts=tab.ele('.pager').eles('tag:a')[-2].text
print(counts)
