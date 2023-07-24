import os
import asyncio
from pyppeteer import launch
from typing import List, Dict
import csv
import json

# Query the list of job item card --> Extract the details on the child elements with selectors
async def get_list_job_and_content(element, page, selectors: str, JOB_ITEM_SELECTOR):
    job_details = []
    job_item_elements = []
    try:
        job_item_elements = await element.querySelectorAll(JOB_ITEM_SELECTOR)
    except Exception as e:
        print(f"ERROR: Can not query with current selector {JOB_ITEM_SELECTOR}, error: {e}")

    for job_item_element in job_item_elements:
        job_detail = {}
        for selector in selectors:
            val = []
            job_detail_elements = await job_item_element.querySelectorAll(selector["selector"])
            for job_detail_element in job_detail_elements:
                try:
                    if selector["type"] == 'TEXT' or selector["type"] == 'MULTI_TEXT':
                        val.append((await page.evaluate('(el) => el.textContent', job_detail_element)).strip())
                    elif selector["type"] == 'IMAGE':
                        val.append(await page.evaluate('(el) => el.src', job_detail_element))
                    elif selector["type"] == 'LINK':
                        val.append(await page.evaluate('(el) => el.href', job_detail_element))
                except Exception as e:
                    print("INFO: object not found", e)
            if len(val) > 0:
                if selector["type"] == 'MULTI_TEXT':
                    job_detail[selector["name"]] = ', '.join(val)
                else:
                    job_detail[selector["name"]] = val[0]
            else: 
                job_detail[selector["name"]] = ""
        job_details.append(job_detail)
    return job_details


# Function to generate a common selector
def generate_common_selector(selectors):
    arr = [s.replace(' > ', '> ').split(' ') for s in selectors]
    arr.sort()
    a1 = arr[0]
    a2 = arr[len(arr) - 1]
    L = len(a1)
    i = 0
    while i < L and a1[i] == a2[i]:
        i += 1
    return ' '.join([s.replace('>', ' >') for s in a1[:i]])

# Define the auto_scroll function
async def auto_scroll(page, auto_scroll_selector):
    await page.evaluate(f"""
        window.scrollBy(0, document.body.scrollHeight);
    """)
    while True:
        len1 = len(await page.querySelectorAll(auto_scroll_selector))
        await asyncio.sleep(2)  # wait a bit for items to load
        len2 = len(await page.querySelectorAll(auto_scroll_selector))
        if len1 == len2:  # no more new elements
            break
        else:
            await page.evaluate("""
                window.scrollBy(0, document.body.scrollHeight);
            """)

# Function to scrape data from the website
async def scrape_data(page, config):
    selectors = config['SELECTORS']
    common_sub_path = generate_common_selector([s["selector"] for s in selectors])
    sub_selectors = [
        {**s, "selector": s["selector"].replace(common_sub_path, "").strip()} for s in selectors
    ]
    common_sub_path = common_sub_path[:-2] if common_sub_path.endswith('>') else common_sub_path

    elements = await page.querySelectorAll(common_sub_path) if common_sub_path else [page]

    scraped_data = []
    for element in elements:
        data = await get_list_job_and_content(element, page, selectors, config['JOB_ITEM_SELECTOR'])
        scraped_data.extend(data)

    return scraped_data

async def main(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
        
    browser = await launch(
        headless=True,
        timeout=100000,
        ignoreDefaultArgs=["--enable-automation"],
        args=[],
        defaultViewport=None
    )

    page = await browser.newPage()

    try:
        await page.goto(config['LINK'], waitUntil=["networkidle2"], timeout=15000)
    except pyppeteer.errors.TimeoutError as e:
        print(f"TimeoutError when loading page, error: {e}")
        return []
    
    await auto_scroll(page, config['AUTO_SCROLL_SELECTOR'])

    scraped_data = await scrape_data(page, config)
            
    await browser.close()

    # Encode scraped_data to UTF-8
    encoded_scraped_data = json.dumps(scraped_data, ensure_ascii=False).encode('utf8')

    print(encoded_scraped_data)
    return encoded_scraped_data

if __name__ == "__main__":
    import sys
    asyncio.run(main(sys.argv[1]))
