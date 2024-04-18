import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup
import time
import re
import random

# outer scope variables
driver_path = '/usr/local/bin/geckodriver'
url = 'https://www.reddit.com/r/CryptoCurrency/'
article_keywords = ['bitcoin', 'ethereum', 'btc', 'eth']


# custom functions
def contains_any(string, contains_list):
    """
    Check if a string contains any of the substrings in a list.
    :param string: string to check
    :param contains_list: list of substrings to check for
    :return: True if the string contains any of the substrings, False otherwise.
    """
    regex_pattern = '|'.join(map(re.escape, contains_list))
    return bool(re.search(regex_pattern, string))


def create_driver():
    """
    Create a Firefox driver.
    :return: A Firefox driver.
    """
    print('Creating driver ...')
    options = Options()
    options.add_argument('--headless')
    driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()), options=options)
    return driver


def generate_scroll_script(scroll_distance=None, min_speed=1000, max_speed=3000):
    """
    Generates a JavaScript string for scrolling a variable distance at a variable speed.

    :param scroll_distance: Optional. The distance to scroll. If None, scrolls to the bottom.
    :param min_speed: Minimum duration (in milliseconds) to complete the scroll.
    :param max_speed: Maximum duration (in milliseconds) to complete the scroll.
    :return: A string containing the JavaScript to execute.
    """
    if scroll_distance is None:
        scroll_command = "window.scrollTo(0, document.body.scrollHeight);"
    else:
        scroll_command = f"window.scrollBy(0, {scroll_distance});"

    speed = random.randint(min_speed, max_speed)

    js_script = (
        f"setTimeout(function() {{ {scroll_command} }}, {speed});"
    )

    return js_script


def scrape_data(request_count: int, driver):
    print('Scraping ...')
    stopper = 0
    page_source_snapshot = ''
    driver.get(url)
    time.sleep(3)
    driver.switch_to.default_content()
    for req in range(1, request_count):
        timer = random.randint(4, 6)
        scroll_script = generate_scroll_script()
        driver.execute_script(scroll_script)
        time.sleep(timer)
        if stopper == 5: break
        if driver.page_source == page_source_snapshot: stopper += 1
        page_source_snapshot = driver.page_source
    yield page_source_snapshot
    print('Scraping done')

@data_loader
def load_data_from_api(*args, **kwargs):
    # set up the service and driver
    driver = create_driver()

    results = {
        'title': [],
        'date': [],
        'votes': [],
        'comments': []
    }

    # scroll down to load more posts andjust the number of iterations to scrape
    for item in scrape_data(100, driver):
        print('Processing results ...')
        soup = BeautifulSoup(item, 'html.parser')
        # get the elements with the required data
        elements = soup.select('[class*="w-full m-0"]')
        # extract the required data filtering by keywords in the title of the post
        for element in elements:
            if element.get('aria-label') is not None:
                if contains_any(element.get('aria-label').lower(), article_keywords):
                    results['title'].append(element.get('aria-label'))
                    results['date'].append(element.select('shreddit-post')[0].get('created-timestamp'))
                    results['votes'].append(element.select('shreddit-post')[0].get('score'))
                    results['comments'].append(element.select('shreddit-post')[0].get('comment-count'))
    driver.close()
    driver.quit()

    df = pd.DataFrame(results)
    
    df['date'] = pd.to_datetime(df['date'])
    df['date-by-day'] = df['date'].dt.date

    return df

