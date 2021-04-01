# This script scrapes reddit posts and extracts urls

import requests
import re
import os
import time
import json
import uuid
import math
import random
import sqlite3
from datetime import datetime, timedelta


SIZE = 100
six_weeks = 604800 * 6
query = """INSERT INTO urls (url_id, url_text, url_domain, url_type,
        post_date, subreddit_name, post_id)
        VALUES (?, ?, ?, ?, ?, ?, ?);"""
command = """
    CREATE TABLE urls 
    (url_id VARCHAR(40),
    url_text VARCHAR(255),
    url_domain VARCHAR(255),
    url_type VARCHAR(40),
    post_date DATETIME,
    subreddit_name VARCHAR(255),
    post_id VARCHAR(40));"""


def get_more_posts(subreddit, before):
    '''
    Gets the subreddit posts.
    Input:
        subreddit (string): name of the subreddit 
        before (int): timestamp to limit the search 
    '''
    url = 'https://api.pushshift.io/reddit/search/submission?'
    url += f'subreddit={subreddit}&before={before}&size={SIZE}&sort=desc'
    print('scraping before:', datetime.fromtimestamp(before))
    resp = requests.get(url, timeout = 200)
    status_code = resp.status_code
    if status_code == 200:
        return resp.json()['data']
    else:
        print('no output; sleeping for 1 min before retrying...')
        time.sleep(60*1)
        return get_more_posts(subreddit, before)


def get_subreddits(subreddits):
    '''
    Downloads posts and writes them into databases.

    Input:
        subreddits (list): subreddits to go through
    '''
    for subreddit_link in subreddits:
        subreddit, day = subreddit_link.split(',')
        day = int(datetime.strptime(day, '%Y-%m-%d').timestamp())
        next_6_weeks = day + six_weeks
        prev_6_weeks = day - six_weeks
        get_farthest = next_6_weeks
        output_length = 100

        connection = sqlite3.connect(f'data/{subreddit}.sql')
        cursor = connection.cursor()
        cursor.execute(command)

        while output_length == 100 and get_farthest >= prev_6_weeks:
            posts = get_more_posts(subreddit, get_farthest)
            output_length = len(posts)
            
            for post in posts:
                id_ = post['id']
                post_date = post['created_utc']
            
                # url in text
                if 'selftext' in post:
                    domain_links = re.findall('''\(https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+''', post['selftext'])
                    domain_links = [re.sub('[()]', '', x) for x in domain_links]
                    all_links = re.findall('\(https?://[^\s]+\)', post['selftext'])
                    all_links = [re.sub('[()]', '', x) for x in all_links]
                    for ind, link in enumerate(all_links):
                        if 'redd.it' not in link and \
                        'reddit.com' not in link:
                            cursor.execute(query,
                                [str(uuid.uuid4()), link, domain_links[ind], 'text',
                                    post_date, subreddit, id_]).fetchall()
                
                # url in media
                all_links = re.findall('https?://[^\s]+', post['url'])
                for a_link in all_links:
                    if 'redd.it' not in a_link and \
                    'reddit.com' not in a_link:
                        domain_link = re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', a_link)
                        cursor.execute(query,
                            [str(uuid.uuid4()), a_link, domain_link[0], 'media',
                                post_date, subreddit, id_]).fetchall()
            
            if get_farthest != post_date:
                get_farthest = post_date
            else:
                get_farthest -= 1
            
            connection.commit()
        
        connection.close()
        time.sleep(1 + random.randint(0, 1))


def go(mode='scrape'):
    '''
    Runs the code.
    Input:
        mode: if not specified or 'scrape', it will use subreddits
        from subreddits.txt
        if specified to anithing other than 'scrape', asks for
        a subreddit to test on.
    Output:
        None, writes an .sql file.
    '''
    if mode == 'scrape':
        links = open('data/subreddits.txt')
        subreddits = links.read().split('\n')
        links.close() 
    else:
        subreddits = ['uchicago,2021-01-15']
    get_subreddits(subreddits)