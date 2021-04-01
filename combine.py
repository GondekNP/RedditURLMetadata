'''
This file will be used to combine the scaper.py output and the url_tools.py file
'''

import url_tools
import sqlite3
import pandas as pd

def sql_to_pd(db_path, tab_name):
    '''
    Reads in sql table from a database as pd.DataFrame.
    
    Inputs:
        db_path: (str) path to sql database
        tab_name: (str) name of desired table

    Output:
        (pd.DataFrame) of desired table
    '''
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    command = f'''
    SELECT * FROM {tab_name};'''
    cursor.execute(command)
    table = cursor.fetchall()
    df = pd.DataFrame(table)
    header = []
    for tup in cursor.description:
        col = tup[0]
        if "." in col:
            col = col[col.find(".")+1:]
        header.append(col)
    df.columns = header
    return df

def init_dbs(domain_cache_path, analysis_path):
    '''
    Creates domain cache and analysis database if they do not exist.

    Inputs:
        domain_cache_path: (str) path to domain sql cache
        analysis_path: (str) path to analysis sql
    '''
    connection = sqlite3.connect(domain_cache_path)
    cursor = connection.cursor()
    tabs = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tabs = [tab[0] for (tab) in tabs]

    if 'domains' not in tabs:
        cursor.execute("""
        CREATE TABLE domains 
        (domain VARCHAR(255),
        ip VARCHAR(255))
        """)
        connection.commit()

    if 'redir' not in tabs:
        cursor.execute("""
        CREATE TABLE redir 
        (url VARCHAR(255),
        eff_url VARCHAR(255),
        success INT)
        """)
        connection.commit()

    connection = sqlite3.connect(analysis_path)
    cursor = connection.cursor()
    tabs = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tabs = [tab[0] for (tab) in tabs]

    if 'analysis_urls' not in tabs:
        cursor.execute("""
        CREATE TABLE analysis_urls 
        (url_id VARCHAR(255) PRIMARY KEY,
        url_text VARCHAR(255),
        subreddit VARCHAR(255), 
        domain VARCHAR(255),
        post_date DATE);
        """)
        connection.commit()

    if 'analysis_ips' not in tabs:
        cursor.execute("""
        CREATE TABLE analysis_ips
        (url_id VARCHAR(255),
        ip_address VARCHAR(255),
        domain VARCHAR(255),
        org_name VARCHAR(255),
        city VARCHAR(255),
        state_prov VARCHAR(255),    
        country VARCHAR(255),
        ip_weight REAL,
        FOREIGN KEY (url_id) REFERENCES analysis_urls(url_id));
        """)
        connection.commit()

    connection.close()

def go(subreddits = None, test = False,
    whois_keys = ['OrgName','City','StateProv','Country','RegDate']):
    '''
    Reads in subreddit post url's from sql databases created by scraper, 
    uses url_tools' url_to_ip to get IP information on each url, and finally 
    uses url_tools' ip_whois to get ARIN WhoIs data for each IP address.
    Results are added to 'analysis_ips' and 'analysis_domains' in the analysis
    sql database.

    Inputs:
        subreddits: (list of strs, or None) list of subreddits to process, if
        None, the list from data/subreddits.txt will be processed.
        whois_keys: (list of strs) target fields to pull from IP WhoIs lookups

    Returns:
        None, but analysis database will be updated with analysis results.
    '''
    if subreddits is None: 
        links = open('data/subreddits_1.txt')
        subreddits = links.read().split('\n')
        links.close()
    if test:
        counter = 0
    connection = sqlite3.connect('data/analysis.sql')
    cursor = connection.cursor()
    domain_cache_path = 'domain_cache.sql'
    analysis_path = 'data/analysis.sql'
    init_dbs(domain_cache_path, analysis_path)

    insert_data_ip = \
        '''INSERT INTO analysis_ips(url_id, ip_address, domain,
                        org_name, city, state_prov, country, ip_weight)
            VALUES (:url_id, :ip, :domain, :OrgName, :City,
                    :StateProv, :Country, :ip_weight);'''

    for subreddit in subreddits:
        sub = subreddit.split(",")[0]
        sql_path = f'data/{sub}.sql'
        subreddit_df = sql_to_pd(sql_path, 'urls')

        for row in subreddit_df.itertuples():
            _, url_id, url, _, _, date, subreddit, _ = row
            domain, ips = url_tools.url_to_ip(url, test = test)
            who_is_scrape = url_tools.ip_whois(ips, test = test)
            row_insert = {'url_id' : url_id, 'url' : url,
                'subreddit': subreddit, 'domain' : domain, 'date' : date} 
            insert_url_data = \
            '''INSERT INTO analysis_urls
                (url_id, url_text, subreddit, domain, post_date)
                VALUES(:url_id, :url, :subreddit, :domain, :date)'''
            cursor.execute(insert_url_data, row_insert)
            
            for ip, page_info in who_is_scrape:
                if ip is None:
                    continue
                if page_info is None:
                    row_insert = {key: None for key in whois_keys}
                elif any([key not in page_info for key in whois_keys]):
                    row_insert = {}
                    for key in whois_keys:
                        if key not in page_info:
                            row_insert[key] = '**WHOIS KEY NOT FOUND**'
                        else:
                            row_insert[key] = page_info[key]
                else:
                    row_insert = {key: val for key, val in page_info.items()\
                        if key in whois_keys}
                
                row_insert.update({'url_id' : url_id, 'ip' : ip,
                    'domain' : domain, 'ip_weight' : (1 / len(ips))})
                cursor.execute(insert_data_ip, row_insert)

            if test:
                counter += 1
                if counter > 3:
                    test = False

            connection.commit()
