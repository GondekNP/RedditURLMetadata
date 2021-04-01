import os
import subprocess
import re
from nslookup import Nslookup
import pycurl
import certifi
import sqlite3
import combine
import pandas as pd
import datetime

def url_to_ip(url, domain_cache_path = 'domain_cache.sql',
    log_file_path = 'cache_log.txt', test = False):
    '''
    Takes in urls, follows any redirects, and uses nslookup to find all IP
    addresses that are associated with the domain of the redirected url. 
    Builds and uses a domain sql cache that stores url/redirected url pairs, and 
    domain name/ip addresses pairs, to avoid re-processing duplicate URLs, or 
    duplicate domain names. 
    
    Input:
        url: (str) url to process
        domain_cache_path: (str) path to location of domain sql cache
    
    Output:
        (dict) associating domain names with their associated IP addresses
    '''
    #using cloudflare's public dns resolver
    dns_query = Nslookup(dns_servers=["1.1.1.1"])
    
    connection = sqlite3.connect(domain_cache_path)
    cursor = connection.cursor()

    if test:
        print('Processing url: ' + str(url))
    #Defining sql queries for cache lookup and insertion
    dom_insert_str = "INSERT INTO domains (domain, ip) VALUES (:dom, :ip)" 
    dom_check_str = "SELECT * from domains WHERE domain == :dom"
    redir_insert_str = '''INSERT INTO redir (url, eff_url, success) VALUES 
                          (:url, :eff_url, :success)''' 
    redir_check_str = "SELECT * from redir WHERE url == :url"

    #Clean up URL
    url = distill_url(url)
    if len(re.findall('\.', url)) == 1:
        url = 'www.' + url

    #Find redirect (use cache if already seen)
    cached_redir = cursor.execute(redir_check_str, {'url':url}).fetchall()
    if len(cached_redir) == 1:
        eff_url = cached_redir[0][1]
    else:
        eff_url, success = follow_redirects(url)
        cursor.execute(redir_insert_str,
            {'url':url, 'eff_url':eff_url, 'success':success})
        connection.commit()
    if test:
        if eff_url is not None:
            print('\'---> Redirected url: ' + str(url))
        else:
            print('No response from remote server, using original url: ' + url)

    #Find ip addresses associated with domain (use cache if already seen)
    domain = re.search('(?:/+|^)([\w\.]*?)(?=/|$)', eff_url).groups()[0]
    if len(re.findall('\.', eff_url)) == 1:
        domain = 'www.' + domain
    cached_result = \
        cursor.execute(dom_check_str, {'dom':domain}).fetchall()
    if len(cached_result) > 0:
        ip_cache = [ip for (dom, ip) in cached_result]
        connection.close()
        return (domain, ip_cache)
    else:
        ip_result = dns_query.dns_lookup(domain).answer
        if ip_result == []:
            ip_result = [None]
        for ip in ip_result:
            cursor.execute(dom_insert_str, {'dom':domain, 'ip':ip})
        connection.commit()
        connection.close()
        return (domain, ip_result)


def ip_whois(ips, max_attempts = 3, test = False):
    '''
    Takes in domains mapped to ip addresses, and generates a dict of dicts, where
    the top level key is an ip adress, and each subkey is an entry in the whois
    lookup for that ip address.

    Input:
        ip map: (dict) dict of domains and associated IP addresses
    
    Output:
        (dict) with (domain, ip) as key and whois information as key:value pairs
    '''
    whois_rv = []
    attempts = 0
    if ips != [None]:
        for ip in ips:
            while attempts < max_attempts:
                whois_stdout = subprocess.check_output(\
                    'whois -h whois.arin.net "n + ' + str(ip) + '"', shell=True)
                whois_parsed = parse_lines(whois_stdout)
                if whois_parsed is not None:
                    if test:
                        print('\tWhoIs lookup using ip address: ' + ip)
                        for key in ['OrgName', 'Country', 'StateProv', 'City']:
                            print('\t\t'+ key + " : " + whois_parsed[key])
                    break
                else:
                    attempts += 1
            whois_rv.append((ip, whois_parsed))
    else:
         whois_rv.append((None, None))
    return whois_rv


def parse_lines(whois_stdout):
    '''
    Takes in whois as a block of text and returns a dict with whois info 
    as key:value pairs

    Input:
        whois_stdout: (str) stdout from whois call in bash
    
    Output:
        (dict) dict of whois key:value pairs
    '''
    line_dict = {}
    for line in str(whois_stdout).split('\\n'):
        if re.search(':', line) and not re.match('#', line):
            field = re.search('.*?(?=:)', line).group()
            val = re.search('(?<=:)(?:\s*)(.*)', line).groups()[0]
            if not re.match('Comment:', field):
                line_dict[field] = val
    if line_dict == {}:
        return None
    return line_dict

def follow_redirects(url, max_attempts = 3, timeout_len = 3):
    '''
    Takes in a curl and uses cURL to follow it through any potential redirects
    to arrive at the actual linked url. 

    Input:
        url: (str) a url to follow redirects
    
    Output:
        (str) the effective url after all redirects
    '''
    attempts = 0
    success = False
    while attempts < max_attempts:
        try:
            timeout = datetime.timedelta(seconds = timeout_len)
            curl_pointer = pycurl.Curl()
            curl_pointer.setopt(curl_pointer.URL, url)
            curl_pointer.setopt(curl_pointer.CAINFO, certifi.where())
            curl_pointer.setopt(curl_pointer.FOLLOWLOCATION, True)
            #To prevent printing and saving of things we aren't interested in
            curl_pointer.setopt(curl_pointer.WRITEFUNCTION, lambda x: None)
            curl_pointer.setopt(curl_pointer.NOPROGRESS, False)
            start_time = datetime.datetime.now()
            curl_pointer.setopt(curl_pointer.XFERINFOFUNCTION,
                lambda dl_t, dl_d, up_t, up_d: 
                curl_progress(dl_t, dl_d, up_t, up_d, start_time, timeout))
            curl_pointer.perform()
            redirected = curl_pointer.getinfo(curl_pointer.EFFECTIVE_URL)
            success = True
            curl_pointer.close()
            break
        except:
            print('Redirect failed (timeout ' + str(timeout_len) +' s), making '
                + str(max_attempts - attempts) + ' more attempts.')
            attempts += 1
            redirected = url
            continue
    return (redirected, success)

def curl_progress(download_t, download_d, upload_t, upload_d, start_time, timeout):
    '''
    Internal function to curl.execute() - this is called periodically throughout
    a cURL call to provide feedback to the user, however it is used in 
    follow_redirects() to return -1 (which stops the pyCurl call) if the length
    of time running exceeds the timeout. This is preferable to the internal 
    cURL timeout mechanism because it doesn't rely on the server-side timeout
    response, and thus will stop if the server is totally unresponsive.

    Inputs:
        download_t, download_d, upload_t, upload_d: cURL response information
        start_time: (DateTime) time when cURL call began
        timeout: (DateTimeDelta) time to wait for a reponse before stopping

    Output:
        -1, if timeout length is reach, None otherwise.
    '''
    run_time = datetime.datetime.now()
    if run_time - start_time > timeout:
        return -1

def distill_url(url):
    '''
    Removes anchors and transfer protocols from URLs for matching purposes.

    Input:
        url: (str) url to distill

    Output:
        (str) url without anchor or transfer protocols
    '''
    distilled = re.sub('#.*', '', url)
    distilled = re.sub('\/$', '', distilled)
    return re.sub('https?://', '', distilled)