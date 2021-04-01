'''
This file provides a way to interact with our program
'''

import sys
import os
import scraper
import combine
import sqlite3
import pprint
import analyze_hosts as hosts
import analyze_spread as spread

START = 1
QUARANTINE_DICT = {1 : "TheRedPill", 2: "The_Donald", 3: "FULLCOMMUNISM", 4:"watchpeopledie"}
COMPARE_DICT = {1 : "politics", 2: "Conservative", 3: "socialism"}
GROUPING_DICT = {1 : "Company", 2: "State-Province", 3: "Domain"}
SPREAD_DICT = {1 : 'politics', 2 : 'Conservative', 3 :'AskThe_Donald', 4 : 'HillaryForPrison',
                5 : 'AskTrumpSupporters'}

TOP_MENU = '''
********* Quarantined Subreddit Analysis *********
Welcome to the quarantined subreddit analysis application! Please
choose an option to perform a task.

(1) Test data pipeline
(2) Analyze the companies that host URLs found on quarantined subreddits
(3) Analyze the spread of URLs from quarantined subreddits
(4) Quit the program
'''

QUARANTINE_MENU = '''
********* Quarantined Subreddit Analysis *********
Please choose a quarantined subreddit to analyze.

(1) r/TheRedPill
(2) r/The_Donald
(3) r/FULLCOMMUNISM
(4) r/watchpeopledie
'''

COMPARE_MENU = '''
********* Quarantined Subreddit Analysis *********
Please choose a similar subreddit to compare against r/The_Donald

(1) r/politics
(2) r/Conservative
(3) r/socialism
'''

GROUPING_MENU = '''
********* Quarantined Subreddit Analysis *********
Please choose a quarantined subreddit to analyze.

(1) Hosting Company
(2) Hosting State/Province
(3) Domain
'''

SPREAD_MENU = '''
********* Quarantined Subreddit Analysis *********
Please choose a similar subreddit to compare against r/The_Donald

(1) r/politics
(2) r/Conservative
(3) r/AskThe_Donald
(4) r/HillaryForPrison
(5) r/AskTrumpSupporters
'''


def retrieve(menu, end_length):
    '''
    A function to interact with the various menus and return the options that
    the user selects.

    Inputs:
        menu: (str) a long string listing out the options a user can select
        end_length: (int) an integer specifying the maximum option a user can select
    
    Returns: (int) the integer signifying the option the user chose
    '''
    option = -1
    while True:
        print(menu)
        option = int(input("Option: "))
        if option >= START and option <= end_length:
            break
        else:
            print(f"Invalid option({option})")
    return option


def main():
    '''
    Main interaction function users will call to access options
    '''
    while True:
        option = retrieve(TOP_MENU, 4)
        if option == 4:
            break
        elif option == 2:
            choice = retrieve(QUARANTINE_MENU, len(QUARANTINE_DICT))
            reddit_choice = QUARANTINE_DICT[choice]
            compare = retrieve(COMPARE_MENU, len(COMPARE_DICT))
            compare_choice = COMPARE_DICT[compare]
            group = retrieve(GROUPING_MENU, len(GROUPING_DICT))
            group_choice = GROUPING_DICT[group]
            print("Preparing the visualization...")
            hosts.go(reddit_choice, compare_choice, group_choice)
            os.system(f'xdg-open output/{reddit_choice}_{compare_choice}_{group_choice}.png')
        elif option == 3:
            choice_2 = retrieve(SPREAD_MENU, len(SPREAD_DICT))
            spread_choice = SPREAD_DICT[choice_2]
            print("Preparing the visualization...")
            spread.go('The_Donald', spread_choice, "2019-06-19")
            os.system(f'xdg-open output/The_Donald_{spread_choice}.png')
        else:
            print("Scraping the past ~12 weeks of r/uchicago")
            scraper.go(mode='test')
            os.system('clear')
            print("Running the URLs found on r/uchicago through WhoIs Lookup")
            combine.go(['uchicago'], test = True)
            print('Scrape complete!\nAbove are the first 3 url results ' +
                    'check the data/analysis.sql database to view all the data' +
                    'scraped from r/uchicago and all subreddits in our analysis.')

if __name__== "__main__":
    main()