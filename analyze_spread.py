'''
This program will be used to analyze the spread of urls between related subreddits
'''

import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as dt


def extract_data(subreddit):
    '''
    A Function to extract relevant information from analysis.sql database

    Inputs:
        subreddit: (str) the name of the subreddit (without /r) which will be used for analysis

    Returns: (pd DataFrame) a dataframe of the data extracted from the database
    '''
    connection = sqlite3.connect('data/analysis.sql')
    cursor = connection.cursor()
    command = '''SELECT url_text AS url, domain, post_date FROM analysis_urls WHERE subreddit = ?'''
    cursor.execute(command, [subreddit])
    table = cursor.fetchall()
    df = pd.DataFrame(table)
    df.columns = ['url', 'domain', 'post_date']
    df['post_date'] = pd.to_datetime(df['post_date'], unit='s')
    return df


def build_comparison_dict(subreddit_df, window_start, window_end):
    '''
    Build a dictionary from the similar subreddit data to compare for spread

    Inputs:
        subreddit_df: (pd DataFrame) the DataFrame extracted for the comparison subreddit
        window_start: (dt DateTime) the start date for the comparison period
        window_end: (dt DateTime) the end date for the comparison period

    Returns: (dict) Keys are each domain found on the comparison subreddit during the comparison
        period and values are lists of the urls associated with that domain
    '''
    comparison_dict = {}
    filt = (subreddit_df['post_date'] >= window_start) & (subreddit_df['post_date'] <= window_end)
    filtered_df = subreddit_df[filt]
    for row in filtered_df.itertuples():
        _, url, domain, _ = row
        if domain not in comparison_dict:
            comparison_dict[domain] = []
        comparison_dict[domain].append(url)
    return comparison_dict


def calc_daily_spread(main_subreddit, compare_subreddit, start_date, end_date):
    '''
    A function to calculate the percentage of URLs that 'spillover' to an adjacent subreddit during
    the analysis period.

    Inputs:
        main_subreddit: (str) The main subreddit of interest - should be a quarantined subreddit
        compare_subreddit: (str) The adjacent subreddit against which we can compare spread
        start_date: (dt DateTime) The start date of the analysis period, defined as 6 weeks before the
            quarantine date
        end_date: (dt DateTime) The end date of the analysis period, defined as 6 weeks before the
            quarantine date
    
    Returns: (dict) A dictionary whose keys are each day in the analysis period and whose values are
        the daily rates of spread on the comparison url
    '''
    current_day = start_date
    add_day = dt.timedelta(days=1)
    add_three_days = dt.timedelta(days=3)
    daily_dict = {}

    for i in range((end_date - start_date).days):
        daily_dict[current_day] = 0
        compare_dict = build_comparison_dict(compare_subreddit, current_day, current_day + add_three_days)
        filt = (main_subreddit['post_date'] > current_day - add_day) & (main_subreddit['post_date'] < current_day + add_day)
        filtered_df = main_subreddit[filt]
        for row in filtered_df.itertuples():
            _, url, domain, _ = row
            urls = compare_dict.get(domain, False)
            if type(urls) is not bool:
                if url in urls:
                    daily_dict[current_day] += 1
        if daily_dict[current_day] != 0:
            daily_dict[current_day] = daily_dict[current_day] / len(filtered_df)
        current_day = current_day + add_day
    
    return daily_dict


def build_line_chart(daily_dict, quar_date, main_subreddit, compare_subreddit):
    '''
    A function that builds a line chart of the analysis then saves it in the directory

    Inputs:
        daily_dict: (dict) A dictionary whose keys are days and values are spread to the adjacent subreddit
        quar_date: (dt DateTime) the date the main subreddit was quarantined
        main_subreddit: (str) the name of the quarantined subreddit
        compare_subreddit: (str) the name of the adjacent subreddit used for comparison
    '''
    plt.clf()
    sns.set_palette("rocket")

    daily_df = pd.DataFrame.from_dict(daily_dict, orient="index")
    daily_df.columns = ['Before']
    daily_df.loc[daily_df.index > quar_date, 'After'] = daily_df['Before']
    daily_df.loc[daily_df.index > quar_date, 'Before'] = None
    lp = sns.lineplot(data=daily_df, legend=False, dashes=False)

    plt.title(f'Percentage of URLs from r/{main_subreddit} found on r/{compare_subreddit}')
    plt.xticks(rotation=15)
    plt.axvline(x=quar_date, linestyle="dashed", color='red', label="Quarantine Date")
    handles, _ = lp.get_legend_handles_labels()
    labels = ["Quarantine Date"]
    plt.legend(handles=handles[3:], labels=labels)
    fig = lp.get_figure()
    fig.savefig(f'output/{main_subreddit}_{compare_subreddit}.png')


def go(main_subreddit, compare_subreddit, quar_date):
    '''
    A function that combines the other functions in this file to create a line chart comparing
    URL spread from one subreddit to another

    Inputs:
        main_subreddit: (str)
        compare_subreddit: (str)
        quar_date: (str)
    '''
    main = extract_data(main_subreddit)
    compare = extract_data(compare_subreddit)

    dt_quar = dt.datetime.strptime(quar_date, '%Y-%m-%d')
    start_date = dt_quar - dt.timedelta(weeks=6)
    end_date = dt_quar + dt.timedelta(weeks=6)

    daily = calc_daily_spread(main, compare, start_date, end_date)
    build_line_chart(daily, dt_quar, main_subreddit, compare_subreddit)