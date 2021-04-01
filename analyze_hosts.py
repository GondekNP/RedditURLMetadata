'''
This file will be used to analyze which companies host the urls posted on subreddits
'''

import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def extract_data(subreddit):
    '''
    A Function to extract relevant information from analysis.sql database

    Inputs:
        subreddit: (str) the name of the subreddit (without /r) which will be used for analysis
    Returns: (pd DataFrame) a dataframe of the data extracted from the database
    '''
    connection = sqlite3.connect('data/analysis.sql')
    cursor = connection.cursor()
    command = '''SELECT SUM(a.ip_weight) AS weighted, b.subreddit, a.domain, a.org_name, a.state_prov
                 FROM analysis_ips AS a INNER JOIN analysis_urls AS b ON a.url_id = b.url_id
                 WHERE b.subreddit = ? GROUP BY b.subreddit, a.domain, a.org_name, a.state_prov'''
    filter = [subreddit]
    cursor.execute(command, filter)
    table = cursor.fetchall()
    df = pd.DataFrame(table)
    df.columns = ['Weighted', 'Subreddit', 'Domain', 'Company', 'State-Province']
    return df


def clean_data(df, grouping='Company'):
    '''
    A function to clean the data extracted to SQL - groups and creates 'Other' category

    Inputs:
        df: (pd Dataframe) a dataframe of the data to be cleaned
        grouping: (str) the attribute to group by, defaults to hosting company but user can
            also input 'Domain' or 'State/prov'
    Returns: (pd Dataframe) a dataframe with the data grouped into appropriate categories
    '''
    data = df.groupby(grouping)["Weighted"].sum()
    cutoff = sum(data.values) * 0.05
    filt = (data.values > cutoff) & (data.index != "")
    filtered_df = data[filt]
    #filtered_df.drop(labels="", inplace=True)
    other = pd.Series([sum(data.values) - sum(filtered_df.values)], index=["Other"])
    return filtered_df.append(other)


def build_chart(quar_df, quar_subreddit, compare_df, compare_subreddit, grouping='Company'):
    '''
    Builds 2 piecharts comparing the relative company share of URL hosting for two different subreddits
    then saves the file to the /output directory

    Inputs:
        quar_df: (pd DataFrame) a DataFrame with the data already grouped into relevant categories
        quar_subreddit: (str) The quarantined subreddit name, without r/, being analyzed
        compare_df: (pd DataFrame) a DataFrame with the data already grouped into relevant categories
        compare_subreddit: (str) The non-quarantined subreddit that will be compared against
        grouping: (str) The category the data is grouped by. Defaults to Company but user can also select
            Domain or State/Province
    '''
    _, axes = plt.subplots(1, 2, figsize=[10,6])

    for i, ax in enumerate(axes.flatten()):
        if i == 0:
            data = [quar_df, quar_df.keys(), quar_subreddit]
        else:
            data = [compare_df, compare_df.keys(), compare_subreddit]
        ax.pie(x=data[0], autopct="%.1f%%", explode=[0.05]*len(data[0]), labels=data[1], pctdistance=0.5)
        ax.set_title(f"r/{data[2]}", fontsize=10)
    plt.suptitle(f'Comparison of URLs Shared on r/{quar_subreddit} (Quarantined) and r/{compare_subreddit} by Hosting {grouping}')
    plt.tight_layout()
    plt.savefig(f"output/{quar_subreddit}_{compare_subreddit}_{grouping}.png")


def go(quar_subreddit, compare_subreddit, grouping="Company"):
    '''
    A function that combines functions in the file to create a pie chart of host information and save
    in the directory.

    Inputs:
        subreddit: (str) The subreddit name, without r/, being analyzed
        grouping: (str) The category the data is grouped by. Defaults to Company but user can also select
            Domain or State/Province 
    '''
    df = extract_data(quar_subreddit)
    df_2 = extract_data(compare_subreddit)
    cleaned_data = clean_data(df, grouping)
    cleaned_2 = clean_data(df_2, grouping)
    return build_chart(cleaned_data, quar_subreddit, cleaned_2, compare_subreddit, grouping)