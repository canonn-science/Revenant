#!/bin/env python3
import pymysql
from os import getenv
from pymysql.err import OperationalError
import httplib2

import httplib2
import os
import requests
import json
from pathlib import Path
from datetime import datetime
from revenant import write_sheet

CAPI="https://api.canonn.tech"

GRAPHQL="https://api.canonn.tech/graphql"

def get_db_secrets():
    dir=os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dir,'canonn_db_secret.json')) as json_file:
       data = json.load(json_file)
       return data


secret=get_db_secrets()

mysql_conn= pymysql.connect(host=secret.get("DB_HOST"),
                             user=secret.get("DB_USER"),
                             password=secret.get("DB_PASSWORD"),
                             db=secret.get("DB_NAME"))



def __get_cursor():
    """
    Helper function to get a cursor
      PyMySQL does NOT automatically reconnect,
      so we must reconnect explicitly using ping()
    """
    try:
        return mysql_conn.cursor()
    except OperationalError:
        mysql_conn.ping(reconnect=True)
        return mysql_conn.cursor()


def get_daily_cmdr_stats():

    cursor =  mysql_conn.cursor(pymysql.cursors.Cursor)
    sql = """
    select date_format(day,'%%d-%%m-%%y') as dday,cmdrs from v_cmdrs_per_day order by day desc limit 30 
       """
    cursor.execute(sql, ())
    return cursor.fetchall()

def get_monthly_cmdr_stats():

    cursor =  mysql_conn.cursor(pymysql.cursors.Cursor)
    sql = """
    select date_format(month,'%%M-%%y') as fmonth,cmdrs from v_cmdrs_per_month order by month desc 
       """
    cursor.execute(sql, ())
    return cursor.fetchall()

def get_find_stats(type):

    cursor =  mysql_conn.cursor(pymysql.cursors.Cursor)
    sql = """
select date_format(signals.first_report,'%%M %%y') as date,count(distinct system,body,index_id) as finds from (
select min(reported_at) as first_report,system,body,index_id 
from codexreport 
where entryid in (select entryid from codex_name_ref where hud_category = %s )  
and index_id is not null
group by system,body,index_id
) signals 
group by date_format(signals.first_report,'%%M %%y') order by signals.first_report desc 
       """

    cursor.execute(sql, (type))
    return cursor.fetchall()


def journal_exists(jlist,journal_id):
    if journal_id in list(jlist.keys()):
        return True
    else:
        return False

def fetch_regions():
    url=f"{CAPI}/regions"
    r=requests.get(url) 
    if r.status_code == requests.codes.ok:
        return r.json()
    else:
        print("failed to get regions")
        quit()

REGIONS=fetch_regions()

def fetch_capi(site):
    retval=[]
    page=0
    while True:
        url=f"{CAPI}/{site}?_start={page}"
        r=requests.get(url) 
        page+=100
        
        if r.status_code == requests.codes.ok:
            j=r.json()
            print(f"length j = {len(j)} page ={page}")
            if j:
                retval.extend(r.json())
            else:
                break;
            if len(j) < 100:
                break

    return retval


STATS_SHEET="1n-595TNhjhSqtZEobDVL675ogpq9xv7uoe-89oIQyHI"

write_sheet(STATS_SHEET,'Cmdrs Per Day!A2:B',get_daily_cmdr_stats())
write_sheet(STATS_SHEET,'Cmdrs Per Month!A2:B',get_monthly_cmdr_stats())
write_sheet(STATS_SHEET,'Biological Finds!A2:B',get_find_stats('Biology'))
write_sheet(STATS_SHEET,'Geological Finds!A2:B',get_find_stats('Geology'))
write_sheet(STATS_SHEET,'Guardian Finds!A2:B',get_find_stats('Guardian'))
write_sheet(STATS_SHEET,'Thargoid Finds!A2:B',get_find_stats('Thargoid'))

