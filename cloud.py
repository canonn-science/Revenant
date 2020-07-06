#!/bin/env python3
import json
import pymysql
import os
from os import getenv
from pymysql.err import OperationalError
import httplib2
import os
import requests
import json
from pathlib import Path
from datetime import datetime
from revenant import write_sheet


CAPI="https://api.canonn.tech"

GRAPHQL="https://api.canonn.tech/graphql"



def fetch_regions():
    url=f"{CAPI}/regions"
    r=requests.get(url)
    if r.status_code == requests.codes.ok:
        return r.json()
    else:
        print("failed to get regions")
        quit()

REGIONS=fetch_regions()


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

def get_cloud_data():
    system="Merope"
    
    cursor =  mysql_conn.cursor(pymysql.cursors.DictCursor)
    sql = """
        select distinct 
            system,
            x,y,z,
            replace(body,system,'') as body,
            c.entryid as entryid,
            replace(raw_json->"$.NearestDestination",'"','') as destination,
            unix_timestamp(reported_at) as timestamp,
            cnr.english_name as english_name,
            cnr.hud_category as hud_category,
            c.name as name,
            cmdrName,
            region_name,
            DATE_FORMAT(reported_at,'%%Y-%%m-%%d %%T') as reported_at
        from codexreport c 
        left join codex_name_ref cnr on cnr.entryid = c.entryid
        where cnr.hud_category in ('Cloud','Anomaly')
        and cmdrname != 'EDSM User'
        order by reported_at asc
       """
    cursor.execute(sql, ())
    return cursor.fetchall()
    cursor.clos()



def get_cloud_items():
    data=get_cloud_data()
    cloud_items={}

    for item in data:
        key="{}:{}:{}:{}:{}".format(item.get("system"),item.get("body"),item.get("entryid"),item.get("destination"),item.get("name"))
        if not cloud_items.get(key) or cloud_items.get(key).get("timestamp") > item.get("timestamp"):
           cloud_items[key]=item
        if not cloud_items.get("last_seen") or cloud_items.get(key).get("last_seen") < item.get("last_seen"):
            cloud_items[key]["last_seen"]=item.get("timestamp")
            cloud_items[key]["last_date"]=item.get("reported_at")
    return cloud_items

#def get_cloud_cells():
#   cloud_data=get_cloud_items()
cloud_data=get_cloud_items()
clouds=[]
contents=[]

for row in cloud_data.values():
    cols=[]
    cols.append(row.get("system"))
    cols.append(row.get("body"))
    cols.append(str(row.get("x")))
    cols.append(str(row.get("y")))
    cols.append(str(row.get("z")))
    cols.append(row.get("entryid"))
    cols.append(row.get("destination"))
    cols.append(row.get("english_name"))
    cols.append(row.get("hud_category"))
    cols.append(row.get("name"))
    cols.append(row.get("cmdrName"))
    region_id=row.get("region_name").split('_')[2].replace(';','')
    if region_id:
        region=REGIONS[int(region_id)-1].get("name")
    else:
        region="Unknown"
    cols.append(region)
    cols.append(row.get("reported_at"))
    cols.append(row.get("last_date"))
    if row.get("hud_category") == "Cloud" and 'Cloud' in row.get("english_name"):
        clouds.append(cols)
    else:
        contents.append(cols)

CLOUD_SHEET='11BCZRci0YlgW0sFdxvB_srq7ssxHzstMAiewhSGHE94'

write_sheet(CLOUD_SHEET,"'Lagrange Clouds'!A2:Z",clouds)
write_sheet(CLOUD_SHEET,"'Cloud Contents'!A2:Z",contents)

