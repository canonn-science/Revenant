#!/bin/env python3
import gzip
import json
import os
import pymysql
import sys
import locale
from pathlib import Path
from pymysql.err import OperationalError
from apiclient.http import MediaFileUpload
from google.oauth2 import service_account
from googleapiclient import discovery
from EliteDangerousRegionMap.RegionMapData import regions
from googleapiclient import discovery
from google.oauth2 import service_account
import os

sys.path.append('EliteDangerousRegionMap')

# a hack to stop vscode studio reformatting
if True:
    from RegionMap import findRegion

from EliteDangerousRegionMap.RegionMap import findRegion

def findRegion64(id):
    id64 = int(id)
    masscode = id64 & 7
    z = (((id64 >> 3) & (0x3FFF >> masscode)) << masscode) * 10 - 24105
    y = (((id64 >> (17 - masscode)) & (0x1FFF >> masscode)) << masscode) * 10 - 40985
    x = (((id64 >> (30 - masscode * 2)) & (0x3FFF >> masscode))
         << masscode) * 10 - 49985
    try:
        return findRegion(x, y, z)
    except:
        return 0, 'Unknown'



def write_sheet(spreadsheet_id,range_name,cells):
    name,dummy=range_name.split('!')
    print(f"Writing to sheet {name}")
    try:
        scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets"]
        secret_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),'client_secret.json')

        #for r,row in enumerate(cells):
        #    for c,cell in enumerate(row):
        #        if cells[r][c] is None:
        #           cells[r][c] = ''


        credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
        service = discovery.build('sheets', 'v4', credentials=credentials)


        values=cells


        data = {
            'values' : values
        }

        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, body=data, range=range_name, valueInputOption='USER_ENTERED').execute()

    except OSError as e:
        print(e)


def get_db_secrets():
    dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dir, 'canonn_db_secret.json')) as json_file:
        data = json.load(json_file)
        return data


secret = get_db_secrets()

mysql_conn = pymysql.connect(host=secret.get("DB_HOST"),
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

def get_data():
    cursor =  mysql_conn.cursor()
    sql = """
        select
        distinct 
            systemname,
            bodyname,
            cmdrName,
            cast(x as char) x,cast(y as char) y,cast(z as char)z,
            cast(lat as char) lat,cast(lon as char) lon,
            replace(raw_event->"$.NearestDestination",'"','') as name,
            replace(raw_event->"$.NearestDestination_Localised",'"','') as name_localised,
            replace(raw_event->"$.SystemAddress",'"','') as id64
            from raw_events where 
                raw_event->"$.NearestDestination" = "$POIScene_Wreckage_UA;" or 
                raw_event->"$.NearestDestination_Localised" = "Nonhuman Signature"
       """
    cursor.execute(sql, ())
    data = cursor.fetchall()
   # return data
    newdata=[]
    for d in data:
        print(d[10])
        region_id,region_name=findRegion64(d[10])
        print(f"{region_id} {region_name}")
        row=[]
        row.append(region_name)
        row.extend(list(d))
        newdata.append(row)
    
    return newdata


data=[]
sheetdata = []
sheetdata.append([
    "Region",
    "System",
    "Body",
    "Cmdr",
    "x",
    "y",
    "z",
    "Lat",
    "Lon",
    "name",
    "name_localised",
    "id64"
])
data=get_data()
sheetdata.extend(data)
SHEET = "1EmQhVIFpIxXsKpTMTTB9IkDp1zmDQgOyFLtvqTknFk8"
write_sheet(SHEET, f"Locations!A1:Z", sheetdata)


