#!/bin/env python3


import httplib2
import locale
import os
import requests
import json
import pymysql
from pathlib import Path
from datetime import datetime
from revenant import write_sheet
from apiclient.http import MediaFileUpload
from google.oauth2 import service_account
from googleapiclient import discovery
import sys
sys.path.append('EliteDangerousRegionMap')

if True:
    from RegionMap import findRegion

HOME = str(Path.home())

CAPI = "https://api.canonn.tech"

GRAPHQL = "https://api.canonn.tech/graphql"


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


scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file",
          "https://www.googleapis.com/auth/spreadsheets"]
secret_file = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), 'client_secret.json')
credentials = service_account.Credentials.from_service_account_file(
    secret_file, scopes=scopes)
drive = discovery.build('drive', 'v3', credentials=credentials)


def get_patrol():
    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT system,cast(x as char) x,cast(y as char) y , cast(z as char) z, instructions,url from v_guardian_tours"
    cursor.execute(sql, ())
    patrol = cursor.fetchall()
    with open('guardian_tour.json', 'w') as outfile:
        json.dump(patrol, outfile)
    return len(patrol)


def fetch_regions():
    url = f"{CAPI}/regions"
    r = requests.get(url)
    if r.status_code == requests.codes.ok:
        return r.json()
    else:
        print("failed to get regions")
        quit()


REGIONS = fetch_regions()


def fetch_capi(site):
    retval = []
    page = 0
    while True:
        url = f"{CAPI}/{site}?_start={page}"
        r = requests.get(url)
        page += 100

        if r.status_code == requests.codes.ok:
            j = r.json()
            print(f"length j = {len(j)} page ={page}")
            if j:
                retval.extend(r.json())
            else:
                break
            if len(j) < 100:
                break

    return retval


def fetch_beacons():
    retval = []
    page = 0
    while True:
        data = "query{ gbsites (start: " + str(page) + " limit: 100){ system { systemName primaryStar edsmCoordX edsmCoordY edsmCoordZ id64   } body { bodyName subType distanceToArrival } siteID gssite { system { systemName } body { bodyName subType } siteID latitude longitude discoveredBy { cmdrName } } gbmessage { messageSystem { systemName } messageBody { bodyName } } discoveredBy { cmdrName } } } "

        r = requests.post(GRAPHQL, json={'query': data})
        page += 100

        if r.status_code == requests.codes.ok:
            j = r.json()
            beacons = j.get("data").get("gbsites")
            print(f"length beacons = {len(beacons)} page ={page}")
            if beacons:
                retval.extend(beacons)
            else:
                break
            if len(beacons) < 100:
                break
        else:
            print("error connecting")
            print(r.status_code)
            print(r.text)

    return retval


def get_grsites():
    retval = []
    retval.append([
        "SiteId",
        "Site Type",
        "System Name",
        "x", "y", "z",
        "Region (Calculated)",
        "Region (Journal)",
        "Primary Star",
        "Body Name",
        "POI Id",
        "Body Sub Type",
        "Gravity",
        "Distance To Arrival",
        "Surface Temperature",
        "Volcanism",
        "Orbital Period",
        "Rotational Period",
        "Orbital Eccentricity",
        "Discovered By"
    ])
    capi = fetch_capi("grsites")
    for row in capi:
        # print(row)
        cols = []
        cols.append(row.get("id"))
        cols.append(row.get("type").get("type"))
        cols.append(row.get("system").get("systemName"))
        cols.append(row.get("system").get("edsmCoordX"))
        cols.append(row.get("system").get("edsmCoordY"))
        cols.append(row.get("system").get("edsmCoordZ"))
        id64 = int(row.get("system").get("id64"))
        masscode = id64 & 7
        z = (((id64 >> 3) & (0x3FFF >> masscode)) << masscode) * 10 - 24105
        y = (((id64 >> (17 - masscode)) & (0x1FFF >> masscode))
             << masscode) * 10 - 40985
        x = (((id64 >> (30 - masscode * 2)) & (0x3FFF >> masscode))
             << masscode) * 10 - 49985
        rid, region = findRegion(x, y, z)
        cols.append(str(region))

        region_id = row.get("system").get("region")
        if region_id:
            region = REGIONS[region_id-1].get("name")
        else:
            region = "Unknown"
        cols.append(region)
        cols.append(row.get("system").get("primaryStar").get("type"))
        cols.append(str(row.get("body").get("bodyName").replace(
            row.get("system").get("systemName"), '')))
        cols.append(row.get("frontierId"))
        cols.append(row.get("body").get("subType"))
        cols.append(row.get("body").get("gravity"))
        cols.append(row.get("body").get("distanceToArrival"))
        cols.append(row.get("body").get("surfaceTemperature"))
        cols.append(row.get("body").get("volcanismType"))
        cols.append(row.get("body").get("orbitalPeriod"))
        cols.append(row.get("body").get("rotationalPeriod"))
        cols.append(row.get("body").get("orbitalEccentricity"))
        cols.append(row.get("discoveredBy").get("cmdrName"))
        retval.append(cols)
        # print(json.dumps(col,indent=4))
    return retval


def get_gbsites():
    retval = []
    retval.append([
        "SiteId",
        "System Name",
        "x", "y", "z",
        "Region",
        "Primary Star",
        "Body Name",
        "Body Sub Type",
        "Distance To Arrival",
        "Guardian Structure System",
        "Guardian Structure Body",
        "Discovered By"
    ])
    data = fetch_beacons()
    for row in data:
        # print(row)
        cols = []
        cols.append(row.get("siteID"))
        cols.append(row.get("system").get("systemName"))
        cols.append(row.get("system").get("edsmCoordX"))
        cols.append(row.get("system").get("edsmCoordY"))
        cols.append(row.get("system").get("edsmCoordZ"))

        id64 = int(row.get("system").get("id64"))
        masscode = id64 & 7
        z = (((id64 >> 3) & (0x3FFF >> masscode)) << masscode) * 10 - 24105
        y = (((id64 >> (17 - masscode)) & (0x1FFF >> masscode))
             << masscode) * 10 - 40985
        x = (((id64 >> (30 - masscode * 2)) & (0x3FFF >> masscode))
             << masscode) * 10 - 49985
        rid, region = findRegion(x, y, z)
        cols.append(str(region))

        cols.append(row.get("system").get("primaryStar").get("type"))
        cols.append(row.get("body").get("bodyName").replace(
            row.get("system").get("systemName"), ''))
        cols.append(row.get("body").get("subType"))
        cols.append(row.get("body").get("distanceToArrival"))
        cols.append(row.get("gssite").get("system").get("systemName"))
        cols.append(row.get("gssite").get("body").get("bodyName"))

        cols.append(row.get("discoveredBy").get("cmdrName"))
        retval.append(cols)
        # print(json.dumps(col,indent=4))
    return retval


def get_grreports():
    retval = []
    retval.append([
        "Report Id",
        "Created",
        "Region (Journal)",
        "System Name",
        "Body Name",
        "Type",
        "POI Id",
        "Latitude",
        "Longitude",
        "Discovered By",
        "Comment"
    ])
    capi = fetch_capi("grreports")
    for row in capi:
        # print(row)
        cols = []
        cols.append(row.get("id"))
        cols.append(row.get("created_at"))

        region_id = row.get("regionID")
        if region_id is not None:
            region = REGIONS[region_id-1].get("name")
        else:
            region = "Unknown"
        cols.append(region)

        cols.append(row.get("systemName"))

        cols.append(row.get("bodyName").replace(row.get("systemName"), ''))
        cols.append(row.get("type"))
        cols.append(row.get("frontierID"))
        cols.append(row.get("latitude"))
        cols.append(row.get("longitude"))
        cols.append(row.get("cmdrName"))
        if not row.get("cmdrComment"):
            comment = row.get("reportComment")
        else:
            comment = row.get("cmdrComment")

        cols.append(comment)

        retval.append(cols)
        # print(json.dumps(col,indent=4))
    return retval


def get_gsreports():
    retval = []
    retval.append([
        "Report Id",
        "Created",
        "System Name",
        "Body Name",
        "Type",
        "POI Id",
        "Latitude",
        "Longitude",
        "Discovered By",
        "Comment"
    ])
    capi = fetch_capi("gsreports")
    for row in capi:
        # print(row)
        cols = []
        cols.append(row.get("id"))
        cols.append(row.get("created_at"))

        cols.append(row.get("systemName"))
        cols.append(row.get("bodyName").replace(row.get("systemName"), ''))
        cols.append(row.get("type"))
        cols.append(row.get("frontierID"))
        cols.append(row.get("latitude"))
        cols.append(row.get("longitude"))
        cols.append(row.get("cmdrName"))
        if not row.get("cmdrComment"):
            comment = row.get("reportComment")
        else:
            comment = row.get("cmdrComment")

        retval.append(cols)
        # print(json.dumps(col,indent=4))
    return retval


def get_gbreports():
    retval = []
    retval.append([
        "Created",
        "Region",
        "System Name",
        "Body Name",
        "Discovered By",
        "Comment"
    ])
    capi = fetch_capi("gbreports")
    for row in capi:
        # print(row)
        cols = []
        cols.append(row.get("created_at"))

        region_id = row.get("regionID")
        if region_id is not None:
            region = REGIONS[region_id-1].get("name")
        else:
            region = "Unknown"
        cols.append(region)
        cols.append(row.get("systemName"))
        cols.append(row.get("bodyName"))
        cols.append(row.get("cmdrName"))
        cols.append(row.get("reportComment"))
        cols.append(row.get("reportComment"))

        retval.append(cols)
        # print(json.dumps(col,indent=4))
    return retval


def get_gssites():
    retval = []
    retval.append([
        "SiteId",
        "Site Type",
        "System Name",
        "x", "y", "z",
        "Region (Calculated)",
        "Region (Journal)",
        "Primary Star",
        "Body Name",
        "Journal Name",
        "POI Number",
        "Body Sub Type",
        "Gravity",
        "Distance To Arrival",
        "Surface Temperature",
        "Volcanism",
        "Orbital Period",
        "Rotational Period",
        "Orbital Eccentricity",
        "Discovered By"
    ])
    capi = fetch_capi("gssites")
    for row in capi:
        # print(row)
        cols = []
        cols.append(row.get("id"))
        cols.append(row.get("type").get("type"))
        cols.append(row.get("system").get("systemName"))
        cols.append(row.get("system").get("edsmCoordX"))
        cols.append(row.get("system").get("edsmCoordY"))
        cols.append(row.get("system").get("edsmCoordZ"))
        id64 = int(row.get("system").get("id64"))
        masscode = id64 & 7
        z = (((id64 >> 3) & (0x3FFF >> masscode)) << masscode) * 10 - 24105
        y = (((id64 >> (17 - masscode)) & (0x1FFF >> masscode))
             << masscode) * 10 - 40985
        x = (((id64 >> (30 - masscode * 2)) & (0x3FFF >> masscode))
             << masscode) * 10 - 49985
        rid, region = findRegion(x, y, z)
        cols.append(str(region))

        region_id = row.get("system").get("region")
        if region_id:
            region = REGIONS[region_id-1].get("name")
        else:
            region = "Unknown"
        cols.append(region)
        cols.append(row.get("system").get("primaryStar").get("type"))
        cols.append(row.get("body").get("bodyName").replace(
            row.get("system").get("systemName"), ''))
        cols.append(row.get("type").get("journalName"))
        cols.append(row.get("frontierID"))
        cols.append(row.get("body").get("subType"))
        cols.append(row.get("body").get("gravity"))
        cols.append(row.get("body").get("distanceToArrival"))
        cols.append(row.get("body").get("surfaceTemperature"))
        cols.append(row.get("body").get("volcanismType"))
        cols.append(row.get("body").get("orbitalPeriod"))
        cols.append(row.get("body").get("rotationalPeriod"))
        cols.append(row.get("body").get("orbitalEccentricity"))
        cols.append(row.get("discoveredBy").get("cmdrName"))
        retval.append(cols)
        # print(json.dumps(col,indent=4))
    return retval


GUARDIAN_SHEET = '1p20iT3HWAcRRJ8Cw60Z2tCVTpcBavhycvE0Jgg0h32Y'


def write_file(id, name, description, count):
    fcount = locale.format_string("%d", count, grouping=True)
    full_desc = f"""{description}

{fcount} patrols
    """
    file_metadata = {"title": name, "description": full_desc}

    media = MediaFileUpload(name, mimetype='application/json')
    file = drive.files().update(body=file_metadata, media_body=media,
                                fields='id', fileId=id).execute()
    print('Upload {} File ID: {}'.format(name, file.get('id')))


patrol_file = "1m8q9lE4_cAI8CotM-oaEm5RWHeeJjoil"
write_file(patrol_file, "guardian_tour.json",
           "Guardian POI Tour", get_patrol())


cells = []
cells.append([str(datetime.now().isoformat(timespec='minutes'))])
write_sheet(GUARDIAN_SHEET, 'Beacon Reports!A1:Z', get_gbreports())
write_sheet(GUARDIAN_SHEET, 'Guardian Beacons!A1:Z', get_gbsites())
write_sheet(GUARDIAN_SHEET, 'Guardian Ruins!A1:Z', get_grsites())
write_sheet(GUARDIAN_SHEET, 'Guardian Structures!A1:Z', get_gssites())
write_sheet(GUARDIAN_SHEET, 'Ruin Reports!A1:Z', get_grreports())
write_sheet(GUARDIAN_SHEET, 'Structure Reports!A1:Z', get_gsreports())
write_sheet(GUARDIAN_SHEET, 'Header!B3', cells)
