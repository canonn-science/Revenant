#!/bin/env python3

import httplib2
import os
import requests
import json
from googleapiclient import discovery
from google.oauth2 import service_account
from pathlib import Path
from datetime import datetime

HOME = str(Path.home())

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
        
def fetch_beacons():
    retval=[]
    page=0
    while True:
        data="query{ gbsites (start: " + str(page) + " limit: 100){ system { systemName primaryStar edsmCoordX edsmCoordY edsmCoordZ} body { bodyName subType distanceToArrival } siteID gssite { system { systemName } body { bodyName subType } siteID latitude longitude discoveredBy { cmdrName } } gbmessage { messageSystem { systemName } messageBody { bodyName } } discoveredBy { cmdrName } } } "

        r=requests.post(GRAPHQL,json={'query': data}) 
        page+=100
        
        if r.status_code == requests.codes.ok:
            j=r.json()
            beacons=j.get("data").get("gbsites")
            print(f"length beacons = {len(beacons)} page ={page}")
            if beacons:
                retval.extend(beacons)
            else:
                break;
            if len(beacons) < 100:
                break
        else:
            print("error connecting")
            print(r.status_code)
            print(r.text)


    return retval

def get_grsites():
    retval=[]
    retval.append([
        "SiteId",
        "Site Type",
        "System Name",
        "x","y","z",
        "Region",
        "Primary Star",
        "Body Name",
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
    capi=fetch_capi("grsites")
    for row in capi:
       #print(row)
       cols=[]
       cols.append(row.get("id"))
       cols.append(row.get("type").get("type"))
       cols.append(row.get("system").get("systemName"))
       cols.append(row.get("system").get("edsmCoordX"))
       cols.append(row.get("system").get("edsmCoordY"))
       cols.append(row.get("system").get("edsmCoordZ"))
       region_id=row.get("system").get("region")
       if region_id:
           region=REGIONS[region_id-1].get("name")
       else:
           region="Unknown"
       cols.append(region)
       cols.append(row.get("system").get("primaryStar").get("type"))
       cols.append(str(row.get("body").get("bodyName").replace(row.get("system").get("systemName"),'')))
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
       #print(json.dumps(col,indent=4))
    return retval


def get_gbsites():
    retval=[]
    retval.append([
        "SiteId",
        "System Name",
        "x","y","z",
        "Primary Star",
        "Body Name",
        "Body Sub Type",
        "Distance To Arrival",
        "Guardian Structure System",
        "Guardian Structure Body",
        "Discovered By"
    ])
    data=fetch_beacons()
    for row in data:
       #print(row)
       cols=[]
       cols.append(row.get("siteID"))
       cols.append(row.get("system").get("systemName"))
       cols.append(row.get("system").get("edsmCoordX"))
       cols.append(row.get("system").get("edsmCoordY"))
       cols.append(row.get("system").get("edsmCoordZ"))
       cols.append(row.get("system").get("primaryStar").get("type"))
       cols.append(row.get("body").get("bodyName").replace(row.get("system").get("systemName"),''))
       cols.append(row.get("body").get("subType"))
       cols.append(row.get("body").get("distanceToArrival"))
       cols.append(row.get("gssite").get("system").get("systemName"))
       cols.append(row.get("gssite").get("body").get("bodyName"))

       cols.append(row.get("discoveredBy").get("cmdrName"))
       retval.append(cols)
       #print(json.dumps(col,indent=4))
    return retval
        

def get_grreports():
    retval=[]
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
    capi=fetch_capi("grreports")
    for row in capi:
       #print(row)
       cols=[]
       cols.append(row.get("id"))
       cols.append(row.get("created_at"))
       cols.append(row.get("systemName"))
       cols.append(row.get("bodyName").replace(row.get("systemName"),''))
       cols.append(row.get("type"))
       cols.append(row.get("frontierID"))
       cols.append(row.get("latitude"))
       cols.append(row.get("longitude"))
       cols.append(row.get("cmdrName"))
       cols.append(row.get("reportComment"))

       retval.append(cols)
       #print(json.dumps(col,indent=4))
    return retval
        

def get_gsreports():
    retval=[]
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
    capi=fetch_capi("gsreports")
    for row in capi:
       #print(row)
       cols=[]
       cols.append(row.get("id"))
       cols.append(row.get("created_at"))
       cols.append(row.get("systemName"))
       cols.append(row.get("bodyName").replace(row.get("systemName"),''))
       cols.append(row.get("type"))
       cols.append(row.get("frontierID"))
       cols.append(row.get("latitude"))
       cols.append(row.get("longitude"))
       cols.append(row.get("cmdrName"))
       cols.append(row.get("reportComment"))

       retval.append(cols)
       #print(json.dumps(col,indent=4))
    return retval

def get_gbreports():
    retval=[]
    retval.append([
        "Created",
        "System Name",
        "Discovered By",
        "Comment"
    ])
    capi=fetch_capi("gbreports")
    for row in capi:
       #print(row)
       cols=[]
       cols.append(row.get("created_at"))
       cols.append(row.get("systemName"))
       cols.append(row.get("cmdrName"))
       cols.append(row.get("reportComment"))

       retval.append(cols)
       #print(json.dumps(col,indent=4))
    return retval
        


def get_gssites():
    retval=[]
    retval.append([
        "SiteId",
        "Site Type",
        "System Name",
        "x","y","z",
        "Region",
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
    capi=fetch_capi("gssites")
    for row in capi:
       #print(row)
       cols=[]
       cols.append(row.get("id"))
       cols.append(row.get("type").get("type"))
       cols.append(row.get("system").get("systemName"))
       cols.append(row.get("system").get("edsmCoordX"))
       cols.append(row.get("system").get("edsmCoordY"))
       cols.append(row.get("system").get("edsmCoordZ"))
       region_id=row.get("system").get("region")
       if region_id:
           region=REGIONS[region_id-1].get("name")
       else:
           region="Unknown"
       cols.append(region)
       cols.append(row.get("system").get("primaryStar").get("type"))
       cols.append(row.get("body").get("bodyName").replace(row.get("system").get("systemName"),''))
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
       #print(json.dumps(col,indent=4))
    return retval

     

     

def write_sheet(spreadsheet_id,range_name,cells):
    name,dummy=range_name.split('!')
    print(f"Writing to sheet {name}")
    try:
        scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets"]
        secret_file = os.path.join(HOME,'sheets', 'client_secret.json')


        credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
        service = discovery.build('sheets', 'v4', credentials=credentials)
    

        values=cells


        data = {
            'values' : values 
        }

        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, body=data, range=range_name, valueInputOption='USER_ENTERED').execute()

    except OSError as e:
        print(e)


cells=[]
cells.append([str(datetime.now().isoformat(timespec='minutes'))])
write_sheet('1p20iT3HWAcRRJ8Cw60Z2tCVTpcBavhycvE0Jgg0h32Y','Beacon Reports!A1:Z',get_gbreports())
write_sheet('1p20iT3HWAcRRJ8Cw60Z2tCVTpcBavhycvE0Jgg0h32Y','Guardian Beacons!A1:Z',get_gbsites())
write_sheet('1p20iT3HWAcRRJ8Cw60Z2tCVTpcBavhycvE0Jgg0h32Y','Guardian Ruins!A1:Z',get_grsites())
write_sheet('1p20iT3HWAcRRJ8Cw60Z2tCVTpcBavhycvE0Jgg0h32Y','Guardian Structures!A1:Z',get_gssites())
write_sheet('1p20iT3HWAcRRJ8Cw60Z2tCVTpcBavhycvE0Jgg0h32Y','Ruin Reports!A1:Z',get_grreports())
write_sheet('1p20iT3HWAcRRJ8Cw60Z2tCVTpcBavhycvE0Jgg0h32Y','Structure Reports!A1:Z',get_gsreports())
write_sheet('1p20iT3HWAcRRJ8Cw60Z2tCVTpcBavhycvE0Jgg0h32Y','Header!B3',cells)
