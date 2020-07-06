#!/bin/env python3
import pymysql
from os import getenv
from pymysql.err import OperationalError
import httplib2

import httplib2
import os
import requests
import json
from googleapiclient import discovery
from google.oauth2 import service_account
from pathlib import Path
from datetime import datetime


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

def get_bio_signals():
    system="Merope"

    cursor =  mysql_conn.cursor(pymysql.cursors.DictCursor)
    sql = """
    select system,replace(body,system,'') as body,count from SAASignals where type = '$SAA_SignalType_Biological;';
       """
    cursor.execute(sql, ())
    return cursor.fetchall()
    cursor.clos()


def journal_exists(jlist,journal_id):
    if journal_id in list(jlist.keys()):
        return True
    else:
        return False


# we will have a global variable that we will update 
bio_systems={}

def populate_bio_systems(system,body,journal_id,journal_name,frontier_id,x,y,z,site_type):
    if not frontier_id or frontier_id == '':
        site_id="None"
    else:
        site_id=frontier_id

    if not bio_systems.get(system):
        bio_systems[system]={}
    bio_systems[system]["coords"]=[x,y,z]
    if not  bio_systems.get(system).get("body"):
        bio_systems[system]["body"]={}
    if not  bio_systems.get(system).get("body").get(body):
        bio_systems[system]["body"][body]={}
    if not  bio_systems.get(system).get("body").get(body).get("journal"):
        bio_systems[system]["body"][body]["journal"]={}
    if not  bio_systems.get(system).get("body").get(body).get("journal").get(journal_id):
        bio_systems[system]["body"][body]["journal"][journal_id]={}

    # if no site id we should either delete the key if a journal entry with a site id exists
    have_journal=journal_exists(bio_systems[system]["body"][body]["journal"],journal_id)
    no_site=(site_id == "None")
    have_site=(site_id != "None")
    journal_none=bio_systems.get(system).get("body").get(body).get("journal").get(journal_id).get("None")

    if have_site and journal_none:
        del bio_systems[system]["body"][body]["journal"][journal_id]["None"]

    if have_journal:
        bio_systems[system]["body"][body]["journal"][journal_id][site_id]={ "type": site_type, "name": journal_name}
    else:
        bio_systems[system]["body"][body]["journal"][journal_id]={}
        bio_systems[system]["body"][body]["journal"][journal_id][site_id]={ "type": site_type, "name": journal_name}
    #if bio_systems[system]["body"][body].get("site_type"):
    #    bio_systems[system]["body"][body]["site_type"]={}
    #bio_systems[system]["body"][body]["site_type"][site_type]=True


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

def get_sites(site):
    retval=[]
    retval.append([
        "SiteId",
        "Type",
        "Journal Name",
        "Entry Id",
        "System Name",
        "x","y","z",
        "Region",
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
    capi=fetch_capi(site)
    for row in capi:
       #print(row)
       system=row.get("system").get("systemName")
       body=str(row.get("body").get("bodyName").replace(row.get("system").get("systemName"),''))
       journal_name=row.get("type").get("journalName")
       journal_id=row.get("type").get("journalID")
       x=row.get("system").get("edsmCoordX")
       y=row.get("system").get("edsmCoordY")
       z=row.get("system").get("edsmCoordZ")
       #belt and braces in case strapi is not consistant
       if row.get("frontierID"):
           frontier_id=row.get("frontierID")
       elif row.get("frontierId"):
           frontier_id=row.get("frontierId")
       else:
           frontier_id = None
       
       populate_bio_systems(system,body,journal_id,journal_name,frontier_id,x,y,z,site)

       cols=[]
       cols.append(row.get("id"))
       cols.append(row.get("type").get("type"))
       cols.append(journal_name)
       cols.append(journal_id)
       cols.append(system)
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
       cols.append(body)
       cols.append(frontier_id)
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


def get_lc():
    retval=[]
    retval.append([
        "SiteId",
        "System Name",
        "Signal Name",
        "Discovered By",
        "Created At"
    ])
    capi=fetch_capi("lcfssreports")
    for row in capi:
       #print(row)
       cols=[]
       cols.append(row.get("id"))
       cols.append(row.get("systemName"))
       cols.append(row.get("signalName"))
       cols.append(row.get("cmdrName"))
       cols.append(row.get("created_at"))
       retval.append(cols)
       #print(json.dumps(col,indent=4))
    return retval
     

def write_sheet(spreadsheet_id,range_name,cells):
    name,dummy=range_name.split('!')
    print(f"Writing to sheet {name}")
    try:
        scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets"]
        secret_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),'client_secret.json')


        credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
        service = discovery.build('sheets', 'v4', credentials=credentials)
    

        values=cells


        data = {
            'values' : values 
        }

        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, body=data, range=range_name, valueInputOption='USER_ENTERED').execute()

    except OSError as e:
        print(e)

def count_sites(bodydata):
    count=0
    for journal,journaldata in bodydata.get("journal").items():
        for site in journaldata.keys():
            count+=1
    return count

def get_type_array(bodydata):
    retval=[0,0,0,0,0]
    for journal,journaldata in bodydata.get("journal").items():
        for site,sitedata in journaldata.items():
            for key,val in enumerate(["apsites","fgsites","btsites","cssites","twsites"]):
                if sitedata.get("type") == val:
                   retval[key]+=1
    return retval


def summarise_biosystems():
    for system,systemdata in bio_systems.items():
        #print(system, '->' ,systemdata )
        for body,bodydata in systemdata.get("body").items():
            bio_systems[system]["body"][body]["codex_sites"]=count_sites(bodydata)
            bio_systems[system]["body"][body]["type_counts"]=get_type_array(bodydata)
            #print(bio_systems[system]["body"][body]["type_counts"])

def get_signal_cells():
    for signal in bio_signals:
        has_system=(bio_systems.get(signal.get("system")))
        has_bodies=(has_system and bio_systems.get(signal.get("system")).get("body"))
        has_body=(has_bodies and bio_systems.get(signal.get("system")).get("body").get(signal.get("body")))
        system=signal.get('system')
        if has_body:
            #print(f"XXX BODY EXISTS {system}")
            bio_systems[system]["body"][signal.get("body")]["signals"]=signal.get("count")
            bio_systems[system]["body"][signal.get("body")]["has_body"]=True
        elif has_bodies:
            #print(f"XXX SOMEBODY EXISTS {system}")
            bio_systems[system]["body"][signal.get("body")]={ "has_bodies": True, "signals": signal.get("count"), "codex_sites": 0}
        elif has_system:
            #print(f"XXX SYSTEM EXISTS {system}")
            bio_systems[signal.get("system")]["body"]={}
            bio_systems[signal.get("system")]["body"][signal.get("body")]={ "has_system": True,"signals": signal.get("count"), "codex_sites": 0}
        else:
            #print(f"XXX ORPHAN {system}")
            bio_systems[signal.get("system")]={}
            bio_systems[signal.get("system")]["body"]={}
            bio_systems[signal.get("system")]["body"][signal.get("body")]={ "orphan": True, "signals": signal.get("count"), "codex_sites": 0}

    signalcells=[]


    for system,systemdata in bio_systems.items():
        for body,bodydata in systemdata.get("body").items():
            codex_sites=bodydata.get("codex_sites")
            type_counts=bodydata.get("type_counts")
            
            if bodydata.get("signals"):
                signals=bodydata.get("signals")
            else:
                signals="-"
            row=[system,body,codex_sites,signals]
            if type_counts:
                row.extend(type_counts)
            else:
                row.extend([0,0,0,0,0])

            signalcells.append(row)

    return signalcells



SHEETID="15lqZtqJk7B2qUV5Jb4tlnst6i1B7pXlAUzQnacX64Kc"
GUARDIAN_SHEET="1p20iT3HWAcRRJ8Cw60Z2tCVTpcBavhycvE0Jgg0h32Y"
CLOUD_SHEET="11BCZRci0YlgW0sFdxvB_srq7ssxHzstMAiewhSGHE94"

cells=[]
cells.append([str(datetime.now().isoformat(timespec='minutes'))])
write_sheet(SHEETID,'Amphora Plant Sites!A1:Z',get_sites("apsites"))


write_sheet(SHEETID,'Anemone Sites!A1:Z',get_sites("fgsites"))
write_sheet(SHEETID,'Crystaline Shard Sites!A1:Z',get_sites("cssites"))
write_sheet(SHEETID,'Sinuous Tuber Sites!A1:Z',get_sites("twsites"))
btcells=get_sites("btsites")
write_sheet(SHEETID,'Brain Tree Sites!A1:Z',btcells)
write_sheet(GUARDIAN_SHEET,'Brain Tree Sites!A1:Z',btcells)


write_sheet(CLOUD_SHEET,'Lagrange Cloud Reports!A1:Z',get_lc())
write_sheet(SHEETID,'Header!B3',cells)
write_sheet(CLOUD_SHEET,'Header!B3',cells)

#summarise the biosytems
summarise_biosystems()
bio_signals=get_bio_signals()

write_sheet(SHEETID,'Signals!A2:Z',get_signal_cells())



