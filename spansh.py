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
from revenant import write_sheet
import string

sys.path.append('EliteDangerousRegionMap')

# a hack to stop vscode studio reformatting
if True:
    from RegionMap import findRegion

# just going to inialise this opject
file_object = open('/tmp/biosigns.jsonl', 'w')
file_object.close()


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


def get_codex_data():
    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sql = """
        SELECT cr.name,cast(reported_at as char) as reported_at,system,body,cr.entryid,english_name,sub_class,IFNULL(id64 ,raw_json->"$.SystemAddress") AS systemaddress,cmdrname FROM codexreport cr
        LEFT JOIN codex_name_ref cnr ON cnr.entryid = cr.entryid
        WHERE body IS NOT NULL AND body != '' AND (hud_category = 'Biology' or english_name like '%%Barnacle%%')
        ORDER BY created_at asc
    """
    cursor.execute(sql, ())

    data = {}

    while True:
        row = cursor.fetchone()
        if row == None:
            break
        system = row.get("system")
        body = row.get("body")
        entryid = row.get("entryid")
        # system has many bodies a body has many categories
        has_system = (data.get(system))
        has_body = (has_system and data.get(system).get("bodies").get(body))
        has_entry = (has_body and data.get(system).get(
            "bodies").get(body).get("entries").get(entryid))

        if not has_entry:
            if not has_system:
                data[system] = {
                    "systemaddress": row.get("systemaddress"),
                    "bodies": {}
                }
            # at this point we have a system so we can add a body
            if not has_body:
                data[system]["bodies"][body] = {"entries": {}}
                data[system]["bodies"][body]["entries"][entryid] = {}

        # at this point we have a system and a body but the category won't exist
                data[system]["bodies"][body]["entries"][entryid] = {
                    "reported_at": row.get("reported_at"),
                    "cmdrname": row.get("cmdrname"),
                    "name": row.get("name"),
                    "english_name": row.get("english_name"),
                    "genus": row.get("sub_class"),
                    "entryid": row.get("entryid"),
                }
    return data


def get_primary_star(system):
    bodies = system.get("bodies")
    for body in bodies:
        if body.get("mainStar"):
            return body.get("subType")


def get_parent_type_beta(system, body):
    bodyName = body.get("name")
    systemName = system.get("name")
    shortName = bodyName.replace(f"{systemName} ", '')
    bodies = system.get("bodies")

    parts = shortName.split(' ')

    for n in range(len(parts)-1, -1, -1):

        newpart = " ".join(parts[:n])
        if newpart.isupper():
            #print(f"converting newpart {newpart} to {newpart[0]}")
            newpart = newpart[0]
        newname = systemName+" "+newpart
        # :qprint(newname)
        for b in bodies:
            if b.get("name") == newname and b.get("type") == "Star":
                #print(f"{newname} = Star")
                #print("{} {}".format(b.get("name"), parentName))
                return b.get("subType")

    # fall back to this
    primary = get_primary_star(system)
    return primary


def record_bio(j):
    bodycount = 0
    for b in j.get("bodies"):
        try:
            if b.get("signals").get("signals").get("$SAA_SignalType_Biological;") or b.get("codex"):
                bodycount = bodycount+1
        except:
            pass

    if bodycount > 0:
        system = j.get("name")
        file_object = open('/tmp/biosigns.jsonl', 'a')
        # Append 'hello' at the end of file
        file_object.write(f"{json.dumps(j)}\n")
        # Close the file
        file_object.close()


def get_parent_type(system, body):
    bodyName = body.get("name")
    systemName = system.get("name")
    shortName = bodyName.replace(f"{systemName} ", '')
    bodies = system.get("bodies")

    parents = body.get("parents")

    get_parent_type_beta(system, body)

    if parents:
        for parent in parents:
            pid = parent.get("Star")
            bid = parent.get("None")
            if pid or bid:
                break
        if pid:
            for b in bodies:
                if b.get("bodyId") == pid:
                    return b.get("subType")
    else:
        print(f"{bodyName} has no parents")

    if bodyName == systemName or shortName[0] in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9'):
        return get_primary_star(system)

    parentName = "{} {}".format(systemName, shortName[0])

    for b in bodies:
        if b.get("name") == parentName:
            #print("{} {}".format(b.get("name"), parentName))
            return b.get("subType")


codex_data = get_codex_data()
print("done getting data")

output_data = []
# print(json.dumps(codex_data, indent=4))

home = str(Path.home())
results = []
classes = {}

with gzip.open(os.path.join(home, 'spansh', 'galaxy.json.gz'), "rt") as f:
    for line in f:
        if line[0] in ["[", "]"]:
            """Do nothing"""
        else:
            try:
                j = json.loads(line[:-2])
            except:
                j = json.loads(line[:-1])
            system = j.get("name")
            has_system = (codex_data.get(system))
            has_biology = False

            if not has_system:
                record_bio(j)
            else:
                for i, b in enumerate(j.get("bodies")):
                    body = b.get("name")

                    has_body = (
                        codex_data.get(system).get("bodies").get(body)
                    )

                    if has_body:
                        has_biology = True
                        # adding codex to j so we can store it
                        j["bodies"][i]["codex"] = codex_data[system]["bodies"][body]["entries"]
                        codex_data[system]["bodies"][body]["edsm"] = b
                        for key, entry in codex_data[system]["bodies"][body]["entries"].items():

                            if not classes.get(entry.get("genus")):
                                classes[entry.get("genus")] = []
                            name = entry.get("english_name")

                            entryid = entry.get("entryid")
                            fdevname = entry.get("name")
                            reported_at = entry.get("reported_at")
                            cmdrname = entry.get("cmdrname")
                            region = findRegion64(j.get("id64"))
                            star = get_primary_star(j)
                            parent_star = get_parent_type_beta(j, b)
                            #print(f"{name}  {star} {parent_star}")
                            bodyType = b.get("subType")
                            atmosphereType = b.get("atmosphereType")
                            surfacePressure = b.get("surfacePressure")
                            volcanism = b.get("volcanismType")
                            gravity = b.get("gravity")
                            surfaceTemperature = b.get("surfaceTemperature")
                            classes[entry.get("genus")].append(
                                [
                                    reported_at,
                                    name,
                                    fdevname,
                                    entryid,
                                    cmdrname,
                                    region[1],
                                    system,
                                    j.get("id64"),
                                    star,
                                    parent_star,
                                    body,
                                    bodyType,
                                    atmosphereType,
                                    surfacePressure,
                                    volcanism,
                                    gravity,
                                    surfaceTemperature,

                                ])

                if has_biology:
                    record_bio(j)

scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file",
          "https://www.googleapis.com/auth/spreadsheets"]
secret_file = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), 'client_secret.json')
credentials = service_account.Credentials.from_service_account_file(
    secret_file, scopes=scopes)
drive = discovery.build('drive', 'v3', credentials=credentials)

HEADERS = [
    'Report Date',
    'Name',
    'Journal Name',
    'EntryId',
    'Cmdr',
    'Region',
    'System',
    'Id64',
    'Primary Star',
    'Local Star',
    'Body',
    'BodyType',
    'Atmosphere',
    'Pressure',
    'Volcanism',
    'Gravity',
    'Temperature'
]


def biosheet(type):
    cells = []
    cells.append(HEADERS)
    cells.extend(classes.get(type))
    try:
        BIOSHEET = "15lqZtqJk7B2qUV5Jb4tlnst6i1B7pXlAUzQnacX64Kc"
        write_sheet(BIOSHEET, f"{type}!A1:Z", cells)
    except:
        print(f"sheet {type} doesn't exist")


# for genus in classes.keys():
#    biosheet(genus)

print(json.dumps(classes, indent=4))

biosheet("Aleoids")
biosheet("Bacterial")
biosheet("Brain Tree")
biosheet("Cactoid")
biosheet("Clypeus")
biosheet("Conchas")
biosheet("Electricae")
biosheet("Fonticulus")
biosheet("Fumerolas")
biosheet("Fungoids")
biosheet("Osseus")
biosheet("Recepta")
biosheet("Shrubs")
biosheet("Stratum")
biosheet("Tubus")
biosheet("Tussocks")

all_bio = []
all_bio.append(HEADERS)
for c in classes.keys():
    all_bio.extend(classes.get(c))

BIOSHEET2 = "1x5vWnq-MON40uswkNmZpyVEarr9a3mEmZUk9dxo9KJo"
write_sheet(BIOSHEET2, "All Biology!A1:Z", all_bio)
