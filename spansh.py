#!/bin/env python3
import gzip
import simplejson as json
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


"""

We want to get codex enrties with no body information
For these we will record region primary and contents but nothing else
We will need to add a specific gather stats stage

"""


def get_codex_data():
    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sql = """
        SELECT cr.name,cast(reported_at as char) as reported_at,system,body,cr.entryid,english_name,sub_class,IFNULL(id64 ,raw_json->"$.SystemAddress") AS systemaddress,cmdrname,cnr.platform,cnr.hud_category FROM codexreport cr
        LEFT JOIN codex_name_ref cnr ON cnr.entryid = cr.entryid
        WHERE hud_category not in ('Tourist','Geology') and english_name not like '%%Barnacle Barbs%%'
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

        # None will be a special case
        if not body or body == "":
            body = "None"

        entryid = row.get("entryid")
        # system has many bodies a body has many categories

        has_system = (data.get(system))
        has_body = (has_system and data.get(system).get("bodies").get(body))
        has_entry = (has_body and data.get(system).get(
            "bodies").get(body).get("entries").get(entryid))

        # if the row has a body

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
            print(f"{row.get('name')} {body}")
            data[system]["bodies"][body]["entries"][entryid] = {
                "reported_at": row.get("reported_at"),
                "cmdrname": row.get("cmdrname"),
                "name": row.get("name"),
                "english_name": row.get("english_name"),
                "genus": row.get("sub_class"),
                "hud_category": row.get("hud_category"),
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
            # print(f"converting newpart {newpart} to {newpart[0]}")
            newpart = newpart[0]
        newname = systemName+" "+newpart
        # :qprint(newname)
        for b in bodies:
            if b.get("name") == newname and b.get("type") == "Star":
                # print(f"{newname} = Star")
                # print("{} {}".format(b.get("name"), parentName))
                return b.get("subType")

    # fall back to this
    primary = get_primary_star(system)
    return primary


def get_sub_types(system):
    types = set()
    bodies = system.get("bodies")
    for body in bodies:
        types.add(body.get("subType"))
    return types


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
            # print("{} {}".format(b.get("name"), parentName))
            return b.get("subType")


codex_data = get_codex_data()
print("done getting data")

output_data = []
# print(json.dumps(codex_data, indent=4))

home = str(Path.home())
results = []
classes = {}
biostats = {}
histograms = {}


def refloat(value):
    if value:
        return float(value)
    else:
        return None


def increment(value):
    if value:
        return value + 1
    else:
        return 1


"""

This will need to be modified to cope with no body codex entries

"""


def initStats(codex, grav, temp, atmo, bodytype, star, parentstar, pressure, solidComp, atmoComp, mats, region, distanceToArrival, volcanism, types):
    global biostats
    global histograms

    if volcanism is None:
        volcanism = "No volcanism"

    biostats[codex.get("entryid")] = {
        "name": codex.get("english_name"),
        "hud_category": codex.get("hud_category"),
        "count": 1,
        "id": codex.get("name"),
        "platform": codex.get("platform"),
        "bodies": set([bodytype]),
        "ming": refloat(grav),
        "maxg": refloat(grav),
        "mint": refloat(temp),
        "maxt": refloat(temp),
        "minp": refloat(pressure),
        "maxp": refloat(pressure),
        "mind": refloat(distanceToArrival),
        "maxd": refloat(distanceToArrival),
        "atmosphereType": set([atmo]),
        "primaryStars": set([star]),
        "localStars": set([parentstar]),
        "regions": set([region]),
        "volcanism": set([volcanism]),
        "solidComposition": set(),
        "atmosComposition": set(),
        "materials": set(),
        "systemBodyTypes": types
    }

    biostats[codex.get("entryid")]["histograms"] = {}
    biostats[codex.get("entryid")]["histograms"]["body_types"] = {}
    biostats[codex.get("entryid")]["histograms"]["primary_stars"] = {}
    biostats[codex.get("entryid")]["histograms"]["atmos_types"] = {}
    biostats[codex.get("entryid")]["histograms"]["local_stars"] = {}
    biostats[codex.get("entryid")]["histograms"]["materials"] = {}

    if bodytype is not None:
        histograms[codex.get("entryid")] = {
            "dist": [refloat(distanceToArrival)],
            "grav": [refloat(grav)],
            "temp": [refloat(temp)],
            "pres": [refloat(pressure)]
        }
        biostats[codex.get("entryid")
                 ]["histograms"]["body_types"][bodytype] = 1
        biostats[codex.get("entryid")]["histograms"]["primary_stars"][star] = 1
        biostats[codex.get("entryid")]["histograms"]["atmos_types"][atmo] = 1
        biostats[codex.get("entryid")
                 ]["histograms"]["local_stars"][parentstar] = 1
    else:
        histograms[codex.get("entryid")] = {
            "dist": [],
            "grav": [],
            "temp": [],
            "pres": []
        }

    if atmoComp:
        biostats[codex.get("entryid")]["atmosComposition"] = set(
            atmoComp.keys())
    if solidComp:
        biostats[codex.get("entryid")]["solidComposition"] = set(
            solidComp.keys())
    if mats:
        biostats[codex.get("entryid")]["materials"] = set(
            mats.keys())
        for mat in mats.keys():
            biostats[codex.get("entryid")]["histograms"]["materials"][mat] = 1


def smin(a, b):
    if a is None and b is None:
        return None
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)


def smax(a, b):
    if a is None and b is None:
        return None
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


"""

This will need to be modified to cope with no body codex entries

"""


def gatherStats(codex, grav, temp, atmo, bodytype, star, parentstar, pressure, solidComp, atmoComp, mats, region, distanceToArrival, volcanism, types):
    global biostats

    if volcanism is None and bodyType is not None:
        volcanism = "No volcanism"
    if atmo is None and bodyType is not None:
        atmo = "No atmosphere"

    if biostats.get(codex.get("entryid")):
        if bodytype is not None:
            biostats[codex.get("entryid")]["bodies"].add(bodytype)
            biostats[codex.get("entryid")]["ming"] = smin(
                refloat(grav), biostats[codex.get("entryid")]["ming"])
            biostats[codex.get("entryid")]["maxg"] = smax(
                refloat(grav), biostats[codex.get("entryid")]["maxg"])
            biostats[codex.get("entryid")]["mint"] = smin(
                refloat(temp), biostats[codex.get("entryid")]["mint"])
            biostats[codex.get("entryid")]["maxt"] = smax(
                refloat(temp), biostats[codex.get("entryid")]["maxt"])
            biostats[codex.get("entryid")]["minp"] = smin(
                refloat(pressure), biostats[codex.get("entryid")]["minp"])
            biostats[codex.get("entryid")]["maxp"] = smax(
                refloat(pressure), biostats[codex.get("entryid")]["maxp"])
            biostats[codex.get("entryid")]["mind"] = smin(
                refloat(distanceToArrival), biostats[codex.get("entryid")]["mind"])
            biostats[codex.get("entryid")]["maxd"] = smax(
                refloat(distanceToArrival), biostats[codex.get("entryid")]["maxd"])
            biostats[codex.get("entryid")]["atmosphereType"].add(atmo)
            biostats[codex.get("entryid")]["localStars"].add(parentstar)
            biostats[codex.get("entryid")]["volcanism"].add(volcanism)

        biostats[codex.get("entryid")]["regions"].add(region)
        biostats[codex.get("entryid")]["count"] += 1
        biostats[codex.get("entryid")]["primaryStars"].add(star)

        if types:
            biostats[codex.get("entryid")]["systemBodyTypes"] = biostats[codex.get("entryid")]["systemBodyTypes"].intersection(
                set(types))

        if solidComp:
            biostats[codex.get("entryid")]["solidComposition"] = biostats[codex.get("entryid")]["solidComposition"].intersection(
                set(solidComp.keys()))

        if atmoComp:
            biostats[codex.get("entryid")]["atmosComposition"] = biostats[codex.get("entryid")]["atmosComposition"].intersection(set(
                atmoComp.keys())
            )

        if mats:
            biostats[codex.get("entryid")]["materials"] = biostats[codex.get(
                "entryid")]["materials"].intersection(set(mats.keys()))

        if bodyType is not None:
            histograms[codex.get("entryid")]["dist"].append(
                refloat(distanceToArrival))
            histograms[codex.get("entryid")]["grav"].append(refloat(grav))
            histograms[codex.get("entryid")]["temp"].append(refloat(temp))
            histograms[codex.get("entryid")]["pres"].append(refloat(pressure))

            biostats[codex.get("entryid")]["histograms"]["body_types"][bodytype] = increment(
                biostats[codex.get("entryid")]["histograms"]["body_types"].get(bodytype))

            biostats[codex.get("entryid")]["histograms"]["local_stars"][parentstar] = increment(
                biostats[codex.get("entryid")]["histograms"]["local_stars"].get(parentstar))
            biostats[codex.get("entryid")]["histograms"]["atmos_types"][atmo] = increment(
                biostats[codex.get("entryid")]["histograms"]["atmos_types"].get(atmo))

        biostats[codex.get("entryid")]["histograms"]["primary_stars"][star] = increment(
            biostats[codex.get("entryid")]["histograms"]["primary_stars"].get(star))

        if mats:
            for mat in mats.keys():
                biostats[codex.get("entryid")]["histograms"]["materials"][mat] = increment(
                    biostats[codex.get("entryid")]["histograms"]["materials"].get(mat))

    else:
        initStats(codex, grav, temp, atmo, bodytype, star, parentstar,
                  pressure, solidComp, atmoComp, mats, region, distanceToArrival, volcanism, types)
    # print(biostats.get(codex.get("entryid")))


def store_non_body_codex(system):
    global codex_data
    global classes

    star = get_primary_star(system)
    region = findRegion64(system.get("id64"))
    body_types = get_sub_types(system)

    if codex_data.get(system.get("name")) and codex_data.get(system.get("name")).get("bodies") and codex_data.get(system.get("name")).get("bodies").get("None"):

        for key, entry in codex_data[system.get("name")]["bodies"]["None"]["entries"].items():
            # print(entry.get("english_name"))
            gatherStats(
                entry,
                None, None, None, None,
                star,
                None, None, None, None, None,
                region[1],
                None, None,
                body_types
            )

            if not classes.get(entry.get("genus")):
                classes[entry.get("genus")] = []

            fdevname = entry.get("name")
            reported_at = entry.get("reported_at")
            cmdrname = entry.get("cmdrname")

            # print(f"{name}  {star} {parent_star}")

            classes[entry.get("genus")].append(
                [
                    reported_at,
                    entry.get("english_name"),
                    fdevname,
                    entry.get("entryid"),
                    entry.get("cmdrname"),
                    region[1],
                    system.get("name"),
                    system.get("id64"),
                    star,
                    None, None, None, None, None, None, None, None, None
                ])


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

            """
            Add in s step to detect no body codex items and process them
            """

            if not has_system:
                record_bio(j)
            else:
                store_non_body_codex(j)
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
                            # print(f"{name}  {star} {parent_star}")
                            bodyType = b.get("subType")
                            atmosphereType = b.get("atmosphereType")
                            surfacePressure = b.get("surfacePressure")
                            volcanism = b.get("volcanismType")
                            gravity = b.get("gravity")
                            surfaceTemperature = b.get("surfaceTemperature")
                            body_types = get_sub_types(j)

                            gatherStats(
                                entry,
                                gravity,
                                surfaceTemperature,
                                atmosphereType,
                                bodyType,
                                star,
                                parent_star,
                                surfacePressure,
                                b.get("solidComposition"),
                                b.get("atmosphereComposition"),
                                b.get("materials"),
                                region[1],
                                b.get("distanceToArrival"),
                                volcanism,
                                body_types
                            )

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
                                    b.get("distanceToArrival")
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
    'Temperature',
    'Distance To Arrival'
]


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


def histogram_data(data, cols):
    # find max and min
    a = min(data)
    z = max(data)
    # get distance between them
    w = z-a
    # increment is the distance / columns
    i = w/cols

    retval = []

    for col in range(1, cols):
        column = {
            "min": col*i,
            "max": (col*i)+i,
            # count values between max and min
            "value":  ((col*i <= data) & (data <= (col*i)+i)).sum()
        }
        retval.append(column)
    return retval


def process_histograms():
    global biostats
    global histograms
    global codex

    for entry in histograms.keys():
        biostats[codex.get("entryid")]["histograms"]["distance"] = histogram_data(
            histograms[codex.get("entryid")]["dist"], 12)
        biostats[codex.get("entryid")]["histograms"]["gravity"] = histogram_data(
            histograms[codex.get("entryid")]["grav"], 12)
        biostats[codex.get("entryid")]["histograms"]["temperature"] = histogram_data(
            histograms[codex.get("entryid")]["temp"], 12)
        biostats[codex.get("entryid")]["histograms"]["pressure"] = histogram_data(
            histograms[codex.get("entryid")]["pres"], 12)


process_histograms()

with open('biostats2.json', 'w') as f:
    json.dump(biostats, f,  iterable_as_array=True)

write_file("14t7SKjLyATHVipuqNiGT-ziA2nRW8sKj", "biostats2.json",
           "Range of Conditions for Biological types\n", len(biostats))


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


#print(json.dumps(classes, indent=4))

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
