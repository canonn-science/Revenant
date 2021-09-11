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
from EliteDangerousRegionMap.RegionMapData import regions
import requests


def get_db_secrets():
    dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dir, 'canonn_db_secret.json')) as json_file:
        data = json.load(json_file)
        return data


speciesdata = {}

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


def capture_codex_ref():
    with __get_cursor() as cursor:
        sql = """
            insert into codex_name_ref
                select name,entryid,category,sub_category,name_localised,'Biology' as hud_category,replace(name_components->"$.value[0]",'"','') as sub_class,'odyssey' as platform  from (
                select
                        v.*,
                        cast(concat('{"value": ["',replace(replace(replace(name,'$Codex_Ent_',''),'_Name;',''),'_','","'),'"]}') as json) as name_components,
                        cast(concat('{"species": "',replace(name_localised,' - ','","colour": "'),'"}') as json) as english_split
                from v_unknown_codex v
                ) data
                where replace(english_split->"$.colour",'"','') in (
                select distinct replace(english_split->"$.colour",'"','') as colour from (
                select replace(replace(name,'$Codex_Ent_',''),'_Name;','') as name,english_name ,
                cast(concat('{"species": "',replace(english_name,' - ','","colour": "'),'"}') as json) as english_split
                from codex_name_ref where platform = 'odyssey'
                order by 1
                ) data2
                )
        """
        cursor.execute(sql, ())
        mysql_conn.commit()
        cursor.close()


def get_region_matrix():
    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sql = """
        SELECT 
            cnr.english_name,c.entryid,
            replace(replace(region_name,'$Codex_RegionName_',''),';','') AS region,
            COUNT(DISTINCT system) AS system_count 
        FROM codexreport c join codex_name_ref cnr on cnr.entryid = c.entryid   
        WHERE hud_category = 'Biology'
        GROUP BY region_name,c.entryid,cnr.english_name
        order by c.entryid asc
       """
    cursor.execute(sql, ())
    data = cursor.fetchall()

    header = ["Species", "EntryId"]
    header.extend(regions[1:])
    cells = [header]
    # Initialise the sheet
    # 42 regions + name and entryid
    row = ['']*44

    entrylist = {}

    for v in data:
        entryid = v.get("entryid")
        name = v.get("english_name")
        region = int(v.get("region"))
        systems = int(v.get("system_count"))

        if not entrylist.get(entryid):
            entrylist[entryid] = row.copy()

        newrow = entrylist[entryid]
        newrow[0] = name
        newrow[1] = entryid
        newrow[1+region] = systems

        entrylist[entryid] = newrow

    for key, item in entrylist.items():
        #print("{} {}".format(key,item[1]))
        cells.append(item)

    return cells


def get_scan_speeds():
    global speciesdata
    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sqltext = """
        select distinct species,seconds,
        case 
        when species = '$Codex_Ent_Seed_Name;' then 'Roseum Brain Tree'
        when species = '$Codex_Ent_SeedEFGH_Name;' then 'Lividum Brain Tree'
        when species = '$Codex_Ent_Sphere_Name;' then 'Luteolum Anemone'
        when species = '$Codex_Ent_SphereEFGH_Name;' then 'Blatteum Bioluminescent Anemone'
        when species = '$Codex_Ent_TubeEFGH_Name;' then 'Blatteum Sinuous Tubers'
        when species = '$Codex_Ent_Tube_Name;' then 'Roseum Sinuous Tubers'
        else replace(sub_species->"$.p[0]",'"','')
        end
        #replace(sub_species->"$.p[0]",'"','') 
        as sub_species from (
        select data3.*,cast(concat('{"p": ["',replace(english_name,' - ','","'),'"]}') as json) sub_species  from (
        select species,min(seconds) as seconds from (
        select cmdr,species,analysed,logged,TIMESTAMPDIFF(SECOND,logged,analysed) seconds from (
        select a.cmdr,a.species,a.reported_at as analysed,
            (
                select b.reported_at as logged 
                from organic_scans b
                where b.scantype = 'Log'
                and a.system = b.system
                and a.body_id = b.body_id
                and b.reported_at < a.reported_at
                and a.cmdr = b.cmdr
                and a.species = b.species
                order by b.reported_at
                limit 1
        ) as logged
        from organic_scans a
        where  scantype = 'Analyse'
        order by reported_at desc
        ) data
        ) data2
        group by species
        ) data3 
        LEFT JOIN codex_name_ref cnr ON cnr.name LIKE
                    REPLACE(data3.species,'_Name;','%%')
        ) data4
        order by 2 asc
    """
    cursor.execute(sqltext, ())

    sheetdata = []

    while True:
        row = cursor.fetchone()

        if row:
            speciesdata[row.get("species")] = row
            sheetdata.append([
                row.get("sub_species"),
                row.get("seconds"),
            ])

        else:
            break

    return sheetdata


def get_challenge_scans():
    global speciesdata
    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sqltext = """
        select species,min(seconds) as seconds from (
        select cmdr,species,analysed,logged,TIMESTAMPDIFF(SECOND,logged,analysed) seconds from (
        select a.cmdr,a.species,a.reported_at as analysed,
            (
                select b.reported_at as logged 
                from organic_scans b
                where b.system = 'Tucanae Sector AF-A d71' 
                and b.body_id = 23 
                and b.scantype = 'Log'
                and b.reported_at < a.reported_at
                and a.cmdr = b.cmdr
                and a.species = b.species
                order by b.reported_at
                limit 1
        ) as logged
        from organic_scans a
        where system = 'Tucanae Sector AF-A d71' and body_id = 23 and scantype = 'Analyse'
        order by reported_at desc
        ) data
        ) data2
        group by species
        order by 2 asc
    """
    cursor.execute(sqltext, ())

    sheetdata = []

    while True:
        row = cursor.fetchone()

        if row:

            sheetdata.append([
                speciesdata.get(row.get("species")).get("sub_species"),
                row.get("seconds"),
                speciesdata.get(row.get("species")).get("seconds")
            ])

        else:
            break

    return sheetdata


def get_challenge_splits():
    global speciesdata
    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sqltext = """
        select * from (
        select cmdr,species,analysed,logged,TIMESTAMPDIFF(SECOND,logged,analysed) seconds from (
        select a.cmdr,a.species,a.reported_at as analysed,
            (
                select b.reported_at as logged 
                from organic_scans b
                where b.system = 'Tucanae Sector AF-A d71' 
                and b.body_id = 23 
                and b.scantype = 'Log'
                and b.reported_at < a.reported_at
                and a.cmdr = b.cmdr
                and a.species = b.species
                order by b.reported_at
                limit 1
        ) as logged
        from organic_scans a
        where system = 'Tucanae Sector AF-A d71' and body_id = 23 and scantype = 'Analyse'
        order by reported_at desc
        ) data
        ) data2
        order by seconds asc
    """
    cursor.execute(sqltext, ())

    sheetdata = []

    while True:
        row = cursor.fetchone()

        if row:

            sheetdata.append([
                row.get("cmdr"),
                speciesdata.get(row.get("species")).get("sub_species"),
                str(row.get("analysed")),
                str(row.get("logged")),
                row.get("seconds")
            ])

        else:
            break

    return sheetdata


def get_codex_data():
    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sqltext = """
SELECT DATA3.reward,DATA2.* FROM (
     select
            replace(english_split->"$.species",'"','') as species,substr(entryid,1,5) species_id,max(c.species_count) as species_count,
            max(case when substr(entryid,-2,2) = '00' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "00",
            max(case when substr(entryid,-2,2) = '01' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "01",
            max(case when substr(entryid,-2,2) = '02' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "02",
            max(case when substr(entryid,-2,2) = '03' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "03",
            max(case when substr(entryid,-2,2) = '04' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "04",
            max(case when substr(entryid,-2,2) = '05' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "05",
            max(case when substr(entryid,-2,2) = '06' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "06",
            max(case when substr(entryid,-2,2) = '07' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "07",
            max(case when substr(entryid,-2,2) = '08' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "08",
            max(case when substr(entryid,-2,2) = '09' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "09",
            max(case when substr(entryid,-2,2) = '10' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "10",
            max(case when substr(entryid,-2,2) = '11' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "11",
            max(case when substr(entryid,-2,2) = '12' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "12",
            max(case when substr(entryid,-2,2) = '13' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "13",
            max(case when substr(entryid,-2,2) = '14' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "14",
            max(case when substr(entryid,-2,2) = '15' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "15",
            max(case when substr(entryid,-2,2) = '16' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "16"
		from (
		select
		cast(concat('{"species": "',
		REPLACE(english_name,' - ','","colour": "'),'"}') as json) as english_split,
			cast(concat('{"value": ["',replace(replace(replace(name,'$Codex_Ent_',''),'_Name;',''),'_','","'),'"]}') as json) as name_components,
		cast(concat('{"p": ["',
		REPLACE(name,'_','","'),'"]}') as json) as name_split,
		cnr.*
		from codex_name_ref cnr where platform = 'odyssey'
		order by entryid asc
		) data
		join (
			select 
				substr(entryid,1,5) as species_id,
				count(distinct system,body) as species_count 
			from codexreport 
			where entryid in (select entryid from codex_name_ref where platform = 'odyssey')
			group by substr(entryid,1,5)
		) c on c.species_id = substr(entryid,1,5)
		group by replace(english_split->"$.species",'"',''),substr(entryid,1,5)
        order by substr(entryid,1,5)
     ) DATA2   
     LEFT JOIN (
     SELECT 
                distinct sub_species->"$.p[0]" as sub_species,reward
                from (		 
                select 
                cnr.entryid,
                cast(concat('{"p": ["',replace(english_name,' - ','","'),'"]}') as json) sub_species,reward,sub_class
            FROM organic_sales os
            LEFT JOIN codex_name_ref cnr ON cnr.name LIKE
            REPLACE(os.species,'_Name;','%%')
            ) data
     ) DATA3 ON DATA3.sub_species = DATA2.species 
     ORDER BY species_id asc
	"""
    cursor.execute(sqltext, ())

    sheetdata = []

    while True:
        row = cursor.fetchone()

        if row:

            sheetdata.append([
                row.get("species"),
                row.get("species_id"),
                row.get("reward"),
                row.get("species_count"),
                row.get("00"),
                row.get("01"),
                row.get("02"),
                row.get("03"),
                row.get("04"),
                row.get("05"),
                row.get("06"),
                row.get("07"),
                row.get("08"),
                row.get("09"),
                row.get("10"),
                row.get("11"),
                row.get("12"),
                row.get("13"),
                row.get("14"),
                row.get("15"),
                row.get("16")
            ])

        else:
            break

    return sheetdata

def get_top_ten(species):
    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sqltext = """
		select * from (
        select cmdr,species,analysed,logged,TIMESTAMPDIFF(SECOND,logged,analysed) seconds from (
        select a.cmdr,a.species,a.reported_at as analysed,
            (
                select b.reported_at as logged 
                from organic_scans b
                where b.system = a.system
                and b.body_id = a.body_id
                and b.scantype = 'Log'
                and b.reported_at < a.reported_at
                and a.cmdr = b.cmdr
                and a.species = b.species
                AND a.species = %s
                order by b.reported_at
                limit 1
        ) as logged
        from organic_scans a
        where scantype = 'Analyse'
        AND species = %s
        order by reported_at desc
        ) data
        ) data2
        WHERE logged IS NOT null
        order by seconds ASC LIMIT 10    
    """
    cursor.execute(sqltext, (species,species))
    
    sheetdata=[]

    while True:
        row = cursor.fetchone()

        if row:

            sheetdata.append([
                row.get("cmdr"),
                str(row.get("analysed")),
                str(row.get("logged")),
                row.get("seconds")
            ])

        else:
            break

    return sheetdata


def get_leaders():
    r = requests.get(
        "https://us-central1-canonn-api-236217.cloudfunctions.net/query/challenge/speed")
    leaders = r.json()

    sheetdata = []
    # Cmdr	Started	Ended	Seconds	Aleoid	Bacterium	Cactoid	Concha	Fungoid	Osseus	Shrub	Stratum	Tussock
    for row in leaders:
        sheetdata.append([
            row.get("cmdr"),
            str(row.get("started") or ""),
            str(row.get("ended") or ""),
            row.get("seconds") or "",
            row.get("aleoid") or "",
            row.get("bacterium") or "",
            row.get("cactoid") or "",
            row.get("concha") or "",
            row.get("fungoid") or "",
            row.get("osseus") or "",
            row.get("shrub") or "",
            row.get("stratum") or "",
            row.get("tussock" or "")
        ])

    return sheetdata


capture_codex_ref()
allscans = get_scan_speeds()
challenge = get_challenge_scans()
splits = get_challenge_splits()
leaderboard = get_leaders()

CHALLENGESHEET = "1Lpu3r2QCPa6BiSPGqCB7WS_QWghBQfVlfpgl92pXJSU"
write_sheet(CHALLENGESHEET, f"Splits!A2:Z", splits)
write_sheet(CHALLENGESHEET, f"Galactic Records!A2:B", allscans)
write_sheet(CHALLENGESHEET, f"Best Times!A2:Z", challenge)
write_sheet(CHALLENGESHEET, f"Leader Board!A2:Z", leaderboard)
write_sheet(CHALLENGESHEET, f"Galactic Records!D3:G12", get_top_ten('$Codex_Ent_Aleoids_02_Name;'))
write_sheet(CHALLENGESHEET, f"Galactic Records!D16:G25", get_top_ten('$Codex_Ent_Bacterial_01_Name;'))
write_sheet(CHALLENGESHEET, f"Galactic Records!D29:G38", get_top_ten('$Codex_Ent_Cactoid_01_Name;'))
write_sheet(CHALLENGESHEET, f"Galactic Records!D42:G51", get_top_ten('$Codex_Ent_Conchas_01_Name;'))
write_sheet(CHALLENGESHEET, f"Galactic Records!D55:G64", get_top_ten('$Codex_Ent_Fungoids_02_Name;'))
write_sheet(CHALLENGESHEET, f"Galactic Records!D68:G77", get_top_ten('$Codex_Ent_Osseus_01_Name;'))
write_sheet(CHALLENGESHEET, f"Galactic Records!D81:G90", get_top_ten('$Codex_Ent_Shrubs_02_Name;'))
write_sheet(CHALLENGESHEET, f"Galactic Records!D94:G103", get_top_ten('$Codex_Ent_Stratum_01_Name;'))
write_sheet(CHALLENGESHEET, f"Galactic Records!D107:G116", get_top_ten('$Codex_Ent_Tussocks_11_Name;'))

sheetdata = []
sheetdata.append([
    "Species",
    "Species Id",
    "Reward",
    "Count",
    "00",
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16"
])
sheetdata.extend(get_codex_data())
BIOSHEET = "15lqZtqJk7B2qUV5Jb4tlnst6i1B7pXlAUzQnacX64Kc"
write_sheet(BIOSHEET, f"Odyssey Codex!A1:Z", sheetdata)

write_sheet(BIOSHEET, f"Regions!A1:ZZ", get_region_matrix())
