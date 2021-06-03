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
    sqltext = """select
		replace(english_split->"$.species",'"','') as species,substr(entryid,1,5) species_id,
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
		max(case when substr(entryid,-2,2) = '00' then replace(concat(english_split->"$.colour",' - ',name_split->"$.p[4]"),'"','') else null end) as "10",
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
		group by replace(english_split->"$.species",'"',''),substr(entryid,1,5)
		order by substr(entryid,1,5)
	"""
    cursor.execute(sqltext, ())

    sheetdata = []

    while True:
        row = cursor.fetchone()

        if row:

            sheetdata.append([
                row.get("species"),
                row.get("species_id"),
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


sheetdata = get_codex_data()
BIOSHEET = "15lqZtqJk7B2qUV5Jb4tlnst6i1B7pXlAUzQnacX64Kc"
write_sheet(BIOSHEET, f"Odyssey Codex!A2:Z", sheetdata)
