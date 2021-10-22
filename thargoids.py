#!/bin/env python3
import json
import os
from datetime import datetime
from os import getenv
from pathlib import Path

import httplib2
import httplib2
import pymysql
import requests
from pymysql.err import OperationalError
from revenant import write_sheet
from math import sqrt
import collections

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

SOL = [0, 0, 0]
MEROPE = [-78.59375, -149.625, -340.53125]
COALSACK = [423.5625, 0.5, 277.75]  # Musca Dark Region PJ-P b6-8
WITCHHEAD = [355.75, -400.5, -707.21875]  # Ronemar
CALIFORNIA = [-299.0625, -229.25, -876.125]  # HIP 18390
CONESECTOR = [609.4375, 154.25, -1503.59375]  # Outotz ST-I d9-4

def getDistance(a, b):
    return round(sqrt(pow(float(a[0])-float(b[0]), 2)+pow(float(a[1])-float(b[1]), 2)+pow(float(a[2])-float(b[2]), 2)),1)

def getNearest(r):
    x = r.get("x")
    y = r.get("y")
    z = r.get("z")
    d = [
        {"name": "Sol", "distance": getDistance(
            [x, y, z], SOL), "coords": SOL},
        {"name": "Merope", "distance": getDistance(
            [x, y, z], MEROPE), "coords": MEROPE},
        {"name": "Coalsack", "distance": getDistance(
            [x, y, z], COALSACK), "coords": COALSACK},
        {"name": "Witchhead", "distance": getDistance(
            [x, y, z], WITCHHEAD), "coords": WITCHHEAD},
        {"name": "California", "distance": getDistance(
            [x, y, z], CALIFORNIA), "coords": CALIFORNIA},
        {"name": "Cone Sector", "distance": getDistance(
            [x, y, z], CONESECTOR), "coords": CONESECTOR},
    ]
    d.sort(key=lambda dx: dx["distance"], reverse=False)
    
    return d[0]


def get_hd_monitor():

    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sql = """
    select * from hd_monitor where x is not null;
       """
    cursor.execute(sql, ())

    return cursor.fetchall()


def get_hd_detected():

    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sql = """
    select * from hd_detected where x is not null order by timestamp asc;
       """
    cursor.execute(sql, ())

    return cursor.fetchall()


def get_nhss_reported():

    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sql = """
    
        select systemName,
 		  min(found_at) as first_seen, 
		  max(found_at) as last_seen, 
		  sum(case when threat_level = 0 then 1 else 0 end) as threat_0,
		  sum(case when threat_level = 1 then 1 else 0 end) as threat_1,
		  sum(case when threat_level = 2 then 1 else 0 end) as threat_2,
		  sum(case when threat_level = 3 then 1 else 0 end) as threat_3,		  		  
		  sum(case when threat_level = 4 then 1 else 0 end) as threat_4,
		  sum(case when threat_level = 5 then 1 else 0 end) as threat_5,
		  sum(case when threat_level = 6 then 1 else 0 end) as threat_6,
		  sum(case when threat_level = 7 then 1 else 0 end) as threat_7,		  		  
		  sum(case when threat_level = 8 then 1 else 0 end) as threat_8,
		  sum(case when threat_level = 9 then 1 else 0 end) as threat_9,
          min(x) as x,
          min(y) as y,
          min(z) as z
        from nhssreports
        group by systemName 
        order by 1 asc

       """
    cursor.execute(sql, ())

    data = []
    header = [
        "System",
        "First Seen",
        "Last Seen",
        "Threat 0",
        "Threat 1",
        "Threat 2",
        "Threat 3",
        "Threat 4",
        "Threat 5",
        "Threat 6",
        "Threat 7",
        "Threat 7",
        "Threat 9",
        "x",
        "y",
        "z",
        "Bubble",
        "Distance (ly)"
    ]
    data.append(header)
    for rec in cursor.fetchall():
        cols = []
        cols.append(str(rec.get("systemName")))
        cols.append(str(rec.get("first_seen")))
        cols.append(str(rec.get("last_seen")))
        cols.append(str(rec.get("threat_0")))
        cols.append(str(rec.get("threat_1")))
        cols.append(str(rec.get("threat_2")))
        cols.append(str(rec.get("threat_3")))
        cols.append(str(rec.get("threat_4")))
        cols.append(str(rec.get("threat_5")))
        cols.append(str(rec.get("threat_6")))
        cols.append(str(rec.get("threat_7")))
        cols.append(str(rec.get("threat_8")))
        cols.append(str(rec.get("threat_9")))
        cols.append(str(rec.get("x")))
        cols.append(str(rec.get("y")))
        cols.append(str(rec.get("z")))
        nearest=getNearest(rec)
        cols.append(str(nearest.get("name")))
        cols.append(str(nearest.get("distance")))
        data.append(cols)
    return data


def get_surface_encounters():

    cursor = mysql_conn.cursor()
    sql = """
    select * from v_surface_encounters;
       """
    cursor.execute(sql, ())

    return cursor.fetchall()


hd_data = {}

THARGOID_SHEET = "1YgQYFYBLXz_t9wBG-kN-y4AZjYsVX2hof_t8jI8vFGQ"
MEROPE = [-78.59375, -149.625, -340.53125]
SOL = [0, 0, 0]
WITCHHEAD = [351.96875, -373.46875, -711.09375]


def update_header():
    cells = []
    cells.append([str(datetime.now().isoformat(timespec='minutes'))])
    write_sheet(THARGOID_SHEET, 'Header!B3', cells)


def dist(c1, c2):
    a, b, c = c1
    x, y, z = c2
    return sqrt(pow(float(x)-float(a), 2)+pow(float(y)-float(b), 2)+pow(float(z)-float(c), 2))


def merge_hd_data(hd_monitor, hd_detected):
    everything = {}
    rows = []
    # we will iterate
    for detected in hd_detected:
        system = detected.get("system")
        cmdr = detected.get("cmdr")
        time = detected.get("timestamp")
        key = f"{system}:{cmdr}:{time}"
        if not everything.get(key):
            everything[key] = {"row": detected, "confirmed": False}

    for monitor in hd_monitor:
        system = monitor.get("system")
        cmdr = monitor.get("cmdr")
        time = monitor.get("timestamp")
        key = f"{system}:{cmdr}:{time}"
        if not everything.get(key):
            everything[key] = {"row": monitor, "confirmed": True}
        else:
            everything[key][confirmed] = True

    for i, value in everything.items():
        cells = []
        cells.append(str(value.get("row").get("timestamp")))
        cells.append(value.get("row").get("system"))
        cells.append(value.get("row").get("cmdr"))
        x = value.get("row").get("x")
        y = value.get("row").get("y")
        z = value.get("row").get("z")

        cells.append(str(x))
        cells.append(str(y))
        cells.append(str(z))

        if value.get("confirmed"):
            cells.append("Y")
        else:
            cells.append("N")

        dm = dist(MEROPE, [x, y, z])
        ds = dist(SOL, [x, y, z])
        dw = dist(WITCHHEAD, [x, y, z])

        if dm < ds and dm < dw:
            centre = "Merope"
        elif ds < dm and ds < dw:
            centre = "Sol"
        elif dw < dm and dw < ds:
            centre = "Witchhead"
        else:
            centre = "Error"

        cells.append(centre)

        cells.append(round(dm, 2))
        cells.append(round(ds, 2))
        cells.append(round(dw, 2))
        rows.append(cells)

    return rows


def get_locations(hd_detected):
    data = []
    header = ["Timestamp", "Commander", "System",
              "x", "y", "z", "Destination", "x", "y", "z", "Jump Distance (ly)"]
    data.append(header)
    for rec in hd_detected:
        cols = []
        if rec.get("dx") and rec.get("dy") and rec.get("dz"):
            distance = round(sqrt(
                pow(rec.get("x")-rec.get("dx"), 2) +
                pow(rec.get("y")-rec.get("dy"), 2) +
                pow(rec.get("z")-rec.get("dz"), 2)
            ), 1)
            cols.append(str(rec.get("timestamp")))
            cols.append(str(rec.get("cmdr")))
            cols.append(str(rec.get("system")))
            cols.append(str(rec.get("x")))
            cols.append(str(rec.get("y")))
            cols.append(str(rec.get("z")))
            cols.append(str(rec.get("destination")))
            cols.append(str(rec.get("dx")))
            cols.append(str(rec.get("dy")))
            cols.append(str(rec.get("dz")))
            cols.append(str("distance"))
            data.append(cols)
    return data


hd_monitor = get_hd_monitor()
hd_detected = get_hd_detected()


hdcells = merge_hd_data(hd_monitor, hd_detected)

write_sheet(THARGOID_SHEET, 'Hyperdictions!A2:Z', sorted(hdcells))
write_sheet(THARGOID_SHEET, 'Hyperdiction Detections!A1:K',
            get_locations(hd_detected))
write_sheet(THARGOID_SHEET, 'NHSS Locations!A1:R',
            get_nhss_reported())
write_sheet(THARGOID_SHEET, 'Surface Encounters!A2:Z',
            get_surface_encounters())
update_header()
