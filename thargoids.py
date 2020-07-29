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


def get_hd_monitor():

    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sql = """
    select * from hd_monitor where x is not null;
       """
    cursor.execute(sql, ())

    return cursor.fetchall()


def get_hd_detected():
    system = "Merope"

    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    sql = """
    select * from hd_detected where x is not null;
       """
    cursor.execute(sql, ())

    return cursor.fetchall()


hd_data = {}

THARGOID_SHEET = "1YgQYFYBLXz_t9wBG-kN-y4AZjYsVX2hof_t8jI8vFGQ"
MEROPE=[ -78.59375 , -149.625 , -340.53125]
SOL=[0,0,0]
WITCHHEAD=[351.96875 , -373.46875 , -711.09375]

def update_header():
    cells = []
    cells.append([str(datetime.now().isoformat(timespec='minutes'))])
    write_sheet(THARGOID_SHEET, 'Header!B3', cells)

def dist(c1,c2):
    a,b,c=c1
    x,y,z=c2
    return sqrt(pow(float(x)-float(a),2)+pow(float(y)-float(b),2)+pow(float(z)-float(c),2))


def merge_hd_data(hd_monitor,hd_detected):
    everything={}
    rows=[]
    #we will iterate
    for detected in hd_detected:
        system=detected.get("system")
        cmdr=detected.get("cmdr")
        time= detected.get("timestamp")
        key=f"{system}:{cmdr}:{time}"
        if not everything.get(key):
            everything[key]={ "row": detected, "confirmed": False}
        
    for monitor in hd_monitor:
        system=monitor.get("system")
        cmdr=monitor.get("cmdr")
        time= monitor.get("timestamp")
        key=f"{system}:{cmdr}:{time}"
        if not everything.get(key):
            everything[key]={ "row": monitor, "confirmed": True}
        else:
            everything[key][confirmed] = True

    for i,value in everything.items():
        cells=[]
        cells.append(str(value.get("row").get("timestamp")))
        cells.append(value.get("row").get("system"))
        cells.append(value.get("row").get("cmdr"))
        x=value.get("row").get("x")
        y=value.get("row").get("y")
        z=value.get("row").get("z")

        cells.append(str(x))
        cells.append(str(y))
        cells.append(str(z))
        

        if value.get("confirmed"):
            cells.append("Y")
        else:
            cells.append("N")
   
        dm=dist(MEROPE,[x,y,z])
        ds=dist(SOL,[x,y,z])
        dw=dist(WITCHHEAD,[x,y,z])


        if dm < ds and dm < dw:
            centre="Merope"
        elif ds < dm and ds < dw:
            centre="Sol"
        elif dw < dm and dw < ds:
            centre="Witchhead"
        else:
            centre="Error"

        cells.append(centre)

        cells.append(round(dm,2))
        cells.append(round(ds,2))
        cells.append(round(dw,2))
        rows.append(cells)
        
    return rows

hd_monitor = get_hd_monitor()
hd_detected = get_hd_detected()


hdcells=merge_hd_data(hd_monitor,hd_detected)

write_sheet(THARGOID_SHEET, 'Hyperdictions!A2:Z', sorted(hdcells))
update_header()
