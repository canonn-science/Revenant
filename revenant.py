from googleapiclient import discovery
from google.oauth2 import service_account
import os

def write_sheet(spreadsheet_id,range_name,cells):
    name,dummy=range_name.split('!')
    print(f"Writing to sheet {name}")
    try:
        scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets"]
        secret_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),'client_secret.json')

        for r,row in enumerate(cells):
            for c,cell in enumerate(row):
                if cells[r][c] is None:
                   cells[r][c] = ''


        credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
        service = discovery.build('sheets', 'v4', credentials=credentials)


        values=cells


        data = {
            'values' : values
        }

        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, body=data, range=range_name, valueInputOption='USER_ENTERED').execute()

    except OSError as e:
        print(e)

