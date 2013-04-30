# Copyright (c) 2013 John Krauss

# This program is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licences/>

# Utils for dealing with Google Drive & Fusion Tables

import httplib
import httplib2
import json
import logging
import pprint
import time

#from copy import copy
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials

def find_file_id(service, q):
    """
    Find one file ID by q.  Returns None if no file found.
    """
    retry = 0
    while True:
        try:
            items = service.files().list( q=q, maxResults=1).execute()['items']
            break
        except httplib.BadStatusLine as e:
            if retry < 5:
                retry += 1
                time.sleep(1)
                logging.error(u"Bad status line ({0}), retrying find_file_id".format(e))
            else:
                raise e

    if len(items) == 0:
        return None
    else:
        return items[0]['id']

def make_public(service, fileId):
    """
    Set public access permissions to fileId.
    """
    service.permissions().insert(
        fileId=fileId,
        body={
            'role': 'reader',
            'type': 'anyone',
            'value': ''
        }).execute()

def add_to_folder(service, file_id, folder_id):
    """
    Add a file to a folder.
    """
    service.parents().insert(fileId=file_id, body={
        'id': folder_id
    }).execute()

# This function sample taken from Google's docs at
# https://developers.google.com/drive/service-accounts#use_service_accounts_as_application-owned_accounts
def create_drive_service(service_account_pkcs12_file_path, service_account_email):
    """Builds and returns a Drive service object authorized with the given service account.

    Returns:
      Drive service object.
    """
    f = file(service_account_pkcs12_file_path, 'rb')
    key = f.read()
    f.close()

    credentials = SignedJwtAssertionCredentials(service_account_email, key,
        scope='https://www.googleapis.com/auth/drive')
    http = httplib2.Http()
    http = credentials.authorize(http)

    return build('drive', 'v2', http=http)

def create_fusion_client(service_account_pkcs12_file_path, service_account_email):
    f = file(service_account_pkcs12_file_path, 'rb')
    key = f.read()
    f.close()

    credentials = SignedJwtAssertionCredentials(service_account_email, key,
        scope='https://www.googleapis.com/auth/fusiontables')

    http = httplib2.Http()
    return credentials.authorize(http)

def create_fusion_table(client, name, columns):
    """
    Create a fusion table.  Returns response from the POST as dict.
    """
    resp = client.request("https://www.googleapis.com/fusiontables/v1/tables",
                          method="POST",
                          headers={"Content-Type": "application/json"},
                          body=json.dumps({
                              "name": name,
                              "columns": columns,
                              "isExportable": True
                          })
                         )
    assert int(resp[0]['status']) == 200
    return json.loads(resp[1])

def set_fusion_style(client, table_id, style):
    """
    Set a fusion table style. Eliminates existing styles.
    """
    styles = json.loads(client.request(
        u"https://www.googleapis.com/fusiontables/v1/tables/{0}/styles".format(table_id))[1])['items']

    # Delete existing styles
    logging.info(u"Deleting {0} existing styles for {1}".format(len(styles), table_id))
    for style in styles:
        style_id = style['styleId']
        resp = client.request(u"https://www.googleapis.com/fusiontables/v1/tables/{0}/styles/{1}".format(
            table_id, style_id),
            method="DELETE"
        )
        assert int(resp[0]['status']) == 204

    logging.info(u"Uploading new style for {0}".format(table_id))
    resp = client.request(u"https://www.googleapis.com/fusiontables/v1/tables/{0}/styles".format(table_id),
                          method="POST",
                          headers={"Content-Type": "application/json"},
                          body = json.dumps(style)
                         )
    assert int(resp[0]['status']) == 200
    return json.loads(resp[1])

def import_rows(client, table_id, file):
    """
    Add rows to table_id from file.

    *Not* generic at all -- adds a lonlat column to the end from the 4th & 5th
    columns.
    """
    with open(file, 'r') as input:
        while True:
            data = input.readlines(300 * 1000)
            # Generate lonlat column
            for i, line in enumerate(data):
                cols = line[0:-1].split(',')
                cols.append('{0} {1}'.format(cols[5], cols[4]))
                data[i] = ','.join(cols)
            if not data:
                break
            else:
                retry = True
                while retry:
                    retry = False
                    resp = client.request(
                        u"https://www.googleapis.com/upload/fusiontables/v1/tables/{0}/import".format(table_id),
                        method="POST",
                        headers={"Content-Type": "application/octet-stream"},
                        body='\n'.join(data)
                    )
                    status = int(resp[0]['status'])
                    if status == 200:
                        logging.info(u"Uploaded {0} rows to {1} from {2}.".format(
                            len(data), table_id, file
                        ))
                    elif status in (417, 503):
                        # 417: Google keeps these locks sometimes, have to
                        # just wait a little bit and try again.
                        # 503: Just gotta retry
                        logging.error(u"Google wants to retry rows in {0} to {1}, error {2}".format(
                            file, table_id, status
                        ))
                        retry = True
                        time.sleep(1)
                    else:
                        logging.error(u"Invalid response uploading {0} to {1}: {2}".format(
                            file, table_id, pprint.pformat(resp)
                        ))
