#!/usr/bin/env python

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

# This uploads CSV data to Google drive for use in fusion tables,
# etc.

import logging
import os
import sys
import json

logging.basicConfig(level=logging.INFO)

from googleUtils import ( find_file_id, make_public, add_to_folder,
                         create_fusion_table, import_rows, create_fusion_client,
                         create_drive_service, set_fusion_style )

SERVICE_ACCOUNT_PKCS12_FILE_PATH = os.environ.get('SERVICE_ACCOUNT_PKCS12_FILE_PATH')
SERVICE_ACCOUNT_EMAIL = os.environ.get('SERVICE_ACCOUNT_EMAIL')

if not SERVICE_ACCOUNT_PKCS12_FILE_PATH or not SERVICE_ACCOUNT_EMAIL:
    sys.stderr.write("""
    Missing required env variables.  Make sure SERVICE_ACCOUNT_PKCS12_FILE_PATH and
    SERVICE_ACCOUNT_EMAIL are set in your env, and run again.

""")
    sys.exit(1)

fusion_client = create_fusion_client(SERVICE_ACCOUNT_PKCS12_FILE_PATH, SERVICE_ACCOUNT_EMAIL)
service = create_drive_service(SERVICE_ACCOUNT_PKCS12_FILE_PATH, SERVICE_ACCOUNT_EMAIL)

# 1. Locate or create public 'nypd-crash-data-bandaid' folder.
PUBLIC_FOLDER_NAME = u'nypd-crash-data-bandaid'
DRIVE_FOLDER_MIME = u'application/vnd.google-apps.folder'

public_folder_id = find_file_id(
    service, u"title='{0}' and mimeType='{1}'".format(PUBLIC_FOLDER_NAME, DRIVE_FOLDER_MIME))

# 1a. Folder doesn't exist, create it.
if public_folder_id is None:
    public_folder_id = service.files().insert(
        body={
            'title': PUBLIC_FOLDER_NAME,
            'mimeType': DRIVE_FOLDER_MIME,
            'shared': True
        }).execute()['id']

make_public(service, public_folder_id)

# 2. Create a complete spreadsheet, make sure it's publicly accessible, if it
#    doesn't exist already.
COMPLETE_TABLE_NAME = 'all-accidents'
#SPREADSHEET_MIME = 'application/vnd.google-apps.spreadsheet'

FUSION_COLUMNS = [{
    "name": "year",
    "type": "NUMBER"
}, {
    "name": "month",
    "type": "STRING"
}, {
    "name": "precinct",
    "type": "STRING"
}, {
    "name": "street_name",
    "type": "STRING"
}, {
    "name": "lon",
    "type": "STRING"
}, {
    "name": "lat",
    "type": "STRING"
}, {
    "name": "accidents_with_injuries",
    "type": "NUMBER"
}, {
    "name": "accidents",
    "type": "NUMBER"
}, {
    "name": "involved",
    "type": "NUMBER"
}, {
    "name": "category",
    "type": "STRING",
}, {
    "name": "injured",
    "type": "NUMBER",
}, {
    "name": "killed",
    "type": "NUMBER"
}, {
    "name": "vehicle_type",
    "type": "STRING"
}, {
    "name": "vehicle_count",
    "type": "NUMBER"
}, {
    "name": "lonlat",
    "type": "LOCATION"
}]

# 2. Loop through months, determine whether spreadsheet exists for each;
#    if not, upload the spreadsheet to the folder, ensuring conversion from
#    csv is handled.  Then ensure each is publicly visible.

# 3. For each month, also ensure that the complete spreadsheet has this data,
#    determined from metadata.  Complete it if necessary.
PATH_TO_DATA = os.path.join('.', 'public', 'data')
fusion_table_style = json.load(open('fusionTableStyle.json', 'r'))
for el in os.walk(PATH_TO_DATA):
    path, dirs, files = el
    if path.endswith('accidents'):
        csvs = filter(lambda filename: filename.endswith('.csv'), files)
        year_month = path.split('/')[-2]

        table_name = year_month

        table_id = find_file_id(service, u"title='{0}'".format(table_name))
        if not table_id:
            table_id = create_fusion_table(fusion_client, table_name, FUSION_COLUMNS)['tableId']
            for filename in csvs:
                path_to_file = os.path.join(path, filename)
                import_rows(fusion_client, table_id, path_to_file)

            make_public(service, table_id)
            add_to_folder(service, table_id, public_folder_id)
            logging.info(u"Added table {0} for {1}".format(table_id, table_name))

        # Handle styles -- TODO only create if none
        set_fusion_style(fusion_client, table_id, fusion_table_style)

# Generate a mapping of file IDs to file names from public folder for
# multi-year queries.
files = service.files().list(q="'" + public_folder_id + "' in parents").execute()['items']
mapping = dict([(f['title'], f['id']) for f in files])

json.dump(mapping,
          open(os.path.join(PATH_TO_DATA, 'fusion_table_mapping.json'), 'w'),
          indent=4)
