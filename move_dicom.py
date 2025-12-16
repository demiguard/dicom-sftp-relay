#!/usr/bin/env python3

# Python standard library
import argparse
import json
import datetime
from pathlib import Path

# Third party modules
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove # type: ignore

from lib import get_cpr, get_ae, associate, get_baseline_query_dataset, get_config

required_config_keys = [
  'ae-title',
  'data-file',
  'patient-id-key',
  'server-ae-title'
]

parser = argparse.ArgumentParser("find_dicom", usage="move_dicom config.json", )

parser.add_argument('config_path', help="Path to config", type=Path)

args = parser.parse_args()

config = get_config(args.config_path, required_config_keys)

ae_title = config['ae-title']
data_path = config['data_file']
cpr_key = config['patient-id-key']
server_ae = config['server-ae-title']

patient_data = get_cpr(data_path, cpr_key)
ae = get_ae(ae_title)

with associate(ae) as assoc:
  for x,y in patient_data.iterrows():
    ds = get_baseline_query_dataset()

    ds.PatientID = getattr(y, cpr_key)
    #ds.StudyDate = datetime.datetime.strptime(y.ct_date, "%Y-%m-%d")

    response = assoc.send_c_move(ds, server_ae, StudyRootQueryRetrieveInformationModelMove)

    for (status, b) in response:
      if 'Status' in status:
        print(hex(status.Status))

    print(f"Moved {ds.PatientID}")