#!/usr/bin/env python3

# Python standard library
import argparse
import json
import datetime
from pathlib import Path
# Third party modules
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind, PatientRootQueryRetrieveInformationModelFind # type: ignore
from pynetdicom import debug_logger

from lib import get_cpr, get_ae, associate, get_baseline_query_dataset, get_config, build_info_mapping

required_config_keys = [
  'ae-title',
  'data-file',
  'patient-id-key'
]



parser = argparse.ArgumentParser("find_dicom", usage="find_dicom config.json")

parser.add_argument('config_path', help="Path to config", type=Path)
parser.add_argument('--verbose', '-v', action='store_true')

args = parser.parse_args()

if args.verbose:
  debug_logger()

config = get_config(args.config_path, required_config_keys)

pacs_ip = config['pacs-ip']
pacs_port = config['pacs-port']
pacs_ae = config['pacs-ae']

ae_title = config['ae-title']
data_path = config['data-file']
cpr_key = config['patient-id-key']

patient_data = get_cpr(data_path, cpr_key)
ae = get_ae(ae_title)

max_queries = 500

found = 0
queries = 0

with associate(ae, pacs_ip, pacs_port, pacs_ae) as assoc:
  for x,y in patient_data.iterrows():
    ds = get_baseline_query_dataset()
    ds.PatientID = y[cpr_key]
    ds.Modality = 'CT'


    if queries == 0:
        print(ds)

    #ds.Modality = "EPS"
    #ds.StudyDate = "".join(getattr(y, 'ProcedureStartDate').split('-'))
    #ds.StudyDate = datetime.datetime.strptime(y.ct_date, "%Y-%m-%d")

    response = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
    queries += 1
    has_found = False

    for (status, b) in response:
      if b is not None:
        has_found = True
    if has_found:
      found += 1

    #print(f"Found {ds.PatientID} - {has_found}")
    if not (queries < max_queries):
        break
#    break

print(f"found: {found} / {queries}")
