#!/usr/bin/env python3

# Python standard library
import argparse
import json
import datetime
from pathlib import Path
import traceback

# Third party modules
import time
import numpy
from pynetdicom import debug_logger
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove # type: ignore

from lib import get_cpr, get_ae, associate, get_baseline_query_dataset,\
  get_config, build_mapping
import lib

required_config_keys = [
  'ae-title',
  'data-file',
  'patient-id-key'
]

## MOVE THIS TO CONFIG
PACS_IP = "10.145.5.63"
PACS_PORT = 7840
PACS_AE = "DICOM_QR_SCP"


parser = argparse.ArgumentParser("find_dicom", usage="move_dicom config.json", )

parser.add_argument('config_path', help="Path to config", type=Path)
parser.add_argument('--max-datasets', '-md', default=0, type=int)
parser.add_argument('--verbose', '-v', action='store_true')

args = parser.parse_args()

config = get_config(args.config_path, required_config_keys)

pacs_ip = config['pacs-ip']
pacs_port = config['pacs-port']
pacs_ae = config['pacs-ae']

ae_title = config['ae-title']
data_path = config['data-file']
cpr_key = config['patient-id-key']
server_ae = config['ae-title']
accession_key = config['accession-key'] if 'accession-key' in config else None

patient_data_frame = get_cpr(data_path, cpr_key, config['sep'])
ae = get_ae(ae_title)

datasets_to_handle = args.max_datasets if args.max_datasets > 0 else patient_data_frame.shape[0]
handled_patients = 0

c_move_time = []

if args.verbose:
  debug_logger()

def map_cpr(cpr: str):
  return "".join(cpr)

query_generator = lib.QueryDatasetGenerator(patient_data_frame, cpr_key, accession_key)

try:
  with associate(ae, pacs_ip, pacs_port, pacs_ae) as assoc:
    uids = lib.find_uids(assoc, query_generator)

    for ds in query_generator:
      for study_uid, series_uid in uids[ds.PatientID]:
        start = time.time()

        ds.StudyInstanceUID = study_uid
        ds.SeriesInstanceUID = series_uid

        if not assoc.is_established:
          assoc = ae.associate(
            PACS_IP,
            PACS_PORT,
            ae_title=PACS_AE
          )
        attempts = 0
        while attempts < 5:
          try:
            response = assoc.send_c_move(ds, server_ae, StudyRootQueryRetrieveInformationModelMove)
            accepted_datasets = 0
            failed_datasets = 0
            for (status, b) in response:
              if args.verbose:
                print(status)

              if 0x0000_1021 in status:
                accepted_datasets = status[0x0000_1021].value

              if 0x0000_1022 in status:
                failed_datasets = status[0x0000_1022].value

            if failed_datasets > 0:
              print(f"Failed to send {failed_datasets} for {ds.PatientID}")

            if accepted_datasets < 100:
              print(f"Somehow we only found {accepted_datasets} for {ds.PatientID}")

            break
          except Exception:
            print("Failed to send re-etablishing")
            attempts += 1
            time.sleep( 2 ** attempts)

            if not assoc.is_established:
              assoc = ae.associate(
                PACS_IP,
                PACS_PORT,
                ae_title=PACS_AE
              )

        end = time.time()

        if attempts < 5:
          print(f"Moved {ds.PatientID}")
        else:
          print(f"Failed {ds.PatientID}")

        c_move_time.append(end - start)
        if handled_patients >= datasets_to_handle:
          break


except Exception as E:
  handled_patients = max(0, handled_patients - 1)
  print(f"Unexpected exit! - {E}")
  print(traceback.format_exc())

print(f"Finished {handled_patients}/{patient_data_frame.shape[0]}")
if len(c_move_time):
  numpy_c_move_time = numpy.array(c_move_time)
  print(f"mean: {numpy_c_move_time.mean()} s")
  print(f"median: {numpy.median(numpy_c_move_time)} s")
  print(f"std dev: {numpy_c_move_time.std()} ")
