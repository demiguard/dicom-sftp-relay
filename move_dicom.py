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
import paramiko
from paramiko import client
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove # type: ignore

from lib import get_cpr, get_ae, associate, get_baseline_query_dataset,\
  get_config, build_mapping

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

args = parser.parse_args()

config = get_config(args.config_path, required_config_keys)

pacs_ip = config['pacs-ip']
pacs_port = config['pacs-port']
pacs_ae = config['pacs-ae']

ae_title = config['ae-title']
data_path = config['data-file']
cpr_key = config['patient-id-key']
anno_key = config['anno-name-key']
server_ae = config['ae-title']

remote_directory = config['remote-directory-name']

sftp_host     = config['sftp-host']
sftp_port     = config['sftp-port']
sftp_username = config['sftp-username']
sftp_password = config['sftp-password']

patient_data_frame = get_cpr(data_path, cpr_key)
ae = get_ae(ae_title)

mapping = build_mapping(patient_data_frame, cpr_key, anno_key)

handled_patients = 0

ssh_client = client.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

ssh_client.connect(
  hostname=sftp_host, port=sftp_port, username=sftp_username, password=sftp_password
)

def already_send(cpr):
  serial_number = mapping[cpr]
  path = f"{remote_directory}/{serial_number}"

  sftp_client = ssh_client.open_sftp()

  try:
    try:
      file_stat = sftp_client.stat(path)
      return True
    except FileNotFoundError:
      return False
  finally:
     sftp_client.close()


c_move_time = []

try:
  with associate(ae, pacs_ip, pacs_port, pacs_ae) as assoc:
    for x, row in patient_data_frame.iterrows():
      cpr = getattr(row, cpr_key)

      start = time.time()

      handled_patients += 1
      if already_send(cpr):
        continue

      ds = get_baseline_query_dataset()

      ds.PatientID = cpr
      #ds.StudyDate = datetime.datetime.strptime(y.ct_date, "%Y-%m-%d")
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
          for (status, b) in response:
            pass

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
        print(f"Moved {cpr}")
      else:
        print(f"Failed {cpr}")


      c_move_time.append(end - start)


except Exception as E:
  handled_patients = max(0, handled_patients - 1)
  print(f"Unexpected exit! - {E}")
  print(traceback.format_exc())
finally:
  ssh_client.close()

print(f"Finished {handled_patients}/{patient_data_frame.shape[0]}")
if len(c_move_time):
  numpy_c_move_time = numpy.array(c_move_time)
  print(f"mean: {numpy_c_move_time.mean()}")
  print(f"median: {numpy.median(numpy_c_move_time)}")
  print(f"std dev: {numpy_c_move_time.std()}")
