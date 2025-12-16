#!/usr/bin/env python3

# Python standard library
import argparse
import json
from io import BytesIO, IOBase
import os
from pathlib import Path

# Third party modules
import paramiko
from paramiko import client

from pydicom import Dataset, dcmwrite

from pynetdicom import evt
from pynetdicom.ae import ApplicationEntity
from pynetdicom.presentation import AllStoragePresentationContexts, VerificationPresentationContexts

# Internal modules
from lib import get_config, build_mapping, get_cpr

# Parsing
required_config_keys = [
  'ae-title',
  'port',
  'sftp-host',
  'sftp-port',
  'sftp-username',
  'sftp-password',
  'remote-directory-name',
  'data-file',
  'patient-id-key',
  'anno-name-key'
]

joined_keys = "\n".join(required_config_keys)

parser = argparse.ArgumentParser(
  prog="dicom-sftp-server",
  usage="dicom-sftp-server config.json",
  description="this program opens the server, that PACS can export data to. It'll forward the data to an sftp server")

parser.add_argument('config', type=Path, help=f"Path to json configuration file. The JSON file must have:\n{joined_keys}")

args = parser.parse_args()

config = get_config(args.config, required_config_keys)

ae_title      = config['ae-title']
port          = config['port']
sftp_host       = config['sftp-host']
sftp_port     = config['sftp-port']
sftp_username = config['sftp-username']
sftp_password = config['sftp-password']
data_file = config['data-file']
cpr_key = config['patient-id-key']
anno_key = config['anno-name-key']
remote_directory_name = config['remote-directory-name']

df = get_cpr(Path(data_file), cpr_key)
mapping = build_mapping(df, cpr_key, anno_key)

ae = ApplicationEntity(ae_title=ae_title)
ae.supported_contexts = AllStoragePresentationContexts + VerificationPresentationContexts

ssh_client = client.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Throws if unable to connect
ssh_client.connect(
  hostname=sftp_host, port=sftp_port, username=sftp_username, password=sftp_password
)

sftp_client = ssh_client.open_sftp()

try:
  sftp_client.mkdir(remote_directory_name)
except OSError:
  pass

def get_file_path_for_dataset(dataset: Dataset) -> Path:
  return Path(remote_directory_name) / str(dataset.PatientID) / (str(dataset.SOPInstanceUID) + '.dcm')

def handle_store(event):
  print("Got event")
  dataset: Dataset = event.dataset
  dataset.file_meta = event.file_meta

  try:
    new_patient_id = mapping[str(dataset.PatientID)]
    dataset.PatientID = new_patient_id
    dataset.PatientName = new_patient_id
  except Exception as e:
    print(f"Missing Patient ID: {dataset.PatientID}")
    return 0x0000

  dataset_path = get_file_path_for_dataset(dataset)
  try:
    sftp_client.mkdir(str(dataset_path.parent))
  except OSError:
    pass

  dicom_bytes = BytesIO()
  dcmwrite(dicom_bytes, dataset, False)
  sftp_client.putfo(dicom_bytes, str(dataset_path))
  print("Saved Dataset")

  return 0x0000

print(f"Opening server for ae: {ae_title}")

try:
  ae.start_server(
    ('0.0.0.0', 11112),

    evt_handlers=[
       (evt.EVT_C_STORE, handle_store)
     ]
  )
finally:
  sftp_client.close()
  ssh_client.close()
