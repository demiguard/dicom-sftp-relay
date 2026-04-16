# Python standard library
import argparse
import stat
from pathlib import Path

# Third party modules
from pynetdicom.ae import ApplicationEntity
import paramiko
from paramiko import client

# My code
from lib import get_config

required_config_keys = [
  "sftp-host",
  "sftp-port",
  "sftp-username",
  "sftp-password",
  "pacs-ip",
  "pacs-port",
  "pacs-ae",
  "ae-title"
]

parser = argparse.ArgumentParser("find_dicom", usage="find_dicom config.json")

parser.add_argument('config_path', help="Path to config", type=Path)
parser.add_argument('--verbose', '-v', action='store_true')

args = parser.parse_args()

config = get_config(args.config_path, required_config_keys)

AE_TITLE = config['ae-title']

pacs_ip = config['pacs-ip']
pacs_port = config['pacs-port']
pacs_ae = config['pacs-ae']

sftp_host = config["sftp-host"]
sftp_port = config["sftp-port"]
sftp_username = config["sftp-username"]
sftp_password = config["sftp-password"]

ssh_client = client.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

BASE_DIRECTORY = "ct_scan"


ssh_client.connect(
  hostname=sftp_host, port=sftp_port, username=sftp_username, password=sftp_password
)
sftp_client = ssh_client.open_sftp()

def yield_files(base: str):
  directories = sftp_client.listdir(base)

  for file_ in directories:
    absolute_path = f"{base}/{file_}"
    st_mode = sftp_client.lstat(absolute_path).st_mode
    if st_mode is None:
      print(f"Skipping {file_} as its st_mode is None")
      continue


    if stat.S_ISDIR(st_mode):
      yield from yield_files(absolute_path)
    else:
      yield absolute_path

files_to_be_print = 5

try:
  files_printed = 0

  for a_file in yield_files(BASE_DIRECTORY):
    print(a_file)
    files_printed += 1
    if files_to_be_print <= files_printed:
      break


finally:
  sftp_client.close()


ssh_client.close()
