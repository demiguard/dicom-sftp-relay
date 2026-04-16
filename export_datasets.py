
import argparse
from pathlib import Path


import paramiko
from paramiko import client

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

pacs_ip = config['pacs-ip']
pacs_port = config['pacs-port']
pacs_ae = config['pacs-ae']

sftp_host = config["sftp-host"]
sftp_port = config["sftp-port"]
sftp_username = config["sftp-username"]
sftp_password = config["sftp-password"]

ssh_client = client.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

ssh_client.connect(
  hostname=sftp_host, port=sftp_port, username=sftp_username, password=sftp_password
)




ssh_client.close()
