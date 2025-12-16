import argparse
import json
from io import BytesIO, IOBase
import os
from pathlib import Path

# Third party modules
from paramiko import client

from pydicom import Dataset, dcmwrite

from pynetdicom import evt
from pynetdicom.ae import ApplicationEntity
from pynetdicom.presentation import AllStoragePresentationContexts, VerificationPresentationContexts

# Internal modules
from lib import get_config

# Parsing
required_config_keys = [
  'ae_title'
  'port',
  'sftp-host',
  'sftp-port',
  'sftp-username',
  'sftp-password',
  'remote-directory-name'
]

joined_keys = "\n".join(required_config_keys)

parser = argparse.ArgumentParser(
  prog="check-sftp-server",
  usage="check-sftp-server config.json",
  description="Checks if you can connect to the sftp server")

parser.add_argument('config', type=Path, help=f"Path to json configuration file. The JSON file must have:\n{joined_keys}")

args = parser.parse_args()

config = get_config(args.config, required_config_keys)

sftp_host       = config['sftp-host']
sftp_port     = config['sftp-port']
sftp_username = config['sftp-username']
sftp_password = config['sftp-password']

ssh_client = client.SSHClient()
ssh_client.connect(
  hostname=sftp_host, port=sftp_port, username=sftp_username, password=sftp_password
)
sftp_client = ssh_client.open_sftp()

sftp_client.close()
ssh_client.close()
