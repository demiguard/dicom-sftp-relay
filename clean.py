#!/usr/env python

if __name__ != '__main__':
  print("This is a script!")
  exit(1)

try:
  import pydicom
except:
  error_message = """
This script requires pydicom to run. Run:

python3 -m venv venv
source venv/bin/activate
pip install pydicom

To resolve this issue

"""
  print(error_message)

  exit(1)


from pathlib import Path
import argparse

parser = argparse.ArgumentParser("clean_dicom", usage="clean_dicom directory")
parser.add_argument("dicom_directory", type=Path)

def clean_dicom(dicom_path: Path):
  dicom = pydicom.dcmread(dicom_path)

  if 0x0010_1002 in dicom:
    del dicom[0x0010_1002]
    pydicom.dcmwrite(dicom_path, dicom, enforce_file_format=True)


def handle_path(path: Path):
  for subpath in path.glob('*'):
    if subpath.is_dir():
      handle_path(subpath)
    if subpath.is_file():
      clean_dicom(subpath)