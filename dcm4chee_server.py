#!/usr/bin/env python3

# Python standard library
import argparse
from json import load
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Iterable, Tuple

# Third party modules
from pydicom import Dataset
from pandas import read_csv
from pynetdicom import debug_logger

# Dicomnode
try:
  from dicomnode.config import DicomnodeConfig
  from dicomnode.data_structures.optional import OptionalPath
  from dicomnode.data_structures.image_tree import DicomTree
  from dicomnode.dicom import gen_uid, make_meta
  from dicomnode.dicom.dimse import Address
  from dicomnode.server.input_container import InputContainer
  from dicomnode.server.output import PipelineOutput, DicomOutput
  from dicomnode.server.grinders import DicomTreeGrinder
  from dicomnode.server.processor import AbstractProcessor
  from dicomnode.server.input import AbstractInput
  from dicomnode.server.nodes import DaemonPipeline


except ImportError as e:
  print("You need to do a pip install git+https://github.com/Rigshospitalet-KFNM/DicomNode.git")
  raise e

# Internal modules
import lib

#### ARGUMENTS ####
parser = argparse.ArgumentParser()
parser.add_argument('config', type=Path, help="Path to json file with all the config")
parser.add_argument('--verbose', '-v', action='store_true')

args = parser.parse_args()

if args.verbose:
  debug_logger()

config = lib.get_config(args.config, [
  'dcm-ip',
  'dcm-port',
  'dcm-ae',
  'data-file',
  'ae-title',
  'port'
])

data_frame = lib.get_cpr(config['data-file'], 'Personnummer')

mapping = {}

for x, row in data_frame.iterrows():
  dashed_cpr: str = row['Personnummer']
  cpr = "".join(dashed_cpr.split('-'))

  mapping[cpr] = row['Anonymized_Exam_ID_CT']

address = Address(
  config['dcm-ip'],
  config['dcm-port'],
  config['dcm-ae']
)


#### DICOMNODE ####

class HungryInput(AbstractInput):
  deadline = timedelta(seconds= 60 * 5)

  required_tags = ["SOPInstanceUID"]

  image_grinder = DicomTreeGrinder()

  def __init__(self, config: DicomnodeConfig, node_path):
    super().__init__(config, node_path)

    self.last_added_image = datetime.now()

  def add_image(self, dicom: Dataset) -> int:
    self.last_added_image = datetime.now()
    return super().add_image(dicom)


  def validate(self) -> bool:
    return self.deadline < datetime.now() - self.last_added_image


class AnnoProcessor(AbstractProcessor):
  def process(self, input_container: InputContainer) -> PipelineOutput:
    dicom_tree: DicomTree = input_container['patient_data']

    datasets = []

    for patient_tree in dicom_tree.patients():
      if patient_tree.PatientID in mapping:
        new_patient_id = mapping[patient_tree.PatientID]
      else:
        self.logger.error(f"Could not find {patient_tree.PatientID}")
        continue

      for studies in patient_tree.studies():
        new_study_id = gen_uid()

        for series in studies.series():
          new_series_id = gen_uid()

          for ds in series:
            ds.SOPInstanceUID = gen_uid()
            ds.SeriesInstanceUID = new_series_id
            ds.StudyInstanceUID = new_study_id

            ds.AccessionNumber = new_patient_id
            ds.PatientName = new_patient_id
            ds.PatientID = new_patient_id
            ds.StudyID = new_patient_id

            make_meta(ds)

            datasets.append(ds)


    return DicomOutput([(address, datasets)], config['ae-title'])

class AnnoPipeline(DaemonPipeline):
  Processor = AnnoProcessor

  log_output = "anno_pipeline.log"
  require_called_aet = False

  ae_title = config['ae-title']
  port = config['port']

  input = {
    "patient_data" : HungryInput
  }

if __name__ == '__main__':
  pipeline = AnnoPipeline()
  pipeline.open()