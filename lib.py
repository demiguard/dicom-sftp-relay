# Python standard library
import contextlib
import json
from pathlib import Path
from typing import Generator, List


# Third party
import pandas

from pydicom import Dataset
from pynetdicom.association import Association
from pynetdicom.ae import ApplicationEntity as AE
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove, StudyRootQueryRetrieveInformationModelFind # type: ignore


PACS_IP = "10.145.5.63"
PACS_PORT = 7840
PACS_AE = "DICOM_QR_SCP"

def get_cpr(data_path: Path, cpr_key):
  if str(data_path).endswith('tsv'):
    return pandas.read_csv(data_path, sep='\t', dtype={ cpr_key : str})
  else:
    return pandas.read_csv(data_path, dtype={ cpr_key : str})



def get_ae(AE_TITLE):
  ae = AE(ae_title=AE_TITLE)
  ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
  ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)

  return ae

@contextlib.contextmanager
def associate(ae) -> Generator[Association, None, None]:
  assoc = ae.associate(
    PACS_IP,
    PACS_PORT,
    ae_title=PACS_AE
  )
  try:
    yield assoc
  finally:
    assoc.release()


def get_baseline_query_dataset():
  ds = Dataset()
  ds.SpecificCharacterSet = 'ISO_IR 100'
  ds.QueryRetrieveLevel = "STUDY"
  #ds.Modality = "CT"

  ds.StudyInstanceUID = ''
  ds.StudyDate = ''
  ds.StudyTime = ''
  ds.AccessionNumber = ''
  ds.StudyDescription = ''
  ds.StudyID = ''

  return ds


def get_config(config_path: Path, required_config_keys: List[str]):
  if not config_path.exists():
    raise Exception(f"Config path: {config_path.absolute()} doesn't exists!")

  try:
    with config_path.open("r") as config_fp:
      config = json.loads(config_fp.read())
  except Exception as exp:
    print("Config wasn't a JSON file")
    raise exp

  for required_key in required_config_keys:
    if required_key not in config:
      raise Exception(f"{required_key} is missing from the config")

  return config