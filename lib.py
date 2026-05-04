# Python standard library
import contextlib
import json
from pathlib import Path
from typing import Dict, Iterable, Generator, List, Set, Tuple


# Third party
import pandas

from dicomnode.data_structures.defaulting_dict import DefaultingDict

from pydicom import Dataset
from pydicom.uid import UID
from pynetdicom.association import Association
from pynetdicom.ae import ApplicationEntity as AE
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove, StudyRootQueryRetrieveInformationModelFind # type: ignore

# Heart PACS
PACS_IP = "10.145.5.63"
PACS_PORT = 7840
PACS_AE = "DICOM_QR_SCP"

def get_cpr(data_path: Path, cpr_key, sep='\t'):
  return pandas.read_csv(data_path, sep=sep, dtype={ cpr_key : str })



def get_ae(AE_TITLE):
  ae = AE(ae_title=AE_TITLE)
  ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
  ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)

  return ae

@contextlib.contextmanager
def associate(ae, pacs_ip, pacs_port, pacs_ae) -> Generator[Association, None, None]:
  assoc: Association = ae.associate(
    pacs_ip,
    pacs_port,
    ae_title=pacs_ae
  )
  try:
    yield assoc
  finally:
    print("Releasing")
    assoc.release()


def get_baseline_query_dataset():
  ds = Dataset()
  ds.SpecificCharacterSet = 'ISO_IR 100'
  ds.QueryRetrieveLevel = "STUDY"
  ds.Modality = ""
  #ds.Modality = "CT"

  ds.StudyInstanceUID = ''
  ds.SeriesInstanceUID = ''
  ds.StudyDate = ''
  ds.StudyTime = ''
  ds.AccessionNumber = ''
  ds.StudyDescription = ''
  ds.StudyID = ''

  return ds

def build_mapping(df: pandas.DataFrame, cpr_key, anno_key):
  mapping = {}
  for x, row in df.iterrows():
    mapping[row[cpr_key]] = row[anno_key]

  return mapping

def build_info_mapping(df: pandas.DataFrame, cpr_key):
  mapping = {}

  for x, row in df.iterrows():
    mapping[row[cpr_key]] = row

  return mapping


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

def safe_del(dataset: Dataset, tag):
  if tag in dataset:
    del dataset[tag]

def anonymise_dataset(dataset: Dataset):
  safe_del(dataset, 0x0010_0010)
  safe_del(dataset, 0x0010_1040)
  safe_del(dataset, 0x0010_1002)

def map_cpr(cpr: str):
  return "".join(cpr.split('-'))

class QueryDatasetGenerator:
  def __init__(self, dataframe: pandas.DataFrame, cpr_key, accession_key = None, study_date_key = None) -> None:
    self.dataframe = dataframe
    self.cpr_key = cpr_key
    self.accession_key = accession_key
    self.study_date_key = study_date_key

  def __iter__(self):
    for x, row in self.dataframe.iterrows():
      ds = get_baseline_query_dataset()

      ds.PatientID = map_cpr(row[self.cpr_key])

      if self.accession_key is not None:
        ds.AccessionNumber = row[self.accession_key]

      if self.study_date_key is not None:
        ds.StudyDate = row[self.study_date_key]

      yield ds


def create_list():
  return list()

def find_uids(assoc: Association, dataset_generator: Iterable[Dataset]):
  found_uids: DefaultingDict[str, List[Tuple[UID, UID]]] = DefaultingDict(create_list)

  for dataset in dataset_generator:
    response = assoc.send_c_find(dataset, StudyRootQueryRetrieveInformationModelFind)

    for status, found_dataset in response:
      if found_dataset is not None:
        found_uids[found_dataset.PatientID].append((found_dataset.StudyInstanceUID, found_dataset.SeriesInstanceUID))
        print("\n")
        print(dataset)
        print("\n")
        print(found_dataset)
        print("\n")

  return found_uids
