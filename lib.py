import  contextlib

import pandas

from pydicom import Dataset

from pynetdicom.ae import ApplicationEntity as AE
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove, StudyRootQueryRetrieveInformationModelFind

AE_TITLE = "LIGHTHOUSE"

PACS_IP = "10.145.5.63"
PACS_PORT = 7840
PACS_AE = "DICOM_QR_SCP"

def get_cpr():
  return pandas.read_csv('cohort_cysts_ct_1to1_matched_cpr.tsv', sep='\t', dtype={'cpr' : str})


def get_ae():
  ae = AE(ae_title=AE_TITLE)
  ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
  ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)

  return ae

@contextlib.contextmanager
def associate(ae):
  assoc = ae.associate(
    PACS_IP,
    PACS_PORT,
    ae_title=PACS_AE
  )
  try:
    yield assoc
  finally:
    assoc.release()


def get_baseline_find_dataset():
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