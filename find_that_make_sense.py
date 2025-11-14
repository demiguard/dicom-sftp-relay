from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind, PatientRootQueryRetrieveInformationModelFind # type: ignore

import datetime
from lib import get_cpr, get_ae, associate, get_baseline_find_dataset



cprs = get_cpr()

ae = get_ae()

i = 0

with associate(ae) as assoc:

  for x,y in cprs.iterrows():
    ds = get_baseline_find_dataset()

    ds.PatientID = y.cpr
    #ds.StudyDate = datetime.datetime.strptime(y.ct_date, "%Y-%m-%d")
    print(ds.PatientID)

    reponse = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)

    for (a, b) in reponse:
      print(a, b)

    i += 1

    if 100 < i:
      break
