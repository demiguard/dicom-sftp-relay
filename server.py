from pathlib import Path
import os

from pydicom import dcmwrite

from pynetdicom import evt
from pynetdicom.ae import ApplicationEntity
from pynetdicom.presentation import AllStoragePresentationContexts, VerificationPresentationContexts

ae = ApplicationEntity(ae_title="LIGHTHOUSE")

ae.supported_contexts = AllStoragePresentationContexts + VerificationPresentationContexts

def handle_store(event):
  dataset = event.dataset
  dataset.file_meta = event.file_meta

  ds_dir_path = Path(os.getcwd()) / str(dataset.StudyInstanceUID) / str(dataset.SeriesInstanceUID)
  ds_dir_path.mkdir(parents=True, exist_ok=True)

  ds_file_path = ds_dir_path / (str(dataset.InstanceNumber)+".dcm")

  dcmwrite(ds_file_path, dataset, False)
  #print(f"saved {ds_file_path}")

  return 0x0000

print("Opening server")

ae.start_server(
  ('0.0.0.0', 11112),

  evt_handlers=[
     (evt.EVT_C_STORE, handle_store)
   ]
)
