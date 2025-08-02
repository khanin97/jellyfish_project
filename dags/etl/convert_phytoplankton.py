import os
import xarray as xr
import pandas as pd

def convert_phytoplankton_nc_to_csv():
    input_folder = "/opt/airflow/nc/phytoplankton"
    output_folder = "/opt/airflow/csv/phytoplankton"
    os.makedirs(output_folder, exist_ok=True)

    for fname in os.listdir(input_folder):
        if fname.endswith(".nc"):
            path = os.path.join(input_folder, fname)
            ds = xr.open_dataset(path)
            df = ds.to_dataframe().reset_index()
            csv_path = os.path.join(output_folder, fname.replace(".nc", ".csv"))
            df.to_csv(csv_path, index=False)
            print(f"✅ Converted phytoplankton: {csv_path}")
