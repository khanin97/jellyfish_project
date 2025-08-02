import os
import xarray as xr
import pandas as pd

def convert_zooplankton_nc_to_csv():
    folder = "csv/zooplankton"
    for fname in os.listdir(folder):
        if fname.endswith(".nc"):
            path = os.path.join(folder, fname)
            ds = xr.open_dataset(path)
            df = ds.to_dataframe().reset_index()
            csv_path = path.replace(".nc", ".csv")
            df.to_csv(csv_path, index=False)
            print(f"✅ Converted zooplankton: {csv_path}")
