from copernicusmarine import subset
from datetime import datetime
import os

def download_salinity():
    output_dir = "/opt/airflow/nc/salinity"

    # ❗ ไม่พยายามสร้างโฟลเดอร์ — ถ้าไม่มี ถือว่า host setup ไม่พร้อม
    if not os.path.exists(output_dir):
        raise FileNotFoundError(
            f"❌ Directory not found: {output_dir}\n"
            f"💡 Please create it on the host machine under ./airflow/nc/salinity before running."
        )

    provinces = {
        'rayong_1': dict(lat=slice(12.4723, 12.5566), lon=slice(100.8553, 101.4210)),
        'rayong_2': dict(lat=slice(12.4723, 12.6146), lon=slice(101.4210, 101.7801)),
        'chanthaburi_1': dict(lat=slice(12.1788, 12.6146), lon=slice(101.7801, 102.1470)),
        'trat_1': dict(lat=slice(11.4615, 12.1788), lon=slice(102.1470, 102.6305)),
    }

    start_date = "2022-06-01T00:00:00"
    end_date = datetime.today().strftime("%Y-%m-%dT00:00:00")

    dataset_id = "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m"
    variable = "so"

    for province, bounds in provinces.items():
        output_file = os.path.join(output_dir, f"{province}_salinity.nc")
        if os.path.exists(output_file):
            os.remove(output_file)
        subset(
            username=os.environ["CMEMS_USERNAME"],
            password=os.environ["CMEMS_PASSWORD"],
            dataset_id=dataset_id,
            variables=[variable],
            minimum_longitude=bounds['lon'].start,
            maximum_longitude=bounds['lon'].stop,
            minimum_latitude=bounds['lat'].start,
            maximum_latitude=bounds['lat'].stop,
            start_datetime=start_date,
            end_datetime=end_date,
            output_filename=output_file,
            force_download=True
        )
        print(f"✅ Downloaded salinity: {output_file}")
