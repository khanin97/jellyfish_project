import os

def debug_env():
    print("✅ CMEMS_USERNAME =", os.environ.get("CMEMS_USERNAME"))
    print("✅ CMEMS_PASSWORD =", os.environ.get("CMEMS_PASSWORD"))
