import json


with open('latest_cbr_data.json', 'r') as f:
    latest_cbr_data = json.load(f)

with open('latest_kremlin_data.json', 'r') as f:
    latest_kremlin_data = json.load(f)

with open('latest_roskazna_data.json', 'r') as f:
    latest_roskazna_data = json.load(f)


print([key for key in latest_cbr_data[0].keys()])
print([key for key in latest_kremlin_data[0].keys()])
print([key for key in latest_roskazna_data[0].keys()])