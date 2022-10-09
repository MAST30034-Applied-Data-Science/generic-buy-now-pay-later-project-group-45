import urllib.request
from zipfile import ZipFile
from io import BytesIO
import sys
import re

# read command line input, and store working directories for data folder and export folder
directory = []
for i in sys.argv:
    if re.match(r"[\/\w+]+", i):
       directory.append(i)

data_directory = directory[1]


################## Download census data #####################

ABS_INCOME_URL = "https://www.abs.gov.au/census/find-census-data/datapacks/download/2021_GCP_POA_for_AUS_short-header.zip"
ABS_INCOME_URL_STATE = "https://www.abs.gov.au/census/find-census-data/datapacks/download/2021_GCP_STE_for_AUS_short-header.zip"
shape_file_url = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/POA_2021_AUST_GDA2020_SHP.zip"

shape_file_directory = data_directory+"/POA_2021_AUST_GDA2020_SHP"
ABS_DATA_Dir = data_directory+"/censusData"

# read the zip file, unzip the folder and export files in censusData folder
with urllib.request.urlopen(ABS_INCOME_URL) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall(ABS_DATA_Dir)
        
# read the zip file, unzip the folder and export files in censusData folder
with urllib.request.urlopen(ABS_INCOME_URL_STATE) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall(ABS_DATA_Dir)

# read the zip file, unzip the folder and export files in censusData folder
with urllib.request.urlopen(shape_file_url) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall(shape_file_directory)

print("Successfully downloaded External dataset!")
