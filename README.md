# Generic Buy Now, Pay Later Project
Groups should generate their own suitable `README.md`.

Note to groups: Make sure to read the `README.md` located in `./data/README.md` for details on how the weekly datasets will be released.

Run the code in following order
1. Run download.py in script folder, command line in terminal should follow the format of:  python3 ../generic-buy-now-pay-later-project-group-45/scripts/download.py --path ../generic-buy-now-pay-later-project-group-45/data/tables

2. Run preprocess.py in script folder, command line in terminal should follow the format of(need to fill the working directory of the script and data folder): 

  python3 ../generic-buy-now-pay-later-project-group-45/scripts/preprocess.py --path ../generic-buy-now-pay-later-project-group-45/data/tables --output ../generic-buy-now-pay-later-project-group-45/data/curated

3. Run ranking.py in script folder, command line in terminal should follow the format of(need to fill the working directory of the script and data folder): 

  python3 ../generic-buy-now-pay-later-project-group-45/scripts/ranking.py --path ../generic-buy-now-pay-later-project-group-45/data/curated

  (note: .. indicates directory to this project folder, needs to be added, otherwise this script will be wrong)

4. Run plots.ipynb to produce plots used in presentation, visualisation of final ranked merchants

5. Run geoplot.ipynb to produce geospatial plot
