# Generic Buy Now, Pay Later Project

## Group 45
#### Team Member:
Aobo Li, Jialiang Shen, Jiqiang Chen, Junkai Zhang, Ying Zhu

**Research Goal:** The research goal is to help the BNPL company to find the best merchants to cooperate with by forecasting the expected revenue of each merchants in the following year. 

**Instruction:**
Run the code in following order:
1. Run download.py in script folder, command line in terminal should follow the format of:  

    python3 ../generic-buy-now-pay-later-project-group-45/scripts/download.py --path ../generic-buy-now-pay-later-project-group-45/data/tables
    
    _note: ".." is the directory path of this project folder, which need to be modified according to your local repository._

2. Run preprocess.py in script folder, command line in terminal should follow the format of: 

    python3 ../generic-buy-now-pay-later-project-group-45/scripts/preprocess.py --path ../generic-buy-now-pay-later-project-group-45/data/tables --output ../generic-buy-now-pay-later-project-group-45/data/curated

3. Run geoplot.ipynb to produce geospatial plot

4. Run ranking.py in script folder, command line in terminal should follow the format of: 

    python3 ../generic-buy-now-pay-later-project-group-45/scripts/ranking.py --path ../generic-buy-now-pay-later-project-group-45/data/curated dddd-dd-dd
    
    _note: dddd-dd-dd is the most recent annual transaction data start date, can also put other dates if period of data do not exceed a year, e.g: most recent data end in 2022-08-28, then type in 2021-08-28_

5. Run plots.ipynb to produce plots used in presentation and visualisation of final ranked merchants

6. Findings of this project is shown in the summary.ipynp in notebook folder






Note: 

1. turnover data is downloaded at: https://www.abs.gov.au/statistics/economy/business-indicators/monthly-business-turnover-indicator/jul-2022#data-download

2. records of the scripts and notebooks are stored in previous_notebook folder in notebook folder
