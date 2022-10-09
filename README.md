# Generic Buy Now, Pay Later Project
Groups should generate their own suitable `README.md`.

Note to groups: Make sure to read the `README.md` located in `./data/README.md` for details on how the weekly datasets will be released.

Run the code in following process
1. Run preprocess.py in script folder, command line in terminal should follow the format of(need to fill the working directory of the script and data folder): 

python3 ../generic-buy-now-pay-later-project-group-45/script/preprocess.py --path ../generic-buy-now-pay-later-project-group-45/data/tables --output ../generic-buy-now-pay-later-project-group-45/data/curated/

2. Run ranking.py in script folder, command line in terminal should follow the format of(need to fill the working directory of the script and data folder): 

python3 ../prject/script/ranking.py --path ../project/data/curated
