# Fairair

## Contributors

Mark Paluta
[<img src="https://github.com/favicon.ico" width="20">](https://github.com/mpaluta)
[<img src="https://www.linkedin.com/favicon.ico" width="20">](https://www.linkedin.com/in/markpaluta/)  
Ben Arnoldy
[<img src="https://github.com/favicon.ico" width="20">](https://github.com/arnoldyb)
[<img src="https://www.linkedin.com/favicon.ico" width="20">](https://www.linkedin.com/in/benarnoldy/)    
Angshuman Paul
[<img src="https://www.linkedin.com/favicon.ico" width="20">](https://www.linkedin.com/in/angshumanpaul/)    
Jake Miller
[<img src="https://www.linkedin.com/favicon.ico" width="20">](https://www.linkedin.com/in/carrolljmiller/)

Sameed Musvee 
[<img src="https://www.linkedin.com/favicon.ico" width="20">](https://www.linkedin.com/in/sameedmusvee/) 

Group inbox:
[contact.fairair@gmail.com](mailto:contact.fairair@gmail.com)

## Project description

FairAir shows environmental activists where air pollution is likely to be worst in a city and recommends where to deploy low-cost sensors to improve pollution mapping. Sensors are currently clustered in wealthier neighborhoods. By using a random forest machine learning model to create "virtual sensors," FairAir can highlight where new monitoring and remediation are most needed.  

## Fairair interactive map

[https://fairair.netlify.com/](https://fairair.netlify.com/)

## Data Ingestion

### Sources
This project uses data from the following sources:
- [Purple Air](https://www2.purpleair.com)
- [National Oceanic and Atmospheric Administration](https://www.noaa.gov)
- [Air Now](https://docs.airnowapi.org)
- [Thingspeak](https://www.mathworks.com/help/thingspeak/index.html)
- [US Geological Survey](https://www.usgs.gov)
- [California Air Resources Board](https://www.arb.ca.gov)

### Ingestion Steps
- Run scripts in [Amazon EC2](https://aws.amazon.com/ec2/) instances to ingest raw data for Purple Air sensor and NOAA wind data and store them in [Amazon S3](https://aws.amazon.com/s3/) after initial processing.
- Download known polluters and topography & land use data and store them in S3.
- Run scripts in [AWS Lambda](https://aws.amazon.com/lambda/) to ingest particulate data from Thingspeak and ambient data from Airnow.
- Combine the ingested data from all the sources and store the output in s3 for use by ML models

### Setup
All the scripts related for data ingestion and related setup are in the [Data Automation](https://github.com/arnoldyb/air_pollution/tree/master/DataAutomation) folder.
- **Amazon EC2 Bootstrap** -
The [bootstrap script](https://github.com/arnoldyb/air_pollution/blob/master/DataAutomation/ec2-bootstrap.sh) is used to create the vitual environment and install the required python packages. This script is run as `User data` when configuring the EC2 instance. This script also installs [tmux](https://github.com/tmux/tmux/wiki) which allows us to run several programs in the instance and keep them running in the background.
- **Raw Purple Air Data** -
The [getPAData.py](https://github.com/arnoldyb/air_pollution/blob/master/DataAutomation/purpleairraw/getPAData.py) is used to ingest raw sensor data from Purple Air every 5 minutes and store it in S3. This script runs in a tmux window in the EC2 instance.
- **Purple Air Data Daily Consolidation** -
The [dailyproc.sh](https://github.com/arnoldyb/air_pollution/blob/master/DataAutomation/purpleairdaily/dailyproc.sh) script is configured in *crontab* to consolidate the purple air raw data files at a daily level. The cron job is scheduled to run at 3 AM Pacific every day. The shell script invokes [dailyproc.py](https://github.com/arnoldyb/air_pollution/blob/master/DataAutomation/purpleairdaily/dailyproc.py) for processing the data and storing it in S3.
- **Raw NOAA Data** -
The [dailyNOAAPull.sh](https://github.com/arnoldyb/air_pollution/blob/master/DataAutomation/noaa/dailyNOAAPull.sh) script is used to ingest the wind data from NOAA. This script is configured in crontab to run at 10:30 AM Pacific every day.
- **Raw Ambient Data** -
The [epaapp.py](https://github.com/arnoldyb/air_pollution/blob/master/DataAutomation/epa/src/epaapp.py) script runs on AWS Lambda and is used to ingest the raw ambient data. The [EPA lambda deployment scripts](https://github.com/arnoldyb/air_pollution/tree/master/DataAutomation/epa/lambda) are used to deploy the python script in AWS. This script is scheduled in [AWS Cloudwatch](https://aws.amazon.com/cloudwatch/) to run at 20 minutes past every hour to ingest the data for the previous hour and store it in S3.
- **Thingspeak Data Ingestion** -
Functions in the [thingSpeak.py](https://github.com/arnoldyb/air_pollution/blob/master/DataAutomation/all_sources/src/thingSpeak.py) script are used to ingest particulate data using Thingspeak api. This is scheduled in Lambda along with the data integration scripts as explained below.  
- **Data Integartion** -
The [Data Integration Scripts](https://github.com/arnoldyb/air_pollution/tree/master/DataAutomation/all_sources/src) are deployed in Lambda using the corresponding [deployment scripts](https://github.com/arnoldyb/air_pollution/tree/master/DataAutomation/all_sources/lambda) and scheduled using AWS Cloudwatch to run at 11:15 AM Pacific every day. The [app.py](https://github.com/arnoldyb/air_pollution/blob/master/DataAutomation/all_sources/src/app.py) is used as the wrapper script to make function calls in the other scripts to get the relevant data files from S3 and combine them. The combined dataset is stored as a gzipped parquet file in S3.

### Modeling pipeline and inputs

We used a grid system for both our features and our predictions. `bigger_500m_grid.csv` (the 'bigger' to distinguish from our earlier file with a smaller bounding box) was generated using `create_grid_label_water.ipynb` using common geospatial Python libraries. Additionally, a GeoTiff file containing NDVI information was downloaded from [USGS Earth Explorer](https://earthexplorer.usgs.gov/), then each grid square was assigned an NDVI value.

Data was pulled from S3 in `generate_data.py` were then processed in `Model_Preprocessing.ipynb`. This step was converted to PySpark after the team decided to expand the size of our bounding box.

### Alternative models considered
A number of models were tested, but only random forest, XGBoost's gradient boosted tree regressor, and K Nearest Neighbors provided any meaningful results. Other models, including various neural networks, simply guessed around the mean and did not capture any variation in air quality.

## Virtual sensing model
`/VirtualSensing/model_showdown.ipynb` shows the best models for random forest and XGBoost, using hyperparameters found through a series of cross-validation runs. The random forest model outscores XGBoost in RMSE and $R^2$. That model is implemented in `latest_predictions.py`, which runs on a daily basis and calculates hourly readings for each of our grid squares. Hourly readings from the previous seven days are averaged and delivered to S3 for use in the UI.

## Location recommendations

### Scoring

We use a composite score to recommend sensor placement locations. This score is made up of two components, predicted particulate level and presence of existing sensors (`preds_normalized` and `lonely_factor_normalized`, respectively, in [`app.py`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/app.py)). We chose this composite score to heuristically approximate a notion of information gain; a good sensor location should be sufficiently far from the existing sensor network so as to add new information, but it should also be located in an area of higher pollution levels where monitoring could drive life-saving policy. Since there is no ground truth or labeled data on what defines a good sensor placement, we worked with our subject matter experts and potential users in establishing this scoring method.

### Delivering recommendations

We consider a grid of possible sensor placements and score each grid cell offline based on this composite score. As the user interacts with the tool, we filter this table based on which grid cells are currently on-screen, rank the candidate locations, and serve the user recommendations from the ranked list.

#### Iterative selection and minimum spacing

Since it is possible that several high-ranking locations would be nearby grid cells on the map, and a user may not want to place all their sensors near one another, we needed a way to handle this scenario. Ideally, the tool would choose the best candidate, re-compute distances from existing sensors with this candidate added into the network, and repeat this cycle until all selections were made. This was too computationally intensive to deliver real-time results to a user as they pan or zoom the map, so we instead use a heuristic-based approach. We define a minimum spacing constraint between recommendations (`min_spacing` in [`app.py`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/app.py)) and require that subsequent selections be at least that far from already chosen recommendations. If the tool runs out of candidates to serve the user based on this spacing constraint, it will divide the minimum spacing in half, and iteratively repeat the process until enough locations have been selected. We chose an initial minimum spacing of approximately 2km, which we believe based on working closely with potential users represents a typical minimum distance that a user would want between two sensors.

## Flask app

A flask app handles communication between the back-end data pipeline and web user interface. The app [`app.py`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/app.py) reads in the following data files, searching for them locally first and then on S3:
* `pasensors.parquet` -  contains coordinates for existing sensors
* `polluters.csv` - contains names, addresses, and coordinates for known polluters
* `latest_avg.csv` - contains PM2.5 predictions (weekly avg), loneliness factor, and coordinates for all virtual sensors  

The app puts the existing sensor and polluters data into a DataFrame, tosses out locations outside the project's bounding box (i.e., the San Francisco Bay Area), and exposes the data as JSON to [`mapscripts.js`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/static/mapscripts.js)
The virtual sensor PM2.5 predictions and loneliness factors, meanwhile, get normalized and combined together into a sortable score. The app requests the current boundaries of the user's map and the number of sensors to place from the web interface. The app limits the list of virtual sensors to the user's map boundaries, sorts the list by combined score, and returns the top n coordinates based on n sensors to place as JSON to [`mapscripts.js`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/static/mapscripts.js).
This virtual sensor predictions are also log transformed and returned as JSON in order to make the heatmap.  

The [`mapscripts.js`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/static/mapscripts.js) takes these JSON feeds and places pins, icons, and heatmap elements based on user inputs on the sidebar of the map. This script listens for events like the pushing of the 'Show' button and panning/zooming of the map to know when to conduct updates.
