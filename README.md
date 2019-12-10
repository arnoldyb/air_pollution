# Fairair

## Contributors
Mark Paluta
[<img src="https://github.com/favicon.ico" width="20">](https://github.com/mpaluta)
[<img src="https://www.linkedin.com/favicon.ico" width="20">](https://www.linkedin.com/in/markpaluta/)  
Ben Arnoldy
[<img src="https://github.com/favicon.ico" width="20">](https://github.com/arnoldyb)
[<img src="https://www.linkedin.com/favicon.ico" width="20">](https://www.linkedin.com/in/benarnoldy/)  
Angshuman Paul  
Jake Miller  
Sameed Musvee  

Group inbox:
[contact.fairair@gmail.com](mailto:contact.fairair@gmail.com)

## Project description

FairAir shows environmental activists where air pollution is likely to be worst in a city and recommends where to deploy low-cost sensors to improve pollution mapping. Sensors are currently clustered in wealthier neighborhoods. By using a random forest machine learning model to create "virtual sensors," FairAir can highlight where new monitoring and remediation are most needed.  

## Fairair interactive map

[https://fairair.netlify.com/](https://fairair.netlify.com/)

## Data ingestion [Angshuman]

## Virtual sensing model [Jake]

### Alternative models considered [Sameed]

### Modeling pipeline and inputs [Jake]
ex. static_csv...

## Location recommendations

### Scoring

We use a composite score to recommend sensor placement locations. This score is made up of two components, predicted particulate level and presence of existing sensors (`preds_normalized` and `lonely_factor_normalized`, respectively, in [`app.py`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/app.py). We chose this composite score to heuristically approximate a notion of information gain; a good sensor location should be sufficiently far from the existing sensor network so as to add new information, but it should also be located in an area of higher pollution levels where monitoring could drive life-saving policy. Since there is no ground truth or labeled data on what defines a good sensor placement, we worked with our subject matter experts and potential users in establishing this scoring method.

### Delivering recommendations

We consider a grid of possible sensor placements and score each grid cell offline based on this composite score. As the user interacts with the tool, we filter this table based on which grid cells are currently on-screen, rank the candidate locations, and serve the user recommendations from the ranked list.

#### Iterative selection and minimum spacing

Since it is possible that several high-ranking locations would be nearby grid cells on the map, and a user may not want to place all their sensors near one another, we needed a way to handle this scenario. Ideally, the tool would choose the best candidate, re-compute distances from existing sensors with this candidate added into the network, and repeat this cycle until all selections were made. This was too computationally intensive to deliver real-time results to a user as they pan or zoom the map, so we instead use a heuristic-based approach. We define a minimum spacing constraint between recommendations (`min_spacing` in [`app.py`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/app.py) and require that subsequent selections be at least that far from already chosen recommendations. If the tool runs out of candidates to serve the user based on this spacing constraint, it will divide the minimum spacing in half, and iteratively repeat the process until enough locations have been selected. We chose an initial minimum spacing of approximately 2km, which we believe based on working closely with potential users represents a typical minimum distance that a user would want between two sensors.

## Flask app

A flask app handles communication between the back-end data pipeline and web user interface. The app [`app.py`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/app.py) reads in the following data files, searching for them locally first and then S3: 
* `pasensors.parquet` -  contains coordinates for existing sensors
* `polluters.csv` - contains names, addresses, and coordinates for known polluters
* `latest_avg.csv` - contains PM2.5 predictions (weekly avg), loneliness factor, and coordinates for all virtual sensors
The app puts the existing sensor and polluters data into a DataFrame, tosses out locations outside the project's bounding box (i.e., the San Francisco Bay Area), and exposes the data as JSON to [`mapscripts.js`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/static/mapscripts.js)
The virtual sensor PM2.5 predictions and loneliness factors, meanwhile, get normalized and combined together into a sortable score. The app requests the current boundaries of the user's map and the number of sensors to place from the web interface. The app limits the list of virtual sensors to the user's map boundaries, sorts the list by combined score, and returns the top n coordinates based on n sensors to place as JSON to [`mapscripts.js`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/static/mapscripts.js).
This virtual sensor predictions are also log transformed and returned as JSON in order to make the heatmap.
The [`mapscripts.js`](https://github.com/arnoldyb/air_pollution/blob/master/website/maps/static/mapscripts.js) takes these JSON feeds and places pins, icons, and heatmap elements based on user inputs on the sidebar of the map. This script listens for events like the pushing of the 'Show' button and panning/zooming of the map to know when to conduct updates. 



