# Wiener-Linien-Line-Delay-Analysis
* Author: Stefan Roland Schwingenschlögl
* email: stefan.roland.schwingenschloegl@gmail.com
___
# Overview
After a first general preparation for the use of the <a href = "http://www.wienerlinien.at/ogd_realtime/doku/"> Wiener Linien Realtime API</a> was developed in the <a href = "https://github.com/stefan-schwingenschloegl/Wiener-Linien-Project">Wiener Linien Project</a>, this repository will focus on the further development of the code. In the last project, I analysed the delays at specific stops. In this project, I want to find out in which sections of the route a particular line generates delays. Once this pipeline has been created, hot spots on different routes could be identified.

The development focus here is clearly on the reusability of the pipeline. I want to make the pipeline as generic as possible and cover it with software tests. The aspect of standardisation should also be taken into account. I would like to develop and use the Framework <a href = "https://kedro.org/Kedro">Kedro</a> for this purpose, which helps to implement software engineering best pratices in Data Science Workflows. 

## Technology used:
* Programming Languages: Python, SQL
* Main focus:
  - Creating robust Data Pipelines
  - Implementing Software Engineering Best pratices
  - Software Testing
* New Libraries:
  - Kedro (Python)
  - pytest (Python)

Projectflow


All in all the project is structured as follows:

- `1_create_Database`: Using code from my old project to set up the Database and load all static .csv files into the database. Since this section is only for *initializing the Database* this code will not be reworked.
- `2_Data_Engineering`: This section contains all Data Pipelines. This section will be implemented in the Kedro-Framework. The designed Pipelines are
    - `API_Call_to_staging_table`: In this pipeline the API gets called. The data will be transformed from the Wiener Linien Realtime API json-Format to tabluar data format and stored in the staging table in the Database.
    - `Data_Transformation_pipeline`: In this pipeline the data from the staging table will be transformed, cleaned and added to the production table. Also Feature Engineering is taking place in this pipeline.
    - `Data_Analysis_pipeline`: After saving all the cleaned data in the database the analysis is carried out here.
- `3_Result_Description`: The overall Result and working steps are described here.