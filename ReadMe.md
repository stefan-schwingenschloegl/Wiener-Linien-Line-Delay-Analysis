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