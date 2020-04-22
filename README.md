
[![Maintainability](https://api.codeclimate.com/v1/badges/9a013b0434bf5dabd26c/maintainability)](https://codeclimate.com/github/Lambda-School-Labs/quake-ds/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/9a013b0434bf5dabd26c/test_coverage)](https://codeclimate.com/github/Lambda-School-Labs/quake-ds/test_coverage)


# Quake API

You can find the project at [Quake Production API](https://quake-ds-production.herokuapp.com).


### 3️⃣ Contributors


|                                       [Eyve Geordan](https://github.com/eyvonne)                                        |                                       [J Tyler Sheppard](https://github.com/jtsheppard)                                        |                                       [Ashwin Swamy](https://github.com/ash12hub)                                        |
| :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: |
|                      [<img src="https://github.com/Lambda-School-Labs/quake-ds/blob/Documentation/IMG_1754%202.jpg" width = "200" />](https://github.com/eyvonne)                       |                      [<img src="https://github.com/Lambda-School-Labs/quake-ds/blob/Documentation/Tyler.jpg" width = "200" />](https://github.com/jtsheppard)                       |                      [<img src="https://github.com/Lambda-School-Labs/quake-ds/blob/Documentation/ashwin.jpg" width = "200" />](https://github.com/ash12hub)                      |
|                 [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/eyvonne)                 |            [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/jtsheppard)             |           [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/ash12hub)            |
| [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/eyvonne-geordan-2a2b55168/) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/jtsheppard/) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/) |




![MIT](https://img.shields.io/packagist/l/doctrine/orm.svg)
![Typescript](https://img.shields.io/npm/types/typescript.svg?style=flat)
[![Netlify Status](https://api.netlify.com/api/v1/badges/b5c4db1c-b10d-42c3-b157-3746edd9e81d/deploy-status)](netlify link goes in these parenthesis)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)

## Project Overview


[Trello Board](https://trello.com/b/5WH9iB9n/labspt7-quake)

[Product Canvas](https://www.notion.so/User-Research-8cd64de109404266b2537457a426738d)

There are several routes to get out information about earthquakes:

/lastQuake/[source]/[magnitude]

Where source is either USGS or EMSC, depending on which data source you would like to draw information from. In a future release this will be updated to include an option for both sources once we merge them.

Magnitude is a number from 0-11 representing the minimum magnitude of earthquakes that you would like to see returned.


/last/[time]/[source]/[magnitude]

For this time is one of 'hour', 'day', 'week', or 'month'. These time frames were selected to match the USGS API, in a future release this may be updated to be more flexible.

Source and Magnitude are the same as before.

/

### Tech Stack

Python, Flask, Aws, Elastic Beanstalk, Heroku

### Data Sources

-   [Source 1] (https://earthquake.usgs.gov/earthquakes/search/)
-   [Source 2] (https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php)

### Python Notebooks

[Python Notebook 1](https://colab.research.google.com/drive/1g_zGrP7LCK4FNdJycQQcRJ_22iKL0_F6)


### How to connect to the data API
### Connect to latest earhquake data

https://quake-ds-staging.herokuapp.com/lastQuake

expected output in JSON
 - {"Oceanic":false,"id":"20km S of Trona, CA","lat":-117.4025,"lon":35.5833333,"mag":1.2,"place":1581475110360}

### Get a list of earthquakes over the past specified period, options are ‘hour’ ‘day’ ‘week’ or ‘month’
https://quake-ds-staging.herokuapp.com/last/{time}

expected output in JSON
 - {"Oceanic":false,"latitude":-122.7941666,"longitude":38.8219986,"magnitude":0.79,"place":"6km NNW of The Geysers, CA","time":1582173089460}


## Contributing

When contributing to this repository, please first discuss the change you wish to make via issue, email, or any other method with the owners of this repository before making a change.

Please note we have a [code of conduct](./code_of_conduct.md.md). Please follow it in all your interactions with the project.

### Issue/Bug Request

 **If you are having an issue with the existing project code, please submit a bug report under the following guidelines:**
 - Check first to see if your issue has already been reported.
 - Check to see if the issue has recently been fixed by attempting to reproduce the issue using the latest master branch in the repository.
 - Create a live example of the problem.
 - Submit a detailed bug report including your environment & browser, steps to reproduce the issue, actual and expected outcomes,  where you believe the issue is originating from, and any potential solutions you have considered.

### Feature Requests

We would love to hear from you about new features which would improve this app and further the aims of our project. Please provide as much detail and information as possible to show us why you think your new feature should be implemented.

### Pull Requests

If you have developed a patch, bug fix, or new feature that would improve this app, please submit a pull request. It is best to communicate your ideas with the developers first before investing a great deal of time into a pull request to ensure that it will mesh smoothly with the project.

Remember that this project is licensed under the MIT license, and by submitting a pull request, you agree that your work will be, too.

#### Pull Request Guidelines

- Ensure any install or build dependencies are removed before the end of the layer when doing a build.
- Update the README.md with details of changes to the interface, including new plist variables, exposed ports, useful file locations and container parameters.
- Ensure that your code conforms to our existing code conventions and test coverage.
- Include the relevant issue number, if applicable.
- You may merge the Pull Request in once you have the sign-off of two other developers, or if you do not have permission to do that, you may request the second reviewer to merge it for you.

### Attribution

These contribution guidelines have been adapted from [this good-Contributing.md-template](https://gist.github.com/PurpleBooth/b24679402957c63ec426).

## Documentation

See [Backend Documentation](https://github.com/Lambda-School-Labs/quake-fe/blob/master/README.md) for details on the backend of our project.

See [Front End Documentation](https://github.com/Lambda-School-Labs/quake-be/blob/master/README.md) for details on the front end of our project.
