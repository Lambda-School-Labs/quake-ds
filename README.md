![MIT](https://img.shields.io/packagist/l/doctrine/orm.svg)
![Python Version](https://img.shields.io/badge/python-v3.7-blue)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)
[![Maintainability](https://api.codeclimate.com/v1/badges/9a013b0434bf5dabd26c/maintainability)](https://codeclimate.com/github/Lambda-School-Labs/quake-ds/maintainability)



# Quake API

## 1️⃣ Project Overview

[Trello Board](https://trello.com/b/5WH9iB9n/labspt7-quake)<br>
[Product Canvas](https://www.notion.so/User-Research-8cd64de109404266b2537457a426738d)

## 2️⃣ Team Members


|                                       [Eyve Geordan](https://github.com/eyvonne)                                        |                                       [J Tyler Sheppard](https://github.com/jtsheppard)                                        |                                       [Ashwin Swamy](https://github.com/ash12hub)                                        |
| :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: |
|                      [<img src="https://media-exp1.licdn.com/dms/image/C5603AQFERauBl6COOA/profile-displayphoto-shrink_200_200/0?e=1593043200&v=beta&t=aYaek2d2OEW_E5NJW52LvbM6XUBY5GLHXzA9-GMY1c8" width = "200" />](https://github.com/eyvonne)                       |                      [<img src="https://media-exp1.licdn.com/dms/image/C4E03AQGF_MRS5-sEFw/profile-displayphoto-shrink_200_200/0?e=1593043200&v=beta&t=hYIR_Dfb7OB5M1kLJWzVZAcJIbFH_k6CPmg7MUy7woQ" width = "200" />](https://github.com/jtsheppard)                       |                      [<img src="ashwin.jpg" width = "200" />](https://github.com/ash12hub)                      |
|                 [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/eyvonne)                 |            [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/jtsheppard)             |           [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/ash12hub)            |
| [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/eyvonne-geordan-2a2b55168/) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/jtsheppard/) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/) |

## 3️⃣ Endpoints

### How to connect to the data API

Production Endpoint 👉 https://quake-ds-production.herokuapp.com/  
Staging Endpoint 👉 https://quake-ds-staging.herokuapp.com/

#### Overview of Main Routes

| Method | Endpoint                | Access Control | Description                                  |
| ------ | ----------------------- | -------------- | -------------------------------------------- |
| GET    | `/lastQuake/SOURCE/MAGNITUDE` | all users      | Returns the last quake over the given magnitude from the source  |
| GET    | `/last/SOURCE/TIME/MAGNITUDE` | all users      | Gets the quakes over the given timeframe |
| GET    | `/history/SOURCE/LAT,LON,DIST` | all users      | Returns all quakes in a given area |

### How to use the routes

1. `/lastQuake/SOURCE/MAGNITUDE` - Returns the last quake over the given magnitude from the source 

`SOURCE`: choice of 'USGS' or 'EMSC' depending on which datasource to pull from 
`MAGNITUDE`: a number 0-11 (accepts floats and ints) defaults to 5.5

2. `/last/SOURCE/TIME/MAGNITUDE` - Gets the quakes over the given timeframe

`SOURCE`: choice of 'USGS' or 'EMSC' depending on which datasource to pull from 
`TIME`: choice of 'hour', 'day', 'week' or 'month', returns quakes over the given time period
`MAGNITUDE`: a number 0-11 (accepts floats and ints) defaults to 5.5

3. `/history/SOURCE/LAT,LON,DIST` - Returns all quakes in a given area

`SOURCE`: choice of 'USGS' or 'EMSC' depending on which datasource to pull from
`LAT` and `LON` are the central latitude and longitude  
`DIST` is the distance in miles from the center to search from

## 4️⃣ Tech Stack 📚 

- [Flask](https://flask.palletsprojects.com/en/1.1.x/)
- [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/)

### Architecture

![architecture](https://www.notion.so/image/https%3A%2F%2Fs3-us-west-2.amazonaws.com%2Fsecure.notion-static.com%2F1b61d2ba-287a-4a01-8c6f-98ae376dc2c9%2Fquake-architect-diagram.jpg)

## 5️⃣ Data Sources

-   [USGS Source 1](https://earthquake.usgs.gov/earthquakes/search/)
-   [USGS Source 2](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php)
-   [ESMC](https://www.emsc-csem.org/Earthquake/seismologist.php)

### Extra Python Notebooks

[Python Notebook 1](https://colab.research.google.com/drive/1g_zGrP7LCK4FNdJycQQcRJ_22iKL0_F6)

## 6️⃣ Contributing

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
