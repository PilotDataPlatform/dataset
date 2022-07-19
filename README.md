# dataset

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg?style=for-the-badge)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.9](https://img.shields.io/badge/python-3.9-brightgreen?style=for-the-badge)](https://www.python.org/)
[![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/pilotdataplatform/dataset/ci/main?style=for-the-badge)](https://github.com/PilotDataPlatform/dataset/actions/workflows/ci.yml)
[![codecov](https://img.shields.io/codecov/c/github/PilotDataPlatform/dataset?style=for-the-badge)](https://codecov.io/gh/PilotDataPlatform/dataset)

Dataset management service for the Pilot Platform.

## Getting Started

### Prerequisites

This project is using:
1. [Poetry](https://python-poetry.org/docs/#installation) to handle the dependencies.

2. [Minio](https://min.io/) to handle the object storage.

3. [Redis](https://redis.io/) to handle cache.

4. [postgresql](https://www.postgresql.org/) as database.


### Installation & Quick Start

1. Clone the project.

       git clone git@github.com:PilotDataPlatform/dataset.git

2. Install dependencies.

       poetry install

4. Add environment variables into `.env`. taking in consideration `.env.schema`


5. Start project dependencies:

        docker-compose up -d redis
        docker-compose up -d postgres


6. Run any initial scripts, migrations or database seeders.

       poetry run alembic upgrade head

7. Run application.

       poetry run python start.py


8. Install [Docker](https://www.docker.com/get-started/).


### Startup using Docker

This project can also be started using [Docker](https://www.docker.com/get-started/).

1. To build and start the service within the Docker container run.

       docker compose up

2. Migrations should run automatically on previous step. They can also be manually triggered:

       docker compose run --rm alembic upgrade head

## Resources

* [Pilot Platform API Documentation](https://pilotdataplatform.github.io/api-docs/#tag/V1-DATASET)
* [Pilot Platform Helm Charts](https://github.com/PilotDataPlatform/helm-charts/)

## Contribution

You can contribute the project in following ways:

* Report a bug
* Suggest a feature
* Open a pull request for fixing issues or adding functionality. Please consider
  using [pre-commit](https://pre-commit.com) in this case.
* For general guidelines how to contribute to the project, please take a look at the [contribution guides](CONTRIBUTING.md)
