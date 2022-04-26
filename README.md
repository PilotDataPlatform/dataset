# dataset

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg?style=for-the-badge)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.7](https://img.shields.io/badge/python-3.7-green?style=for-the-badge)](https://www.python.org/)
[![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/pilotdataplatform/dataset/ci/main?style=for-the-badge)](https://github.com/PilotDataPlatform/dataset/actions/workflows/ci.yml)


Dataset management service for the Pilot Platform.

### Start

1. Install [Docker](https://www.docker.com/get-started/).

2. Add environment variables into `.env`. taking in consideration `.env.schema`

2. Run docker compose

       docker-compose --env-file .env up

### Development

1. Install [Poetry](https://python-poetry.org/docs/#installation).

2. Install dependencies.

       poetry install

3. Install [Pre Commit](https://pre-commit.com/#installation)

       pre-commit install

3. Add environment variables into `.env`.
4. Run application.

       poetry run python run.py

### Running Tests

1. You will need to start a redis:

        docker-compose up -d redis

2. Run tests

        poetry run pytest
