# Contribution Guide

## Bug Reports

For bug reports [submit an issue](https://github.com/PilotDataPlatform/dataset/issues).

## Pull Requests

1. Fork the [main repository](https://github.com/PilotDataPlatform/dataset)
2. Create a feature branch to hold your changes
3. Work on the changes in your feature branch
4. Add [Unit Tests](#unit-tests)
5. Follow the Getting Started instruction to setup service.
6. Create the alembic [migration](#migrations) environment only when it's needed
7. Test the code and create pull request

### Migrations

To create new migration with alembic.

       docker compose run --rm alembic revision -m "Migration name"

### Unit Tests

When adding a new feature or fixing a bug, unit test is necessary to write. Currently we adopted pytest as our testing framework and all test cases are written under tests directory


1. Start redis.

       docker-compose up -d redis

2. Run test cases.

       poetry run pytest
