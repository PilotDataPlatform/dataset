# Copyright (C) 2022 Indoc Research
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

FROM python:3.9.11-buster AS production-environment

ENV PYTHONDONTWRITEBYTECODE=true \
    PYTHONIOENCODING=UTF-8 \
    POETRY_VERSION=1.1.13 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false \
    MINIO_USERNAME=minioadmin \
    MINIO_PASSWORD=minioadmin \
    MINIO_URL=http://minio.minio:9000

ENV PATH="${POETRY_HOME}/bin:${PATH}"

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        build-essential

RUN pip install --no-cache-dir poetry==1.1.13

WORKDIR /app

COPY poetry.lock pyproject.toml ./

RUN poetry install --no-dev --no-root --no-interaction


FROM production-environment AS dataset-image

COPY app ./app
COPY run.py ./
COPY gunicorn_starter.sh ./

RUN wget -O /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/archive/mc.RELEASE.2021-07-27T06-46-19Z
RUN chmod +x gunicorn_starter.sh
RUN chmod +x /usr/local/bin/mc

ENTRYPOINT ["sh", "-c", "mc alias set minio $MINIO_URL $MINIO_USERNAME $MINIO_PASSWORD && ./gunicorn_starter.sh"]


FROM production-environment AS development-environment

RUN poetry install --no-root --no-interaction


FROM development-environment AS alembic-image

ENV ALEMBIC_CONFIG=migrations/alembic.ini

COPY app ./app
COPY migrations ./migrations

ENTRYPOINT ["python3", "-m", "alembic"]

CMD ["upgrade", "head"]
