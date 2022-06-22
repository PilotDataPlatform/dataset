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

FROM python:3.7-buster

ENV TZ=America/Toronto

WORKDIR /usr/src/app

ENV TZ=America/Toronto
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    apt-get update && \
    apt-get install -y vim-tiny less && \
    ln -s /usr/bin/vim.tiny /usr/bin/vim && \
    rm -rf /var/lib/apt/lists/*
RUN wget -O /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/archive/mc.RELEASE.2021-07-27T06-46-19Z
RUN chmod +x /usr/local/bin/mc
COPY poetry.lock pyproject.toml ./
RUN pip install --no-cache-dir poetry==1.1.12
RUN poetry config virtualenvs.create false
RUN poetry install --no-root --no-interaction
COPY . ./

ENV MINIO_USERNAME=minioadmin
ENV MINIO_PASSWORD=minioadmin
ENV MINIO_URL=http://minio.minio:9000
RUN chmod +x gunicorn_starter.sh
CMD ["sh", "-c", "mc alias set minio $MINIO_URL $MINIO_USERNAME $MINIO_PASSWORD && ./gunicorn_starter.sh"]
