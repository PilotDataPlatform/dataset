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

import logging

from fastapi import Depends
from sqlalchemy import MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.declarative import declarative_base

from app.config import ConfigClass

DBModel = declarative_base(metadata=MetaData(schema=ConfigClass.RDS_SCHEMA_DEFAULT))

logger = logging.getLogger(__name__)


class GetDBEngine:
    """Create a FastAPI callable dependency for SQLAlchemy single AsyncEngine instance."""

    def __init__(self) -> None:
        self.instance = None

    async def __call__(self) -> AsyncEngine:
        """Return an instance of AsyncEngine class."""

        if not self.instance:
            try:
                self.instance = create_async_engine(ConfigClass.OPS_DB_URI, echo=ConfigClass.RDS_ECHO_SQL_QUERIES)
            except SQLAlchemyError:
                logger.exception('Error DB connect')
        return self.instance


db_engine = GetDBEngine()


async def get_db_session(engine: AsyncEngine = Depends(db_engine)) -> AsyncSession:
    db = AsyncSession(bind=engine, expire_on_commit=False)
    try:
        yield db
    finally:
        await db.close()


async def is_db_connected(db: AsyncSession = Depends(get_db_session)) -> bool:
    """Validates DB connection."""

    try:
        connection = await db.connection()
        raw_connection = await connection.get_raw_connection()
        if not raw_connection.is_valid:
            return False
    except SQLAlchemyError:
        logger.exception('DB connection failed, SQLAlchemyError')
        return False
    except Exception:
        logger.exception('DB connection failed, unknown Exception')
        return False
    return True
