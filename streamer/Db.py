import datetime
import asyncio

import sqlalchemy.exc
from sqlalchemy_aio import ASYNCIO_STRATEGY
from typing import List

from sqlalchemy import and_

from sqlalchemy import (
    Column, Integer, String, DateTime, Float, MetaData, Table, Text, create_engine, select)
from sqlalchemy.schema import CreateTable, DropTable

from contextlib import contextmanager

from streamer.logs import logger

metadata = MetaData()
segments = Table(
    'segment', metadata,
    Column('id', Integer, primary_key=True),
    Column('channel_name', String),
    Column('original_filename', String),
    Column('filename', String),
    Column('start_datetime', DateTime),
    Column('duration', Float),
    Column('media_sequence', Integer)
)


class DbManager:
    def __init__(self):
        self.engine = create_engine('sqlite:///base.sqlite', echo=True, strategy=ASYNCIO_STRATEGY)
        # self.conn = await self.engine.connect()

    async def create_db(self):
        await self.engine.execute(CreateTable(segments))

    async def add_segment(self, filename: str, duration: float,
                          start_datetime: datetime.datetime,
                          original_filename: str,
                          media_sequence: int,
                          channel_name: str) -> bool:
        async with self.engine.connect() as conn:
            # try:
            await conn.execute(segments.insert().values(filename=filename,
                                                        start_datetime=start_datetime,
                                                        duration=duration,
                                                        original_filename=original_filename,
                                                        media_sequence=media_sequence,
                                                        channel_name=channel_name))
            return True
            # except sqlalchemy.exc.IntegrityError:
            #     logger.warning(f'chunk {original_filename} already in db')
            #     return False

    async def get_segments(self, from_datetime: datetime.datetime,
                           to_datetime: datetime.datetime, channel_name: str) -> List[str]:
        query = select([segments.columns.filename,
                        segments.columns.start_datetime,
                        segments.columns.duration,
                        segments.columns.media_sequence
                        ]).where(and_(segments.columns.start_datetime >= from_datetime,
                                      segments.columns.start_datetime <= to_datetime,
                                      segments.columns.channel_name == channel_name
                                      )).order_by(segments.columns.start_datetime.asc())

        async with self.engine.connect() as conn:
            execution = await conn.execute(query)
            result = await execution.fetchall()
        return [segment for segment in result]

    async def delete_segments(self, older_then: datetime.datetime, channel_name: str) -> None:
        async with self.engine.connect() as conn:
            await conn.execute(segments.delete().where(and_(segments.columns.start_datetime < older_then,
                                                            segments.columns.channel_name == channel_name)))


if __name__ == '__main__':
    db_manager = DbManager()

    # db_manager.add_segment(filename='1.ts',
    #                        start_datetime=datetime.datetime(2024, 6, 18, 15, 10, 0, 0),
    #                        duration=5.0)

    print(db_manager.get_segments(from_datetime=datetime.datetime(2024, 6, 18, 0, 0, 0, 0),
                                  to_datetime=datetime.datetime.now()))
