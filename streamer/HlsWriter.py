import os
from typing import Union

import aiofiles
from aiohttp import ClientSession

from streamer import HlsReader
from streamer.Db import DbManager
from streamer.logs import logger


class HlsWriter(HlsReader):
    def __init__(self, source_url: str, storage: os.path,
                 channel_name: str):
        super().__init__(source_url)
        self.storage = storage
        self.db_manager = DbManager()
        self.channel_name = channel_name

    async def _download(self, download_url: str, segment_name: str):
        async with ClientSession() as session:
            async with session.get(download_url) as resp:
                if resp.status == 200:
                    f = await aiofiles.open(os.path.join(self.storage, segment_name),
                                            mode='wb')
                    await f.write(await resp.read())
                    await f.close()

    async def check_for_new_segment_and_save(self) -> Union[float, None]:
        new_segment, segment_start_datetime, segment_media_sequence = self.check_for_new_segment()
        if new_segment is not None:
            download_url = new_segment.absolute_uri
            original_segment_name = new_segment.uri
            segment_name = segment_start_datetime.strftime('%Y%m%d_%H%M%S.ts')
            segment_duration = new_segment.duration

            added = await self.db_manager.add_segment(filename=segment_name,
                                                      duration=segment_duration,
                                                      start_datetime=segment_start_datetime,
                                                      original_filename=original_segment_name,
                                                      media_sequence=segment_media_sequence,
                                                      channel_name=self.channel_name)
            if added:
                logger.debug(
                    f'New segment with old name {original_segment_name} started {segment_start_datetime} ' 
                    f'with media_sequence {segment_media_sequence} '
                    f'has been saved to db with new name {segment_name}')
                await self._download(download_url=download_url, segment_name=segment_name)
                logger.debug(
                    f'New segment {segment_name} has been downloaded to storage')

            return new_segment.duration
