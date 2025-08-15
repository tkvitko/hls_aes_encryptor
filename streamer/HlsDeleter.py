import datetime
import os
import time
import _thread

from streamer import HlsWriter
from streamer.logs import logger


class HlsDeleter(HlsWriter):
    def __init__(self, storage: os.path, depth_in_hours: float, channel_name: str):
        super().__init__(source_url=None, storage=storage, channel_name=channel_name)
        self.depth_in_hours = depth_in_hours

    def _get_the_oldest_date(self):
        return datetime.datetime.utcnow() - datetime.timedelta(hours=self.depth_in_hours)

    async def _clear_db(self):
        await self.db_manager.delete_segments(older_then=self._get_the_oldest_date(),
                                              channel_name=self.channel_name)

    def _get_file_last_modification_time(self, file: os.DirEntry) -> datetime.datetime:
        return datetime.datetime.utcfromtimestamp(os.path.getmtime(os.path.join(self.storage, file.name)))

    def _sync_clear_storage(self):
        for file in os.scandir(self.storage):
            if self._get_file_last_modification_time(file) < self._get_the_oldest_date():
                os.remove(os.path.join(self.storage, file.name))
                logger.debug(f'old chunk {file.name} has been removed')

    async def clear_storage_and_db(self):
        await self._clear_db()
        _thread.start_new_thread(self._sync_clear_storage, ())
