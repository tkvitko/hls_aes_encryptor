import configparser
import datetime
import os.path
from typing import List

import m3u8

from streamer.Db import DbManager


class PlaylistGenerator:
    def __init__(self, channel_name: str):
        self.segments = []
        self.db_manager = DbManager()
        self.channel_name = channel_name

        config = configparser.ConfigParser()
        config.read("pvr.ini")
        self.chunk_prefix = config['plstgen']['chunk_prefix']

    async def _get_segments_from_db(self, from_datetime: datetime.datetime,
                                    to_datetime: datetime.datetime):
        self.segments = await self.db_manager.get_segments(from_datetime=from_datetime,
                                                           to_datetime=to_datetime,
                                                           channel_name=self.channel_name)

    async def generate_vod_playlist(self, from_datetime: datetime.datetime,
                                    to_datetime: datetime.datetime):
        await self._get_segments_from_db(from_datetime=from_datetime,
                                         to_datetime=to_datetime)
        if self.segments:
            playlist = m3u8.model.M3U8()
            discontinuity = False
            prev_media_sequence = None
            for segment in self.segments:
                media_sequence = segment.media_sequence
                if prev_media_sequence is not None and media_sequence - prev_media_sequence != 1:
                    discontinuity = True
                else:
                    discontinuity = False
                uri = os.path.join(self.chunk_prefix, self.channel_name, segment.filename)
                segment_obj = m3u8.model.Segment(uri=uri,
                                                 duration=segment.duration,
                                                 discontinuity=discontinuity)
                playlist.add_segment(segment_obj)
                prev_media_sequence = media_sequence
            playlist.files = [segment.filename for segment in self.segments]
            playlist.is_variant = False
            playlist.__dict__['media_sequence'] = 1
            playlist.__dict__['version'] = 3
            playlist.__dict__['target_duration'] = self.segments[0].duration
            playlist.__dict__['playlist_type'] = 'VOD'
            playlist.__dict__['is_endlist'] = True
            return playlist.dumps()
        return None

    async def get_metadata_for_interval(self, from_datetime: datetime.datetime,
                                        to_datetime: datetime.datetime) -> List[dict]:
        await self._get_segments_from_db(from_datetime=from_datetime,
                                         to_datetime=to_datetime)
        result = []
        margin = 2  # запас в секундах, компенсирующий неточность расчета старта чанка (для исключения ложных дыр)

        current_range = {}
        duration_of_previous_segment = 0
        start_of_previous_segment = 0

        for segment in self.segments:
            if current_range == {}:
                # если только начали заполнять новый интервал
                current_range['start_datetime'] = segment.start_datetime
                # данные для использования на следующем шаге
                duration_of_previous_segment = segment.duration
                start_of_previous_segment = segment.start_datetime
            else:
                # если интервал уже начат и это следующий чанк-кандидат на продление интервала
                if segment.start_datetime < start_of_previous_segment \
                        + datetime.timedelta(seconds=duration_of_previous_segment) \
                        + datetime.timedelta(seconds=margin):
                    # чанк вовремя, интервал не прерывается, идем дальше
                    # данные для использования на следующем шаге
                    duration_of_previous_segment = segment.duration
                    start_of_previous_segment = segment.start_datetime
                else:
                    # была дыра, закрываем текущий интервал и начинаем новый
                    current_range['end_datetime'] = start_of_previous_segment \
                                                    + datetime.timedelta(seconds=duration_of_previous_segment)
                    result.append(current_range)
                    current_range = {}

        # закрываем последний интервал
        current_range['end_datetime'] = start_of_previous_segment \
                                        + datetime.timedelta(seconds=duration_of_previous_segment)
        result.append(current_range)

        return result
