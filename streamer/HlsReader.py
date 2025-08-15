import datetime
from collections import deque

import m3u8

from streamer.logs import logger


class HlsReader:

    def __init__(self, source_url: str):
        self.live_playlist = m3u8.model.M3U8()
        self.live_segments_cache = deque()
        self.source_url = source_url
        self.live_playlist_length = 0

    def _read_playlist(self):
        try:
            self.live_playlist = m3u8.load(self.source_url)
            while self.live_playlist.is_variant:
                self.live_playlist = m3u8.load(self.live_playlist.playlists[0].uri)
            self.live_playlist_length = len(self.live_playlist.segments)
        except Exception as e:
            logger.error(f'Cant download source playlist {self.source_url}: {e} - {e.__class__.__name__}')

    def _flush_queue(self):
        self.live_segments_cache.clear()

    def check_for_new_segment(self) -> (m3u8.Segment, datetime.datetime):
        self._read_playlist()
        if self.live_playlist:
            for position, segment in enumerate(self.live_playlist.segments):
                # logger.debug(f'CACHE: {self.live_segments_cache}')
                segment_media_sequence = self.live_playlist.media_sequence + position

                if (segment.uri, segment_media_sequence) not in self.live_segments_cache:
                    self.live_segments_cache.append((segment.uri, segment_media_sequence))
                    logger.debug(f'New segment {segment.uri} has been added to local cache')
                    delay = (self.live_playlist_length - position) * self.live_playlist.data['targetduration']
                    segment_start_datetime = datetime.datetime.utcnow() - datetime.timedelta(seconds=delay)
                    if len(self.live_segments_cache) > self.live_playlist_length:
                        self.live_segments_cache.popleft()
                    return segment, segment_start_datetime, segment_media_sequence
        return None, None, None
