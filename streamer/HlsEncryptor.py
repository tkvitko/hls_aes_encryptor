import os
from collections import deque
from time import sleep
from typing import Tuple

import m3u8
import requests
from aiohttp import ClientSession

from streamer import HlsReader
from streamer.logs import logger

ENC_SUFFIX = 'enc_'
CLEAR_SUFFIX = '_clear'


class HlsEncryptor(HlsReader):
    def __init__(self, source_name: str, storage: os.path,
                 key_encryptor_url: str, key_client_url: str,
                 content_id: str):
        super().__init__(source_url=os.path.join(storage, source_name))  # чтение только локального m3u8
        self.encrypted_segments_cache = deque()
        self.storage = storage
        self.key_encryptor_url = key_encryptor_url
        self.key_client_url = key_client_url
        self.content_id = content_id
        self.key = None
        self.iv = None

        self.sync_remove_files_from_previous_start()
        self.sync_get_key(self.content_id)

    def sync_remove_files_from_previous_start(self) -> None:
        """Removes encrypted files after the last start if exist"""

        for file in os.scandir(self.storage):
            if file.name.startswith(ENC_SUFFIX):
                os.remove(os.path.join(self.storage, file.name))
                logger.debug(f'old chunk {file.name} has been removed')

    def sync_get_key(self, content_id: str):
        drm_response = requests.get(url=f'{self.key_encryptor_url}{content_id}')
        data = drm_response.json()
        logger.debug('Got key and iv from DRM server')
        self.key = data['key']
        self.iv = data['iv']

    async def get_key(self, content_id: str):
        """
        Request key from MultiDRM
        :param content_id: content id to get key for
        :return: key, iv
        """

        async with ClientSession() as session:
            async with session.get(f'{self.key_encryptor_url}{content_id}') as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logger.debug(f'Got key and iv from DRM server')
                    self.key = data['key']
                    self.iv = data['iv']

    def sync_encrypt(self, input_file_name: str) -> str:
        """
        Encrypts file
        :param input_file_name: file to encrypt
        :param key: key
        :param iv: iv
        :return: encrypted file name
        """

        input_file = os.path.join(self.storage, input_file_name)
        output_file_name = ENC_SUFFIX + input_file_name
        output_file = os.path.join(self.storage, output_file_name)
        logger.debug(f'Start encryption: {input_file}')
        os.system(
            f"openssl enc -aes-128-cbc -e -in {input_file} -out {output_file} -nosalt -K {self.key} -iv {self.iv}")
        logger.debug(f'End encryption: {output_file}')

        self.encrypted_segments_cache.append(output_file_name)
        if len(self.encrypted_segments_cache) > self.live_playlist_length:
            the_oldest_encrypted_chunk = self.encrypted_segments_cache.popleft()
            os.remove(os.path.join(self.storage, the_oldest_encrypted_chunk))
            logger.debug(f'The oldest encrypted chunk {the_oldest_encrypted_chunk} has been removed')

        return output_file_name

    def update_encrypted_playlist(self) -> None:
        """Updates encrypted playlist (based on clear playlist) with new key"""

        self._read_playlist()
        logger.debug(f'Encrypted playlist renewing started')
        uri = f'{self.key_client_url}{self.content_id}.bin'
        new_key = m3u8.Key(method="AES-128", base_uri=uri, uri=uri, iv=self.iv)

        for segment in self.live_playlist.segments:
            segment.key = new_key
            segment.uri = ENC_SUFFIX + segment.uri
        self.live_playlist.keys[-1] = new_key
        self.live_playlist.dump(os.path.join(self.storage, self.source_url.replace(CLEAR_SUFFIX, '')))
        logger.debug(f'Encrypted playlist renewing ended')

    def update_encrypted_data(self, file: str) -> None:
        """Encrypts clear chunk and updates encrypted playlist"""

        self.sync_encrypt(input_file_name=file)
        self.update_encrypted_playlist()

    def process(self):
        while True:
            new_segment, _, _ = self.check_for_new_segment()
            if new_segment:
                self.update_encrypted_data(file=new_segment.uri)
            sleep(1)

