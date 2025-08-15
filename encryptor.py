#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import configparser

from streamer import HlsEncryptor

PWD = os.getcwd()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("config.ini")
    hls_dir = config['encryptor']['hls_dir']
    key_encryptor_url = config['encryptor']['key_encryptor_url']
    key_client_url = config['encryptor']['key_client_url']
    chunks_number = int(config['encryptor']['chunks_number'])
    refresh_interval = int(config['encryptor']['refresh_interval'])
    content_id = config['encryptor']['content_id']
    clear_playlist_suffix = config['encryptor']['clear_playlist_suffix']
    clear_playlist_name = config['encryptor']['clear_playlist_name']

    encryptor = HlsEncryptor(content_id=content_id, source_name=clear_playlist_name, storage=hls_dir,
                             key_encryptor_url=key_encryptor_url, key_client_url=key_client_url )
    encryptor.process()
