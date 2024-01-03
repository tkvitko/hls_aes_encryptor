import argparse
import configparser
import os
from collections import deque
from time import sleep
from typing import Tuple

import m3u8
import requests

config = configparser.ConfigParser()
config.read("config.ini")
hls_dir = config['encryptor']['hls_dir']
key_encryptor_url = config['encryptor']['key_encryptor_url']
key_client_url = config['encryptor']['key_client_url']
chunks_number = int(config['encryptor']['chunks_number'])
refresh_interval = int(config['encryptor']['refresh_interval'])
content_id = config['encryptor']['content_id']
clear_playlist_suffix = config['encryptor']['clear_playlist_suffix']

ENC_SUFFIX = 'enc_'

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', type=bool, default=False)
args = parser.parse_args()
debug = args.debug

clear_segments_cache = deque()
encrypted_segments_cache = deque()


def debug_print(str_: str):
    global debug
    if debug:
        print(str_)


def remove_files_from_previous_start(work_dir: str) -> None:
    """
    Removes encrypted files after the last start if exist
    :param work_dir: directory to clean
    :return: None
    """

    files = os.listdir(work_dir)
    old_segments = [file for file in files if file.startswith(ENC_SUFFIX)]
    for old_segment in old_segments:
        os.remove(os.path.join(work_dir, old_segment))


def get_key(content_id: str) -> tuple:
    """
    Request key from MultiDRM
    :param content_id: content id to get key for
    :return: key, iv
    """

    drm_response = requests.get(url=f'{key_encryptor_url}{content_id}')
    data = drm_response.json()
    debug_print('Got key and iv from DRM server')
    return data['key'], data['iv']


def encrypt(input_file_name: str, key: str, iv: str, work_dir: str) -> str:
    """
    Encrypts file
    :param input_file_name: file to encrypt
    :param key: key
    :param iv: iv
    :return: encrypted file name
    """

    input_file = os.path.join(work_dir, input_file_name)
    output_file_name = ENC_SUFFIX + input_file_name
    output_file = os.path.join(work_dir, output_file_name)
    debug_print(f'Start encryption: {input_file}')
    os.system(f"openssl enc -aes-128-cbc -e -in {input_file} -out {output_file} -nosalt -K {key} -iv {iv}")
    debug_print(f'End encryption: {output_file}')

    global encrypted_segments_cache
    encrypted_segments_cache.append(output_file_name)
    if len(encrypted_segments_cache) > chunks_number:
        the_oldest_encrypted_chunk = encrypted_segments_cache.popleft()
        os.remove(os.path.join(work_dir, the_oldest_encrypted_chunk))
        debug_print(f'The oldest encrypted chunk {the_oldest_encrypted_chunk} has been removed')

    return output_file_name


# def check_for_new_file(dir: str) -> str:
#     """
#     Directory listener
#     :param dir: directory to listen
#     :return: new file name if got
#     """
#
#     files = os.listdir(dir)
#     clear_segments = [file for file in files if file.startswith('segment') and file.endswith('.ts')]
#
#     global clear_segments_cache
#     for clear_segment in clear_segments[::-1]:
#         if clear_segment not in clear_segments_cache:
#             # print('New clear segment has been added to local cache')
#             clear_segments_cache.append(clear_segment)
#             if len(clear_segments_cache) > chunks_number:
#                 clear_segments_cache.popleft()
#
#             return clear_segment


def load_clear_playlist(work_dir: str) -> Tuple[str, m3u8.model.M3U8]:
    """
    Search for clear playlist by suffix in directory
    :param work_dir:
    :return: name of the playlist and playlist object
    """

    clear_playlist_name = [file for file in os.listdir(work_dir) if file.endswith(f'{clear_playlist_suffix}.m3u8')][0]
    playlist = m3u8.load(os.path.join(work_dir, clear_playlist_name))
    return clear_playlist_name, playlist


def check_for_new_clear_chunk(work_dir: str) -> str:
    """
    Check if new clear chunk is in playlist now
    :param work_dir:
    :return: chunk name
    """

    clear_playlist_name, playlist = load_clear_playlist(work_dir=work_dir)
    global clear_segments_cache
    for clear_segment in playlist.files[2:]:
        if clear_segment not in clear_segments_cache:
            debug_print('New clear segment has been added to local cache')
            clear_segments_cache.append(clear_segment)
            if len(clear_segments_cache) > chunks_number:
                clear_segments_cache.popleft()

            return clear_segment


def update_encrypted_playlist(iv: str, content_id: str, work_dir: str) -> None:
    """
    Updates encrypted playlist (based on clear playlist) with new key
    :param iv: iv
    :param content_id: content id
    :param work_dir: work dir
    :return: None
    """

    clear_playlist_name, playlist = load_clear_playlist(work_dir=work_dir)
    debug_print(f'Encrypted playlist renewing started')
    uri = f'{key_client_url}{content_id}.bin'
    new_key = m3u8.Key(method="AES-128", base_uri=uri,
                       uri=uri, iv=iv)

    for segment in playlist.segments:
        segment.key = new_key
        segment.uri = ENC_SUFFIX + segment.uri
    playlist.keys[-1] = new_key
    playlist.dump(os.path.join(work_dir, clear_playlist_name.replace(clear_playlist_suffix, '')))
    debug_print(f'Encrypted playlist renewing ended')


def update_encrypted_data(file: str, key: str, iv: str, content_id: str, work_dir: str) -> None:
    """
    Encrypts clear chunk and updates encrypted playlist
    :param file: chunk name
    :param key: key
    :param iv: iv
    :param content_id: content id
    :param work_dir: work dir
    :return: None
    """

    encrypt(input_file_name=file, key=key, iv=iv, work_dir=work_dir)
    update_encrypted_playlist(iv=iv, content_id=content_id, work_dir=work_dir)


if __name__ == '__main__':

    remove_files_from_previous_start(work_dir=hls_dir)  # remove files from previous start
    key, iv = get_key(content_id=content_id)  # get key and iv from MultiDRM

    # encrypt HLS
    while True:
        new_segment = check_for_new_clear_chunk(work_dir=hls_dir)
        if new_segment:
            update_encrypted_data(file=new_segment, key=key, iv=iv, content_id=content_id, work_dir=hls_dir)
        sleep(refresh_interval)
