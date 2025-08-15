#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import asyncio
import configparser
from asyncio import sleep
from typing import List

from streamer import HlsWriter, HlsDeleter

PWD = os.getcwd()


async def write(writer_: HlsWriter):
    sleep_time = 1
    while True:
        target_duration = await writer_.check_for_new_segment_and_save()
        if target_duration is not None:
            sleep_time = target_duration / 2
        await sleep(sleep_time)


async def clean(cleaner_: HlsDeleter):
    sleep_time = 10
    while True:
        await cleaner_.clear_storage_and_db()
        await sleep(sleep_time)


async def process_writing_and_cleaning(channels: List):
    tasks = []
    for channel in channels:
        channel_name = channel['source'].split('/')[-1].split('.')[0]
        channel_storage = os.path.join(storage, channel_name)
        if not os.path.exists(channel_storage):
            os.makedirs(channel_storage)

        writer = HlsWriter(channel_name=channel_name, storage=channel_storage, source_url=channel['source'])
        cleaner = HlsDeleter(channel_name=channel_name, storage=channel_storage, depth_in_hours=channel['depth_in_hours'])
        write_task = asyncio.create_task(write(writer))
        clean_task = asyncio.create_task(clean(cleaner))
        tasks.append(write_task)
        tasks.append(clean_task)

    await asyncio.gather(*tasks)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("pvr.ini")
    storage = config['writer']['pvr_dir']

    with open('channels.json', encoding='utf-8') as f:
        channels_ = json.load(f)

    asyncio.run(process_writing_and_cleaning(channels=channels_))
