import datetime

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from starlette.responses import PlainTextResponse

from streamer.PlaylistGenerator import PlaylistGenerator

API_PREFIX = 'streamer'

app = FastAPI()


# @app.get(f"/{API_PREFIX}/GetNPVRPlayList")
# def sgw_format_get_playlist(request: Request):
#     redirect_url = request.url_for('get_playlist').include_query_params(**request.query_params)
#     return RedirectResponse(redirect_url)


@app.get(f"/{API_PREFIX}/GetNPVRPlayList", response_class=PlainTextResponse)
@app.get(f"/{API_PREFIX}/{{channel_name}}.m3u8", response_class=PlainTextResponse)
async def get_playlist(channel_name: str,
                       startTime: str = None,
                       endTime: str = None,
                       startTimestamp: int = None,
                       endTimestamp: int = None):
    if startTime is not None:
        from_datetime = datetime.datetime.strptime(startTime, '%d/%m/%YT%H:%M:%S')
    elif startTimestamp is not None:
        from_datetime = datetime.datetime.fromtimestamp(startTimestamp)

    if endTime is not None:
        to_datetime = datetime.datetime.strptime(endTime, '%d/%m/%YT%H:%M:%S')
    elif endTimestamp is not None:
        to_datetime = datetime.datetime.fromtimestamp(endTimestamp)
    else:
        to_datetime = datetime.datetime.now()

    generator = PlaylistGenerator(channel_name=channel_name)
    playlist_str = await generator.generate_vod_playlist(from_datetime=from_datetime,
                                                         to_datetime=to_datetime)

    return playlist_str


@app.get(f"/{API_PREFIX}/{{channel_name}}/metadata")
async def get_metadata(channel_name: str,
                       startTime: str,
                       endTime: str = None):
    from_datetime = datetime.datetime.strptime(startTime, '%d/%m/%YT%H:%M:%S')
    to_datetime = datetime.datetime.strptime(endTime,
                                             '%d/%m/%YT%H:%M:%S') if endTime is not None else datetime.datetime.now()

    generator = PlaylistGenerator(channel_name=channel_name)
    metadata = await generator.get_metadata_for_interval(from_datetime=from_datetime,
                                                         to_datetime=to_datetime)

    return metadata
