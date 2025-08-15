# HLS Encryptor and Local PVR

## Установка
``pip install -r requirements.txt``

## Encryptor
### Запуск
``cp config.ini.example config.ini; vi config.ini``
``python encryptor.py``

### Функциональность
- сервис слушает playlist ``clear_playlist_name`` в папке ``hls_dir`` на предмет появления новых HLS-чанков
- при появлении нового чанка шифрует его (через openssl) ключом, полученным с DRM-бэкенда
- и обновляет выходной манифест (с шифрованным контентом), добавляя новую запись о появившемся чанке

## PVR
### Запуск
#### Запуск writer (и cleaner)
``cp pvr.ini.example pvr.ini; vi pvr.ini``
``cp channels.json.example channels.json; vi channels.json``
``python3 init_db.py``
``python writer.py``
#### Запуск API (develop server)
``fastapi dev api.py``

### Функциональность
- сервис скачивает hls live playlist по ``source_url`` и записывает чанки в архив в ``pvr_dir`` с глубиной хранения ``depth_in_hours``
- по web API отдает hls vod playlist на запрошенный интервал времени, а также статистику по наличию записей на запрошенный интервал времени

### Пример запроса playlist и ответа по API
``http://127.0.0.1:8000/streamer/domophone-1-sensor_camera0.m3u8?startTime=5/7/2024T12:22:03&endTime=5/7/2024T12:24:00``
``http://127.0.0.1:8000/streamer/GetNPVRPlayList?channel_name=domophone-1-sensor_camera0&startTime=5/7/2024T12:22:03&endTime=5/7/2024T12:24:00``
``http://127.0.0.1:8000/streamer/domophone-1-sensor_camera0.m3u8?startTimestamp=1718708400&endTimestamp=1718726400``
``http://127.0.0.1:8000/streamer/GetNPVRPlayList?channel_name=domophone-1-sensor_camera0&startTimestamp=1718708400&endTimestamp=1718726400``

```
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:5
#EXT-X-PLAYLIST-TYPE:VOD
#EXTINF:5,
c_19888564002095173.ts
#EXTINF:5,
c_19888564002095174.ts
#EXTINF:5,
c_19888564002095175.ts
#EXTINF:5,
c_19888564002095176.ts
#EXTINF:5,
c_19888564002095177.ts
#EXTINF:5,
c_19888564002095178.ts
#EXTINF:5,
c_19888564002095179.ts
#EXTINF:5,
c_19888564002095180.ts
#EXTINF:5,
c_19888564002095181.ts
#EXTINF:5,
c_19888564002095182.ts
#EXTINF:5,
c_19888564002095183.ts
#EXTINF:5,
c_19888564002095184.ts
#EXTINF:5,
c_19888564002095185.ts
#EXT-X-ENDLIST
```

### Пример запроса заполненности интервала и ответа
``http://127.0.0.1:8000/metadata?startTime=18/06/2024T00:00:00&endTime=18/06/2024T19:00:33``
```json
[
{
"start_datetime": "2024-06-18T17:45:24.005249",
"end_datetime": "2024-06-18T17:55:06.011263"
},
{
"start_datetime": "2024-06-18T18:19:56.065960",
"end_datetime": "2024-06-18T18:26:27.939446"
}
]
```
