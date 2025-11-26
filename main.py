import os
from pathlib import Path
import re
import subprocess
import dotenv
import gspread
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaFileUpload
from googleapiclient.discovery import build
import requests
import yt_dlp

import config

session = requests.Session()
dotenv.load_dotenv()
api_key = config.YTAPI_KEY
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file('credentials/sauce-plus-api.json', scopes=scopes)
drive_service = build('drive', 'v3', credentials=creds)
client = gspread.authorize(creds)
sheets_id = '1y1E7CT-1TxGXdGpLFfioYDcOknlyuDs22fOOKcZmXvo'
sheet = client.open_by_key(sheets_id)

def get_sheet_index(sheet: gspread.Spreadsheet):
    index_sheet = sheet.worksheet('Index')
    records = index_sheet.get_all_records()
    index = {}
    for record in records:
        creator_info: dict = index.setdefault(record['Key'], {})
        creator_info['handle'] = record['Handle'] or None
        creator_info['video_drive_link'] = record['Video Drive Link'] or None
        creator_info['channel_id'] = record['Channel ID'] or None
        creator_info['title'] = record['Title'] or None
        creator_info['created'] = record['Created'] or None
        creator_info['description'] = record['Description'] or None
        creator_info['country'] = record['Country'] or None
        creator_info['keywords'] = record['Keywords'] or None
        creator_info['icon'] = record['Icon'] or None
        creator_info['banner'] = record['Banner'] or None
        creator_info['uploads_id'] = record['Uploads ID'] or None
    return index

def get_channel_id(channel_handle: str, api_key: str) -> str:
    handle = channel_handle.lstrip('@')
    url = f'https://www.googleapis.com/youtube/v3/channels?part=id&forHandle={handle}&key={api_key}'
    response = session.get(url)
    response.raise_for_status()
    data = response.json()
    if not data.get('items'):
        raise ValueError(f'Channel not found: {channel_handle}')
    return data['items'][0]['id']

def get_channel_branding(data, channel_id):
    icon_url = None
    if 'high' in data['snippet']['thumbnails']:
        icon_url = data['snippet']['thumbnails']['high']['url']
    elif 'medium' in data['snippet']['thumbnails']:
        icon_url = data['snippet']['thumbnails']['medium']['url']
    elif 'default' in data['snippet']['thumbnails']:
        icon_url = data['snippet']['thumbnails']['default']['url']
    banner_url = None
    if 'image' in data['brandingSettings'] and 'bannerExternalUrl' in data['brandingSettings']['image']:
        banner_url = data['brandingSettings']['image']['bannerExternalUrl']
    elif 'image' in data['brandingSettings'] and 'bannerImageUrl' in data['brandingSettings']['image']:
        banner_url = data['brandingSettings']['image']['bannerImageUrl']
    if not banner_url:
        banner_url = f"https://yt3.googleusercontent.com/banner-vfl/{channel_id}"
    return {
        'icon_url': icon_url,
        'banner_url': f"{banner_url}=w2560-fcrop64=1,00000000ffffffff-k-c0xffffffff-no-nd-rj"
    }

def update_creator_index(key: str, index: dict):
    if index[key]['channel_id'] != None:
        return
    channel_id = get_channel_id(index[key]['handle'], api_key)
    index[key]['channel_id'] = channel_id
    url = f'https://www.googleapis.com/youtube/v3/channels?part=snippet,brandingSettings,contentDetails&id={channel_id}&key={api_key}'
    response = session.get(url)
    response.raise_for_status()
    data = response.json()['items'][0]
    index[key]['title'] = data['brandingSettings']['channel']['title']
    index[key]['created'] = data['snippet']['publishedAt']
    if 'description' in data['brandingSettings']['channel']:
        index[key]['description'] = data['brandingSettings']['channel']['description']
    if 'keywords' in data['brandingSettings']['channel']:
        index[key]['keywords'] = data['brandingSettings']['channel']['keywords']
    if 'country' in data['brandingSettings']['channel']:
        index[key]['country'] = data['brandingSettings']['channel']['country']
    branding = get_channel_branding(data, channel_id)
    index[key]['icon'] = branding['icon_url']
    index[key]['banner'] = branding['banner_url']
    index[key]['uploads_id'] = data['contentDetails']['relatedPlaylists']['uploads']

def set_sheet_index(sheet: gspread.Spreadsheet, index: dict[str, dict]):
    index_sheet = sheet.worksheet('Index')
    headers = ['Key', 'Handle', 'Video Drive Link', 'Channel ID', 'Title', 'Created', 'Description', 'Country', 'Keywords', 'Icon', 'Banner', 'Uploads ID']
    rows = [headers]
    for key, creator_info in index.items():
        row = [
            key,
            creator_info.get('handle', ''),
            creator_info.get('video_drive_link', ''),
            creator_info.get('channel_id', ''),
            creator_info.get('title', ''),
            creator_info.get('created', ''),
            creator_info.get('description', ''),
            creator_info.get('country', ''),
            creator_info.get('keywords', ''),
            creator_info.get('icon', ''),
            creator_info.get('banner', ''),
            creator_info.get('uploads_id', '')
        ]
        rows.append(row)
    index_sheet.clear()
    index_sheet.update(rows, 'A1')

def get_video_ids(uploads_playlist_id, api_key):
    video_ids = []
    next_page_token = None
    while True:
        url = f'https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId={uploads_playlist_id}&key={api_key}'
        if next_page_token:
            url += f'&pageToken={next_page_token}'
        response = session.get(url)
        response.raise_for_status()
        data = response.json()
        video_ids += [item['contentDetails']['videoId'] for item in data['items']]
        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            break
    return video_ids

def format_seconds(seconds):
    total_seconds = round(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}:{minutes:02}:{seconds:02}"

async def get_sponsorblock_data_async(session_async: aiohttp.ClientSession, video_id) -> list[str]:
    url = 'https://sponsor.ajay.app/api/skipSegments'
    params = {'videoID': video_id}
    try:
        async with session_async.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                json_data = await response.json()
                output = []
                for item in json_data:
                    output.append(f'{format_seconds(item["segment"][0])} - {format_seconds(item["segment"][1])}')
                return output
            else:
                return []
    except Exception:
        return []

async def fetch_all_sponsorblock_data(video_ids: list[str]) -> dict[str, list[str]]:
    async with aiohttp.ClientSession() as session_async:
        tasks = [get_sponsorblock_data_async(session_async, vid_id) for vid_id in video_ids]
        results = await asyncio.gather(*tasks)
        return dict(zip(video_ids, results))

def get_video_metadata(video_ids, api_key):
    video_details: list[dict] = []
    for i in range(0, len(video_ids), 50):
        ids = ','.join(video_ids[i:i+50])
        url = f'https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics&id={ids}&key={api_key}'
        response = session.get(url)
        response.raise_for_status()
        video_details += response.json()['items']
    output = {}
    for item in video_details:
        item_id = item.pop('id')
        output[item_id] = item
    return output

def duration_to_seconds(duration_str):
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration_str)
    if not match:
        return 0
    hours, minutes, seconds = match.groups()
    hours = int(hours) if hours else 0
    minutes = int(minutes) if minutes else 0
    seconds = int(seconds) if seconds else 0
    return hours * 3600 + minutes * 60 + seconds

def get_video_thumbnail_url(metadata):
    thumbnails = metadata.get('snippet', {}).get('thumbnails', {})
    for quality in ['maxres', 'standard', 'high']:
        if quality in thumbnails:
            return thumbnails[quality]['url']
    return None

def get_files_in_folder(folder_id: str, creds) -> list[dict]:
    files = []
    page_token = None
    while True:
        results = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            pageSize=100,
            fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size, webViewLink)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=page_token
        ).execute()
        files.extend(results.get('files', []))
        page_token = results.get('nextPageToken')
        if not page_token:
            break
    return files

def get_list_of_mp4_files(folder_id: str, creds) -> list[str]:
    files = get_files_in_folder(folder_id, creds)
    return [str(file['name']).removesuffix('.mp4') for file in files if file['mimeType'] == 'video/mp4']

def extract_creator_index() -> tuple[dict, list[str]]:
    index = get_sheet_index(sheet)
    worksheets = sheet.worksheets()
    creator_keys = [ws.title for ws in worksheets if ws.title != 'Index' and ws.title in index.keys()]
    for key in creator_keys:
        update_creator_index(key, index)
    set_sheet_index(sheet, index)
    return index, creator_keys

def check_uploaded_videos(index: dict, key: str, video_ids: list[str], video_index: dict):
    mp4_files = get_list_of_mp4_files(index[key]['video_drive_link'], creds)
    for idx, yt_id in enumerate(reversed(video_ids)):
        internal_id = f'{key}_{str(idx+1).zfill(5)}'
        video_data: dict = video_index.setdefault(yt_id, {})
        video_data['internal_id'] = internal_id
        if internal_id in mp4_files:
            video_data['status'] = 'uploaded'

def index_videos(index: dict, key: str):
    creator_sheet = sheet.worksheet(key)
    headers = ['YouTube ID', 'Internal ID', 'Status', 'Title', 'Publish Date', 'Duration', 'Description', 'Ad Timestamps', 'Thumbnail', 'Tags', 'Views', 'Likes', 'Comments']
    creator_sheet.update([headers], 'A1')
    video_ids = get_video_ids(index[key]['uploads_id'], api_key)
    records = creator_sheet.get_all_records()
    video_index = {}
    for record in records:
        video_data: dict = video_index.setdefault(record['YouTube ID'], {})
        video_data['internal_id'] = record['Internal ID'] or None
        video_data['status'] = record['Status'] or None
        video_data['title'] = record['Title'] or None
        video_data['publish_date'] = record['Publish Date'] or None
        video_data['duration'] = record['Duration'] or None
        video_data['description'] = record['Description'] or None
        video_data['ad_timestamps'] = record['Ad Timestamps'] or None
        video_data['thumbnail'] = record['Thumbnail'] or None
        video_data['tags'] = record['Tags'] or None
        video_data['views'] = record['Views'] or None
        video_data['likes'] = record['Likes'] or None
        video_data['comments'] = record['Comments'] or None
    missing_ids = [id for id in video_ids if id not in video_index.keys()]
    video_metadata = get_video_metadata(video_ids, api_key)
    check_uploaded_videos(index, key, video_ids, video_index)
    sponsorblock_results = asyncio.run(fetch_all_sponsorblock_data(missing_ids))
    for yt_id in missing_ids:
        video_data: dict = video_index.setdefault(yt_id, {})
        metadata = video_metadata[yt_id]
        sponsorblock_data = sponsorblock_results.get(yt_id, [])
        video_data['status'] = 'indexed'
        video_data['title'] = metadata['snippet']['title']
        video_data['publish_date'] = metadata['snippet']['publishedAt']
        video_data['duration'] = duration_to_seconds(metadata['contentDetails']['duration'])
        video_data['description'] = metadata['snippet']['description']
        video_data['ad_timestamps'] = ', '.join(sponsorblock_data)
        video_data['thumbnail'] = get_video_thumbnail_url(metadata)
        video_data['tags'] = str(metadata['snippet'].get('tags', []))
        video_data['views'] = metadata['statistics']['viewCount']
        video_data['likes'] = metadata['statistics'].get('likeCount', 'N/A')
        video_data['comments'] = metadata['statistics'].get('commentCount', 'N/A')
    rows = [headers]
    for yt_id in video_ids:
        video_data = video_index.get(yt_id, {})
        row = [
            yt_id,
            video_data.get('internal_id', ''),
            video_data.get('status', ''),
            video_data.get('title', ''),
            video_data.get('publish_date', ''),
            video_data.get('duration', ''),
            video_data.get('description', ''),
            video_data.get('ad_timestamps', ''),
            video_data.get('thumbnail', ''),
            video_data.get('tags', ''),
            video_data.get('views', ''),
            video_data.get('likes', ''),
            video_data.get('comments', '')
        ]
        rows.append(row)
    creator_sheet.clear()
    creator_sheet.update(rows, 'A1')

def download_video(video_id: str, internal_id: str) -> bool:
    os.makedirs('downloaded', exist_ok=True)
    output_path = f"downloaded/{internal_id}.mp4"
    if os.path.exists(output_path):
        return True
    ydl_opts = {
        'cookiefile': 'credentials/youtube_cookies.txt',
        'quiet': True,
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'outtmpl': output_path,
        'merge_output_format': 'mp4',
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f'https://www.youtube.com/watch?v={video_id}'])
        return True
    except Exception as e:
        return False

def download_videos(key: str):
    creator_sheet = sheet.worksheet(key)
    records = creator_sheet.get_all_records()
    videos_to_download = [
        (record['YouTube ID'], record['Internal ID'])
        for record in records
        if record['Status'] == 'indexed' and record['YouTube ID'] and record['Internal ID']
    ]
    for yt_id, internal_id in videos_to_download:
        download_video(yt_id, internal_id)

def get_codecs(input_path: str) -> tuple[str, str]:
    def probe(stream_type: str) -> str:
        result = subprocess.run(
            ['ffprobe', '-v', 'error',
             '-select_streams', f'{stream_type}:0',
             '-show_entries', 'stream=codec_name',
             '-of', 'default=nw=1:nk=1',
             input_path],
            capture_output=True, text=True
        )
        return result.stdout.strip()
    video_codec = probe('v')
    audio_codec = probe('a')
    return video_codec, audio_codec

def reencode_video(input_path: str, output_path: str) -> bool:
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        video_codec, audio_codec = get_codecs(input_path)
        if video_codec == 'h264' and (audio_codec == 'aac' or audio_codec == ''):
            subprocess.run([
                'ffmpeg', '-y',
                '-i', input_path,
                '-c', 'copy',
                output_path
            ], check=True, capture_output=True)
        else:
            subprocess.run([
                'ffmpeg', '-y',
                '-i', input_path,
                '-c:v', 'libx264',
                '-preset', 'fast',
                '-crf', '23',
                '-c:a', 'aac',
                '-b:a', '128k',
                output_path
            ], check=True, capture_output=True)
        return True
    except:
        return False

def encode_videos():
    downloaded_path = Path('downloaded')
    encoded_path = Path('encoded')
    if not downloaded_path.exists():
        return
    video_files = list(downloaded_path.glob('**/*.mp4'))
    if not video_files:
        return
    for input_file in video_files:
        relative_path = input_file.relative_to(downloaded_path)
        output_file = encoded_path / relative_path
        if output_file.exists():
            continue
        reencode_video(str(input_file), str(output_file))

def upload_file(folder_id: str, internal_id: str) -> str:
    file_name = f"{internal_id}.mp4"
    file_path = f"encoded/{file_name}"
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, mimetype='video/mp4', resumable=True)
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id',
        supportsAllDrives=True
    ).execute()
    return file.get('id')

if __name__ == '__main__':
    download_video('N0VFuy-OC4o', 'ALNP_00113')
    encode_videos()
    print(upload_file('18Ff1kTAtibzcSwR4YAtmGmjruMTn4zM9', 'ALNP_00113'))
    # index, creator_keys = extract_creator_index()
    # for key in creator_keys:
    #     # index_videos(index, key)
    #     download_videos(key)
