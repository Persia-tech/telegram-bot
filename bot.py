import asyncio
import json
import logging
import mimetypes
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse, urlunparse

import boto3
import yt_dlp
from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# 1) CONFIG
# =========================
TOKEN = "8610722970:AAGZl0imUOo5RF0AsgbiUSD5cHWdh1RJ9rI"

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
TASK_DOWNLOAD_DIR = DOWNLOAD_DIR / "tasks"
ARCHIVE_FILE = BASE_DIR / "archive.txt"
CACHE_DB = BASE_DIR / "download_cache.sqlite3"

DOWNLOAD_DIR.mkdir(exist_ok=True)
TASK_DOWNLOAD_DIR.mkdir(exist_ok=True)

MAX_TELEGRAM_UPLOAD_BYTES = 50 * 1024 * 1024
MAX_CONCURRENT_DOWNLOADS = 4
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = [2, 4, 8]
CLEANUP_AFTER_HOURS = 24
CLEANUP_SCAN_INTERVAL_SECONDS = 3600
YT_DLP_SOCKET_TIMEOUT_SECONDS = 30
PROGRESS_UPDATE_INTERVAL_SECONDS = 2
QUEUE_PREVIEW_LIMIT = 10
RECENT_HISTORY_LIMIT = 20
TOP_USERS_PREVIEW_LIMIT = 10

# R2 settings
R2_ACCOUNT_ID = "a0a9c6a7e45d4ae96756a2db856066fd"
R2_ACCESS_KEY_ID = "fd9d27ba0b057574f0749a7cdad5ace0"
R2_SECRET_ACCESS_KEY = "5db417d6068fdd77fd6ecac454acf08248687e7132ce43bfc29d7f37ca3783e3"
R2_BUCKET_NAME = "telegram-media-bot"
R2_PUBLIC_BASE_URL = "https://pub-5ff93ff86e2940c3b6cbb16f285bb524.r2.dev"
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# Optional browser cookies support
USE_BROWSER_COOKIES = False
BROWSER_NAME = "firefox"

# =========================
# 2) LOGGING
# =========================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("telegram_media_bot")


# =========================
# 3) DATA MODELS
# =========================
@dataclass
class Requester:
    chat_id: int
    status_message_id: Optional[int] = None
    requested_at: float = field(default_factory=time.time)


@dataclass
class DownloadTask:
    url: str
    url_key: str
    requesters: List[Requester] = field(default_factory=list)
    enqueued_at: float = field(default_factory=time.time)


@dataclass
class RuntimeStats:
    started_at: float = field(default_factory=time.time)
    total_requests: int = 0
    completed_downloads: int = 0
    failed_downloads: int = 0
    cache_hits: int = 0
    upload_to_telegram: int = 0
    upload_to_r2: int = 0
    unique_users: set = field(default_factory=set)
    user_requests: Dict[int, int] = field(default_factory=dict)
    recent_events: List[str] = field(default_factory=list)

    def add_user_request(self, chat_id: int) -> None:
        self.unique_users.add(chat_id)
        self.user_requests[chat_id] = self.user_requests.get(chat_id, 0) + 1

    def add_recent(self, message: str) -> None:
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        self.recent_events.append(f"{timestamp} | {message}")
        if len(self.recent_events) > RECENT_HISTORY_LIMIT:
            self.recent_events = self.recent_events[-RECENT_HISTORY_LIMIT:]


# =========================
# 4) CACHE + ADMIN STORAGE
# =========================
class BotStorage:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS download_cache (
                    url_key TEXT PRIMARY KEY,
                    original_url TEXT NOT NULL,
                    results_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    last_accessed REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER PRIMARY KEY,
                    created_at REAL NOT NULL
                )
                """
            )
            conn.commit()

    def get_cache(self, url_key: str) -> Optional[List[dict]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT results_json FROM download_cache WHERE url_key = ?",
                (url_key,),
            ).fetchone()
            if not row:
                return None
            conn.execute(
                "UPDATE download_cache SET last_accessed = ? WHERE url_key = ?",
                (time.time(), url_key),
            )
            conn.commit()
            return json.loads(row["results_json"])

    def set_cache(self, url_key: str, original_url: str, results: List[dict]) -> None:
        now = time.time()
        results_json = json.dumps(results, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO download_cache (url_key, original_url, results_json, created_at, last_accessed)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(url_key) DO UPDATE SET
                    original_url = excluded.original_url,
                    results_json = excluded.results_json,
                    last_accessed = excluded.last_accessed
                """,
                (url_key, original_url, results_json, now, now),
            )
            conn.commit()

    def delete_cache(self, url_key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM download_cache WHERE url_key = ?", (url_key,))
            conn.commit()

    def all_cache_entries(self) -> List[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT url_key, original_url, results_json FROM download_cache"
            ).fetchall()

    def count_cache_entries(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM download_cache").fetchone()
            return int(row["count"])

    def clear_cache(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM download_cache").fetchone()
            deleted = int(row["count"])
            conn.execute("DELETE FROM download_cache")
            conn.commit()
            return deleted

    def add_admin(self, user_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO admins (user_id, created_at) VALUES (?, ?)",
                (user_id, time.time()),
            )
            conn.commit()

    def remove_admin(self, user_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
            conn.commit()

    def is_admin(self, user_id: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM admins WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            return row is not None

    def admin_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM admins").fetchone()
            return int(row["count"])

    def list_admins(self) -> List[int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT user_id FROM admins ORDER BY created_at").fetchall()
            return [int(row["user_id"]) for row in rows]


# =========================
# 5) GLOBAL STATE
# =========================
download_queue: asyncio.Queue[DownloadTask] = asyncio.Queue()
pending_tasks: Dict[str, DownloadTask] = {}
pending_lock = asyncio.Lock()
storage = BotStorage(CACHE_DB)
worker_tasks: List[asyncio.Task] = []
cleanup_task: Optional[asyncio.Task] = None
runtime_stats = RuntimeStats()

progress_snapshots: Dict[str, dict] = {}
progress_lock = threading.Lock()


# =========================
# 6) HELPERS
# =========================
def looks_like_url(text: str) -> bool:
    try:
        parsed = urlparse(text.strip())
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    except Exception:
        return False


def normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    query_items = []
    if parsed.query:
        query_items = sorted(
            (part for part in parsed.query.split("&") if part),
            key=lambda x: x.lower(),
        )
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        fragment="",
        query="&".join(query_items),
    )
    return urlunparse(normalized)


def make_safe_task_dir(url_key: str) -> Path:
    safe_name = "".join(ch for ch in url_key if ch.isalnum())[:40] or "task"
    task_dir = TASK_DOWNLOAD_DIR / safe_name
    task_dir.mkdir(parents=True, exist_ok=True)
    return task_dir


def human_bytes(value: Optional[float]) -> str:
    if value is None:
        return "?"
    size = float(value)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def human_seconds(value: Optional[float]) -> str:
    if value is None:
        return "?"
    seconds = max(0, int(value))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def progress_bar(percent: float, width: int = 12) -> str:
    percent = max(0.0, min(100.0, percent))
    filled = int(round((percent / 100.0) * width))
    return "█" * filled + "░" * (width - filled)


def format_progress_text(snapshot: dict) -> str:
    status = snapshot.get("status", "queued")
    filename = snapshot.get("filename") or "media file"
    if status == "finished":
        return f"Download finished. Preparing delivery for {filename}..."

    percent = float(snapshot.get("percent", 0.0))
    downloaded = human_bytes(snapshot.get("downloaded_bytes"))
    total = human_bytes(snapshot.get("total_bytes"))
    speed = human_bytes(snapshot.get("speed"))
    eta = human_seconds(snapshot.get("eta"))
    bar = progress_bar(percent)
    return (
        f"Downloading {filename}\n"
        f"[{bar}] {percent:.1f}%\n"
        f"{downloaded} / {total} • {speed}/s • ETA {eta}"
    )


def safe_edit_message_text_request(chat_id: int, message_id: int, text: str) -> tuple[int, int, str]:
    return (chat_id, message_id, text)


async def edit_message_if_possible(application: Application, chat_id: int, message_id: int, text: str) -> None:
    try:
        await application.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
    except Exception:
        pass


def update_progress_snapshot(url_key: str, data: dict) -> None:
    status = data.get("status")
    downloaded_bytes = data.get("downloaded_bytes") or 0
    total_bytes = data.get("total_bytes") or data.get("total_bytes_estimate") or 0
    speed = data.get("speed")
    eta = data.get("eta")
    filename = Path(data.get("filename", "media")).name

    percent = 0.0
    if total_bytes:
        percent = (downloaded_bytes / total_bytes) * 100

    with progress_lock:
        progress_snapshots[url_key] = {
            "status": status,
            "percent": percent,
            "downloaded_bytes": downloaded_bytes,
            "total_bytes": total_bytes,
            "speed": speed,
            "eta": eta,
            "filename": filename,
            "updated_at": time.time(),
        }


def get_progress_snapshot(url_key: str) -> Optional[dict]:
    with progress_lock:
        snapshot = progress_snapshots.get(url_key)
        return dict(snapshot) if snapshot else None


def clear_progress_snapshot(url_key: str) -> None:
    with progress_lock:
        progress_snapshots.pop(url_key, None)


def make_progress_hook(url_key: str):
    def hook(data: dict) -> None:
        status = data.get("status")
        if status in {"downloading", "finished"}:
            update_progress_snapshot(url_key, data)

    return hook


def build_ydl_opts(url: str, task_dir: Path, progress_hook=None) -> dict:
    normalized = url.lower()
    is_youtube = "youtube.com" in normalized or "youtu.be" in normalized
    is_instagram = "instagram.com" in normalized

    opts = {
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "outtmpl": str(
            task_dir
            / "%(extractor)s"
            / "%(uploader,channel|unknown)s"
            / "%(playlist_title,playlist|single)s"
            / "%(playlist_index|NA)s - %(title).80s [%(id)s].%(ext)s"
        ),
        "writeinfojson": True,
        "download_archive": str(ARCHIVE_FILE),
        "quiet": True,
        "no_warnings": False,
        "noprogress": True,
        "ignoreerrors": True,
        "restrictfilenames": False,
        "noplaylist": False,
        "retries": 5,
        "fragment_retries": 5,
        "concurrent_fragment_downloads": 8,
        "socket_timeout": YT_DLP_SOCKET_TIMEOUT_SECONDS,
        "file_access_retries": 3,
        "http_chunk_size": 10485760,
        "buffersize": 1048576,
        "format_sort": ["res", "fps", "codec:h264"],
    }

    if is_youtube:
        opts.update(
            {
                "js_runtimes": {"node": {}},
                "remote_components": ["ejs:github"],
                "extractor_args": {
                    "youtube": {
                        "player_client": ["android", "web"],
                    }
                },
            }
        )
        youtube_cookies = BASE_DIR / "youtube_cookies.txt"
        if youtube_cookies.exists():
            opts["cookiefile"] = str(youtube_cookies)
    elif is_instagram:
        instagram_cookies = BASE_DIR / "instagram_cookies.txt"
        if instagram_cookies.exists():
            opts["cookiefile"] = str(instagram_cookies)

    if progress_hook:
        opts["progress_hooks"] = [progress_hook]

    if USE_BROWSER_COOKIES:
        opts["cookiesfrombrowser"] = (BROWSER_NAME,)

    return opts


async def run_blocking(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


def upload_file_to_r2(file_path: str) -> str:
    s3 = boto3.client(
        service_name="s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )

    object_key = Path(file_path).name
    content_type, _ = mimetypes.guess_type(file_path)
    if content_type is None:
        content_type = "application/octet-stream"

    with open(file_path, "rb") as file_obj:
        s3.upload_fileobj(
            file_obj,
            R2_BUCKET_NAME,
            object_key,
            ExtraArgs={"ContentType": content_type},
        )

    return f"{R2_PUBLIC_BASE_URL}/{object_key}"


def collect_media_files(task_dir: Path) -> List[Path]:
    media_files: List[Path] = []
    for path in task_dir.rglob("*"):
        if not path.is_file():
            continue
        lower_name = path.name.lower()
        if lower_name.endswith(".info.json"):
            continue
        if lower_name.endswith(".part"):
            continue
        media_files.append(path)
    return sorted(media_files)


def perform_download(url: str, task_dir: Path, url_key: str) -> List[Path]:
    ydl_opts = build_ydl_opts(url, task_dir, progress_hook=make_progress_hook(url_key))
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        if info is None:
            raise RuntimeError("yt-dlp could not extract media from this URL.")

    media_files = collect_media_files(task_dir)
    if not media_files:
        raise RuntimeError(
            "No media file was produced. It may already be archived, skipped, or unsupported."
        )
    return media_files


async def download_with_retry(url: str, task_dir: Path, url_key: str) -> List[Path]:
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return await run_blocking(perform_download, url, task_dir, url_key)
        except Exception as exc:
            last_error = exc
            logger.warning(
                "download_failed_attempt | url=%s | attempt=%s/%s | error=%s",
                url,
                attempt,
                MAX_RETRIES,
                exc,
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(
                    RETRY_BACKOFF_SECONDS[min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1)]
                )

    raise RuntimeError(f"Download failed after {MAX_RETRIES} attempts: {last_error}")


def validate_cached_results(results: List[dict]) -> List[dict]:
    valid_results: List[dict] = []
    for item in results:
        kind = item.get("kind")
        if kind == "local":
            file_path = item.get("path")
            if file_path and Path(file_path).exists():
                valid_results.append(item)
        elif kind == "r2":
            if item.get("url"):
                valid_results.append(item)
    return valid_results


def guess_media_kind(file_path: Path) -> str:
    suffix = file_path.suffix.lower()
    if suffix in {".mp4", ".mkv", ".mov", ".webm", ".m4v"}:
        return "video"
    if suffix in {".mp3", ".m4a", ".wav", ".aac", ".ogg", ".flac"}:
        return "audio"
    return "document"


async def send_results(application: Application, requester: Requester, results: List[dict], source: str) -> None:
    delivered = 0
    for item in results:
        kind = item.get("kind")
        if kind == "local":
            file_path = item.get("path")
            if not file_path or not Path(file_path).exists():
                continue

            file_obj = open(file_path, "rb")
            try:
                path = Path(file_path)
                media_kind = guess_media_kind(path)
                caption = path.name[:100]
                if media_kind == "video":
                    await application.bot.send_video(
                        chat_id=requester.chat_id,
                        video=file_obj,
                        caption=caption,
                        supports_streaming=True,
                    )
                elif media_kind == "audio":
                    await application.bot.send_audio(
                        chat_id=requester.chat_id,
                        audio=file_obj,
                        caption=caption,
                    )
                else:
                    await application.bot.send_document(
                        chat_id=requester.chat_id,
                        document=file_obj,
                        caption=caption,
                    )
                runtime_stats.upload_to_telegram += 1
                delivered += 1
            finally:
                file_obj.close()

        elif kind == "r2":
            public_url = item.get("url")
            file_name = item.get("name") or "file"
            if public_url:
                await application.bot.send_message(
                    chat_id=requester.chat_id,
                    text=f"File too large for Telegram. Download here:\n{file_name}\n{public_url}",
                )
                delivered += 1

    if requester.status_message_id:
        if delivered == 0:
            await edit_message_if_possible(
                application,
                requester.chat_id,
                requester.status_message_id,
                "Nothing could be delivered. The cached files are no longer available.",
            )
        else:
            await edit_message_if_possible(
                application,
                requester.chat_id,
                requester.status_message_id,
                f"Done. Delivered {delivered} file(s) from {source}.",
            )


async def notify_failure(application: Application, requester: Requester, error_text: str) -> None:
    if requester.status_message_id:
        await edit_message_if_possible(
            application,
            requester.chat_id,
            requester.status_message_id,
            f"Error: {error_text}",
        )
    else:
        await application.bot.send_message(chat_id=requester.chat_id, text=f"Error: {error_text}")


async def prepare_results(media_files: List[Path]) -> List[dict]:
    results: List[dict] = []

    for file_path in media_files:
        file_size = file_path.stat().st_size
        if file_size > MAX_TELEGRAM_UPLOAD_BYTES:
            public_url = await run_blocking(upload_file_to_r2, str(file_path))
            results.append(
                {
                    "kind": "r2",
                    "url": public_url,
                    "name": file_path.name,
                    "size": file_size,
                }
            )
        else:
            results.append(
                {
                    "kind": "local",
                    "path": str(file_path.resolve()),
                    "name": file_path.name,
                    "size": file_size,
                }
            )

    return results


async def progress_notifier(application: Application, task: DownloadTask, stop_event: asyncio.Event) -> None:
    last_text = ""
    while not stop_event.is_set():
        snapshot = get_progress_snapshot(task.url_key)
        if snapshot:
            text = format_progress_text(snapshot)
            if text != last_text:
                await asyncio.gather(
                    *[
                        edit_message_if_possible(application, requester.chat_id, requester.status_message_id, text)
                        for requester in task.requesters
                        if requester.status_message_id
                    ],
                    return_exceptions=True,
                )
                last_text = text
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=PROGRESS_UPDATE_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            continue


async def is_admin_user(user_id: int) -> bool:
    return await run_blocking(storage.is_admin, user_id)


async def require_admin(update: Update) -> bool:
    user = update.effective_user
    if not user:
        return False
    if await is_admin_user(user.id):
        return True
    if update.message:
        await update.message.reply_text("This command is only for admins. Use /claimadmin first on a new bot.")
    return False


# =========================
# 7) COMMANDS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Send me a link from YouTube, X, Instagram, or TikTok.\n"
        "I will place it in the queue and download it.\n\n"
        "Small files are sent in Telegram. Large files are uploaded to Cloudflare R2 and returned as links.\n\n"
        "Useful commands:\n"
        "/help\n"
        "/whoami\n"
        "/claimadmin"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Usage:\n"
        "- Send a single video link\n"
        "- Send a playlist link\n\n"
        "Bot behavior:\n"
        "- Requests go into a global queue\n"
        f"- Up to {MAX_CONCURRENT_DOWNLOADS} downloads run in parallel\n"
        "- Repeated links use cache when possible\n"
        "- Large files are uploaded to Cloudflare R2\n"
        f"- Old local files are cleaned up after {CLEANUP_AFTER_HOURS} hours\n"
        "- Status messages show a live progress bar\n\n"
        "Admin commands:\n"
        "/claimadmin - first admin claims this bot\n"
        "/admin - admin menu\n"
        "/stats - runtime stats\n"
        "/queue - queue preview\n"
        "/admins - list admins\n"
        "/users - top users\n"
        "/recent - recent events\n"
        "/cache - cache info\n"
        "/clearcache - clear cache records\n"
        "/addadmin <user_id> - add admin\n"
        "/deladmin <user_id> - remove admin\n"
        "/whoami - show your Telegram user ID"
    )


async def whoami_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    if not user or not update.message or not chat:
        return
    await update.message.reply_text(
        f"Your Telegram user ID: {user.id}\nChat ID: {chat.id}"
    )


async def claimadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not update.message:
        return

    admin_count = await run_blocking(storage.admin_count)
    if admin_count == 0:
        await run_blocking(storage.add_admin, user.id)
        await update.message.reply_text(
            f"Done. You are now the first admin for this bot.\nYour admin user ID is {user.id}."
        )
        return

    if await is_admin_user(user.id):
        await update.message.reply_text("You are already an admin.")
        return

    await update.message.reply_text(
        "An admin already exists for this bot. Ask the current admin to add you manually in the database if needed."
    )


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return

    uptime = human_seconds(time.time() - runtime_stats.started_at)
    cache_entries = await run_blocking(storage.count_cache_entries)
    await update.message.reply_text(
        "Admin panel\n\n"
        f"Uptime: {uptime}\n"
        f"Queue size: {download_queue.qsize()}\n"
        f"Pending URLs: {len(pending_tasks)}\n"
        f"Cache entries: {cache_entries}\n"
        f"Cache hits: {runtime_stats.cache_hits}\n"
        f"Completed downloads: {runtime_stats.completed_downloads}\n"
        f"Failed downloads: {runtime_stats.failed_downloads}\n"
        f"Telegram sends: {runtime_stats.upload_to_telegram}\n"
        f"R2 link sends: {runtime_stats.upload_to_r2}\n"
        f"Unique users: {len(runtime_stats.unique_users)}\n\n"
        "Admin commands:\n"
        "/stats\n/queue\n/admins\n/users\n/recent\n/cache\n/clearcache\n/addadmin <user_id>\n/deladmin <user_id>"
    )


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return

    uptime = human_seconds(time.time() - runtime_stats.started_at)
    admins = await run_blocking(storage.list_admins)
    await update.message.reply_text(
        "Runtime stats\n\n"
        f"Uptime: {uptime}\n"
        f"Total requests: {runtime_stats.total_requests}\n"
        f"Cache hits: {runtime_stats.cache_hits}\n"
        f"Completed downloads: {runtime_stats.completed_downloads}\n"
        f"Failed downloads: {runtime_stats.failed_downloads}\n"
        f"Queue size: {download_queue.qsize()}\n"
        f"Pending URLs: {len(pending_tasks)}\n"
        f"Admin IDs: {', '.join(str(item) for item in admins) if admins else 'none'}"
    )


async def queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return

    queue_items = list(pending_tasks.values())[:QUEUE_PREVIEW_LIMIT]
    if not queue_items:
        await update.message.reply_text("Queue is empty.")
        return

    lines = [f"Queue preview (showing up to {QUEUE_PREVIEW_LIMIT})"]
    for index, item in enumerate(queue_items, start=1):
        parsed = urlparse(item.url)
        lines.append(f"{index}. {parsed.netloc} | requesters={len(item.requesters)}")
    await update.message.reply_text("\n".join(lines))


async def admins_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return

    admins = await run_blocking(storage.list_admins)
    if not admins:
        await update.message.reply_text("No admins found.")
        return
    await update.message.reply_text("Admin IDs:\n" + "\n".join(str(item) for item in admins))


async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    if not runtime_stats.user_requests:
        await update.message.reply_text("No user activity yet.")
        return
    items = sorted(runtime_stats.user_requests.items(), key=lambda x: x[1], reverse=True)[:TOP_USERS_PREVIEW_LIMIT]
    lines = ["Top users by requests"]
    for idx, (chat_id, count) in enumerate(items, start=1):
        lines.append(f"{idx}. {chat_id} | {count} request(s)")
    await update.message.reply_text("\n".join(lines))


async def recent_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    if not runtime_stats.recent_events:
        await update.message.reply_text("No recent events yet.")
        return
    await update.message.reply_text("Recent events\n\n" + "\n".join(runtime_stats.recent_events[-10:]))


async def cache_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    cache_entries = await run_blocking(storage.count_cache_entries)
    await update.message.reply_text(
        "Cache info\n\n"
        f"Cache entries: {cache_entries}\n"
        f"Cache hits: {runtime_stats.cache_hits}\n"
        f"Download dir: {DOWNLOAD_DIR}"
    )


async def clearcache_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    deleted = await run_blocking(storage.clear_cache)
    await run_blocking(cleanup_local_files_and_cache)
    runtime_stats.add_recent(f"admin cleared cache | deleted_entries={deleted}")
    noun = "entry" if deleted == 1 else "entries"
    await update.message.reply_text(f"Cache cleared. Deleted {deleted} database {noun}.")


async def addadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /addadmin <user_id>")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("User ID must be a number.")
        return
    await run_blocking(storage.add_admin, user_id)
    runtime_stats.add_recent(f"admin added | user_id={user_id}")
    await update.message.reply_text(f"Admin added: {user_id}")


async def deladmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /deladmin <user_id>")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("User ID must be a number.")
        return
    current_user = update.effective_user.id if update.effective_user else None
    if current_user == user_id:
        admins = await run_blocking(storage.list_admins)
        if len(admins) <= 1:
            await update.message.reply_text("You cannot remove the last admin.")
            return
    await run_blocking(storage.remove_admin, user_id)
    runtime_stats.add_recent(f"admin removed | user_id={user_id}")
    await update.message.reply_text(f"Admin removed: {user_id}")


# =========================
# 8) MESSAGE HANDLER
# =========================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    if not looks_like_url(text):
        return

    runtime_stats.total_requests += 1
    runtime_stats.add_user_request(update.effective_chat.id)

    normalized_url = normalize_url(text)
    url_key = normalized_url

    status_message = await update.message.reply_text("Checking cache and queue...")
    requester = Requester(
        chat_id=update.effective_chat.id,
        status_message_id=status_message.message_id,
    )

    cached_results = await run_blocking(storage.get_cache, url_key)
    if cached_results:
        valid_results = validate_cached_results(cached_results)
        if valid_results:
            runtime_stats.cache_hits += 1
            logger.info("cache_hit | url=%s | chat_id=%s", normalized_url, requester.chat_id)
            runtime_stats.add_recent(f"cache hit | chat_id={requester.chat_id} | host={urlparse(normalized_url).netloc}")
            await status_message.edit_text("Cache hit. Sending saved result...")
            await send_results(context.application, requester, valid_results, source="cache")
            return
        await run_blocking(storage.delete_cache, url_key)

    async with pending_lock:
        existing_task = pending_tasks.get(url_key)
        if existing_task:
            existing_task.requesters.append(requester)
            logger.info("joined_existing_queue_item | url=%s | chat_id=%s", normalized_url, requester.chat_id)
            await status_message.edit_text(
                "This link is already in the queue. I will send the result when it is ready."
            )
            return

        new_task = DownloadTask(url=normalized_url, url_key=url_key, requesters=[requester])
        pending_tasks[url_key] = new_task
        await download_queue.put(new_task)
        queue_size = download_queue.qsize()

    logger.info("queued | url=%s | chat_id=%s | queue_size=%s", normalized_url, requester.chat_id, queue_size)
    runtime_stats.add_recent(f"queued | chat_id={requester.chat_id} | host={urlparse(normalized_url).netloc} | queue={queue_size}")
    await status_message.edit_text(
        f"Added to queue. Waiting position: about {queue_size}."
    )


# =========================
# 9) WORKERS
# =========================
async def queue_worker(application: Application, worker_id: int):
    logger.info("worker_started | worker_id=%s", worker_id)

    while True:
        task = await download_queue.get()
        try:
            await process_download_task(application, task, worker_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("worker_unhandled_error | worker_id=%s | url=%s", worker_id, task.url)
        finally:
            download_queue.task_done()


async def process_download_task(application: Application, task: DownloadTask, worker_id: int):
    logger.info(
        "download_start | worker_id=%s | url=%s | requesters=%s",
        worker_id,
        task.url,
        len(task.requesters),
    )

    for requester in task.requesters:
        if requester.status_message_id:
            await edit_message_if_possible(
                application,
                requester.chat_id,
                requester.status_message_id,
                "Downloading now...",
            )

    task_dir = make_safe_task_dir(task.url_key)
    stop_event = asyncio.Event()
    notifier_task = asyncio.create_task(progress_notifier(application, task, stop_event))

    try:
        media_files = await download_with_retry(task.url, task_dir, task.url_key)
        stop_event.set()
        await asyncio.gather(notifier_task, return_exceptions=True)

        for requester in task.requesters:
            if requester.status_message_id:
                await edit_message_if_possible(
                    application,
                    requester.chat_id,
                    requester.status_message_id,
                    "Uploading to Telegram or Cloudflare R2...",
                )

        results = await prepare_results(media_files)
        await run_blocking(storage.set_cache, task.url_key, task.url, results)
        runtime_stats.completed_downloads += 1

        logger.info(
            "download_success | worker_id=%s | url=%s | files=%s",
            worker_id,
            task.url,
            len(results),
        )

        for requester in task.requesters:
            await send_results(application, requester, results, source="download")

    except Exception as exc:
        stop_event.set()
        await asyncio.gather(notifier_task, return_exceptions=True)
        runtime_stats.failed_downloads += 1
        logger.exception("download_failure | worker_id=%s | url=%s", worker_id, task.url)
        for requester in task.requesters:
            await notify_failure(application, requester, str(exc))

    finally:
        clear_progress_snapshot(task.url_key)
        async with pending_lock:
            pending_tasks.pop(task.url_key, None)


# =========================
# 10) CLEANUP
# =========================
def cleanup_local_files_and_cache() -> None:
    cutoff = time.time() - (CLEANUP_AFTER_HOURS * 3600)

    for path in DOWNLOAD_DIR.rglob("*"):
        if not path.is_file():
            continue
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
        except Exception:
            logger.exception("cleanup_file_error | path=%s", path)

    for row in storage.all_cache_entries():
        url_key = row["url_key"]
        results = json.loads(row["results_json"])
        valid_results = validate_cached_results(results)
        if not valid_results:
            storage.delete_cache(url_key)
        elif valid_results != results:
            storage.set_cache(url_key, row["original_url"], valid_results)


async def cleanup_loop():
    logger.info("cleanup_task_started")
    while True:
        try:
            await run_blocking(cleanup_local_files_and_cache)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("cleanup_task_error")
        await asyncio.sleep(CLEANUP_SCAN_INTERVAL_SECONDS)


# =========================
# 11) LIFECYCLE
# =========================
async def on_startup(application: Application):
    global worker_tasks, cleanup_task

    worker_tasks = [
        asyncio.create_task(queue_worker(application, worker_id=i + 1))
        for i in range(MAX_CONCURRENT_DOWNLOADS)
    ]
    cleanup_task = asyncio.create_task(cleanup_loop())
    logger.info("startup_complete | workers=%s", MAX_CONCURRENT_DOWNLOADS)
    runtime_stats.add_recent(f"startup complete | workers={MAX_CONCURRENT_DOWNLOADS}")


async def on_shutdown(application: Application):
    del application
    global cleanup_task

    for task in worker_tasks:
        task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)

    if cleanup_task:
        cleanup_task.cancel()
        await asyncio.gather(cleanup_task, return_exceptions=True)

    logger.info("shutdown_complete")


# =========================
# 12) GLOBAL ERROR HANDLER
# =========================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("unhandled_exception", exc_info=context.error)


# =========================
# 13) RUN APP
# =========================
def main():
    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("whoami", whoami_command))
    app.add_handler(CommandHandler("claimadmin", claimadmin_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("queue", queue_command))
    app.add_handler(CommandHandler("admins", admins_command))
    app.add_handler(CommandHandler("users", users_command))
    app.add_handler(CommandHandler("recent", recent_command))
    app.add_handler(CommandHandler("cache", cache_command))
    app.add_handler(CommandHandler("clearcache", clearcache_command))
    app.add_handler(CommandHandler("addadmin", addadmin_command))
    app.add_handler(CommandHandler("deladmin", deladmin_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    print("Bot started...")
    app.run_polling()


if __name__ == "__main__":
    main()
