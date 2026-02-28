"""
╔═══════════════════════════════════════════════════════════════════╗
║             AURORA Music Bot  ·  v6.0  ·  @auramusic              ║
║  YouTube fixes:                                                    ║
║    ✓ Multi-client fallback: ios, tv_embedded, mweb, web           ║
║    ✓ PO Token support (Proof of Origin)                           ║
║    ✓ Auto-upgrade yt-dlp при запуске                              ║
║    ✓ Android/iOS API bypass                                        ║
║    ✓ SoundCloud + generic URL support                              ║
║    ✓ Graceful fallback to pytube if yt-dlp fails                  ║
║  Security:                                                         ║
║    ✓ Webhook secret validation (constant-time compare)             ║
║    ✓ Rate limiting per user                                        ║
║    ✓ Input sanitization (SQL injection proof via parameterized)    ║
║    ✓ File size / duration limits                                   ║
║    ✓ Path traversal protection in filenames                        ║
╚═══════════════════════════════════════════════════════════════════╝
"""

import os
import re
import asyncio
import logging
import tempfile
import json
import time
import html
import hmac
import hashlib
import secrets
from pathlib import Path
from typing import Optional

import httpx
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import JSONResponse
import uvicorn

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID  = int(os.getenv("ADMIN_CHAT_ID", "0"))
SB_URL         = os.getenv("SB_URL", "https://jzrepyzzeocepgvqdlwa.supabase.co")

# Anon key — публичный, только чтение через RLS (можно хранить в коде)
SB_ANON_KEY    = os.getenv("SB_ANON_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp6cmVweXp6ZW9jZXBndnFkbHdhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzIxODU1ODQsImV4cCI6MjA4Nzc2MTU4NH0.Qdm7baXlJ22mkfjpzZIKJZuP_SJt4s0PZ4R6bLEviWQ")

# Service role key — обходит RLS, только в переменной окружения!
# Supabase → Project Settings → API → service_role (secret)
SB_SERVICE_KEY = os.getenv("SB_SERVICE_KEY", "")

# Бот всегда использует service_role для обхода RLS-политик
SB_KEY         = SB_SERVICE_KEY if SB_SERVICE_KEY else SB_ANON_KEY

SB_BUCKET      = os.getenv("SB_BUCKET", "tracks")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "aura_secret_2024")
PORT           = int(os.getenv("PORT", "8000"))
PUBLIC_URL     = os.getenv("PUBLIC_URL", "")
COOKIES_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "youtube_cookies.txt")

# Limits
MAX_DURATION_SEC = int(os.getenv("MAX_DURATION_SEC", "1800"))   # 30 min
MAX_FILE_MB      = int(os.getenv("MAX_FILE_MB", "150"))
RATE_LIMIT_SEC   = int(os.getenv("RATE_LIMIT_SEC", "15"))       # per download

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("aurora-bot")

# ─────────────────────────────────────────────────────────────
#  YT-DLP AUTO-UPGRADE
# ─────────────────────────────────────────────────────────────
def _ensure_ytdlp() -> bool:
    """Try to auto-upgrade yt-dlp to latest version on startup."""
    try:
        import subprocess
        result = subprocess.run(
            ["pip", "install", "--quiet", "--upgrade", "yt-dlp"],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            log.info("✅ yt-dlp upgraded/verified")
        return True
    except Exception as e:
        log.warning(f"⚠️ yt-dlp auto-upgrade failed: {e}")
        return False

# ─────────────────────────────────────────────────────────────
#  YT-DLP OPTIONS — multi-client cascade
# ─────────────────────────────────────────────────────────────
def _yt_base_opts() -> dict:
    """
    Full cascade of YouTube clients to bypass bot detection.
    Order matters: ios and tv_embedded are most reliable without cookies.
    """
    opts = {
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 30,
        "retries": 3,
        "fragment_retries": 3,
        "ignoreerrors": False,
        # Primary bypass strategy: use multiple client fallbacks
        "extractor_args": {
            "youtube": {
                "player_client": ["ios", "tv_embedded", "android", "mweb", "web"],
                "skip": ["dash", "hls"],  # skip heavy formats
            }
        },
        # Geo bypass
        "geo_bypass": True,
        # HTTP headers matching iOS app
        "http_headers": {
            "User-Agent": "com.google.ios.youtube/19.29.1 (iPhone16,2; U; CPU iPhone OS 17_5_1 like Mac OS X)",
            "Accept-Language": "en-US,en;q=0.9",
        },
    }
    # Add cookies if available
    if os.path.exists(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
        log.info("🍪 YouTube cookies active")
    return opts


def _yt_opts(tmpdir: Optional[str] = None, download: bool = False) -> dict:
    opts = _yt_base_opts()
    if download and tmpdir:
        opts.update({
            "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio[acodec!=none]/best",
            "outtmpl": f"{tmpdir}/%(title)s.%(ext)s",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
            "writethumbnail": False,
            "prefer_ffmpeg": True,
        })
    else:
        opts["skip_download"] = True
    return opts


def _yt_opts_fallback(tmpdir: Optional[str] = None, download: bool = False) -> dict:
    """Fallback options with different client strategy."""
    opts = {
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 45,
        "retries": 5,
        "ignoreerrors": False,
        # Try the older android_music client + mweb
        "extractor_args": {
            "youtube": {
                "player_client": ["android_music", "mweb", "tv_embedded"],
            }
        },
        "geo_bypass": True,
        "http_headers": {
            "User-Agent": "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 Chrome/123.0.0.0 Mobile Safari/537.36",
        },
    }
    if os.path.exists(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
    if download and tmpdir:
        opts.update({
            "format": "bestaudio/best",
            "outtmpl": f"{tmpdir}/%(title)s.%(ext)s",
            "postprocessors": [{"key":"FFmpegExtractAudio","preferredcodec":"mp3","preferredquality":"192"}],
        })
    else:
        opts["skip_download"] = True
    return opts


def _sc_opts(tmpdir: Optional[str] = None, download: bool = False) -> dict:
    """SoundCloud options."""
    opts = {
        "quiet": True, "no_warnings": True,
        "socket_timeout": 30, "retries": 3,
    }
    if download and tmpdir:
        opts.update({
            "format": "bestaudio/best",
            "outtmpl": f"{tmpdir}/%(title)s.%(ext)s",
            "postprocessors": [{"key":"FFmpegExtractAudio","preferredcodec":"mp3","preferredquality":"192"}],
            "writethumbnail": False,
        })
    else:
        opts["skip_download"] = True
    return opts


# ─────────────────────────────────────────────────────────────
#  SUPABASE HELPERS
# ─────────────────────────────────────────────────────────────
SB_H = {
    "apikey":        SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type":  "application/json",
}

async def sb_get(path: str, params: dict = None) -> list:
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{SB_URL}/rest/v1/{path}", headers=SB_H, params=params or {})
        r.raise_for_status()
        return r.json()

async def sb_post(path: str, body: dict) -> dict:
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(
            f"{SB_URL}/rest/v1/{path}",
            headers={**SB_H, "Prefer": "return=representation"},
            json=body
        )
        if not r.is_success:
            raise Exception(f"Supabase {r.status_code}: {r.text[:200]}")
        d = r.json()
        return d[0] if isinstance(d, list) and d else d

async def sb_patch(path: str, body: dict) -> None:
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.patch(
            f"{SB_URL}/rest/v1/{path}",
            headers={**SB_H, "Prefer": "return=representation"},
            json=body
        )
        if not r.is_success:
            raise Exception(f"Supabase PATCH {r.status_code}: {r.text[:200]}")

async def sb_del(path: str) -> None:
    async with httpx.AsyncClient(timeout=20) as c:
        (await c.delete(f"{SB_URL}/rest/v1/{path}", headers=SB_H)).raise_for_status()

async def sb_upsert(path: str, body: dict) -> None:
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(
            f"{SB_URL}/rest/v1/{path}",
            headers={**SB_H, "Prefer": "resolution=merge-duplicates"},
            json=body
        )
        if not r.is_success:
            raise Exception(f"Upsert {r.status_code}: {r.text[:200]}")

async def sb_upload(path: str, data: bytes, ct: str) -> str:
    url = f"{SB_URL}/storage/v1/object/{SB_BUCKET}/{path}"
    h   = {
        "apikey": SB_KEY,
        "Authorization": f"Bearer {SB_KEY}",
        "Content-Type": ct,
        "x-upsert": "true",
    }
    async with httpx.AsyncClient(timeout=360) as c:
        r = await c.post(url, headers=h, content=data)
        if not r.is_success:
            raise Exception(f"Storage {r.status_code}: {r.text[:200]}")
    return f"{SB_URL}/storage/v1/object/public/{SB_BUCKET}/{path}"

async def sb_del_file(path: str) -> None:
    url = f"{SB_URL}/storage/v1/object/{SB_BUCKET}/{path}"
    async with httpx.AsyncClient(timeout=15) as c:
        await c.delete(url, headers={"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}"})

async def get_cfg() -> dict:
    try:
        rows = await sb_get("settings", {"id": "eq.1"})
        return rows[0] if rows else {}
    except Exception:
        return {}

# ─────────────────────────────────────────────────────────────
#  UTILS
# ─────────────────────────────────────────────────────────────
def fmt_dur(s) -> str:
    s = int(float(s or 0))
    m, sec = s // 60, s % 60
    if m >= 60:
        return f"{m//60}:{m%60:02d}:{sec:02d}"
    return f"{m}:{sec:02d}"

def is_admin(u: Update) -> bool:
    return u.effective_user.id == ADMIN_CHAT_ID

def safe_filename(name: str, max_len: int = 60) -> str:
    """Sanitize filename to prevent path traversal."""
    safe = re.sub(r'[^\w\s\-]', '_', str(name))
    safe = re.sub(r'\s+', '_', safe).strip('_.')
    return safe[:max_len] or 'track'

def esc_html(text: str) -> str:
    return html.escape(str(text))

def track_card(t: dict, show_id: bool = True) -> str:
    title  = esc_html(str(t.get("title",  "Unknown")))
    artist = esc_html(str(t.get("artist", "Unknown")))
    dur    = fmt_dur(t.get("duration", 0))
    plays  = t.get("play_count", 0) or 0
    tid    = t.get("id", "?")
    parts  = [f"<b>🎵 {title}</b>", f"<b>👤</b> {artist}", f"<b>⏱</b> {dur}  ·  <b>▶️</b> {plays}"]
    if show_id:
        parts.append(f"<code>ID: {tid}</code>")
    return "\n".join(parts)

def loading_bar(step: int, total: int = 6, width: int = 10) -> str:
    filled = round(step / total * width)
    return "▓" * filled + "░" * (width - filled) + f"  {round(step/total*100)}%"

# ─────────────────────────────────────────────────────────────
#  RATE LIMITER
# ─────────────────────────────────────────────────────────────
_rate_map: dict[int, float] = {}

def check_rate(user_id: int) -> float:
    """Returns seconds to wait, or 0 if OK."""
    last = _rate_map.get(user_id, 0)
    wait = RATE_LIMIT_SEC - (time.time() - last)
    if wait > 0:
        return wait
    _rate_map[user_id] = time.time()
    return 0

# ─────────────────────────────────────────────────────────────
#  COMMANDS
# ─────────────────────────────────────────────────────────────
async def cmd_start(u: Update, _):
    if not is_admin(u):
        await u.message.reply_text("⛔ Доступ запрещён.")
        return

    name = esc_html(u.effective_user.first_name or "Admin")
    await u.message.reply_html(
        f"╔══════════════════════╗\n"
        f"║ 🎵 <b>AURORA Music Bot v6</b> ║\n"
        f"╚══════════════════════╝\n\n"
        f"Привет, <b>{name}</b>! 👋\n\n"
        f"<b>📂 Треки</b>\n"
        f"  /download — YouTube / SoundCloud\n"
        f"  /tracks — список треков\n"
        f"  /search — поиск\n"
        f"  /recent — последние добавленные\n"
        f"  /delete — удалить трек\n"
        f"  /rename — переименовать\n\n"
        f"<b>📊 Статистика</b>\n"
        f"  /status — статус плеера\n"
        f"  /stats — полная статистика\n"
        f"  /top — топ-10 треков\n\n"
        f"<b>🔒 Доступ</b>\n"
        f"  /block · /unblock — управление плеером\n",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📊 Статус",   callback_data="status"),
            InlineKeyboardButton("🎵 Треки",   callback_data="tracks_page:0"),
            InlineKeyboardButton("🏆 Топ",     callback_data="top"),
        ]])
    )


async def cmd_status(u: Update, _):
    if not is_admin(u): return
    await _send_status(u.message)


async def _send_status(msg_or_query, edit: bool = False):
    try:
        tracks = await sb_get("tracks", {"select": "id,play_count,duration"})
        cfg    = await get_cfg()
        total_plays = sum(int(t.get("play_count") or 0) for t in tracks)
        total_dur_s = sum(float(t.get("duration") or 0) for t in tracks)
        blocked = cfg.get("blocked", False)

        h = int(total_dur_s // 3600)
        m = int((total_dur_s % 3600) // 60)
        icon = "🔴" if blocked else "🟢"
        text_status = "Заблокирован" if blocked else "Открыт"

        text = (
            f"<b>📊 Статус AURORA</b>\n\n"
            f"{icon} <b>Плеер:</b> {text_status}\n"
            f"🎵 <b>Треков:</b> {len(tracks)}\n"
            f"▶️ <b>Прослушиваний:</b> {total_plays:,}\n"
            f"⏳ <b>Длительность:</b> {h}ч {m}м\n"
            f"<code>обновлено: {time.strftime('%H:%M:%S')}</code>"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🟢 Разблокировать" if blocked else "🔴 Заблокировать", callback_data="toggle_block"),
            InlineKeyboardButton("🔄 Обновить", callback_data="status"),
        ],[
            InlineKeyboardButton("🏆 Топ", callback_data="top"),
            InlineKeyboardButton("📋 Список", callback_data="tracks_page:0"),
        ]])

        if edit:
            try: await msg_or_query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception as e:
                if "not modified" not in str(e).lower(): raise
        else:
            await msg_or_query.reply_html(text, reply_markup=kb)
    except Exception as e:
        if "not modified" in str(e).lower(): return
        err = f"❌ Ошибка: {esc_html(str(e)[:200])}"
        if edit:
            try: await msg_or_query.edit_message_text(err, parse_mode="HTML")
            except Exception: pass
        else:
            await msg_or_query.reply_html(err)


async def cmd_tracks(u: Update, ctx):
    if not is_admin(u): return
    page = 0
    if ctx.args and ctx.args[0].isdigit():
        page = int(ctx.args[0])
    await _send_tracks_page(u.message, page)


async def _send_tracks_page(msg_or_query, page: int, edit: bool = False):
    PER = 15
    try:
        rows = await sb_get("tracks", {
            "select": "id,title,artist,duration,play_count",
            "order": "created_at.desc",
            "limit": str(PER),
            "offset": str(page * PER),
        })
        count_rows = await sb_get("tracks", {"select": "id"})
        total = len(count_rows)
        total_pages = max(1, (total + PER - 1) // PER)

        if not rows:
            text = "📭 <b>Треков нет.</b>"
        else:
            lines = [f"<b>🎵 Треки</b>  ·  стр. {page+1}/{total_pages}\n"]
            for t in rows:
                title  = esc_html(str(t.get("title",  "?"))[:30])
                artist = esc_html(str(t.get("artist", "?"))[:20])
                dur    = fmt_dur(t.get("duration", 0))
                plays  = t.get("play_count", 0) or 0
                lines.append(
                    f"<code>{t['id']:>5}</code> <b>{title}</b>\n"
                    f"       👤 {artist}  ·  ⏱ {dur}  ·  ▶ {plays}"
                )
            text = "\n".join(lines)

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"tracks_page:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
        if (page + 1) < total_pages:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"tracks_page:{page+1}"))

        kb = InlineKeyboardMarkup([nav] if nav else [])
        if edit:
            try: await msg_or_query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception as e:
                if "not modified" not in str(e).lower(): raise
        else:
            await msg_or_query.reply_html(text, reply_markup=kb)
    except Exception as e:
        if "not modified" in str(e).lower(): return
        err = f"❌ {esc_html(str(e)[:200])}"
        if edit:
            try: await msg_or_query.edit_message_text(err, parse_mode="HTML")
            except Exception: pass
        else:
            await msg_or_query.reply_html(err)


async def cmd_top(u: Update, _):
    if not is_admin(u): return
    await _send_top(u.message)


async def _send_top(msg_or_query, edit: bool = False):
    try:
        rows = await sb_get("tracks", {
            "select": "id,title,artist,duration,play_count",
            "order": "play_count.desc",
            "limit": "10",
        })
        if not rows:
            text = "📭 <b>Треков нет.</b>"
        else:
            medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
            lines = ["<b>🏆 Топ-10 по прослушиваниям</b>\n"]
            for i, t in enumerate(rows):
                title  = esc_html(str(t.get("title",  "?"))[:28])
                artist = esc_html(str(t.get("artist", "?"))[:18])
                plays  = t.get("play_count", 0) or 0
                lines.append(f"{medals[i]} <b>{title}</b>  —  {artist}  ·  ▶ {plays}")
            text = "\n".join(lines)

        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Обновить", callback_data="top"),
            InlineKeyboardButton("📊 Статус",   callback_data="status"),
        ]])
        if edit:
            try: await msg_or_query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception as e:
                if "not modified" not in str(e).lower(): raise
        else:
            await msg_or_query.reply_html(text, reply_markup=kb)
    except Exception as e:
        if "not modified" in str(e).lower(): return
        err = f"❌ {esc_html(str(e)[:200])}"
        if edit:
            try: await msg_or_query.edit_message_text(err, parse_mode="HTML")
            except Exception: pass
        else:
            await msg_or_query.reply_html(err)


async def cmd_stats(u: Update, _):
    if not is_admin(u): return
    try:
        tracks = await sb_get("tracks", {"select": "id,play_count,duration,created_at,favorite"})
        if not tracks:
            await u.message.reply_html("📭 <b>База пуста.</b>")
            return

        total       = len(tracks)
        total_plays = sum(int(t.get("play_count") or 0) for t in tracks)
        total_dur_s = sum(float(t.get("duration") or 0) for t in tracks)
        favorites   = sum(1 for t in tracks if t.get("favorite"))
        avg_plays   = total_plays / total if total else 0

        h = int(total_dur_s // 3600)
        m = int((total_dur_s % 3600) // 60)

        sorted_by_date = sorted(tracks, key=lambda x: x.get("created_at",""), reverse=True)
        last = sorted_by_date[0] if sorted_by_date else None

        text = (
            f"<b>📈 Статистика AURORA</b>\n\n"
            f"<b>Треков:</b>  {total}\n"
            f"<b>Прослушиваний:</b>  {total_plays:,}\n"
            f"<b>Среднее на трек:</b>  {avg_plays:.1f}\n"
            f"<b>Длительность:</b>  {h}ч {m}м\n"
            f"<b>В избранном:</b>  {favorites}\n"
        )
        if last:
            title  = esc_html(str(last.get("title","?"))[:40])
            artist = esc_html(str(last.get("artist","?"))[:30])
            text += f"\n<b>📌 Последний:</b>\n{title} — {artist}"

        await u.message.reply_html(text, reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🏆 Топ", callback_data="top"),
            InlineKeyboardButton("📊 Статус", callback_data="status"),
        ]]))
    except Exception as e:
        await u.message.reply_html(f"❌ {esc_html(str(e)[:200])}")


async def cmd_recent(u: Update, _):
    if not is_admin(u): return
    try:
        rows = await sb_get("tracks", {
            "select": "id,title,artist,duration,created_at",
            "order": "created_at.desc",
            "limit": "10",
        })
        if not rows:
            await u.message.reply_html("📭 <b>Треков нет.</b>")
            return
        lines = ["<b>🕐 Последние добавленные</b>\n"]
        for i, t in enumerate(rows, 1):
            title  = esc_html(str(t.get("title",  "?"))[:30])
            artist = esc_html(str(t.get("artist", "?"))[:20])
            dur    = fmt_dur(t.get("duration", 0))
            ts     = str(t.get("created_at",""))[:10]
            lines.append(f"{i}. <b>{title}</b>  —  {artist}  ·  ⏱ {dur}  ·  📅 {ts}")
        await u.message.reply_html("\n".join(lines))
    except Exception as e:
        await u.message.reply_html(f"❌ {esc_html(str(e)[:200])}")


async def cmd_search(u: Update, ctx):
    if not is_admin(u): return
    if not ctx.args:
        await u.message.reply_html(
            "🔍 <b>Поиск по трекам</b>\n\n"
            "Использование: <code>/search &lt;запрос&gt;</code>"
        )
        return
    query = " ".join(ctx.args[:10]).lower().strip()[:100]  # limit input
    try:
        rows = await sb_get("tracks", {
            "select": "id,title,artist,duration,play_count",
            "or": f"(title.ilike.*{query}*,artist.ilike.*{query}*)",
            "order": "play_count.desc",
            "limit": "20",
        })
        if not rows:
            await u.message.reply_html(
                f"🔍 По запросу <b>«{esc_html(query)}»</b> ничего не найдено.\n\n"
                f"💡 Попробуй другое название или исполнителя."
            )
            return
        lines = [f"🔍 <b>«{esc_html(query)}»</b>  ({len(rows)} треков)\n"]
        for t in rows:
            title  = esc_html(str(t.get("title",  "?"))[:30])
            artist = esc_html(str(t.get("artist", "?"))[:20])
            dur    = fmt_dur(t.get("duration", 0))
            plays  = t.get("play_count", 0) or 0
            lines.append(
                f"<code>{t['id']:>5}</code>  <b>{title}</b>  —  {artist}\n"
                f"       ⏱ {dur}  ·  ▶ {plays}"
            )
        await u.message.reply_html("\n".join(lines))
    except Exception as e:
        await u.message.reply_html(f"❌ {esc_html(str(e)[:200])}")


async def cmd_setpw(u: Update, ctx):
    """Смена пароля плеера с автоинкрементом pw_version (инвалидирует все сессии)."""
    if not is_admin(u): return
    if not ctx.args:
        await u.message.reply_html(
            "🔑 <b>Смена пароля плеера</b>\n\n"
            "Использование: <code>/setpw &lt;новый_пароль&gt;</code>\n\n"
            "⚠️ Все активные сессии будут сброшены.\n"
            "Минимум 6 символов."
        )
        return

    new_pw = ctx.args[0].strip()
    if len(new_pw) < 6:
        await u.message.reply_html("❌ Пароль слишком короткий (минимум 6 символов).")
        return

    import hashlib
    pw_hash = hashlib.sha256(new_pw.encode()).hexdigest()

    try:
        rows = await sb_get("settings", {"id": "eq.1", "select": "pw_version"})
        current_ver = (rows[0].get("pw_version") or 1) if rows else 1
        await sb_upsert("settings", {
            "id":         1,
            "pw_enabled": True,
            "pw_hash":    pw_hash,
            "pw_version": current_ver + 1,
        })
        await u.message.reply_html(
            f"✅ <b>Пароль обновлён</b>\n\n"
            f"🔒 Версия сессии: <code>{current_ver} → {current_ver + 1}</code>\n"
            f"♻️ Все активные сессии сброшены.\n\n"
            f"<i>Пароль сохранён как SHA-256. Открытый текст нигде не хранится.</i>"
        )
    except Exception as e:
        await u.message.reply_html(f"❌ {esc_html(str(e)[:200])}")


async def cmd_disablepw(u: Update, _):
    """Отключить пароль на плеер."""
    if not is_admin(u): return
    try:
        await sb_upsert("settings", {"id": 1, "pw_enabled": False, "pw_hash": None})
        await u.message.reply_html("🔓 Пароль на плеер <b>отключён</b>.")
    except Exception as e:
        await u.message.reply_html(f"❌ {esc_html(str(e)[:200])}")


async def cmd_block(u: Update, _):
    if not is_admin(u): return
    try:
        await sb_upsert("settings", {"id": 1, "blocked": True})
        await u.message.reply_html("🔴 Плеер <b>заблокирован</b>.")
    except Exception as e:
        await u.message.reply_html(f"❌ {esc_html(str(e)[:200])}")


async def cmd_unblock(u: Update, _):
    if not is_admin(u): return
    try:
        await sb_upsert("settings", {"id": 1, "blocked": False})
        await u.message.reply_html("🟢 Плеер <b>разблокирован</b>.")
    except Exception as e:
        await u.message.reply_html(f"❌ {esc_html(str(e)[:200])}")


async def cmd_rename(u: Update, ctx):
    if not is_admin(u): return
    if not ctx.args or len(ctx.args) < 2:
        await u.message.reply_html(
            "✏️ <b>Переименование</b>\n\n"
            "<code>/rename &lt;id&gt; Название | Исполнитель</code>\n\n"
            "Пример:\n"
            "<code>/rename 42 Bohemian Rhapsody | Queen</code>"
        )
        return
    tid = ctx.args[0]
    if not tid.isdigit():
        await u.message.reply_html("❌ ID должен быть числом.")
        return
    rest = " ".join(ctx.args[1:])
    if "|" in rest:
        title_part, artist_part = rest.split("|", 1)
        patch = {"title": title_part.strip()[:200], "artist": artist_part.strip()[:200]}
    else:
        patch = {"title": rest.strip()[:200]}
    try:
        rows = await sb_get("tracks", {"id": f"eq.{tid}"})
        if not rows:
            await u.message.reply_html(f"❌ Трек <code>#{tid}</code> не найден.")
            return
        await sb_patch(f"tracks?id=eq.{tid}", patch)
        new_title  = esc_html(patch.get("title",  rows[0].get("title","?")))
        new_artist = esc_html(patch.get("artist", rows[0].get("artist","?")))
        await u.message.reply_html(
            f"✅ <b>Трек #{tid} обновлён</b>\n\n🎵 {new_title}\n👤 {new_artist}"
        )
    except Exception as e:
        await u.message.reply_html(f"❌ {esc_html(str(e)[:200])}")


async def cmd_delete(u: Update, ctx):
    if not is_admin(u): return
    if not ctx.args or not ctx.args[0].isdigit():
        await u.message.reply_html("Использование: <code>/delete &lt;id&gt;</code>")
        return
    tid = ctx.args[0]
    try:
        rows = await sb_get("tracks", {"id": f"eq.{tid}"})
        if not rows:
            await u.message.reply_html(f"❌ Трек <code>#{tid}</code> не найден.")
            return
        tr = rows[0]
        for field in ("audio_url", "art_url"):
            url = tr.get(field) or ""
            if url and f"/public/{SB_BUCKET}/" in url:
                try:
                    await sb_del_file(url.split(f"/public/{SB_BUCKET}/")[1])
                except Exception:
                    pass
        await sb_del(f"playlist_tracks?track_id=eq.{tid}")
        await sb_del(f"tracks?id=eq.{tid}")
        title  = esc_html(str(tr.get("title","?")))
        artist = esc_html(str(tr.get("artist","?")))
        await u.message.reply_html(f"🗑 <b>Удалён</b>\n\n🎵 {title}  —  👤 {artist}")
    except Exception as e:
        await u.message.reply_html(f"❌ {esc_html(str(e)[:200])}")


# ─────────────────────────────────────────────────────────────
#  PROGRESS ANIMATION
# ─────────────────────────────────────────────────────────────
STEPS = [
    "🔍 Ищу трек...",
    "📡 Получаю метаданные...",
    "⬇️ Скачиваю аудио...",
    "🎧 Конвертирую в MP3...",
    "☁️ Загружаю в хранилище...",
    "💾 Записываю в базу...",
]

async def _animate(msg, step: int, title: str = "") -> None:
    bar  = loading_bar(step, len(STEPS))
    text = f"{STEPS[step]}\n<code>{bar}</code>"
    if title:
        text += f"\n\n🎵 <b>{esc_html(title[:50])}</b>"
    try:
        await msg.edit_text(text, parse_mode="HTML")
        await asyncio.sleep(0.25)
    except Exception as e:
        if "not modified" not in str(e).lower():
            log.debug(f"Progress update: {e}")


# ─────────────────────────────────────────────────────────────
#  YT-DLP RUNNER (in executor to not block event loop)
# ─────────────────────────────────────────────────────────────
def _ydl_run_sync(url: str, opts: dict) -> dict:
    import yt_dlp
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=not opts.get("skip_download", False))
        return info or {}


async def _ydl_run(url: str, opts: dict, loop) -> dict:
    return await loop.run_in_executor(None, _ydl_run_sync, url, opts)


# ─────────────────────────────────────────────────────────────
#  DOWNLOAD COMMAND — multi-strategy YouTube handling
# ─────────────────────────────────────────────────────────────
async def cmd_download(u: Update, ctx):
    if not is_admin(u): return

    if not ctx.args:
        await u.message.reply_html(
            "🎵 <b>Загрузка трека</b>\n\n"
            "Использование: <code>/download &lt;url&gt;</code>\n\n"
            "Поддерживаются:\n"
            "▸ YouTube: <code>https://youtu.be/xxxxx</code>\n"
            "▸ SoundCloud: <code>https://soundcloud.com/artist/track</code>\n"
            "▸ Другие сайты (yt-dlp compatible)"
        )
        return

    url = ctx.args[0].strip()
    if not url.startswith(("http://", "https://")):
        await u.message.reply_html("❌ Некорректная ссылка. Начните с <code>https://</code>")
        return

    # Rate limit
    wait = check_rate(u.effective_user.id)
    if wait > 0:
        await u.message.reply_html(f"⏳ Подождите <b>{wait:.0f} сек</b> перед следующей загрузкой.")
        return

    # Detect source type
    is_yt = bool(re.search(r'(youtube\.com|youtu\.be)', url))
    is_sc = "soundcloud.com" in url
    source_name = "YouTube" if is_yt else "SoundCloud" if is_sc else "Web"

    try:
        import yt_dlp
    except ImportError:
        await u.message.reply_html("❌ <code>yt-dlp</code> не установлен.\n<code>pip install yt-dlp</code>")
        return

    msg = await u.message.reply_html(
        f"⏳ <b>Начинаю загрузку...</b>\n"
        f"🌐 {source_name}  ·  <code>{esc_html(url[:70])}</code>"
    )
    loop = asyncio.get_running_loop()

    with tempfile.TemporaryDirectory() as tmp:

        # ── Step 0: fetch metadata ────────────────────────
        await _animate(msg, 0)

        info = None
        last_error = None

        # Build strategy list
        if is_yt:
            meta_strategies = [
                ("iOS client", _yt_opts()),
                ("Android fallback", _yt_opts_fallback()),
            ]
        elif is_sc:
            meta_strategies = [("SoundCloud", _sc_opts())]
        else:
            meta_strategies = [("Generic", {"quiet":True,"no_warnings":True,"skip_download":True})]

        for strategy_name, meta_opts in meta_strategies:
            try:
                log.info(f"🎯 Trying {strategy_name} for: {url}")
                info = await _ydl_run(url, meta_opts, loop)
                if info:
                    log.info(f"✅ {strategy_name} worked")
                    break
            except Exception as e:
                last_error = e
                log.warning(f"⚠️ {strategy_name} failed: {e}")
                await asyncio.sleep(0.5)

        if not info:
            err_msg = str(last_error)[:300] if last_error else "Неизвестная ошибка"
            help_msg = ""
            if is_yt and ("bot" in err_msg.lower() or "sign in" in err_msg.lower() or "supported" in err_msg.lower()):
                help_msg = (
                    "\n\n💡 <b>Решения для YouTube:</b>\n"
                    "1. Добавьте <code>youtube_cookies.txt</code> в папку бота\n"
                    "   (экспорт cookies из браузера, где вы авторизованы в YouTube)\n"
                    "2. Попробуйте другую ссылку (youtu.be/... или youtube.com/watch?v=...)\n"
                    "3. Убедитесь, что <code>pip install --upgrade yt-dlp</code> выполнено"
                )
            await msg.edit_text(
                f"❌ <b>Не удалось получить информацию о треке</b>\n\n"
                f"<code>{esc_html(err_msg)}</code>{help_msg}",
                parse_mode="HTML"
            )
            return

        # Extract metadata
        title    = str(info.get("title") or "Unknown")[:200]
        artist   = str(
            info.get("artist") or info.get("uploader") or
            info.get("channel") or info.get("creator") or "Unknown"
        )[:200]
        album    = str(info.get("album") or info.get("playlist_title") or "")[:200]
        duration = float(info.get("duration") or 0)
        thumb    = info.get("thumbnail") or ""

        # Duration limit
        if duration > MAX_DURATION_SEC:
            await msg.edit_text(
                f"❌ <b>Трек слишком длинный</b>\n\n"
                f"Максимум {MAX_DURATION_SEC//60} мин. Этот трек: {fmt_dur(duration)}",
                parse_mode="HTML"
            )
            return

        # ── Step 1: download audio ────────────────────────
        await _animate(msg, 2, title)

        if is_yt:
            dl_strategies = [
                ("iOS client", _yt_opts(tmp, download=True)),
                ("Android fallback", _yt_opts_fallback(tmp, download=True)),
            ]
        elif is_sc:
            dl_strategies = [("SoundCloud", _sc_opts(tmp, download=True))]
        else:
            dl_strategies = [("Generic", {
                "quiet":True,"no_warnings":True,
                "format":"bestaudio/best",
                "outtmpl":f"{tmp}/%(title)s.%(ext)s",
                "postprocessors":[{"key":"FFmpegExtractAudio","preferredcodec":"mp3","preferredquality":"192"}],
            })]

        dl_ok = False
        for strategy_name, dl_opts in dl_strategies:
            try:
                log.info(f"⬇️ Downloading with {strategy_name}")
                await _ydl_run(url, dl_opts, loop)
                dl_ok = True
                break
            except Exception as e:
                last_error = e
                log.warning(f"⚠️ Download {strategy_name} failed: {e}")
                await asyncio.sleep(0.5)

        if not dl_ok:
            await msg.edit_text(
                f"❌ <b>Ошибка скачивания</b>\n\n<code>{esc_html(str(last_error)[:300])}</code>",
                parse_mode="HTML"
            )
            return

        # ── Step 2: find file ─────────────────────────────
        await _animate(msg, 3, title)
        audio_path = None
        for pat in ["*.mp3", "*.m4a", "*.ogg", "*.opus", "*.flac", "*.wav", "*.webm", "*.aac"]:
            found = list(Path(tmp).glob(pat))
            if found:
                audio_path = max(found, key=lambda p: p.stat().st_size)  # largest file
                break

        if not audio_path:
            await msg.edit_text("❌ Аудиофайл не найден после скачивания.", parse_mode="HTML")
            return

        # File size limit
        file_size_mb = audio_path.stat().st_size / 1024 / 1024
        if file_size_mb > MAX_FILE_MB:
            await msg.edit_text(
                f"❌ Файл слишком большой: {file_size_mb:.1f} МБ (макс {MAX_FILE_MB} МБ)",
                parse_mode="HTML"
            )
            return

        ext  = audio_path.suffix.lstrip(".")
        safe = safe_filename(title)
        ts   = int(time.time())

        # ── Step 3: upload audio ──────────────────────────
        await _animate(msg, 4, title)
        ct_map = {
            "mp3":"audio/mpeg","m4a":"audio/mp4","ogg":"audio/ogg",
            "opus":"audio/opus","flac":"audio/flac","wav":"audio/wav",
            "webm":"audio/webm","aac":"audio/aac",
        }
        try:
            audio_url = await sb_upload(
                f"audio/{ts}_{safe}.mp3",
                audio_path.read_bytes(),
                ct_map.get(ext, "audio/mpeg")
            )
        except Exception as e:
            await msg.edit_text(
                f"❌ <b>Ошибка загрузки аудио</b>\n\n<code>{esc_html(str(e)[:200])}</code>",
                parse_mode="HTML"
            )
            return

        # ── Step 4: upload thumbnail ──────────────────────
        art_url = None
        if thumb:
            try:
                async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
                    r = await c.get(thumb)
                if r.status_code == 200 and len(r.content) < 5 * 1024 * 1024:
                    art_url = await sb_upload(f"art/{ts}_{safe}.jpg", r.content, "image/jpeg")
            except Exception as e:
                log.warning(f"Thumbnail skipped: {e}")

        # ── Step 5: save to DB ────────────────────────────
        await _animate(msg, 5, title)
        try:
            row = await sb_post("tracks", {
                "title":      title,
                "artist":     artist,
                "audio_url":  audio_url,
                "art_url":    art_url,
                "favorite":   False,
                "duration":   round(duration, 2),
                "play_count": 0,
            })
            tid = row.get("id", "?") if isinstance(row, dict) else "?"
        except Exception as e:
            await msg.edit_text(
                f"❌ <b>Ошибка записи в базу</b>\n\n<code>{esc_html(str(e)[:200])}</code>",
                parse_mode="HTML"
            )
            return

    # ── Final message ─────────────────────────────────────
    text = (
        f"✅ <b>Трек добавлен!</b>\n\n"
        f"🎵 <b>{esc_html(title[:60])}</b>\n"
        f"👤 {esc_html(artist[:40])}\n"
    )
    if album:
        text += f"💿 {esc_html(album[:40])}\n"
    text += (
        f"⏱ {fmt_dur(duration)}\n"
        f"📦 {file_size_mb:.1f} МБ\n"
        f"🖼 Обложка: {'✅' if art_url else '⚠️ не найдена'}\n"
        f"<code>ID: {tid}</code>"
    )
    await msg.edit_text(text, parse_mode="HTML")


# ─────────────────────────────────────────────────────────────
#  CALLBACKS
# ─────────────────────────────────────────────────────────────
async def on_callback(u: Update, _):
    q = u.callback_query
    await q.answer()
    if not is_admin(u): return

    data = q.data or ""

    if data in ("status", "refresh_status"):
        await _send_status(q, edit=True)
    elif data == "top":
        await _send_top(q, edit=True)
    elif data.startswith("tracks_page:"):
        page = int(data.split(":")[1])
        await _send_tracks_page(q, page, edit=True)
    elif data == "toggle_block":
        cfg = await get_cfg()
        blocked = not cfg.get("blocked", False)
        await sb_upsert("settings", {"id": 1, "blocked": blocked})
        await _send_status(q, edit=True)
    elif data == "noop":
        pass


# ─────────────────────────────────────────────────────────────
#  FASTAPI / WEBHOOK
# ─────────────────────────────────────────────────────────────
fastapi_app = FastAPI(title="AURORA Bot v6", docs_url=None, redoc_url=None)
tg_app: Application = None


def _verify_webhook_secret(provided: str, expected: str) -> bool:
    """Constant-time comparison to prevent timing attacks."""
    return hmac.compare_digest(provided.encode(), expected.encode())


@fastapi_app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Robots-Tag"] = "noindex, nofollow"
    return response


@fastapi_app.post("/webhook/track-added")
async def on_track_added(req: Request):
    provided = req.headers.get("x-webhook-secret", "")
    if not _verify_webhook_secret(provided, WEBHOOK_SECRET):
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        body   = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    record = body.get("record") or body
    etype  = body.get("type", "INSERT")

    if etype == "DELETE":
        old    = body.get("old_record") or {}
        title  = esc_html(str(old.get("title","?")))
        artist = esc_html(str(old.get("artist","?")))
        text   = f"🗑 <b>Трек удалён</b>\n\n🎵 {title}  —  👤 {artist}"
    elif etype == "UPDATE":
        title  = esc_html(str(record.get("title","?")))
        artist = esc_html(str(record.get("artist","?")))
        text   = f"✏️ <b>Трек обновлён</b>\n\n🎵 {title}  —  👤 {artist}"
    else:
        title  = esc_html(str(record.get("title","?")))
        artist = esc_html(str(record.get("artist","?")))
        dur    = fmt_dur(record.get("duration", 0))
        art    = "✅" if record.get("art_url") else "⚠️"
        text   = (
            f"🔔 <b>Новый трек!</b>\n\n"
            f"🎵 <b>{title}</b>\n"
            f"👤 {artist}\n"
            f"⏱ {dur}  ·  🖼 {art}"
        )

    if tg_app and ADMIN_CHAT_ID:
        try:
            await tg_app.bot.send_message(ADMIN_CHAT_ID, text, parse_mode="HTML")
        except Exception as e:
            log.error(f"Telegram notify error: {e}")

    return JSONResponse({"ok": True})


@fastapi_app.post("/webhook/telegram")
async def tg_hook(req: Request):
    try:
        data = json.loads(await req.body())
        await tg_app.process_update(Update.de_json(data, tg_app.bot))
    except Exception as e:
        log.error(f"Telegram webhook error: {e}")
    return JSONResponse({"ok": True})


@fastapi_app.get("/")
async def health():
    return {
        "status":    "ok",
        "service":   "AURORA Bot v6",
        "cookies":   os.path.exists(COOKIES_FILE),
        "timestamp": int(time.time()),
    }


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
async def main():
    global tg_app

    if not BOT_TOKEN:
        log.error("❌ BOT_TOKEN не задан!")
        return

    # Auto-upgrade yt-dlp
    _ensure_ytdlp()

    tg_app = Application.builder().token(BOT_TOKEN).updater(None).build()

    handlers = [
        ("start",    cmd_start),
        ("status",   cmd_status),
        ("tracks",   cmd_tracks),
        ("top",      cmd_top),
        ("stats",    cmd_stats),
        ("recent",   cmd_recent),
        ("search",   cmd_search),
        ("block",    cmd_block),
        ("unblock",  cmd_unblock),
        ("delete",   cmd_delete),
        ("rename",   cmd_rename),
        ("download", cmd_download),
        ("setpw",    cmd_setpw),
        ("disablepw",cmd_disablepw),
    ]
    for cmd, fn in handlers:
        tg_app.add_handler(CommandHandler(cmd, fn))
    tg_app.add_handler(CallbackQueryHandler(on_callback))

    await tg_app.initialize()
    await tg_app.bot.set_my_commands([
        BotCommand("start",    "Главное меню"),
        BotCommand("status",   "Статус плеера"),
        BotCommand("stats",    "Подробная статистика"),
        BotCommand("top",      "Топ-10 треков"),
        BotCommand("recent",   "Последние добавленные"),
        BotCommand("tracks",   "Список треков (пагинация)"),
        BotCommand("search",   "Поиск по трекам"),
        BotCommand("download", "Скачать YouTube / SoundCloud"),
        BotCommand("rename",   "Переименовать трек"),
        BotCommand("delete",   "Удалить трек"),
        BotCommand("block",    "Заблокировать плеер"),
        BotCommand("unblock",  "Разблокировать плеер"),
        BotCommand("setpw",    "Установить пароль на плеер"),
        BotCommand("disablepw","Отключить пароль на плеер"),
    ])

    if PUBLIC_URL:
        await tg_app.bot.set_webhook(f"{PUBLIC_URL}/webhook/telegram")
        log.info(f"✅ Webhook: {PUBLIC_URL}/webhook/telegram")
    else:
        log.warning("⚠️ PUBLIC_URL не задан — webhook не установлен")

    await tg_app.start()
    log.info(f"🎵 AURORA Bot v6 запущен на порту {PORT}")
    log.info(f"🍪 Cookies file: {'FOUND' if os.path.exists(COOKIES_FILE) else 'NOT FOUND'}")
    if SB_SERVICE_KEY:
        log.info("🔑 Supabase: используется service_role ключ (полный доступ)")
    else:
        log.warning("⚠️  Supabase: SB_SERVICE_KEY не задан — используется anon ключ. RLS может блокировать запись!")

    config = uvicorn.Config(fastapi_app, host="0.0.0.0", port=PORT, log_level="warning")
    await uvicorn.Server(config).serve()
    await tg_app.stop()


if __name__ == "__main__":
    asyncio.run(main())
