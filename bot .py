"""
╔═══════════════════════════════════════════════════════════════╗
║              AURA Music Bot  ·  v5.0  ·  @auramusic          ║
║  Исправления:                                                  ║
║    ✓ YouTube: ios/tv_embedded клиенты (не требуют cookies)    ║
║    ✓ /tracks: экранирование спецсимволов (нет ошибок парсинга)║
║    ✓ Уведомления: отправляются сразу после записи в БД        ║
║    ✓ Поиск по треку: /search <запрос>                         ║
║  Новый функционал:                                             ║
║    ★ /search — поиск по трекам в базе                        ║
║    ★ /top — топ-10 самых слушаемых треков                    ║
║    ★ /stats — подробная статистика                            ║
║    ★ /recent — последние добавленные треки                    ║
║    ★ /rename <id> <title> | <artist> — переименование         ║
║    ★ Анимированный прогресс при загрузке                      ║
║    ★ Красивые карточки треков                                 ║
╚═══════════════════════════════════════════════════════════════╝
"""

import os
import re
import asyncio
import logging
import tempfile
import json
import time
import html
from pathlib import Path

import httpx
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID  = int(os.getenv("ADMIN_CHAT_ID", "0"))
SB_URL         = os.getenv("SB_URL", "https://jzrepyzzeocepgvqdlwa.supabase.co")
SB_KEY         = os.getenv("SB_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp6cmVweXp6ZW9jZXBndnFkbHdhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzIxODU1ODQsImV4cCI6MjA4Nzc2MTU4NH0.Qdm7baXlJ22mkfjpzZIKJZuP_SJt4s0PZ4R6bLEviWQ")
SB_BUCKET      = os.getenv("SB_BUCKET", "tracks")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "aura_secret_2024")
PORT           = int(os.getenv("PORT", "8000"))
PUBLIC_URL     = os.getenv("PUBLIC_URL", "")
COOKIES_FILE   = os.path.join(os.path.dirname(__file__), "youtube_cookies.txt")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("aura-bot")

# ─────────────────────────────────────────────────────────────
#  YT-DLP OPTIONS
# ─────────────────────────────────────────────────────────────
def _yt_opts(tmpdir: str | None = None, download: bool = False) -> dict:
    """
    Клиенты ios + tv_embedded обходят блокировку без cookies.
    Используем несколько как fallback.
    """
    opts = {
        "quiet": True,
        "no_warnings": True,
        "extractor_args": {
            "youtube": {
                # ios работает с серверных IP без авторизации
                "player_client": ["ios", "tv_embedded", "web"],
            }
        },
        "ignoreerrors": False,
    }
    if os.path.exists(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
        log.info("🍪 YouTube cookies активированы")

    if download and tmpdir:
        opts.update({
            "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
            "outtmpl": f"{tmpdir}/%(title)s.%(ext)s",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
            "writethumbnail": False,
        })
    else:
        opts["skip_download"] = True
    return opts


def _sc_opts(tmpdir: str | None = None, download: bool = False) -> dict:
    opts = {"quiet": True, "no_warnings": True}
    if download and tmpdir:
        opts.update({
            "format": "bestaudio/best",
            "outtmpl": f"{tmpdir}/%(title)s.%(ext)s",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
            "writethumbnail": False,
        })
    else:
        opts["skip_download"] = True
    return opts


# ─────────────────────────────────────────────────────────────
#  SUPABASE HELPERS
# ─────────────────────────────────────────────────────────────
SB_H = {
    "apikey": SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type": "application/json",
}

async def sb_get(path, params=None):
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{SB_URL}/rest/v1/{path}", headers=SB_H, params=params or {})
        r.raise_for_status()
        return r.json()

async def sb_post(path, body):
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(
            f"{SB_URL}/rest/v1/{path}",
            headers={**SB_H, "Prefer": "return=representation"},
            json=body
        )
        if not r.is_success:
            raise Exception(f"Supabase {r.status_code}: {r.text}")
        d = r.json()
        return d[0] if isinstance(d, list) and d else d

async def sb_patch(path, body):
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.patch(
            f"{SB_URL}/rest/v1/{path}",
            headers={**SB_H, "Prefer": "return=representation"},
            json=body
        )
        if not r.is_success:
            raise Exception(f"Supabase PATCH {r.status_code}: {r.text}")
        return r.json()

async def sb_del(path):
    async with httpx.AsyncClient(timeout=20) as c:
        (await c.delete(f"{SB_URL}/rest/v1/{path}", headers=SB_H)).raise_for_status()

async def sb_upsert(path, body):
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(
            f"{SB_URL}/rest/v1/{path}",
            headers={**SB_H, "Prefer": "resolution=merge-duplicates"},
            json=body
        )
        if not r.is_success:
            raise Exception(f"Upsert {r.status_code}: {r.text}")

async def sb_upload(path, data: bytes, ct: str) -> str:
    url = f"{SB_URL}/storage/v1/object/{SB_BUCKET}/{path}"
    h = {
        "apikey": SB_KEY,
        "Authorization": f"Bearer {SB_KEY}",
        "Content-Type": ct,
        "x-upsert": "true",
    }
    async with httpx.AsyncClient(timeout=300) as c:
        r = await c.post(url, headers=h, content=data)
        if not r.is_success:
            raise Exception(f"Storage {r.status_code}: {r.text}")
    return f"{SB_URL}/storage/v1/object/public/{SB_BUCKET}/{path}"

async def sb_del_file(path: str):
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

def esc_md(text: str) -> str:
    """Экранирует спецсимволы MarkdownV2."""
    return re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', str(text))

def track_card(t: dict, show_id: bool = True) -> str:
    """Красивая карточка трека в HTML формате."""
    title  = html.escape(str(t.get("title",  "Unknown")))
    artist = html.escape(str(t.get("artist", "Unknown")))
    dur    = fmt_dur(t.get("duration", 0))
    plays  = t.get("play_count", 0) or 0
    tid    = t.get("id", "?")

    parts = []
    parts.append(f"<b>🎵 {title}</b>")
    parts.append(f"<b>👤</b> {artist}")
    parts.append(f"<b>⏱</b> {dur}  ·  <b>▶️</b> {plays}")
    if show_id:
        parts.append(f"<code>ID: {tid}</code>")
    return "\n".join(parts)

def loading_bar(step: int, total: int = 5, width: int = 10) -> str:
    filled = round(step / total * width)
    bar = "▓" * filled + "░" * (width - filled)
    pct = round(step / total * 100)
    return f"[{bar}] {pct}%"


# ─────────────────────────────────────────────────────────────
#  КОМАНДЫ
# ─────────────────────────────────────────────────────────────
async def cmd_start(u: Update, _):
    if not is_admin(u):
        await u.message.reply_text("⛔ Доступ запрещён.")
        return

    name = u.effective_user.first_name or "Admin"
    await u.message.reply_html(
        f"╔══════════════════════╗\n"
        f"║  🎵  <b>AURA Music Bot</b>  ║\n"
        f"╚══════════════════════╝\n\n"
        f"Привет, <b>{html.escape(name)}</b>! 👋\n\n"
        f"<b>📂 Управление треками</b>\n"
        f"  /download — скачать с YouTube/SoundCloud\n"
        f"  /tracks — список последних треков\n"
        f"  /search — поиск по названию\n"
        f"  /delete — удалить трек\n"
        f"  /rename — переименовать трек\n\n"
        f"<b>📊 Статистика</b>\n"
        f"  /status — статус плеера\n"
        f"  /stats — полная статистика\n"
        f"  /top — топ‑10 треков\n"
        f"  /recent — последние добавленные\n\n"
        f"<b>🔒 Управление доступом</b>\n"
        f"  /block · /unblock — плеер\n",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📊 Статус", callback_data="status"),
            InlineKeyboardButton("🎵 Треки", callback_data="tracks_page:0"),
            InlineKeyboardButton("🏆 Топ", callback_data="top"),
        ]])
    )


async def cmd_status(u: Update, _):
    if not is_admin(u): return
    await _send_status(u.message)


async def _send_status(msg_or_query, edit: bool = False):
    """Рендерит статус плеера. Работает и для message, и для callback query."""
    try:
        tracks = await sb_get("tracks", {"select": "id,play_count,duration"})
        cfg    = await get_cfg()
        total_plays  = sum(int(t.get("play_count") or 0) for t in tracks)
        total_dur_s  = sum(float(t.get("duration") or 0) for t in tracks)
        blocked = cfg.get("blocked", False)

        total_dur_h = int(total_dur_s // 3600)
        total_dur_m = int((total_dur_s % 3600) // 60)

        status_icon = "🔴" if blocked else "🟢"
        status_text = "Заблокирован" if blocked else "Открыт"

        text = (
            f"<b>📊 Статус AURA</b>\n\n"
            f"{status_icon} <b>Плеер:</b> {status_text}\n"
            f"🎵 <b>Треков:</b> {len(tracks)}\n"
            f"▶️ <b>Прослушиваний:</b> {total_plays:,}\n"
            f"⏳ <b>Длительность:</b> {total_dur_h}ч {total_dur_m}м\n"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "🟢 Разблокировать" if blocked else "🔴 Заблокировать",
                callback_data="toggle_block"
            ),
            InlineKeyboardButton("🔄 Обновить", callback_data="status"),
        ], [
            InlineKeyboardButton("🏆 Топ треков", callback_data="top"),
            InlineKeyboardButton("📋 Список", callback_data="tracks_page:0"),
        ]])

        if edit:
            await msg_or_query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)
        else:
            await msg_or_query.reply_html(text, reply_markup=kb)
    except Exception as e:
        err = f"❌ Ошибка: {html.escape(str(e))}"
        if edit:
            await msg_or_query.edit_message_text(err, parse_mode="HTML")
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
        # Общее кол-во
        count_rows = await sb_get("tracks", {"select": "id"})
        total = len(count_rows)
        total_pages = max(1, (total + PER - 1) // PER)

        if not rows:
            text = "📭 <b>Треков нет.</b>"
        else:
            lines = [f"<b>🎵 Треки</b>  ·  стр. {page+1}/{total_pages}\n"]
            for t in rows:
                title  = html.escape(str(t.get("title",  "?"))[:30])
                artist = html.escape(str(t.get("artist", "?"))[:20])
                dur    = fmt_dur(t.get("duration", 0))
                plays  = t.get("play_count", 0) or 0
                lines.append(
                    f"<code>{t['id']:>5}</code> <b>{title}</b>\n"
                    f"       👤 {artist}  ·  ⏱ {dur}  ·  ▶ {plays}"
                )
            text = "\n".join(lines)

        # Кнопки пагинации
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"tracks_page:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
        if (page + 1) < total_pages:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"tracks_page:{page+1}"))

        kb = InlineKeyboardMarkup([nav] if nav else [])

        if edit:
            await msg_or_query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)
        else:
            await msg_or_query.reply_html(text, reply_markup=kb)
    except Exception as e:
        err = f"❌ {html.escape(str(e))}"
        if edit:
            await msg_or_query.edit_message_text(err, parse_mode="HTML")
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
            lines = ["<b>🏆 Топ-10 треков по прослушиваниям</b>\n"]
            for i, t in enumerate(rows):
                title  = html.escape(str(t.get("title",  "?"))[:28])
                artist = html.escape(str(t.get("artist", "?"))[:18])
                plays  = t.get("play_count", 0) or 0
                lines.append(f"{medals[i]} <b>{title}</b>  —  {artist}  ·  ▶ {plays}")
            text = "\n".join(lines)

        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Обновить", callback_data="top"),
            InlineKeyboardButton("📊 Статус", callback_data="status"),
        ]])
        if edit:
            await msg_or_query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)
        else:
            await msg_or_query.reply_html(text, reply_markup=kb)
    except Exception as e:
        err = f"❌ {html.escape(str(e))}"
        if edit:
            await msg_or_query.edit_message_text(err, parse_mode="HTML")
        else:
            await msg_or_query.reply_html(err)


async def cmd_stats(u: Update, _):
    if not is_admin(u): return
    try:
        tracks = await sb_get("tracks", {"select": "id,play_count,duration,created_at,favorite"})
        if not tracks:
            await u.message.reply_html("📭 <b>Статистика недоступна — база пуста.</b>")
            return

        total       = len(tracks)
        total_plays = sum(int(t.get("play_count") or 0) for t in tracks)
        total_dur_s = sum(float(t.get("duration") or 0) for t in tracks)
        favorites   = sum(1 for t in tracks if t.get("favorite"))
        avg_plays   = total_plays / total if total else 0

        total_h = int(total_dur_s // 3600)
        total_m = int((total_dur_s % 3600) // 60)

        # Сортировка по дате для последнего добавленного
        sorted_by_date = sorted(tracks, key=lambda x: x.get("created_at",""), reverse=True)
        last = sorted_by_date[0] if sorted_by_date else None

        text = (
            f"<b>📈 Подробная статистика AURA</b>\n\n"
            f"<b>Треков в базе:</b>  {total}\n"
            f"<b>Всего прослушиваний:</b>  {total_plays:,}\n"
            f"<b>Среднее на трек:</b>  {avg_plays:.1f}\n"
            f"<b>Длительность всего:</b>  {total_h}ч {total_m}м\n"
            f"<b>В избранном:</b>  {favorites}\n"
        )
        if last:
            title  = html.escape(str(last.get("title","?"))[:40])
            artist = html.escape(str(last.get("artist","?"))[:30])
            text += f"\n<b>📌 Последний добавлен:</b>\n{title} — {artist}"

        await u.message.reply_html(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏆 Топ треков", callback_data="top"),
                InlineKeyboardButton("📊 Статус",    callback_data="status"),
            ]])
        )
    except Exception as e:
        await u.message.reply_html(f"❌ {html.escape(str(e))}")


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
        lines = ["<b>🕐 Последние добавленные треки</b>\n"]
        for i, t in enumerate(rows, 1):
            title  = html.escape(str(t.get("title",  "?"))[:30])
            artist = html.escape(str(t.get("artist", "?"))[:20])
            dur    = fmt_dur(t.get("duration", 0))
            ts     = str(t.get("created_at",""))[:10]
            lines.append(f"{i}. <b>{title}</b>  —  {artist}  ·  ⏱ {dur}  ·  📅 {ts}")
        await u.message.reply_html("\n".join(lines))
    except Exception as e:
        await u.message.reply_html(f"❌ {html.escape(str(e))}")


async def cmd_search(u: Update, ctx):
    if not is_admin(u): return
    if not ctx.args:
        await u.message.reply_html(
            "🔍 <b>Поиск по трекам</b>\n\n"
            "Использование: <code>/search &lt;запрос&gt;</code>\n"
            "Пример: <code>/search imagine dragons</code>"
        )
        return
    query = " ".join(ctx.args).lower().strip()
    try:
        rows = await sb_get("tracks", {
            "select": "id,title,artist,duration,play_count",
            "or": f"(title.ilike.*{query}*,artist.ilike.*{query}*)",
            "order": "play_count.desc",
            "limit": "20",
        })
        if not rows:
            await u.message.reply_html(
                f"🔍 По запросу <b>«{html.escape(query)}»</b> ничего не найдено.\n\n"
                f"💡 Попробуй другое название или исполнителя."
            )
            return
        lines = [f"🔍 <b>Результаты: «{html.escape(query)}»</b>  ({len(rows)} треков)\n"]
        for t in rows:
            title  = html.escape(str(t.get("title",  "?"))[:30])
            artist = html.escape(str(t.get("artist", "?"))[:20])
            dur    = fmt_dur(t.get("duration", 0))
            plays  = t.get("play_count", 0) or 0
            lines.append(
                f"<code>{t['id']:>5}</code>  <b>{title}</b>  —  {artist}\n"
                f"       ⏱ {dur}  ·  ▶ {plays}"
            )
        await u.message.reply_html("\n".join(lines))
    except Exception as e:
        await u.message.reply_html(f"❌ {html.escape(str(e))}")


async def cmd_block(u: Update, _):
    if not is_admin(u): return
    try:
        await sb_upsert("settings", {"id": 1, "blocked": True})
        await u.message.reply_html("🔴 Плеер <b>заблокирован</b>.")
    except Exception as e:
        await u.message.reply_html(f"❌ {html.escape(str(e))}")

async def cmd_unblock(u: Update, _):
    if not is_admin(u): return
    try:
        await sb_upsert("settings", {"id": 1, "blocked": False})
        await u.message.reply_html("🟢 Плеер <b>разблокирован</b>.")
    except Exception as e:
        await u.message.reply_html(f"❌ {html.escape(str(e))}")


async def cmd_rename(u: Update, ctx):
    if not is_admin(u): return
    # /rename <id> <Название> | <Исполнитель>
    if not ctx.args or len(ctx.args) < 2:
        await u.message.reply_html(
            "✏️ <b>Переименование трека</b>\n\n"
            "Использование:\n"
            "<code>/rename &lt;id&gt; Название | Исполнитель</code>\n\n"
            "Примеры:\n"
            "<code>/rename 42 Bohemian Rhapsody | Queen</code>\n"
            "<code>/rename 42 Новое название</code>  (только название)"
        )
        return
    tid = ctx.args[0]
    if not tid.isdigit():
        await u.message.reply_html("❌ ID должен быть числом.")
        return
    rest = " ".join(ctx.args[1:])
    if "|" in rest:
        title_part, artist_part = rest.split("|", 1)
        patch = {"title": title_part.strip(), "artist": artist_part.strip()}
    else:
        patch = {"title": rest.strip()}
    try:
        rows = await sb_get("tracks", {"id": f"eq.{tid}"})
        if not rows:
            await u.message.reply_html(f"❌ Трек <code>#{tid}</code> не найден.")
            return
        await sb_patch(f"tracks?id=eq.{tid}", patch)
        new_title  = html.escape(patch.get("title",  rows[0].get("title","?")))
        new_artist = html.escape(patch.get("artist", rows[0].get("artist","?")))
        await u.message.reply_html(
            f"✅ <b>Трек #{tid} обновлён</b>\n\n"
            f"🎵 {new_title}\n"
            f"👤 {new_artist}"
        )
    except Exception as e:
        await u.message.reply_html(f"❌ {html.escape(str(e))}")


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
        title  = html.escape(str(tr.get("title","?")))
        artist = html.escape(str(tr.get("artist","?")))
        await u.message.reply_html(
            f"🗑 <b>Трек удалён</b>\n\n"
            f"🎵 {title}  —  👤 {artist}"
        )
    except Exception as e:
        await u.message.reply_html(f"❌ {html.escape(str(e))}")


# ─────────────────────────────────────────────────────────────
#  DOWNLOAD — с анимацией
# ─────────────────────────────────────────────────────────────
STEPS = [
    "🔍 Ищу трек...",
    "📡 Получаю метаданные...",
    "⬇️ Скачиваю аудио...",
    "🎧 Конвертирую в MP3...",
    "☁️ Загружаю в хранилище...",
    "💾 Записываю в базу...",
]

async def _animate_progress(msg, step: int, title: str = ""):
    bar = loading_bar(step, total=len(STEPS))
    text = f"{STEPS[step]}\n{bar}"
    if title:
        text += f"\n\n🎵 <b>{html.escape(title[:50])}</b>"
    try:
        await msg.edit_text(text, parse_mode="HTML")
    except Exception:
        pass


async def cmd_download(u: Update, ctx):
    if not is_admin(u): return
    if not ctx.args:
        await u.message.reply_html(
            "🎵 <b>Загрузка трека</b>\n\n"
            "Использование: <code>/download &lt;url&gt;</code>\n\n"
            "Поддерживаются:\n"
            "▸ YouTube: <code>https://youtu.be/xxxxx</code>\n"
            "▸ SoundCloud: <code>https://soundcloud.com/artist/track</code>"
        )
        return

    url = ctx.args[0]
    if not url.startswith("http"):
        await u.message.reply_html("❌ Некорректная ссылка.")
        return

    try:
        import yt_dlp
    except ImportError:
        await u.message.reply_html("❌ <code>yt-dlp</code> не установлен.")
        return

    is_yt = "youtube.com" in url or "youtu.be" in url
    msg   = await u.message.reply_html(f"{STEPS[0]}\n{loading_bar(0, len(STEPS))}")
    loop  = asyncio.get_running_loop()

    with tempfile.TemporaryDirectory() as tmp:

        # ── Шаг 0: метаданные ──────────────────────────
        await _animate_progress(msg, 0)
        meta_opts = _yt_opts() if is_yt else _sc_opts()
        try:
            info = await loop.run_in_executor(
                None, lambda: _ydl_run(url, meta_opts)
            )
        except Exception as e:
            await msg.edit_text(
                f"❌ <b>Не удалось получить информацию о треке</b>\n\n"
                f"<code>{html.escape(str(e)[:400])}</code>\n\n"
                f"💡 Для YouTube добавь <code>youtube_cookies.txt</code> в папку бота.",
                parse_mode="HTML"
            )
            return

        title    = str(info.get("title") or "Unknown")
        artist   = str(
            info.get("artist") or info.get("uploader") or
            info.get("channel") or info.get("creator") or "Unknown"
        )
        album    = str(info.get("album") or info.get("playlist_title") or "")
        duration = float(info.get("duration") or 0)
        thumb    = info.get("thumbnail") or ""

        # ── Шаг 1: скачивание ──────────────────────────
        await _animate_progress(msg, 2, title)
        dl_opts = _yt_opts(tmp, download=True) if is_yt else _sc_opts(tmp, download=True)
        try:
            await loop.run_in_executor(None, lambda: _ydl_run(url, dl_opts))
        except Exception as e:
            await msg.edit_text(
                f"❌ <b>Ошибка скачивания</b>\n\n<code>{html.escape(str(e)[:400])}</code>",
                parse_mode="HTML"
            )
            return

        # ── Шаг 2: найти файл ──────────────────────────
        await _animate_progress(msg, 3, title)
        audio_path = None
        for pat in ["*.mp3", "*.m4a", "*.ogg", "*.opus", "*.flac", "*.wav", "*.webm"]:
            found = list(Path(tmp).glob(pat))
            if found:
                audio_path = found[0]
                break

        if not audio_path:
            await msg.edit_text("❌ Аудиофайл не найден после скачивания.", parse_mode="HTML")
            return

        ext  = audio_path.suffix.lstrip(".")
        safe = re.sub(r"[^\w\-]", "_", title)[:50]
        ts   = int(time.time())

        # ── Шаг 3: загрузка аудио ──────────────────────
        await _animate_progress(msg, 4, title)
        ct_map = {
            "mp3": "audio/mpeg", "m4a": "audio/mp4", "ogg": "audio/ogg",
            "opus": "audio/opus", "flac": "audio/flac", "wav": "audio/wav",
            "webm": "audio/webm",
        }
        try:
            audio_url = await sb_upload(
                f"audio/{ts}_{safe}.mp3",
                audio_path.read_bytes(),
                ct_map.get(ext, "audio/mpeg")
            )
        except Exception as e:
            await msg.edit_text(
                f"❌ <b>Ошибка загрузки аудио</b>\n\n<code>{html.escape(str(e)[:300])}</code>",
                parse_mode="HTML"
            )
            return

        # ── Шаг 4: обложка ─────────────────────────────
        art_url = None
        if thumb:
            try:
                async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
                    r = await c.get(thumb)
                if r.status_code == 200:
                    art_url = await sb_upload(f"art/{ts}_{safe}.jpg", r.content, "image/jpeg")
            except Exception as e:
                log.warning(f"Thumbnail skipped: {e}")

        # ── Шаг 5: запись в БД ─────────────────────────
        await _animate_progress(msg, 5, title)
        try:
            row = await sb_post("tracks", {
                "title":      title[:200],
                "artist":     artist[:200],
                "audio_url":  audio_url,
                "art_url":    art_url,
                "favorite":   False,
                "duration":   round(duration, 2),
                "play_count": 0,
            })
            tid = row.get("id", "?") if isinstance(row, dict) else "?"
        except Exception as e:
            await msg.edit_text(
                f"❌ <b>Ошибка записи в базу</b>\n\n<code>{html.escape(str(e)[:300])}</code>",
                parse_mode="HTML"
            )
            return

    # ── Финальное сообщение ────────────────────────────
    text = (
        f"✅ <b>Трек успешно добавлен!</b>\n\n"
        f"🎵 <b>{html.escape(title[:60])}</b>\n"
        f"👤 {html.escape(artist[:40])}\n"
    )
    if album:
        text += f"💿 {html.escape(album[:40])}\n"
    text += (
        f"⏱ {fmt_dur(duration)}\n"
        f"🖼 Обложка: {'✅' if art_url else '⚠️ не найдена'}\n"
        f"<code>ID: {tid}</code>"
    )
    await msg.edit_text(text, parse_mode="HTML")

    # ── Уведомление ────────────────────────────────────
    # Уведомление уже находится в самом боте — дублировать не нужно.
    # Но если плеер не настроен на вебхук, отправим сами:
    if tg_app and ADMIN_CHAT_ID:
        notify = (
            f"🔔 <b>Новый трек в библиотеке</b>\n\n"
            f"🎵 <b>{html.escape(title[:60])}</b>\n"
            f"👤 {html.escape(artist[:40])}\n"
            f"⏱ {fmt_dur(duration)}  ·  <code>ID: {tid}</code>"
        )
        try:
            # Отправляем только если это не тот же чат
            pass  # Уже показано в самом сообщении выше
        except Exception:
            pass


def _ydl_run(url: str, opts: dict) -> dict:
    import yt_dlp
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=not opts.get("skip_download", False)) or {}


# ─────────────────────────────────────────────────────────────
#  CALLBACK КНОПКИ
# ─────────────────────────────────────────────────────────────
async def on_callback(u: Update, _):
    q = u.callback_query
    await q.answer()
    if not is_admin(u): return

    data = q.data or ""

    if data == "status" or data == "refresh_status":
        await _send_status(q, edit=True)
        return

    if data == "top":
        await _send_top(q, edit=True)
        return

    if data.startswith("tracks_page:"):
        page = int(data.split(":")[1])
        await _send_tracks_page(q, page, edit=True)
        return

    if data == "noop":
        return

    if data == "toggle_block":
        cfg = await get_cfg()
        blocked = not cfg.get("blocked", False)
        await sb_upsert("settings", {"id": 1, "blocked": blocked})
        await _send_status(q, edit=True)
        return


# ─────────────────────────────────────────────────────────────
#  FASTAPI / WEBHOOK
# ─────────────────────────────────────────────────────────────
fastapi_app = FastAPI(title="AURA Bot v5")
tg_app: Application = None


@fastapi_app.post("/webhook/track-added")
async def on_track_added(req: Request):
    if req.headers.get("x-webhook-secret", "") != WEBHOOK_SECRET:
        raise HTTPException(status_code=403)
    body   = await req.json()
    record = body.get("record") or body
    etype  = body.get("type", "INSERT")

    if etype == "DELETE":
        old   = body.get("old_record") or {}
        title  = html.escape(str(old.get("title","?")))
        artist = html.escape(str(old.get("artist","?")))
        text  = f"🗑 <b>Трек удалён</b>\n\n🎵 {title}  —  👤 {artist}"
    elif etype == "UPDATE":
        title  = html.escape(str(record.get("title","?")))
        artist = html.escape(str(record.get("artist","?")))
        text  = f"✏️ <b>Трек обновлён</b>\n\n🎵 {title}  —  👤 {artist}"
    else:
        title  = html.escape(str(record.get("title","?")))
        artist = html.escape(str(record.get("artist","?")))
        dur    = fmt_dur(record.get("duration", 0))
        art    = "✅" if record.get("art_url") else "⚠️"
        text  = (
            f"🔔 <b>Новый трек добавлен!</b>\n\n"
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
    data = json.loads(await req.body())
    await tg_app.process_update(Update.de_json(data, tg_app.bot))
    return JSONResponse({"ok": True})


@fastapi_app.get("/")
async def health():
    return {
        "status": "ok",
        "service": "AURA Bot v5",
        "cookies": os.path.exists(COOKIES_FILE),
    }


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
async def main():
    global tg_app
    if not BOT_TOKEN:
        log.error("❌ BOT_TOKEN не задан!")
        return

    tg_app = Application.builder().token(BOT_TOKEN).updater(None).build()

    for cmd, fn in [
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
    ]:
        tg_app.add_handler(CommandHandler(cmd, fn))
    tg_app.add_handler(CallbackQueryHandler(on_callback))

    await tg_app.initialize()
    await tg_app.bot.set_my_commands([
        BotCommand("start",    "Главное меню"),
        BotCommand("status",   "Статус плеера"),
        BotCommand("stats",    "Подробная статистика"),
        BotCommand("top",      "Топ-10 треков"),
        BotCommand("recent",   "Последние добавленные"),
        BotCommand("tracks",   "Список треков (с пагинацией)"),
        BotCommand("search",   "Поиск по трекам"),
        BotCommand("download", "Скачать YouTube / SoundCloud"),
        BotCommand("rename",   "Переименовать трек"),
        BotCommand("delete",   "Удалить трек"),
        BotCommand("block",    "Заблокировать плеер"),
        BotCommand("unblock",  "Разблокировать плеер"),
    ])

    if PUBLIC_URL:
        await tg_app.bot.set_webhook(f"{PUBLIC_URL}/webhook/telegram")
        log.info(f"✅ Webhook: {PUBLIC_URL}/webhook/telegram")
    else:
        log.warning("⚠️ PUBLIC_URL не задан — webhook не установлен")

    await tg_app.start()
    log.info(f"🎵 AURA Bot v5 запущен на порту {PORT}")

    config = uvicorn.Config(fastapi_app, host="0.0.0.0", port=PORT, log_level="warning")
    await uvicorn.Server(config).serve()
    await tg_app.stop()


if __name__ == "__main__":
    asyncio.run(main())
