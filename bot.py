"""
AURA Music Player — Telegram Bot v3
────────────────────────────────────
Исправления v3:
  - YouTube: cookies файл для обхода Sign in бана
  - YouTube: tv_embedded клиент — не требует авторизации
  - SoundCloud: исправлена запись в базу
  - Метаданные: title, artist, album, обложка — авто
"""

import os
import re
import asyncio
import logging
import tempfile
import json
import time
from pathlib import Path

import httpx
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

# ──────────────────────────────────────────────────────────
#  CONFIG
# ──────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────
#  HELPERS
# ──────────────────────────────────────────────────────────
def _cookie_opts() -> dict:
    if os.path.exists(COOKIES_FILE):
        log.info(f"Cookies: {COOKIES_FILE}")
        return {"cookiefile": COOKIES_FILE}
    return {}

def fmt_dur(s) -> str:
    s = int(float(s or 0))
    return f"{s//60}:{s%60:02d}"

def is_admin(u: Update) -> bool:
    return u.effective_user.id == ADMIN_CHAT_ID

# ──────────────────────────────────────────────────────────
#  SUPABASE
# ──────────────────────────────────────────────────────────
SB_H = {"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}", "Content-Type": "application/json"}

async def sb_get(path, params=None):
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{SB_URL}/rest/v1/{path}", headers=SB_H, params=params or {})
        r.raise_for_status()
        return r.json()

async def sb_post(path, body):
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(f"{SB_URL}/rest/v1/{path}", headers={**SB_H, "Prefer": "return=representation"}, json=body)
        if not r.is_success:
            raise Exception(f"Supabase {r.status_code}: {r.text}")
        d = r.json()
        return d[0] if isinstance(d, list) and d else d

async def sb_del(path):
    async with httpx.AsyncClient(timeout=20) as c:
        (await c.delete(f"{SB_URL}/rest/v1/{path}", headers=SB_H)).raise_for_status()

async def sb_upsert(path, body):
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{SB_URL}/rest/v1/{path}", headers={**SB_H, "Prefer": "resolution=merge-duplicates"}, json=body)
        if not r.is_success:
            raise Exception(f"{r.status_code}: {r.text}")

async def sb_upload(path, data, ct) -> str:
    url = f"{SB_URL}/storage/v1/object/{SB_BUCKET}/{path}"
    h = {"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}", "Content-Type": ct, "x-upsert": "true"}
    async with httpx.AsyncClient(timeout=300) as c:
        r = await c.post(url, headers=h, content=data)
        if not r.is_success:
            raise Exception(f"Storage {r.status_code}: {r.text}")
    return f"{SB_URL}/storage/v1/object/public/{SB_BUCKET}/{path}"

async def sb_del_file(path):
    url = f"{SB_URL}/storage/v1/object/{SB_BUCKET}/{path}"
    async with httpx.AsyncClient(timeout=15) as c:
        await c.delete(url, headers={"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}"})

async def get_cfg() -> dict:
    try:
        rows = await sb_get("settings", {"id": "eq.1"})
        return rows[0] if rows else {}
    except Exception:
        return {}

# ──────────────────────────────────────────────────────────
#  КОМАНДЫ
# ──────────────────────────────────────────────────────────
async def cmd_start(u: Update, _):
    if not is_admin(u): await u.message.reply_text("⛔ Доступ запрещён."); return
    await u.message.reply_text(
        "🎵 *AURA Bot*\n\n"
        "/status — статус плеера\n"
        "/tracks — список треков\n"
        "/download `<url>` — YouTube/SoundCloud\n"
        "/block /unblock — управление доступом\n"
        "/delete `<id>` — удалить трек",
        parse_mode="Markdown"
    )

async def cmd_status(u: Update, _):
    if not is_admin(u): return
    try:
        tracks = await sb_get("tracks", {"select": "id,play_count"})
        cfg = await get_cfg()
        total = sum(int(t.get("play_count") or 0) for t in tracks)
        blocked = cfg.get("blocked", False)
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🟢 Разблокировать" if blocked else "🔴 Заблокировать", callback_data="toggle_block"),
            InlineKeyboardButton("🔄 Обновить", callback_data="refresh_status"),
        ]])
        await u.message.reply_text(
            f"📊 *Статус AURA*\n\n"
            f"🌐 {'🔴 Заблокирован' if blocked else '🟢 Открыт'}\n"
            f"🎵 Треков: *{len(tracks)}*\n▶️ Прослушиваний: *{total}*",
            parse_mode="Markdown", reply_markup=kb
        )
    except Exception as e:
        await u.message.reply_text(f"❌ {e}")

async def cmd_tracks(u: Update, _):
    if not is_admin(u): return
    try:
        rows = await sb_get("tracks", {"select": "id,title,artist,duration,play_count", "order": "created_at.desc", "limit": "50"})
        if not rows: await u.message.reply_text("📭 Треков нет."); return
        lines = ["🎵 *Треки* (последние 50):\n"]
        for t in rows:
            lines.append(f"`{t['id']:>4}` | {fmt_dur(t.get('duration',0))} | ▶{t.get('play_count',0)} | {t.get('title','?')} — {t.get('artist','?')}")
        text = "\n".join(lines)
        await u.message.reply_text(text[:4000], parse_mode="Markdown")
    except Exception as e:
        await u.message.reply_text(f"❌ {e}")

async def cmd_block(u: Update, _):
    if not is_admin(u): return
    try:
        await sb_upsert("settings", {"id": 1, "blocked": True})
        await u.message.reply_text("🔴 Плеер *заблокирован*.", parse_mode="Markdown")
    except Exception as e:
        await u.message.reply_text(f"❌ {e}")

async def cmd_unblock(u: Update, _):
    if not is_admin(u): return
    try:
        await sb_upsert("settings", {"id": 1, "blocked": False})
        await u.message.reply_text("🟢 Плеер *разблокирован*.", parse_mode="Markdown")
    except Exception as e:
        await u.message.reply_text(f"❌ {e}")

async def cmd_delete(u: Update, ctx):
    if not is_admin(u): return
    if not ctx.args or not ctx.args[0].isdigit():
        await u.message.reply_text("Использование: /delete <id>"); return
    tid = ctx.args[0]
    try:
        rows = await sb_get("tracks", {"id": f"eq.{tid}"})
        if not rows: await u.message.reply_text(f"❌ Трек #{tid} не найден."); return
        tr = rows[0]
        for f in ("audio_url", "art_url"):
            url = tr.get(f) or ""
            if url and f"/public/{SB_BUCKET}/" in url:
                try: await sb_del_file(url.split(f"/public/{SB_BUCKET}/")[1])
                except Exception: pass
        await sb_del(f"playlist_tracks?track_id=eq.{tid}")
        await sb_del(f"tracks?id=eq.{tid}")
        await u.message.reply_text(f"✅ *{tr.get('title','?')}* удалён.", parse_mode="Markdown")
    except Exception as e:
        await u.message.reply_text(f"❌ {e}")

# ──────────────────────────────────────────────────────────
#  СКАЧИВАНИЕ
# ──────────────────────────────────────────────────────────
async def cmd_download(u: Update, ctx):
    if not is_admin(u): return
    if not ctx.args:
        await u.message.reply_text(
            "Использование: /download <url>\n\n"
            "`/download https://youtu.be/xxxxx`\n"
            "`/download https://soundcloud.com/artist/track`",
            parse_mode="Markdown"
        ); return

    url = ctx.args[0]
    if not url.startswith("http"):
        await u.message.reply_text("❌ Некорректная ссылка."); return

    try:
        import yt_dlp
    except ImportError:
        await u.message.reply_text("❌ yt-dlp не установлен."); return

    is_yt = "youtube.com" in url or "youtu.be" in url
    msg   = await u.message.reply_text("⏳ Получаю информацию...")
    loop  = asyncio.get_event_loop()

    with tempfile.TemporaryDirectory() as tmp:

        # ── Метаданные ──
        meta = {
            "quiet": True, "no_warnings": True, "skip_download": True,
            **_cookie_opts(),
        }
        if is_yt:
            meta["extractor_args"] = {
                "youtube": {
                    "player_client": ["tv_embedded", "web_creator"],
                    "player_skip": ["webpage", "configs"],
                }
            }

        try:
            info = await loop.run_in_executor(None, lambda: _ydl(url, meta))
        except Exception as e:
            hint = "\n\n💡 Для YouTube добавь файл `youtube_cookies.txt` в папку бота." if is_yt else ""
            await msg.edit_text(f"❌ Ошибка получения информации:\n`{e}`{hint}", parse_mode="Markdown")
            return

        title    = str(info.get("title") or "Unknown")
        artist   = str(info.get("artist") or info.get("uploader") or info.get("channel") or "Unknown")
        album    = str(info.get("album") or info.get("playlist_title") or "")
        duration = float(info.get("duration") or 0)
        thumb    = info.get("thumbnail") or ""

        await msg.edit_text(f"⏳ Скачиваю...\n🎵 *{title}*\n👤 {artist}", parse_mode="Markdown")

        # ── Скачивание ──
        dl_opts = {
            "format": "bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best",
            "outtmpl": f"{tmp}/%(title)s.%(ext)s",
            "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}],
            "quiet": True, "no_warnings": True, "writethumbnail": False,
            **_cookie_opts(),
        }
        if is_yt:
            dl_opts["extractor_args"] = {
                "youtube": {
                    "player_client": ["tv_embedded", "web_creator"],
                    "player_skip": ["webpage", "configs"],
                }
            }

        try:
            await loop.run_in_executor(None, lambda: _ydl(url, dl_opts))
        except Exception as e:
            await msg.edit_text(f"❌ Ошибка скачивания:\n`{e}`", parse_mode="Markdown")
            return

        # ── Найти файл ──
        audio_path = None
        for pat in ["*.mp3", "*.m4a", "*.ogg", "*.opus", "*.flac", "*.wav", "*.webm"]:
            found = list(Path(tmp).glob(pat))
            if found: audio_path = found[0]; break

        if not audio_path:
            await msg.edit_text("❌ Файл не найден после скачивания."); return

        ext = audio_path.suffix.lstrip(".")
        safe = re.sub(r"[^\w\-]", "_", title)[:50]
        ts   = int(time.time())

        await msg.edit_text(f"⏳ Загружаю в хранилище...\n🎵 *{title}*", parse_mode="Markdown")

        # ── Загрузка аудио ──
        ct_map = {"mp3":"audio/mpeg","m4a":"audio/mp4","ogg":"audio/ogg","opus":"audio/opus","flac":"audio/flac","wav":"audio/wav","webm":"audio/webm"}
        try:
            audio_url = await sb_upload(f"audio/{ts}_{safe}.mp3", audio_path.read_bytes(), ct_map.get(ext, "audio/mpeg"))
        except Exception as e:
            await msg.edit_text(f"❌ Ошибка загрузки аудио:\n`{e}`", parse_mode="Markdown"); return

        # ── Загрузка обложки ──
        art_url = None
        if thumb:
            try:
                async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
                    r = await c.get(thumb)
                if r.status_code == 200:
                    art_url = await sb_upload(f"art/{ts}_{safe}.jpg", r.content, "image/jpeg")
            except Exception as e:
                log.warning(f"Thumbnail failed: {e}")

        # ── Запись в БД ──
        try:
            row = await sb_post("tracks", {
                "title": title[:200], "artist": artist[:200],
                "audio_url": audio_url, "art_url": art_url,
                "favorite": False, "duration": round(duration, 2), "play_count": 0,
            })
            tid = row.get("id", "?") if isinstance(row, dict) else "?"
        except Exception as e:
            await msg.edit_text(f"❌ Ошибка записи в базу:\n`{e}`", parse_mode="Markdown"); return

    text = f"✅ *Трек добавлен!*\n\n🎵 *{title}*\n👤 {artist}\n"
    if album: text += f"💿 {album}\n"
    text += f"⏱ {fmt_dur(duration)}\n🖼 {'✅' if art_url else '⚠️ нет'}\n🆔 ID: `{tid}`"
    await msg.edit_text(text, parse_mode="Markdown")


def _ydl(url: str, opts: dict) -> dict:
    import yt_dlp
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=not opts.get("skip_download", False)) or {}


# ──────────────────────────────────────────────────────────
#  CALLBACKS
# ──────────────────────────────────────────────────────────
async def on_callback(u: Update, _):
    q = u.callback_query
    await q.answer()
    if not is_admin(u): return

    cfg     = await get_cfg()
    blocked = cfg.get("blocked", False)

    if q.data == "toggle_block":
        blocked = not blocked
        await sb_upsert("settings", {"id": 1, "blocked": blocked})

    tracks = await sb_get("tracks", {"select": "id,play_count"})
    total  = sum(int(t.get("play_count") or 0) for t in tracks)
    await q.edit_message_text(
        f"📊 *Статус AURA*\n\n"
        f"🌐 {'🔴 Заблокирован' if blocked else '🟢 Открыт'}\n"
        f"🎵 Треков: *{len(tracks)}*\n▶️ Прослушиваний: *{total}*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🟢 Разблокировать" if blocked else "🔴 Заблокировать", callback_data="toggle_block"),
            InlineKeyboardButton("🔄 Обновить", callback_data="refresh_status"),
        ]])
    )


# ──────────────────────────────────────────────────────────
#  FASTAPI
# ──────────────────────────────────────────────────────────
fastapi_app = FastAPI()
tg_app: Application = None


@fastapi_app.post("/webhook/track-added")
async def on_track_added(req: Request):
    if req.headers.get("x-webhook-secret", "") != WEBHOOK_SECRET:
        raise HTTPException(403)
    body   = await req.json()
    record = body.get("record") or body
    etype  = body.get("type", "INSERT")
    if etype == "DELETE":
        old  = body.get("old_record") or {}
        text = f"🗑 *Трек удалён*\n\n*{old.get('title','?')}* — {old.get('artist','?')}"
    elif etype == "UPDATE":
        text = f"✏️ *Трек обновлён*\n\n*{record.get('title','?')}* — {record.get('artist','?')}"
    else:
        text = f"🎵 *Новый трек!*\n\n🎤 *{record.get('artist','?')}*\n🎼 {record.get('title','?')}\n⏱ {fmt_dur(record.get('duration',0))}"
    if tg_app and ADMIN_CHAT_ID:
        try: await tg_app.bot.send_message(ADMIN_CHAT_ID, text, parse_mode="Markdown")
        except Exception as e: log.error(e)
    return JSONResponse({"ok": True})


@fastapi_app.post("/webhook/telegram")
async def tg_hook(req: Request):
    data = json.loads(await req.body())
    await tg_app.process_update(Update.de_json(data, tg_app.bot))
    return JSONResponse({"ok": True})


@fastapi_app.get("/")
async def health():
    return {"status": "ok", "cookies": os.path.exists(COOKIES_FILE)}


# ──────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────
async def main():
    global tg_app
    if not BOT_TOKEN: log.error("BOT_TOKEN не задан!"); return

    tg_app = Application.builder().token(BOT_TOKEN).updater(None).build()
    for cmd, fn in [("start",cmd_start),("status",cmd_status),("tracks",cmd_tracks),
                    ("block",cmd_block),("unblock",cmd_unblock),("delete",cmd_delete),("download",cmd_download)]:
        tg_app.add_handler(CommandHandler(cmd, fn))
    tg_app.add_handler(CallbackQueryHandler(on_callback))

    await tg_app.initialize()
    await tg_app.bot.set_my_commands([
        BotCommand("start","Главное меню"), BotCommand("status","Статус плеера"),
        BotCommand("tracks","Список треков"), BotCommand("download","Скачать YouTube/SoundCloud"),
        BotCommand("block","Заблокировать плеер"), BotCommand("unblock","Разблокировать плеер"),
        BotCommand("delete","Удалить трек"),
    ])

    if PUBLIC_URL:
        await tg_app.bot.set_webhook(f"{PUBLIC_URL}/webhook/telegram")
        log.info(f"Webhook: {PUBLIC_URL}/webhook/telegram")

    await tg_app.start()
    log.info(f"AURA Bot v3 on port {PORT}")
    config = uvicorn.Config(fastapi_app, host="0.0.0.0", port=PORT, log_level="warning")
    await uvicorn.Server(config).serve()
    await tg_app.stop()


if __name__ == "__main__":
    asyncio.run(main())
