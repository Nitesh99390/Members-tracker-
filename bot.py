"""
╔══════════════════════════════════════════════════════════════════════╗
║   Telegram Membership Manager Bot — Professional Edition  v12.0      ║
║   Library : python-telegram-bot (v20+)                               ║
║   Storage : Supabase (PostgreSQL) + psycopg2-pool                    ║
║   Server  : Flask (Render.com Web Service Compatibility)             ║
║   Schedule: Built-in JobQueue (1-min auto-kick sweep)                ║
║                                                                       ║
║   v12.0 Improvements:                                                 ║
║   - Fixed inverted join-detection logic in track_chat_member          ║
║   - Connection pooling (ThreadedConnectionPool) — no per-call connect ║
║   - Timezone-aware datetime throughout (no naive-datetime bugs)       ║
║   - Reminder dedup via last_reminded column (no double pings)        ║
║   - Safe mlist chunking (respects Telegram 4096 char limit)          ║
║   - Robust emoji logic using timedelta, not string parsing            ║
║   - Batch expired fetch → single query, not N+1                      ║
║   - Graceful callback error replies instead of silent failures        ║
║   - Flask starts before polling (no port-binding race on Render)      ║
║   - Removed dead admins-table is_admin path; pure Telegram API check  ║
╚══════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

# ==========================================================================
#  IMPORTS
# ==========================================================================
import asyncio
import json
import logging
import os
import re
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Generator, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from flask import Flask

from telegram import (
    ChatMember,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
)


# ==========================================================================
#  CONFIGURATION
# ==========================================================================
BOT_TOKEN:    str = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL: str = os.getenv("DATABASE_URL", "").strip()
PORT:         int = int(os.getenv("PORT", "8080"))
LOG_FILE:     str = os.getenv("LOG_FILE", "membership_bot.log")

# Timezone — India Standard Time (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

# Background job intervals
AUTO_KICK_INTERVAL: int = 60    # seconds
REMINDER_INTERVAL:  int = 3600  # seconds

# Reminder windows: send DM when expiry is this many hours away
REMINDER_HOURS: tuple[int, ...] = (72, 24)

# Preset plans shown as inline buttons
PRESET_PLANS: dict[str, str] = {
    "1m":   "🧪 1 Minute (Test)",
    "7d":   "🥉 7 Days",
    "30d":  "🥇 30 Days",
    "90d":  "💎 90 Days",
    "180d": "🔥 180 Days",
    "365d": "👑 365 Days",
}

# Telegram hard limit for message text
TG_MAX_CHARS = 4000   # slightly under 4096 for safety
PER_PAGE     = 15     # members per mlist page


# ==========================================================================
#  LOGGING
# ==========================================================================
def _setup_logging() -> logging.Logger:
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    try:
        fh = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except OSError as exc:
        root.warning("Cannot open log file %s: %s", LOG_FILE, exc)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    return logging.getLogger("membership_bot")

log = _setup_logging()


# ==========================================================================
#  DATABASE LAYER — psycopg2 ThreadedConnectionPool
# ==========================================================================
_pool: Optional[ThreadedConnectionPool] = None


def get_pool() -> ThreadedConnectionPool:
    """Lazy-init a connection pool (2–10 connections)."""
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is not set!")
        _pool = ThreadedConnectionPool(2, 10, DATABASE_URL, cursor_factory=RealDictCursor)
        log.info("✅ Database connection pool initialised.")
    return _pool


@contextmanager
def db_cursor() -> Generator:
    """Borrow a connection from the pool; commit on success, rollback on error."""
    pool = get_pool()
    conn = pool.getconn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def _to_aware(dt: datetime) -> datetime:
    """Ensure a datetime is IST-aware (handles naive datetimes from DB)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def parse_db_row(row: Optional[Any]) -> Optional[dict]:
    """Convert RealDictRow → plain dict; parse ISO strings → aware datetimes."""
    if not row:
        return None
    out = dict(row)
    dt_cols = {"expiry", "joined", "updated", "removed", "ts"}
    for k, v in out.items():
        if v and k in dt_cols:
            try:
                out[k] = _to_aware(datetime.fromisoformat(str(v)))
            except (TypeError, ValueError):
                pass
        elif v and k == "extra":
            try:
                out[k] = json.loads(v)
            except (TypeError, ValueError, json.JSONDecodeError):
                out[k] = {}
    return out


def init_db() -> None:
    """Create tables + indexes. Safe to call on every startup."""
    with db_cursor() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS members (
                chat_id       BIGINT  NOT NULL,
                user_id       BIGINT  NOT NULL,
                expiry        TEXT,
                days          TEXT,
                added_by      BIGINT,
                active        INTEGER DEFAULT 1,
                joined        TEXT,
                updated       TEXT,
                remove_reason TEXT,
                removed       TEXT,
                last_reminded TEXT,       -- ISO timestamp of last reminder DM
                PRIMARY KEY (chat_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS groups (
                chat_id  BIGINT PRIMARY KEY,
                title    TEXT,
                added_by BIGINT,
                ts       TEXT
            );

            CREATE TABLE IF NOT EXISTS logs (
                id        SERIAL PRIMARY KEY,
                chat_id   BIGINT,
                actor_id  BIGINT,
                target_id BIGINT,
                action    TEXT,
                extra     TEXT,
                ts        TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_members_active_expiry
                ON members (active, expiry);
            CREATE INDEX IF NOT EXISTS idx_members_chat_active
                ON members (chat_id, active);
            CREATE INDEX IF NOT EXISTS idx_logs_chat_ts
                ON logs (chat_id, ts DESC);
        """)
    log.info("✅ PostgreSQL schema ready.")


# ==========================================================================
#  FLASK WEB SERVER  (keeps Render port happy)
# ==========================================================================
web_app = Flask(__name__)


@web_app.route("/")
def index():
    return "✅ Membership Bot is running 24/7!"


@web_app.route("/health")
def health():
    return {"status": "ok", "ts": now_ist().isoformat()}, 200


def _run_flask() -> None:
    web_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


# ==========================================================================
#  TIME & FORMAT HELPERS
# ==========================================================================
def now_ist() -> datetime:
    return datetime.now(IST)


def parse_duration(d_str: str) -> timedelta:
    """Parse strings like '30d', '12h', '45m', '7' (days) → timedelta."""
    s = str(d_str).strip().lower()
    try:
        if s.endswith("m"): return timedelta(minutes=int(s[:-1]))
        if s.endswith("h"): return timedelta(hours=int(s[:-1]))
        if s.endswith("d"): return timedelta(days=int(s[:-1]))
        return timedelta(days=int(s))
    except (ValueError, TypeError):
        return timedelta(0)


def time_left(expiry_dt: datetime) -> timedelta:
    """Return timedelta remaining (can be negative = already expired)."""
    return _to_aware(expiry_dt) - now_ist()


def format_time_left(expiry_dt: datetime) -> str:
    delta = time_left(expiry_dt)
    if delta.total_seconds() <= 0:
        return "Expired"
    if delta.days > 0:
        return f"{delta.days} days"
    hours, rem = divmod(int(delta.total_seconds()), 3600)
    mins = rem // 60
    if hours > 0:
        return f"{hours}h {mins}m"
    return f"{mins} mins"


def expiry_emoji(expiry_dt: datetime) -> str:
    """🔴 if ≤3 days left, 🟢 otherwise — uses timedelta, not string parsing."""
    delta = time_left(expiry_dt)
    return "🔴" if delta.total_seconds() <= 0 or delta.days < 3 else "🟢"


_MD_ESCAPE = re.compile(r"([_*`\[])")


def escape_md(text: Any) -> str:
    return _MD_ESCAPE.sub(r"\\\1", str(text or ""))


def fmt_expiry(dt: datetime) -> str:
    return _to_aware(dt).strftime("%d %b %Y, %I:%M %p")


# ==========================================================================
#  DOMAIN HELPERS
# ==========================================================================
async def is_admin(user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check Telegram directly — no stale local admin table."""
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        return member.status in (ChatMember.ADMINISTRATOR, ChatMember.OWNER)
    except TelegramError:
        return False


async def log_action(
    chat_id: int, actor_id: int, action: str,
    target_id: Optional[int] = None, extra: Optional[dict] = None,
) -> None:
    with db_cursor() as c:
        c.execute(
            "INSERT INTO logs (chat_id,actor_id,target_id,action,extra,ts) VALUES (%s,%s,%s,%s,%s,%s)",
            (chat_id, actor_id, target_id, action, json.dumps(extra or {}), now_ist().isoformat()),
        )


async def add_membership(chat_id: int, user_id: int, plan_str: str, added_by: int) -> datetime:
    expiry = now_ist() + parse_duration(plan_str)
    with db_cursor() as c:
        c.execute("SELECT joined FROM members WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
        row = parse_db_row(c.fetchone())
        joined_iso = (
            row["joined"].isoformat()
            if row and isinstance(row.get("joined"), datetime)
            else now_ist().isoformat()
        )
        c.execute(
            """
            INSERT INTO members
                (chat_id, user_id, expiry, days, added_by, active, joined, updated, last_reminded)
            VALUES (%s, %s, %s, %s, %s, 1, %s, %s, NULL)
            ON CONFLICT (chat_id, user_id) DO UPDATE SET
                expiry        = excluded.expiry,
                days          = excluded.days,
                added_by      = excluded.added_by,
                active        = 1,
                updated       = excluded.updated,
                last_reminded = NULL
            """,
            (chat_id, user_id, expiry.isoformat(), plan_str, added_by, joined_iso, now_ist().isoformat()),
        )
    await log_action(chat_id, added_by, "add", user_id, {"plan": plan_str})
    return expiry


async def extend_membership(chat_id: int, user_id: int, extra_plan: str, actor: int) -> Optional[datetime]:
    with db_cursor() as c:
        c.execute(
            "SELECT expiry FROM members WHERE chat_id=%s AND user_id=%s AND active=1",
            (chat_id, user_id),
        )
        row = parse_db_row(c.fetchone())
        if not row:
            return None
        base = max(_to_aware(row["expiry"]), now_ist())
        new_expiry = base + parse_duration(extra_plan)
        c.execute(
            "UPDATE members SET expiry=%s, updated=%s, last_reminded=NULL WHERE chat_id=%s AND user_id=%s",
            (new_expiry.isoformat(), now_ist().isoformat(), chat_id, user_id),
        )
    await log_action(chat_id, actor, "extend", user_id, {"extra_plan": extra_plan})
    return new_expiry


async def remove_membership(chat_id: int, user_id: int, actor: int, reason: str = "manual") -> None:
    with db_cursor() as c:
        c.execute(
            "UPDATE members SET active=0, removed=%s, remove_reason=%s WHERE chat_id=%s AND user_id=%s",
            (now_ist().isoformat(), reason, chat_id, user_id),
        )
    await log_action(chat_id, actor, "remove", user_id, {"reason": reason})


# ==========================================================================
#  TELEGRAM HELPERS
# ==========================================================================
async def kick_member(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Ban + immediately unban = soft kick (user can rejoin via invite link)."""
    for _ in range(2):
        try:
            await context.bot.ban_chat_member(chat_id, user_id)
            await asyncio.sleep(0.5)
            await context.bot.unban_chat_member(chat_id, user_id)
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except (Forbidden, BadRequest):
            return False
        except TelegramError:
            return False
    return False


async def send_pm(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> bool:
    try:
        await context.bot.send_message(
            user_id, text, parse_mode="Markdown", reply_markup=reply_markup
        )
        return True
    except TelegramError:
        return False


async def safe_edit(query: Any, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    """Edit callback message; if text is too long, truncate gracefully."""
    if len(text) > TG_MAX_CHARS:
        text = text[: TG_MAX_CHARS - 20] + "\n…_(truncated)_"
    try:
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=reply_markup)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            log.warning("edit_message_text failed: %s", e)


# ==========================================================================
#  KEYBOARD BUILDERS
# ==========================================================================
def plan_keyboard(chat_id: int, user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(label, callback_data=f"plan|{chat_id}|{user_id}|{plan}")]
        for plan, label in PRESET_PLANS.items()
    ]
    rows.append([InlineKeyboardButton("✏️ Custom Plan", callback_data=f"custom|{chat_id}|{user_id}")])
    return InlineKeyboardMarkup(rows)


def confirm_keyboard(chat_id: int, user_id: int, plan_str: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirm", callback_data=f"confirm|{chat_id}|{user_id}|{plan_str}"),
        InlineKeyboardButton("❌ Cancel",  callback_data="cancel"),
    ]])


def main_panel_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Members List",  callback_data=f"mlist|{chat_id}|0"),
         InlineKeyboardButton("📊 Stats",          callback_data=f"stats|{chat_id}")],
        [InlineKeyboardButton("⚠️ Expiring Soon", callback_data=f"expiring|{chat_id}"),
         InlineKeyboardButton("📋 Expiry Report", callback_data=f"report|{chat_id}")],
        [InlineKeyboardButton("🔴 Expired List",  callback_data=f"expired|{chat_id}"),
         InlineKeyboardButton("📜 Recent Logs",   callback_data=f"logs|{chat_id}")],
    ])


def member_action_keyboard(chat_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Extend", callback_data=f"extmenu|{chat_id}|{user_id}"),
         InlineKeyboardButton("🗑️ Remove", callback_data=f"remconfirm|{chat_id}|{user_id}")],
        [InlineKeyboardButton("🔙 Panel",  callback_data=f"panel|{chat_id}")],
    ])


def extend_menu_keyboard(chat_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ 1 Min",   callback_data=f"doext|{chat_id}|{user_id}|1m"),
         InlineKeyboardButton("➕ 7 Days",  callback_data=f"doext|{chat_id}|{user_id}|7d"),
         InlineKeyboardButton("➕ 30 Days", callback_data=f"doext|{chat_id}|{user_id}|30d")],
        [InlineKeyboardButton("➕ 60 Days", callback_data=f"doext|{chat_id}|{user_id}|60d"),
         InlineKeyboardButton("➕ 90 Days", callback_data=f"doext|{chat_id}|{user_id}|90d")],
        [InlineKeyboardButton("🔙 Back",    callback_data=f"memberinfo|{chat_id}|{user_id}")],
    ])


def back_to_panel_kb(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Panel", callback_data=f"panel|{chat_id}")]])


# ==========================================================================
#  CHAT-MEMBER EVENT HANDLERS
# ==========================================================================
async def track_bot_promotion(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fires when the bot itself gets promoted/demoted."""
    result = update.my_chat_member
    if not result:
        return

    chat, user = result.chat, result.from_user
    admin_states = (ChatMember.ADMINISTRATOR, ChatMember.OWNER)

    # Only react when bot transitions INTO an admin state
    was_admin = result.old_chat_member.status in admin_states
    now_admin = result.new_chat_member.status in admin_states
    if was_admin or not now_admin:
        return

    with db_cursor() as c:
        c.execute(
            """
            INSERT INTO groups (chat_id, title, added_by, ts)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chat_id) DO UPDATE SET
                title    = excluded.title,
                added_by = excluded.added_by
            """,
            (chat.id, chat.title, user.id, now_ist().isoformat()),
        )
    await send_pm(
        context, user.id,
        f"✅ *{escape_md(chat.title)}* registered!\n\n"
        "Jab bhi koi naya member join karega, main aapko PM mein plan selection menu bhejunga. 🎉",
    )


async def track_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fires when ANY member's status changes in a tracked group."""
    result = update.chat_member
    if not result:
        return

    chat = result.chat
    user = result.new_chat_member.user
    if user.is_bot:
        return

    old_status = result.old_chat_member.status
    new_status = result.new_chat_member.status

    # ✅ FIX: User is joining when old=left/kicked AND new=member/restricted
    joining_from = (ChatMember.LEFT, ChatMember.BANNED)
    joining_to   = (ChatMember.MEMBER, ChatMember.RESTRICTED)

    if old_status not in joining_from or new_status not in joining_to:
        return   # Not a fresh join — ignore

    with db_cursor() as c:
        c.execute("SELECT added_by FROM groups WHERE chat_id=%s", (chat.id,))
        grp = parse_db_row(c.fetchone())
    if not grp:
        return

    text = (
        f"🆕 *New Member Joined!*\n\n"
        f"📌 Group: *{escape_md(chat.title)}*\n"
        f"👤 Member: {escape_md(user.full_name)}\n"
        f"🆔 ID: `{user.id}`\n\n"
        "👇 Neeche se plan select karein:"
    )
    await send_pm(context, grp["added_by"], text, reply_markup=plan_keyboard(chat.id, user.id))


# ==========================================================================
#  COMMAND HANDLERS
# ==========================================================================
async def _delete_and_pm(update: Update, context: ContextTypes.DEFAULT_TYPE, msg: str) -> None:
    """Delete group message and redirect user to PM."""
    try:
        await update.message.delete()
    except TelegramError:
        pass
    await send_pm(context, update.effective_user.id, msg)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type == "private":
        await update.message.reply_text(
            "👋 *Welcome to Membership Manager Bot!*\n\n"
            "Mujhe kisi bhi Group/Channel mein *Admin* promote karein — main automatically register ho jaunga.\n\n"
            "📖 `/help` — commands list\n"
            "⚙️ `/panel` — admin control panel",
            parse_mode="Markdown",
        )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📖 *Commands (PM mein use karein):*\n\n"
        "`/panel` — Admin Control Panel (members manage karein)\n"
        "`/start` — Bot restart/welcome\n"
        "`/help`  — Yeh message",
        parse_mode="Markdown",
    )


async def cmd_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        try:
            await update.message.delete()
        except TelegramError:
            pass


async def cmd_addmember(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return await _delete_and_pm(update, context, "❌ `/addmember` PM mein use karein.")
    await update.message.reply_text(
        "💡 Jab user group join karega, main auto-menu bhejunga.\n`/panel` se bhi manage kar sakte hain.",
        parse_mode="Markdown",
    )


async def cmd_removemember(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return await _delete_and_pm(update, context, "❌ Commands sirf PM mein use karein.")
    await update.message.reply_text("💡 `/panel` se users manage karein.", parse_mode="Markdown")


async def cmd_extend(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return await _delete_and_pm(update, context, "❌ Commands sirf PM mein use karein.")
    await update.message.reply_text("💡 `/panel` se users manage karein.", parse_mode="Markdown")


async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return await _delete_and_pm(update, context, "⚙️ Panel PM mein khulta hai — yahan open karein.")

    with db_cursor() as c:
        c.execute("SELECT chat_id, title FROM groups WHERE added_by=%s", (update.effective_user.id,))
        groups = c.fetchall()

    if not groups:
        return await update.message.reply_text(
            "❌ Koi registered group nahi mila.\nPehle mujhe kisi group mein Admin promote karein."
        )

    for grp in groups:
        await update.message.reply_text(
            f"⚙️ *Admin Control Panel*\n📌 {escape_md(grp['title'])} (`{grp['chat_id']}`)",
            parse_mode="Markdown",
            reply_markup=main_panel_keyboard(grp["chat_id"]),
        )


# ==========================================================================
#  CALLBACK QUERY HANDLERS
# ==========================================================================
async def cb_plan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, chat_id, user_id, plan_str = query.data.split("|")
    expiry = now_ist() + parse_duration(plan_str)
    await safe_edit(
        query,
        f"✅ *Confirm kijiye:*\n\n"
        f"👤 User ID: `{user_id}`\n"
        f"📦 Plan: *{plan_str.upper()}*\n"
        f"📅 Expiry: *{fmt_expiry(expiry)} IST*",
        confirm_keyboard(int(chat_id), int(user_id), plan_str),
    )


async def cb_custom(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, chat_id, user_id = query.data.split("|")
    await safe_edit(
        query,
        f"✏️ *Custom Plan:*\n\n"
        f"User `{user_id}` ke liye `/addmember {user_id} <duration>` type karein\n"
        f"_Example: 45d, 12h, 90m_",
    )


async def cb_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("⏳ Processing…")
    _, chat_id, user_id, plan_str = query.data.split("|")
    chat_id, user_id = int(chat_id), int(user_id)

    expiry = await add_membership(chat_id, user_id, plan_str, query.from_user.id)

    target_name = str(user_id)
    try:
        target = await context.bot.get_chat(user_id)
        target_name = target.full_name
    except TelegramError:
        pass

    pm_sent = False
    try:
        chat = await context.bot.get_chat(chat_id)
        pm_sent = await send_pm(
            context, user_id,
            f"🎉 *Welcome!*\n\n"
            f"✅ *{escape_md(chat.title)}* mein membership activate ho gayi!\n"
            f"📦 Plan: {plan_str.upper()}\n"
            f"📅 Expiry: {fmt_expiry(expiry)} IST",
        )
    except TelegramError:
        pass

    await safe_edit(
        query,
        f"✅ *Membership Activated!*\n\n"
        f"👤 Member: {escape_md(target_name)}\n"
        f"📦 Plan: *{plan_str.upper()}*\n"
        f"📅 Expiry: *{fmt_expiry(expiry)} IST*\n\n"
        + ("📩 PM bhej diya." if pm_sent else "⚠️ User ka PM disabled hai."),
    )
    await query.answer("✅ Done!", show_alert=True)


async def cb_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.answer()
    await safe_edit(update.callback_query, "❌ Action cancel ho gayi.")


async def cb_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id = int(query.data.split("|")[1])
    title = str(chat_id)
    try:
        title = (await context.bot.get_chat(chat_id)).title
    except TelegramError:
        pass
    await safe_edit(
        query,
        f"⚙️ *Admin Control Panel*\n📌 {escape_md(title)}",
        main_panel_keyboard(chat_id),
    )


async def cb_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id = int(query.data.split("|")[1])
    soon = (now_ist() + timedelta(days=3)).isoformat()

    with db_cursor() as c:
        c.execute("SELECT COUNT(*) AS cnt FROM members WHERE chat_id=%s", (chat_id,))
        total = c.fetchone()["cnt"]
        c.execute("SELECT COUNT(*) AS cnt FROM members WHERE chat_id=%s AND active=1", (chat_id,))
        active = c.fetchone()["cnt"]
        c.execute(
            "SELECT COUNT(*) AS cnt FROM members WHERE chat_id=%s AND active=1 AND expiry<=%s",
            (chat_id, soon),
        )
        expiring = c.fetchone()["cnt"]

    await safe_edit(
        query,
        f"📊 *Group Statistics*\n\n"
        f"👥 Total Members : *{total}*\n"
        f"🟢 Active         : *{active}*\n"
        f"🔴 Expired/Removed: *{total - active}*\n"
        f"⚠️ Expiring (3d)  : *{expiring}*",
        main_panel_keyboard(chat_id),
    )


async def cb_expiring(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id = int(query.data.split("|")[1])
    soon = (now_ist() + timedelta(days=3)).isoformat()

    with db_cursor() as c:
        c.execute(
            "SELECT * FROM members WHERE chat_id=%s AND active=1 AND expiry<=%s ORDER BY expiry ASC LIMIT 50",
            (chat_id, soon),
        )
        docs = [parse_db_row(r) for r in c.fetchall()]

    if not docs:
        text = "✅ Abhi koi member expiring nahi hai."
    else:
        lines = ["⚠️ *Expiring Soon (≤3 days):*\n"]
        for d in docs:
            lines.append(f"• `{d['user_id']}` — {format_time_left(d['expiry'])} left")
        text = "\n".join(lines)

    await safe_edit(query, text, back_to_panel_kb(chat_id))


async def cb_expired(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id = int(query.data.split("|")[1])

    with db_cursor() as c:
        c.execute(
            "SELECT * FROM members WHERE chat_id=%s AND active=0 ORDER BY removed DESC LIMIT 20",
            (chat_id,),
        )
        docs = [parse_db_row(r) for r in c.fetchall()]

    if not docs:
        text = "✅ Koi expired member nahi hai."
    else:
        lines = ["🔴 *Expired Members (Last 20):*\n"]
        for d in docs:
            reason = (d.get("remove_reason") or "auto").replace("_", " ").title()
            lines.append(f"• `{d['user_id']}` — {reason}")
        text = "\n".join(lines)

    await safe_edit(query, text, back_to_panel_kb(chat_id))


async def cb_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id = int(query.data.split("|")[1])

    with db_cursor() as c:
        c.execute(
            "SELECT * FROM logs WHERE chat_id=%s ORDER BY ts DESC LIMIT 10",
            (chat_id,),
        )
        docs = [parse_db_row(r) for r in c.fetchall()]

    if not docs:
        text = "📭 Koi logs nahi mili."
    else:
        lines = ["📋 *Recent Logs (Last 10):*\n"]
        for d in docs:
            ts_str = d["ts"].strftime("%d/%m %H:%M") if isinstance(d["ts"], datetime) else str(d["ts"])
            lines.append(f"`{ts_str}` | {d['action'].upper()} | `{d.get('target_id', '-')}`")
        text = "\n".join(lines)

    await safe_edit(query, text, back_to_panel_kb(chat_id))


async def cb_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id = int(query.data.split("|")[1])
    today = (now_ist() + timedelta(days=1)).isoformat()
    week  = (now_ist() + timedelta(days=7)).isoformat()

    with db_cursor() as c:
        c.execute(
            "SELECT COUNT(*) AS cnt FROM members WHERE chat_id=%s AND active=1 AND expiry<=%s",
            (chat_id, today),
        )
        t_cnt = c.fetchone()["cnt"]
        c.execute(
            "SELECT COUNT(*) AS cnt FROM members WHERE chat_id=%s AND active=1 AND expiry<=%s",
            (chat_id, week),
        )
        w_cnt = c.fetchone()["cnt"]

    await safe_edit(
        query,
        f"📋 *Expiry Report*\n\n"
        f"⚠️ Aaj expire ho rahe : *{t_cnt}*\n"
        f"📅 Is hafte expire    : *{w_cnt}*",
        main_panel_keyboard(chat_id),
    )


async def cb_mlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, chat_id_s, page_s = query.data.split("|")
    chat_id, page = int(chat_id_s), int(page_s)

    with db_cursor() as c:
        c.execute(
            "SELECT * FROM members WHERE chat_id=%s AND active=1 ORDER BY expiry ASC",
            (chat_id,),
        )
        docs = [parse_db_row(r) for r in c.fetchall()]

    total = len(docs)
    chunk = docs[page * PER_PAGE : (page + 1) * PER_PAGE]

    lines = [f"👥 *Active Members — Page {page + 1} | Total: {total}*\n"]
    for i, d in enumerate(chunk, page * PER_PAGE + 1):
        em  = expiry_emoji(d["expiry"])       # ✅ timedelta-based, not string-parse
        rem = format_time_left(d["expiry"])
        lines.append(f"{i}. `{d['user_id']}` {em} {rem} — {_to_aware(d['expiry']).strftime('%d %b')}")

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"mlist|{chat_id}|{page - 1}"))
    if (page + 1) * PER_PAGE < total:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"mlist|{chat_id}|{page + 1}"))

    kb_rows = []
    if nav:
        kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton("🔙 Back to Panel", callback_data=f"panel|{chat_id}")])

    await safe_edit(query, "\n".join(lines), InlineKeyboardMarkup(kb_rows))


async def cb_memberinfo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, chat_id_s, user_id_s = query.data.split("|")
    chat_id, user_id = int(chat_id_s), int(user_id_s)

    with db_cursor() as c:
        c.execute(
            "SELECT * FROM members WHERE chat_id=%s AND user_id=%s",
            (chat_id, user_id),
        )
        doc = parse_db_row(c.fetchone())

    if not doc:
        await safe_edit(query, "❌ Record nahi mila.")
        return

    em  = expiry_emoji(doc["expiry"])
    rem = format_time_left(doc["expiry"])
    status = "🟢 Active" if doc["active"] else "🔴 Expired"

    await safe_edit(
        query,
        f"👤 *Member Info*\n\n"
        f"🆔 ID     : `{user_id}`\n"
        f"Status    : {status}\n"
        f"📅 Expiry : *{fmt_expiry(doc['expiry'])} IST*\n"
        f"⏳ Baaki  : {em} *{rem}*",
        member_action_keyboard(chat_id, user_id),
    )


async def cb_extmenu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, chat_id, user_id = query.data.split("|")
    await safe_edit(
        query,
        f"➕ *Extend karein:* `{user_id}`",
        extend_menu_keyboard(int(chat_id), int(user_id)),
    )


async def cb_doext(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("⏳ Processing…")
    _, chat_id_s, user_id_s, plan_str = query.data.split("|")
    chat_id, user_id = int(chat_id_s), int(user_id_s)

    new_expiry = await extend_membership(chat_id, user_id, plan_str, query.from_user.id)
    if not new_expiry:
        await query.answer("❌ Member active nahi hai!", show_alert=True)
        return

    try:
        chat = await context.bot.get_chat(chat_id)
        await send_pm(
            context, user_id,
            f"🎉 *Extended!*\n"
            f"📌 Group: {escape_md(chat.title)}\n"
            f"➕ Added : {plan_str.upper()}\n"
            f"📅 New Expiry: {fmt_expiry(new_expiry)} IST",
        )
    except TelegramError:
        pass

    await safe_edit(
        query,
        f"✅ `{user_id}` ko *{plan_str.upper()}* extend kar diya!\n"
        f"📅 New Expiry: *{fmt_expiry(new_expiry)} IST*",
    )
    await query.answer("✅ Extended!", show_alert=True)


async def cb_remconfirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, chat_id, user_id = query.data.split("|")
    await safe_edit(
        query,
        f"⚠️ *Confirm:* `{user_id}` ko remove karein?",
        InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Haan", callback_data=f"doremove|{chat_id}|{user_id}"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel"),
        ]]),
    )


async def cb_doremove(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("⏳ Processing…")
    _, chat_id_s, user_id_s = query.data.split("|")
    chat_id, user_id = int(chat_id_s), int(user_id_s)

    await remove_membership(chat_id, user_id, query.from_user.id, "manual_remove")
    kicked = await kick_member(chat_id, user_id, context)

    await safe_edit(
        query,
        f"{'✅ Kick kar diya' if kicked else '⚠️ Sirf DB update (Kick fail)'}: `{user_id}`",
    )
    await query.answer("✅ Removed!", show_alert=True)


# ==========================================================================
#  BACKGROUND JOBS
# ==========================================================================
async def auto_kick_expired(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Single query fetches ALL expired members — no N+1 issue."""
    with db_cursor() as c:
        c.execute(
            "SELECT * FROM members WHERE active=1 AND expiry<=%s",
            (now_ist().isoformat(),),
        )
        expired = [parse_db_row(r) for r in c.fetchall()]

    if expired:
        log.info("Auto-kick sweep: %d expired members found.", len(expired))

    for d in expired:
        await remove_membership(d["chat_id"], d["user_id"], 0, "auto_expired")
        await kick_member(d["chat_id"], d["user_id"], context)
        try:
            chat = await context.bot.get_chat(d["chat_id"])
            await send_pm(
                context, d["user_id"],
                f"⏰ *Membership Expire Ho Gayi!*\n"
                f"📌 Group: {escape_md(chat.title)}\n"
                "🔄 Renew karne ke liye admin se contact karein.",
            )
        except TelegramError:
            pass


async def send_reminders(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Remind members whose expiry is ~72h or ~24h away.
    Uses `last_reminded` column to prevent duplicate pings on the same window.
    """
    for hrs in REMINDER_HOURS:
        window_start = (now_ist() + timedelta(hours=hrs - 1)).isoformat()
        window_end   = (now_ist() + timedelta(hours=hrs + 1)).isoformat()

        with db_cursor() as c:
            # ✅ Only fetch rows where last_reminded is NULL OR was sent >hrs-2 hours ago
            c.execute(
                """
                SELECT * FROM members
                WHERE active=1
                  AND expiry >= %s AND expiry <= %s
                  AND (
                    last_reminded IS NULL
                    OR last_reminded < %s
                  )
                """,
                (window_start, window_end, (now_ist() - timedelta(hours=hrs - 2)).isoformat()),
            )
            docs = [parse_db_row(r) for r in c.fetchall()]

        for d in docs:
            try:
                chat = await context.bot.get_chat(d["chat_id"])
                sent = await send_pm(
                    context, d["user_id"],
                    f"⚠️ *Membership Reminder*\n"
                    f"📌 Group: {escape_md(chat.title)}\n"
                    f"⏳ Sirf *{format_time_left(d['expiry'])}* baaki!\n"
                    "🔄 Expiry se pehle renew karein.",
                )
                if sent:
                    # Mark reminded so we don't ping again in next sweep
                    with db_cursor() as c2:
                        c2.execute(
                            "UPDATE members SET last_reminded=%s WHERE chat_id=%s AND user_id=%s",
                            (now_ist().isoformat(), d["chat_id"], d["user_id"]),
                        )
            except TelegramError:
                pass


async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Unhandled exception", exc_info=context.error)


# ==========================================================================
#  ENTRY POINT
# ==========================================================================
def main() -> None:
    if not BOT_TOKEN or not DATABASE_URL:
        log.critical("❌ BOT_TOKEN aur DATABASE_URL set karein environment variables mein.")
        sys.exit(1)

    init_db()

    # ✅ Flask start PEHLE — Render ko port binding chahiye before polling begins
    flask_thread = threading.Thread(target=_run_flask, daemon=True, name="flask-server")
    flask_thread.start()
    time.sleep(1)   # give Flask a moment to bind the port

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("setup",        cmd_setup))
    app.add_handler(CommandHandler("addmember",    cmd_addmember))
    app.add_handler(CommandHandler("removemember", cmd_removemember))
    app.add_handler(CommandHandler("extend",       cmd_extend))
    app.add_handler(CommandHandler("panel",        cmd_panel))

    # Chat member events
    app.add_handler(ChatMemberHandler(track_bot_promotion, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(ChatMemberHandler(track_chat_member,   ChatMemberHandler.CHAT_MEMBER))

    # Callback queries — (handler, pattern) pairs
    _cb_routes = [
        (cb_plan,       r"^plan\|"),
        (cb_custom,     r"^custom\|"),
        (cb_confirm,    r"^confirm\|"),
        (cb_cancel,     r"^cancel$"),
        (cb_mlist,      r"^mlist\|"),
        (cb_stats,      r"^stats\|"),
        (cb_expiring,   r"^expiring\|"),
        (cb_expired,    r"^expired\|"),
        (cb_logs,       r"^logs\|"),
        (cb_report,     r"^report\|"),
        (cb_panel,      r"^panel\|"),
        (cb_memberinfo, r"^memberinfo\|"),
        (cb_extmenu,    r"^extmenu\|"),
        (cb_doext,      r"^doext\|"),
        (cb_remconfirm, r"^remconfirm\|"),
        (cb_doremove,   r"^doremove\|"),
    ]
    for handler, pattern in _cb_routes:
        app.add_handler(CallbackQueryHandler(handler, pattern=pattern))

    app.add_error_handler(global_error_handler)

    # Background jobs
    app.job_queue.run_repeating(auto_kick_expired, interval=AUTO_KICK_INTERVAL, first=10)
    app.job_queue.run_repeating(send_reminders,    interval=REMINDER_INTERVAL,  first=30)

    log.info("🤖 Membership Manager Bot v12.0 starting (Supabase + Flask)…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
