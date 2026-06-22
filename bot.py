"""
╔══════════════════════════════════════════════════════════════════════╗
║   Telegram Membership Manager Bot — Professional Edition  v11.0      ║
║   Library : python-telegram-bot (v20+)                               ║
║   Storage : Supabase (PostgreSQL)                                    ║
║   Server  : Flask (Render.com Web Service Compatibility)             ║
║   Schedule: Built-in JobQueue (1-min auto-kick sweep)                ║
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
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Iterable, Optional

# PostgreSQL & Flask Imports
import psycopg2
from psycopg2.extras import RealDictCursor
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
BOT_TOKEN: str    = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL: str = os.getenv("DATABASE_URL", "").strip()
PORT: int         = int(os.getenv("PORT", "8080"))
LOG_FILE: str     = os.getenv("LOG_FILE", "membership_bot.log")

# Timezone — India Standard Time (UTC + 5:30)
IST = timezone(timedelta(hours=5, minutes=30))

# Background-job intervals (seconds)
AUTO_KICK_INTERVAL: int = 60        # 1 minute
REMINDER_INTERVAL:  int = 3600      # 1 hour

# Reminder windows (hours before expiry to ping the user)
REMINDER_HOURS: tuple[int, ...] = (72, 24)

# Preset plans surfaced as inline buttons
PRESET_PLANS: dict[str, str] = {
    "1m":   "🧪 1 Minute (Test)",
    "7d":   "🥉 7 Days",
    "30d":  "🥇 30 Days",
    "90d":  "💎 90 Days",
    "180d": "🔥 180 Days",
    "365d": "👑 365 Days",
}


# ==========================================================================
#  LOGGING SETUP
# ==========================================================================
def _setup_logging() -> logging.Logger:
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)

    try:
        file_h = RotatingFileHandler(
            LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        file_h.setFormatter(fmt)
        root.addHandler(file_h)
    except OSError as exc:
        root.warning("Cannot create log file %s: %s", LOG_FILE, exc)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)

    return logging.getLogger("membership_bot")

log = _setup_logging()


# ==========================================================================
#  FLASK WEB SERVER (For Render Port Binding)
# ==========================================================================
web_app = Flask(__name__)

@web_app.route('/')
def index():
    return "✅ Telegram Membership Bot is running 24/7!"

def run_web_server():
    """Runs the Flask server in a separate thread."""
    web_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


# ==========================================================================
#  DATABASE LAYER (Supabase / PostgreSQL)
# ==========================================================================
@contextmanager
def db_cursor():
    """Context manager for PostgreSQL database connections."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set!")
        
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error("Database Error: %s", e)
        raise
    finally:
        cur.close()
        conn.close()

def parse_db_row(row: Optional[dict]) -> Optional[dict]:
    """Convert a RealDictRow into a standard dictionary and parse strings to objects."""
    if not row:
        return None
    out = dict(row)
    datetime_cols = {"expiry", "joined", "updated", "removed", "ts"}
    for k, v in out.items():
        if v and k in datetime_cols:
            try:
                out[k] = datetime.fromisoformat(str(v))
            except (TypeError, ValueError):
                pass
        elif v and k == "extra":
            try:
                out[k] = json.loads(v)
            except (TypeError, ValueError, json.JSONDecodeError):
                out[k] = {}
    return out

def init_db() -> None:
    """Create tables + indexes for PostgreSQL."""
    with db_cursor() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS members (
                chat_id       BIGINT NOT NULL,
                user_id       BIGINT NOT NULL,
                expiry        TEXT,
                days          TEXT,
                added_by      BIGINT,
                active        INTEGER DEFAULT 1,
                joined        TEXT,
                updated       TEXT,
                remove_reason TEXT,
                removed       TEXT,
                PRIMARY KEY (chat_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS admins (
                chat_id  BIGINT NOT NULL,
                user_id  BIGINT NOT NULL,
                added_by BIGINT,
                ts       TEXT,
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
            CREATE INDEX IF NOT EXISTS idx_groups_added_by
                ON groups (added_by);
            """
        )
    log.info("✅ PostgreSQL Database schema ready.")


# ==========================================================================
#  TIME & FORMATTING HELPERS
# ==========================================================================
def now_ist() -> datetime:
    return datetime.now(IST)

def parse_duration(d_str: str) -> timedelta:
    s = str(d_str).strip().lower()
    try:
        if s.endswith("m"): return timedelta(minutes=int(s[:-1]))
        if s.endswith("h"): return timedelta(hours=int(s[:-1]))
        if s.endswith("d"): return timedelta(days=int(s[:-1]))
        return timedelta(days=int(s))
    except (ValueError, TypeError):
        return timedelta(0)

def format_time_left(expiry_dt: datetime) -> str:
    delta = expiry_dt - now_ist()
    if delta.total_seconds() <= 0: return "Expired"
    if delta.days > 0: return f"{delta.days} days"
    hours, rem = divmod(delta.seconds, 3600)
    minutes, _ = divmod(rem, 60)
    if hours > 0: return f"{hours}h {minutes}m"
    return f"{minutes} mins"

_MD_ESCAPE_RE = re.compile(r"([_*`\[])")

def escape_md(text: Any) -> str:
    return _MD_ESCAPE_RE.sub(r"\\\1", str(text or ""))

def fmt_expiry(dt: datetime) -> str:
    return dt.strftime("%d %b %Y, %I:%M %p")


# ==========================================================================
#  CORE DOMAIN HELPERS
# ==========================================================================
async def is_admin(user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    with db_cursor() as c:
        c.execute("SELECT 1 FROM admins WHERE user_id=%s AND chat_id=%s", (user_id, chat_id))
        if c.fetchone(): return True
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        return member.status in (ChatMember.ADMINISTRATOR, ChatMember.OWNER)
    except TelegramError:
        return False

async def log_action(
    chat_id: int, actor_id: int, action: str, target_id: Optional[int] = None, extra: Optional[dict] = None
) -> None:
    with db_cursor() as c:
        c.execute(
            """
            INSERT INTO logs (chat_id, actor_id, target_id, action, extra, ts)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (chat_id, actor_id, target_id, action, json.dumps(extra or {}), now_ist().isoformat())
        )

async def add_membership(chat_id: int, user_id: int, plan_str: str, added_by: int) -> datetime:
    expiry = now_ist() + parse_duration(plan_str)
    with db_cursor() as c:
        c.execute("SELECT joined FROM members WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
        row = parse_db_row(c.fetchone())
        joined_iso = (
            row["joined"].isoformat() if row and isinstance(row.get("joined"), datetime)
            else now_ist().isoformat()
        )

        c.execute(
            """
            INSERT INTO members
                (chat_id, user_id, expiry, days, added_by, active, joined, updated)
            VALUES (%s, %s, %s, %s, %s, 1, %s, %s)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET
                expiry   = excluded.expiry,
                days     = excluded.days,
                added_by = excluded.added_by,
                active   = 1,
                updated  = excluded.updated
            """,
            (chat_id, user_id, expiry.isoformat(), plan_str, added_by, joined_iso, now_ist().isoformat())
        )

    await log_action(chat_id, added_by, "add", user_id, {"plan": plan_str})
    return expiry

async def extend_membership(chat_id: int, user_id: int, extra_plan: str, actor: int) -> Optional[datetime]:
    with db_cursor() as c:
        c.execute("SELECT expiry FROM members WHERE chat_id=%s AND user_id=%s AND active=1", (chat_id, user_id))
        row = parse_db_row(c.fetchone())
        if not row: return None

        base = max(row["expiry"], now_ist())
        new_expiry = base + parse_duration(extra_plan)

        c.execute(
            "UPDATE members SET expiry=%s, updated=%s WHERE chat_id=%s AND user_id=%s",
            (new_expiry.isoformat(), now_ist().isoformat(), chat_id, user_id)
        )

    await log_action(chat_id, actor, "extend", user_id, {"extra_plan": extra_plan})
    return new_expiry

async def remove_membership(chat_id: int, user_id: int, actor: int, reason: str = "manual") -> None:
    with db_cursor() as c:
        c.execute(
            "UPDATE members SET active=0, removed=%s, remove_reason=%s WHERE chat_id=%s AND user_id=%s",
            (now_ist().isoformat(), reason, chat_id, user_id)
        )
    await log_action(chat_id, actor, "remove", user_id, {"reason": reason})


# ==========================================================================
#  TELEGRAM I/O HELPERS
# ==========================================================================
async def kick_member(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    for attempt in (1, 2):
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

async def send_pm(context: ContextTypes.DEFAULT_TYPE, user_id: int, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> bool:
    try:
        await context.bot.send_message(user_id, text, parse_mode="Markdown", reply_markup=reply_markup)
        return True
    except TelegramError:
        return False


# ==========================================================================
#  KEYBOARD BUILDERS
# ==========================================================================
def plan_keyboard(chat_id: int, user_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(label, callback_data=f"plan|{chat_id}|{user_id}|{plan}")] for plan, label in PRESET_PLANS.items()]
    rows.append([InlineKeyboardButton("✏️ Custom Plan", callback_data=f"custom|{chat_id}|{user_id}")])
    return InlineKeyboardMarkup(rows)

def confirm_keyboard(chat_id: int, user_id: int, plan_str: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirm", callback_data=f"confirm|{chat_id}|{user_id}|{plan_str}"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel"),
    ]])

def main_panel_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Members List", callback_data=f"mlist|{chat_id}|0"), InlineKeyboardButton("📊 Stats", callback_data=f"stats|{chat_id}")],
        [InlineKeyboardButton("⚠️ Expiring Soon", callback_data=f"expiring|{chat_id}"), InlineKeyboardButton("📋 Expiry Report", callback_data=f"report|{chat_id}")],
        [InlineKeyboardButton("🔴 Expired List", callback_data=f"expired|{chat_id}"), InlineKeyboardButton("📜 Recent Logs", callback_data=f"logs|{chat_id}")],
    ])

def member_action_keyboard(chat_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Extend", callback_data=f"extmenu|{chat_id}|{user_id}"), InlineKeyboardButton("🗑️ Remove", callback_data=f"remconfirm|{chat_id}|{user_id}")],
        [InlineKeyboardButton("🔙 Panel", callback_data=f"panel|{chat_id}")],
    ])

def extend_menu_keyboard(chat_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ 1 Min", callback_data=f"doext|{chat_id}|{user_id}|1m"), InlineKeyboardButton("➕ 7 Days", callback_data=f"doext|{chat_id}|{user_id}|7d"), InlineKeyboardButton("➕ 30 Days", callback_data=f"doext|{chat_id}|{user_id}|30d")],
        [InlineKeyboardButton("➕ 60 Days", callback_data=f"doext|{chat_id}|{user_id}|60d"), InlineKeyboardButton("➕ 90 Days", callback_data=f"doext|{chat_id}|{user_id}|90d")],
        [InlineKeyboardButton("🔙 Back", callback_data=f"memberinfo|{chat_id}|{user_id}")],
    ])

def back_to_panel_kb(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Panel", callback_data=f"panel|{chat_id}")]])


# ==========================================================================
#  CHAT-MEMBER EVENT HANDLERS
# ==========================================================================
async def track_bot_promotion(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    result = update.my_chat_member
    if not result: return

    chat, user = result.chat, result.from_user
    admin_states = (ChatMember.ADMINISTRATOR, ChatMember.OWNER)
    
    if result.old_chat_member.status in admin_states or not result.new_chat_member.status in admin_states:
        return

    with db_cursor() as c:
        c.execute(
            """
            INSERT INTO groups (chat_id, title, added_by, ts)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT(chat_id) DO UPDATE SET
                title    = excluded.title,
                added_by = excluded.added_by
            """,
            (chat.id, chat.title, user.id, now_ist().isoformat())
        )

    await send_pm(context, user.id, f"✅ I have successfully registered *{escape_md(chat.title)}*!\n\nWhenever a new member joins, I will send you the plan-selection menu here in PM.")

async def track_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    result = update.chat_member
    if not result: return

    chat, user = result.chat, result.new_chat_member.user
    if user.is_bot: return

    member_states = (ChatMember.MEMBER, ChatMember.OWNER, ChatMember.ADMINISTRATOR, ChatMember.RESTRICTED)
    joining_states = (ChatMember.MEMBER, ChatMember.RESTRICTED)

    if result.old_chat_member.status in member_states or not result.new_chat_member.status in joining_states:
        return

    with db_cursor() as c:
        c.execute("SELECT added_by FROM groups WHERE chat_id=%s", (chat.id,))
        grp = parse_db_row(c.fetchone())
        
    if not grp: return

    text = f"🆕 *New Member Joined!*\n\n📌 Chat: *{escape_md(chat.title)}*\n👤 Member: {escape_md(user.full_name)}\n🆔 ID: `{user.id}`\n\nPlease select an expiry plan for this user below:"
    await send_pm(context, grp["added_by"], text, reply_markup=plan_keyboard(chat.id, user.id))


# ==========================================================================
#  COMMAND HANDLERS
# ==========================================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type == "private":
        await update.message.reply_text("👋 *Welcome to the Membership Manager Bot!*\n\nPromote me to *Admin* in any Group/Channel. I will automatically register it.\nType /help for commands.", parse_mode="Markdown")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("📖 *Command List (Use in Private Message):*\n\n`/panel` – Access Admin Control Panel\n_Add, remove, and manage members easily via the interactive buttons._", parse_mode="Markdown")

async def _redirect_to_pm(update: Update, context: ContextTypes.DEFAULT_TYPE, msg: str) -> None:
    try: await update.message.delete()
    except TelegramError: pass
    await send_pm(context, update.effective_user.id, msg)

async def cmd_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        try: await update.message.delete()
        except TelegramError: pass

async def cmd_addmember(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return await _redirect_to_pm(update, context, "❌ Please use `/addmember` here in PM.")
    await update.message.reply_text("💡 Note: When a user joins the group, I will auto-send you a menu.", parse_mode="Markdown")

async def cmd_removemember(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return await _redirect_to_pm(update, context, "❌ Use commands strictly in my PM.")
    await update.message.reply_text("💡 Manage users via `/panel`.", parse_mode="Markdown")

async def cmd_extend(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return await _redirect_to_pm(update, context, "❌ Use commands strictly in my PM.")
    await update.message.reply_text("💡 Manage users via `/panel`.", parse_mode="Markdown")

async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return await _redirect_to_pm(update, context, "⚙️ Please open the Admin panel here.")

    with db_cursor() as c:
        c.execute("SELECT chat_id, title FROM groups WHERE added_by=%s", (update.effective_user.id,))
        groups = c.fetchall()

    if not groups:
        return await update.message.reply_text("❌ No registered groups found. Promote me to Admin first.")

    for grp in groups:
        await update.message.reply_text(f"⚙️ *Admin Control Panel*\n📌 {escape_md(grp['title'])} (`{grp['chat_id']}`)", parse_mode="Markdown", reply_markup=main_panel_keyboard(grp["chat_id"]))


# ==========================================================================
#  CALLBACK QUERY HANDLERS
# ==========================================================================
async def cb_plan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    _, chat_id, user_id, plan_str = query.data.split("|")
    expiry = now_ist() + parse_duration(plan_str)
    await query.edit_message_text(f"✅ *Please Confirm:*\n\n👤 User ID: `{user_id}`\n📦 Plan: *{plan_str.upper()}*\n📅 Expiry: *{fmt_expiry(expiry)} IST*", parse_mode="Markdown", reply_markup=confirm_keyboard(int(chat_id), int(user_id), plan_str))
    await query.answer()

async def cb_custom(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    _, _, user_id = query.data.split("|")
    await query.edit_message_text(f"✏️ *Custom Plan:*\nUse `/addmember {user_id} <duration>` (e.g. 45d, 12h) in PM.", parse_mode="Markdown")
    await query.answer()

async def cb_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    _, chat_id, user_id, plan_str = query.data.split("|")
    chat_id, user_id = int(chat_id), int(user_id)
    expiry = await add_membership(chat_id, user_id, plan_str, query.from_user.id)
    
    try:
        target = await context.bot.get_chat(user_id)
        target_name = target.full_name
    except TelegramError:
        target_name = str(user_id)

    try:
        chat = await context.bot.get_chat(chat_id)
        pm_sent = await send_pm(context, user_id, f"🎉 *Welcome!*\n\n✅ Membership activated for *{escape_md(chat.title)}*\n📦 Duration: {plan_str.upper()}\n📅 Expiry Date: {fmt_expiry(expiry)} IST")
    except TelegramError:
        pm_sent = False

    await query.edit_message_text(f"✅ *Membership Activated!*\n\n👤 Member: {escape_md(target_name)}\n📦 Plan: *{plan_str.upper()}*\n📅 Expiry: *{fmt_expiry(expiry)} IST*\n\n" + ("📩 PM sent." if pm_sent else "⚠️ User PM disabled."), parse_mode="Markdown")
    await query.answer("✅ Success!", show_alert=True)

async def cb_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.edit_message_text("❌ Action Cancelled.")
    await update.callback_query.answer()

async def cb_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = int(query.data.split("|")[1])
    try: title = (await context.bot.get_chat(chat_id)).title
    except TelegramError: title = str(chat_id)
    await query.edit_message_text(f"⚙️ *Admin Control Panel*\n📌 {escape_md(title)}", parse_mode="Markdown", reply_markup=main_panel_keyboard(chat_id))
    await query.answer()

async def cb_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = int(query.data.split("|")[1])
    soon = (now_ist() + timedelta(days=3)).isoformat()
    
    with db_cursor() as c:
        c.execute("SELECT COUNT(*) AS cnt FROM members WHERE chat_id=%s", (chat_id,))
        total = c.fetchone()["cnt"]
        c.execute("SELECT COUNT(*) AS cnt FROM members WHERE chat_id=%s AND active=1", (chat_id,))
        active = c.fetchone()["cnt"]
        c.execute("SELECT COUNT(*) AS cnt FROM members WHERE chat_id=%s AND active=1 AND expiry <= %s", (chat_id, soon))
        expiring = c.fetchone()["cnt"]

    await query.edit_message_text(f"📊 *Chat Statistics*\n\n👥 Total: *{total}*\n🟢 Active: *{active}*\n🔴 Expired/Removed: *{total - active}*\n⚠️ Expiring (3 Days): *{expiring}*", parse_mode="Markdown", reply_markup=main_panel_keyboard(chat_id))
    await query.answer()

async def cb_expiring(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = int(query.data.split("|")[1])
    soon = (now_ist() + timedelta(days=3)).isoformat()
    
    with db_cursor() as c:
        c.execute("SELECT * FROM members WHERE chat_id=%s AND active=1 AND expiry <= %s ORDER BY expiry ASC LIMIT 50", (chat_id, soon))
        docs = [parse_db_row(r) for r in c.fetchall()]

    text = "✅ No members are expiring soon." if not docs else "\n".join([f"⚠️ *Expiring Soon:*"] + [f"• `{d['user_id']}` — {format_time_left(d['expiry'])} left" for d in docs])
    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_to_panel_kb(chat_id))
    await query.answer()

async def cb_expired(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = int(query.data.split("|")[1])
    
    with db_cursor() as c:
        c.execute("SELECT * FROM members WHERE chat_id=%s AND active=0 ORDER BY removed DESC LIMIT 20", (chat_id,))
        docs = [parse_db_row(r) for r in c.fetchall()]

    text = "✅ No expired members." if not docs else "\n".join([f"🔴 *Expired Members:*"] + [f"• `{d['user_id']}` — {(d.get('remove_reason') or 'Auto').title()}" for d in docs])
    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_to_panel_kb(chat_id))
    await query.answer()

async def cb_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = int(query.data.split("|")[1])
    
    with db_cursor() as c:
        c.execute("SELECT * FROM logs WHERE chat_id=%s ORDER BY ts DESC LIMIT 10", (chat_id,))
        docs = [parse_db_row(r) for r in c.fetchall()]

    text = "📭 No logs found." if not docs else "\n".join(["📋 *Recent Logs:*"] + [f"`{d['ts'].strftime('%d/%m %H:%M')}` | {d['action'].upper()} | User:`{d.get('target_id')}`" for d in docs])
    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_to_panel_kb(chat_id))
    await query.answer()

async def cb_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = int(query.data.split("|")[1])
    today, week = (now_ist() + timedelta(days=1)).isoformat(), (now_ist() + timedelta(days=7)).isoformat()
    
    with db_cursor() as c:
        c.execute("SELECT COUNT(*) AS cnt FROM members WHERE chat_id=%s AND active=1 AND expiry <= %s", (chat_id, today))
        t_cnt = c.fetchone()["cnt"]
        c.execute("SELECT COUNT(*) AS cnt FROM members WHERE chat_id=%s AND active=1 AND expiry <= %s", (chat_id, week))
        w_cnt = c.fetchone()["cnt"]

    await query.edit_message_text(f"📋 *Expiry Report*\n\n⚠️ Expiring Today: {t_cnt}\n📅 Expiring this Week: {w_cnt}", parse_mode="Markdown", reply_markup=main_panel_keyboard(chat_id))
    await query.answer()

async def cb_mlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    _, chat_id_s, page_s = query.data.split("|")
    chat_id, page, per_page = int(chat_id_s), int(page_s), 15
    
    with db_cursor() as c:
        c.execute("SELECT * FROM members WHERE chat_id=%s AND active=1 ORDER BY expiry ASC", (chat_id,))
        docs = [parse_db_row(r) for r in c.fetchall()]

    total, chunk = len(docs), docs[page * per_page:(page + 1) * per_page]
    lines = [f"👥 *Active Members (Page {page + 1}) | Total: {total}*\n"]
    
    for i, d in enumerate(chunk, page * per_page + 1):
        rem = format_time_left(d['expiry'])
        emoji = "🔴" if ("mins" in rem or "h " in rem or (rem.endswith("days") and int(rem.split()[0]) <= 3)) else "🟢"
        lines.append(f"{i}. `{d['user_id']}` {emoji} {rem} — {d['expiry'].strftime('%d %b')}")

    nav, kb_rows = [], []
    if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"mlist|{chat_id}|{page - 1}"))
    if (page + 1) * per_page < total: nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"mlist|{chat_id}|{page + 1}"))
    if nav: kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton("🔙 Back to Panel", callback_data=f"panel|{chat_id}")])

    await query.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb_rows))
    await query.answer()

async def cb_memberinfo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    _, chat_id_s, user_id_s = query.data.split("|")
    chat_id, user_id = int(chat_id_s), int(user_id_s)
    
    with db_cursor() as c:
        c.execute("SELECT * FROM members WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
        doc = parse_db_row(c.fetchone())

    if not doc: return await query.edit_message_text("❌ Record not found.")
    rem = format_time_left(doc["expiry"])
    emoji = "🔴" if "mins" in rem or "h " in rem or (rem.endswith("days") and int(rem.split()[0]) <= 3) else "🟢"

    await query.edit_message_text(f"👤 *Member Info*\n\nID: `{user_id}`\nStatus: {'🟢 Active' if doc['active'] else '🔴 Expired'}\n📅 Expiry: *{fmt_expiry(doc['expiry'])} IST*\n⏳ Remaining: {emoji} *{rem}*", parse_mode="Markdown", reply_markup=member_action_keyboard(chat_id, user_id))
    await query.answer()

async def cb_extmenu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    _, chat_id, user_id = query.data.split("|")
    await query.edit_message_text(f"➕ *Extend user:* `{user_id}`", parse_mode="Markdown", reply_markup=extend_menu_keyboard(int(chat_id), int(user_id)))
    await query.answer()

async def cb_doext(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    _, chat_id_s, user_id_s, plan_str = query.data.split("|")
    chat_id, user_id = int(chat_id_s), int(user_id_s)
    new_expiry = await extend_membership(chat_id, user_id, plan_str, query.from_user.id)
    
    if not new_expiry: return await query.answer("❌ Inactive member!", show_alert=True)
    try:
        chat = await context.bot.get_chat(chat_id)
        await send_pm(context, user_id, f"🎉 *Extended!*\n📌 Chat: {escape_md(chat.title)}\n➕ Added: {plan_str.upper()}\n📅 New Expiry: {fmt_expiry(new_expiry)}")
    except TelegramError: pass

    await query.edit_message_text(f"✅ Extended `{user_id}` by *{plan_str.upper()}*!\n📅 New Expiry: *{fmt_expiry(new_expiry)}*", parse_mode="Markdown")
    await query.answer(f"✅ Extended!", show_alert=True)

async def cb_remconfirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    _, chat_id, user_id = query.data.split("|")
    await query.edit_message_text(f"⚠️ Remove `{user_id}`?", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes", callback_data=f"doremove|{chat_id}|{user_id}"), InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]))
    await query.answer()

async def cb_doremove(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    _, chat_id_s, user_id_s = query.data.split("|")
    chat_id, user_id = int(chat_id_s), int(user_id_s)
    await remove_membership(chat_id, user_id, query.from_user.id, "manual_remove")
    kicked = await kick_member(chat_id, user_id, context)

    await query.edit_message_text(f"{'✅ Kicked' if kicked else '⚠️ DB Only (Kick Failed)'}: `{user_id}`", parse_mode="Markdown")
    await query.answer("✅ Removed!", show_alert=True)


# ==========================================================================
#  BACKGROUND JOBS
# ==========================================================================
async def auto_kick_expired(context: ContextTypes.DEFAULT_TYPE) -> None:
    with db_cursor() as c:
        c.execute("SELECT * FROM members WHERE active=1 AND expiry <= %s", (now_ist().isoformat(),))
        expired = [parse_db_row(r) for r in c.fetchall()]

    for d in expired:
        await remove_membership(d["chat_id"], d["user_id"], 0, "auto_expired")
        await kick_member(d["chat_id"], d["user_id"], context)
        try:
            chat = await context.bot.get_chat(d["chat_id"])
            await send_pm(context, d["user_id"], f"⏰ *Expired!*\n📌 Chat: {escape_md(chat.title)}\n🔄 Contact admin to renew.")
        except TelegramError: pass

async def send_reminders(context: ContextTypes.DEFAULT_TYPE) -> None:
    for hrs in REMINDER_HOURS:
        start, end = (now_ist() + timedelta(hours=hrs - 1)).isoformat(), (now_ist() + timedelta(hours=hrs + 1)).isoformat()
        with db_cursor() as c:
            c.execute("SELECT * FROM members WHERE active=1 AND expiry >= %s AND expiry <= %s", (start, end))
            docs = [parse_db_row(r) for r in c.fetchall()]
            
        for d in docs:
            try:
                chat = await context.bot.get_chat(d["chat_id"])
                await send_pm(context, d["user_id"], f"⚠️ *Reminder*\n📌 Chat: {escape_md(chat.title)}\n⏳ Strict {format_time_left(d['expiry'])} left!\n🔄 Renew before expiry.")
            except TelegramError: pass

async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Unhandled exception", exc_info=context.error)


# ==========================================================================
#  ENTRY POINT
# ==========================================================================
def main() -> None:
    if not BOT_TOKEN or not DATABASE_URL:
        log.critical("❌ Please set BOT_TOKEN and DATABASE_URL in environment variables.")
        sys.exit(1)

    init_db()

    # Start Flask Web Server for Render in background thread
    threading.Thread(target=run_web_server, daemon=True).start()

    # Start Telegram Bot Polling
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("setup", cmd_setup))
    app.add_handler(CommandHandler("addmember", cmd_addmember))
    app.add_handler(CommandHandler("removemember", cmd_removemember))
    app.add_handler(CommandHandler("extend", cmd_extend))
    app.add_handler(CommandHandler("panel", cmd_panel))
    app.add_handler(ChatMemberHandler(track_bot_promotion, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(ChatMemberHandler(track_chat_member, ChatMemberHandler.CHAT_MEMBER))

    for fn, pat in [
        (cb_plan, r"^plan\|"), (cb_custom, r"^custom\|"), (cb_confirm, r"^confirm\|"), (cb_cancel, r"^cancel$"),
        (cb_mlist, r"^mlist\|"), (cb_stats, r"^stats\|"), (cb_expiring, r"^expiring\|"), (cb_expired, r"^expired\|"),
        (cb_logs, r"^logs\|"), (cb_report, r"^report\|"), (cb_panel, r"^panel\|"), (cb_memberinfo, r"^memberinfo\|"),
        (cb_extmenu, r"^extmenu\|"), (cb_doext, r"^doext\|"), (cb_remconfirm, r"^remconfirm\|"), (cb_doremove, r"^doremove\|")
    ]: app.add_handler(CallbackQueryHandler(fn, pattern=pat))

    app.add_error_handler(global_error_handler)
    app.job_queue.run_repeating(auto_kick_expired, interval=AUTO_KICK_INTERVAL, first=10)
    app.job_queue.run_repeating(send_reminders, interval=REMINDER_INTERVAL, first=20)

    log.info("🤖 Bot is starting up with Supabase and Flask...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
