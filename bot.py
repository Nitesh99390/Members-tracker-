"""
Telegram Membership Bot — Lite v13
==================================
Simple, button-free, low-resource membership manager.

Env vars:
  BOT_TOKEN      Telegram bot token (required)
  DATABASE_URL   PostgreSQL / Supabase URL (required)
  PORT           If set (Render/Railway), a tiny health server starts on it

Admin flow (sab kuch PM me, plain text commands):
  1. Bot ko group me Admin banao  -> group auto-register
  2. Naya member join kare        -> bot admin ko PM karta hai
  3. Us PM par reply karo: /add 30 -> 30 din ki membership
  4. Expiry par bot khud member ko group se nikaal deta hai

Design for low footprint:
  - No Flask, no APScheduler, no inline keyboards, no file logs
  - 1-3 DB connections (SimpleConnectionPool), tuple cursors
  - Only 3 update types polled (message, chat_member, my_chat_member)
  - One background asyncio loop does kick + reminder sweep
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

import psycopg2
from psycopg2 import pool as pg_pool

from telegram import ChatMember, Update
from telegram.error import RetryAfter, TelegramError
from telegram.ext import (
    Application,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    filters,
)

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
PORT = os.getenv("PORT", "").strip()

IST = timezone(timedelta(hours=5, minutes=30))
SWEEP_INTERVAL = int(os.getenv("SWEEP_INTERVAL", "60"))   # seconds between expiry checks
REMIND_EVERY = 60                                         # sweeps (~1 hour) between reminder checks
REMIND_HOURS = 24                                         # remind when ~24h left
TG_MAX = 3900

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("bot")
log.setLevel(logging.INFO)


# --------------------------------------------------------------------------
# Time helpers
# --------------------------------------------------------------------------
def now() -> datetime:
    return datetime.now(IST)


def to_dt(s) -> Optional[datetime]:
    if not s:
        return None
    try:
        d = datetime.fromisoformat(str(s))
        return d.replace(tzinfo=IST) if d.tzinfo is None else d.astimezone(IST)
    except ValueError:
        return None


def parse_days(s: str) -> Optional[timedelta]:
    """'30' / '30d' -> days, '12h' -> hours, '45m' -> minutes."""
    s = s.strip().lower()
    try:
        if s.endswith("h"):
            td = timedelta(hours=int(s[:-1]))
        elif s.endswith("m"):
            td = timedelta(minutes=int(s[:-1]))
        else:
            td = timedelta(days=int(s.rstrip("d")))
    except ValueError:
        return None
    return td if td.total_seconds() > 0 else None


def left(expiry: datetime) -> str:
    d = expiry - now()
    secs = int(d.total_seconds())
    if secs <= 0:
        return "expired"
    days, rem = divmod(secs, 86400)
    hours, rem = divmod(rem, 3600)
    mins = rem // 60
    if days:
        return f"{days} din" + (f" {hours} ghante" if hours else "")
    if hours:
        return f"{hours} ghante" + (f" {mins} min" if mins else "")
    return f"{max(1, mins)} min"


def fmt(dt: datetime) -> str:
    return dt.strftime("%d %b %Y, %I:%M %p")


# --------------------------------------------------------------------------
# Database (tiny pool, tuple rows, auto-reconnect)
# --------------------------------------------------------------------------
_pool: Optional[pg_pool.SimpleConnectionPool] = None


def _get_pool() -> pg_pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = pg_pool.SimpleConnectionPool(1, 3, DATABASE_URL)
    return _pool


def q(sql: str, args: tuple = (), fetch: str = "") -> object:
    """Run one statement. fetch = '' | 'one' | 'all'. Retries once on a dead connection."""
    p = _get_pool()
    for attempt in (1, 2):
        conn = p.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, args)
                out = cur.fetchone() if fetch == "one" else cur.fetchall() if fetch == "all" else None
            conn.commit()
            p.putconn(conn)
            return out
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            p.putconn(conn, close=True)
            if attempt == 2:
                raise
        except Exception:
            conn.rollback()
            p.putconn(conn)
            raise


def init_db() -> None:
    q("""
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
            last_reminded TEXT,
            PRIMARY KEY (chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS groups (
            chat_id  BIGINT PRIMARY KEY,
            title    TEXT,
            added_by BIGINT,
            ts       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_members_active_expiry ON members (active, expiry);
        CREATE INDEX IF NOT EXISTS idx_members_chat_active  ON members (chat_id, active);
    """)


def my_groups(user_id: int) -> list[tuple[int, str]]:
    return q("SELECT chat_id, title FROM groups WHERE added_by=%s ORDER BY ts", (user_id,), "all") or []


def upsert_member(chat_id: int, user_id: int, td: timedelta, plan: str, by: int) -> datetime:
    """New member -> now+td. Active member -> expiry+td (renew). Returns new expiry."""
    row = q("SELECT expiry, active FROM members WHERE chat_id=%s AND user_id=%s", (chat_id, user_id), "one")
    base = now()
    if row and row[1] == 1:
        cur = to_dt(row[0])
        if cur and cur > base:
            base = cur
    expiry = base + td
    ts = now().isoformat()
    q("""
        INSERT INTO members (chat_id,user_id,expiry,days,added_by,active,joined,updated,last_reminded)
        VALUES (%s,%s,%s,%s,%s,1,%s,%s,NULL)
        ON CONFLICT (chat_id,user_id) DO UPDATE SET
            expiry=EXCLUDED.expiry, days=EXCLUDED.days, added_by=EXCLUDED.added_by,
            active=1, updated=EXCLUDED.updated, last_reminded=NULL,
            removed=NULL, remove_reason=NULL
    """, (chat_id, user_id, expiry.isoformat(), plan, by, ts, ts))
    return expiry


def deactivate(chat_id: int, user_id: int, reason: str) -> None:
    q("UPDATE members SET active=0, removed=%s, remove_reason=%s WHERE chat_id=%s AND user_id=%s",
      (now().isoformat(), reason, chat_id, user_id))


# --------------------------------------------------------------------------
# Telegram helpers
# --------------------------------------------------------------------------
async def pm(bot, user_id: int, text: str) -> bool:
    try:
        await bot.send_message(user_id, text)
        return True
    except TelegramError:
        return False


async def kick(bot, chat_id: int, user_id: int) -> bool:
    """Ban + unban = remove but allow re-join via invite link."""
    try:
        await bot.ban_chat_member(chat_id, user_id)
        await bot.unban_chat_member(chat_id, user_id, only_if_banned=True)
        return True
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
        try:
            await bot.ban_chat_member(chat_id, user_id)
            await bot.unban_chat_member(chat_id, user_id, only_if_banned=True)
            return True
        except TelegramError:
            return False
    except TelegramError:
        return False


async def reply(update: Update, text: str) -> None:
    for i in range(0, len(text), TG_MAX):
        await update.message.reply_text(text[i:i + TG_MAX])


HELP = (
    "👋 Membership Bot\n\n"
    "Setup: mujhe apne group me Admin banao (members remove karne ki permission ke saath). Bas itna hi.\n\n"
    "Commands:\n"
    "/add <user_id> <days>  — membership do / badhao\n"
    "/remove <user_id>  — member ko group se nikaalo\n"
    "/check <user_id>  — ek member ki expiry dekho\n"
    "/list  — sab active members\n"
    "/groups  — apne groups dekho / select karo\n\n"
    "Tip: jab koi naya member join karega, main yahan message bhejunga. "
    "Us message par reply karke sirf  /add 30  likho — ho gaya."
)

_ID_RE = re.compile(r"ID:\s*(\d+)")
_GID_RE = re.compile(r"Group ID:\s*(-?\d+)")


def _target_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> tuple[Optional[int], Optional[str]]:
    """Figure out which group a command refers to. Returns (chat_id, error_text)."""
    rep = update.message.reply_to_message
    if rep and rep.text:
        m = _GID_RE.search(rep.text)
        if m:
            return int(m.group(1)), None

    groups = my_groups(update.effective_user.id)
    if not groups:
        return None, "❌ Koi group registered nahi hai. Pehle mujhe group me Admin banao."
    if len(groups) == 1:
        return groups[0][0], None

    sel = context.user_data.get("g")
    if sel and any(sel == g[0] for g in groups):
        return sel, None
    return None, f"Aapke {len(groups)} groups hain. Pehle /groups se ek select karo."


def _target_user(update: Update, args: list[str]) -> tuple[Optional[int], list[str]]:
    """User id from args, or from the replied-to join message. Returns (user_id, remaining_args)."""
    if args and args[0].isdigit():
        return int(args[0]), args[1:]
    rep = update.message.reply_to_message
    if rep and rep.text:
        m = _ID_RE.search(rep.text)
        if m:
            return int(m.group(1)), args
    return None, args


# --------------------------------------------------------------------------
# Commands (private chat only)
# --------------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await reply(update, HELP)


async def cmd_groups(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    groups = my_groups(update.effective_user.id)
    if not groups:
        return await reply(update, "❌ Koi group registered nahi hai. Pehle mujhe group me Admin banao.")

    if context.args and context.args[0].isdigit():
        n = int(context.args[0])
        if 1 <= n <= len(groups):
            context.user_data["g"] = groups[n - 1][0]
            return await reply(update, f"✅ Selected: {groups[n - 1][1]}")
        return await reply(update, "❌ Galat number.")

    sel = context.user_data.get("g")
    lines = ["📌 Aapke groups:\n"]
    for i, (cid, title) in enumerate(groups, 1):
        mark = " ✅" if cid == sel or len(groups) == 1 else ""
        lines.append(f"{i}. {title}{mark}")
    if len(groups) > 1:
        lines.append("\nSelect karne ke liye:  /groups <number>")
    await reply(update, "\n".join(lines))


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid, rest = _target_user(update, context.args)
    if uid is None or not rest:
        return await reply(update, "Use:  /add <user_id> <days>\nExample:  /add 123456789 30\n\n"
                                   "Ya join-message par reply karke:  /add 30")
    td = parse_days(rest[0])
    if td is None:
        return await reply(update, "❌ Days galat hai. Example: 30  (ya 12h, 45m)")

    cid, err = _target_group(update, context)
    if err:
        return await reply(update, err)

    expiry = upsert_member(cid, uid, td, rest[0], update.effective_user.id)
    title = next((t for c, t in my_groups(update.effective_user.id) if c == cid), str(cid))

    sent = await pm(context.bot, uid,
                    f"✅ {title} me aapki membership active hai.\n"
                    f"📅 Expiry: {fmt(expiry)} IST ({left(expiry)} baaki)")
    await reply(update,
                f"✅ Done\n👤 {uid}\n📌 {title}\n📅 Expiry: {fmt(expiry)} IST\n"
                + ("📩 User ko PM bhej diya." if sent else "ℹ️ User ka PM band hai (membership phir bhi active hai)."))


async def cmd_remove(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid, _ = _target_user(update, context.args)
    if uid is None:
        return await reply(update, "Use:  /remove <user_id>")
    cid, err = _target_group(update, context)
    if err:
        return await reply(update, err)

    deactivate(cid, uid, "manual")
    ok = await kick(context.bot, cid, uid)
    await reply(update, f"{'✅ Member group se nikaal diya' if ok else '⚠️ Record update hua, par kick nahi ho paya (bot admin hai?)'}: {uid}")


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid, _ = _target_user(update, context.args)
    if uid is None:
        return await reply(update, "Use:  /check <user_id>")
    cid, err = _target_group(update, context)
    if err:
        return await reply(update, err)

    row = q("SELECT expiry, active FROM members WHERE chat_id=%s AND user_id=%s", (cid, uid), "one")
    if not row:
        return await reply(update, f"❌ {uid} ka koi record nahi.")
    exp = to_dt(row[0])
    status = "🟢 Active" if row[1] == 1 and exp and exp > now() else "🔴 Inactive"
    await reply(update, f"👤 {uid}\n{status}\n📅 Expiry: {fmt(exp) if exp else '-'} IST\n⏳ {left(exp) if exp else '-'} baaki")


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cid, err = _target_group(update, context)
    if err:
        return await reply(update, err)
    rows = q("SELECT user_id, expiry FROM members WHERE chat_id=%s AND active=1 ORDER BY expiry", (cid,), "all") or []
    if not rows:
        return await reply(update, "Koi active member nahi hai.")
    lines = [f"👥 Active members: {len(rows)}\n"]
    for i, (uid, exp) in enumerate(rows, 1):
        d = to_dt(exp)
        dot = "🔴" if d and (d - now()) < timedelta(days=3) else "🟢"
        lines.append(f"{i}. {uid} {dot} {left(d) if d else '-'}")
    await reply(update, "\n".join(lines))


async def cmd_in_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Commands sent inside a group: delete silently, keep group clean."""
    try:
        await update.message.delete()
    except TelegramError:
        pass


# --------------------------------------------------------------------------
# Membership events
# --------------------------------------------------------------------------
_ADMIN = (ChatMember.ADMINISTRATOR, ChatMember.OWNER)


async def on_my_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    r = update.my_chat_member
    if r.chat.type == "private":
        return
    was = r.old_chat_member.status in _ADMIN
    is_now = r.new_chat_member.status in _ADMIN
    if is_now and not was:
        q("""INSERT INTO groups (chat_id,title,added_by,ts) VALUES (%s,%s,%s,%s)
             ON CONFLICT (chat_id) DO UPDATE SET title=EXCLUDED.title, added_by=EXCLUDED.added_by""",
          (r.chat.id, r.chat.title, r.from_user.id, now().isoformat()))
        await pm(context.bot, r.from_user.id,
                 f"✅ {r.chat.title} register ho gaya.\n\n"
                 "Ab jab bhi koi naya member join karega, main yahan bataunga. "
                 "Us message par reply karke  /add 30  likhna.\n\n/start — sab commands")
    elif was and not is_now and r.new_chat_member.status in (ChatMember.LEFT, ChatMember.BANNED):
        q("DELETE FROM groups WHERE chat_id=%s", (r.chat.id,))


async def on_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    r = update.chat_member
    u = r.new_chat_member.user
    if u.is_bot:
        return
    joined = (r.old_chat_member.status in (ChatMember.LEFT, ChatMember.BANNED)
              and r.new_chat_member.status in (ChatMember.MEMBER, ChatMember.RESTRICTED))
    if not joined:
        return
    row = q("SELECT added_by FROM groups WHERE chat_id=%s", (r.chat.id,), "one")
    if not row:
        return
    await pm(context.bot, row[0],
             f"🆕 Naya member\n"
             f"👤 {u.full_name}\n"
             f"ID: {u.id}\n"
             f"📌 {r.chat.title}\n"
             f"Group ID: {r.chat.id}\n\n"
             f"👉 Is message par reply karo:  /add 30")


# --------------------------------------------------------------------------
# Background sweep: auto-kick expired + 24h reminder
# --------------------------------------------------------------------------
async def sweep(bot) -> None:
    rows = q("""SELECT m.chat_id, m.user_id, g.title FROM members m
                LEFT JOIN groups g ON g.chat_id = m.chat_id
                WHERE m.active=1 AND m.expiry<=%s LIMIT 200""", (now().isoformat(),), "all") or []
    for cid, uid, title in rows:
        deactivate(cid, uid, "expired")
        await kick(bot, cid, uid)
        await pm(bot, uid, f"⏰ {title or 'Group'} ki membership khatam ho gayi.\nRenew ke liye admin se contact karo.")
        await asyncio.sleep(0.3)  # gentle on Telegram rate limits


async def remind(bot) -> None:
    t = now()
    lo, hi = (t + timedelta(hours=REMIND_HOURS - 1)).isoformat(), (t + timedelta(hours=REMIND_HOURS + 1)).isoformat()
    rows = q("""SELECT m.chat_id, m.user_id, m.expiry, g.title FROM members m
                LEFT JOIN groups g ON g.chat_id = m.chat_id
                WHERE m.active=1 AND m.expiry BETWEEN %s AND %s AND m.last_reminded IS NULL LIMIT 200""",
             (lo, hi), "all") or []
    for cid, uid, exp, title in rows:
        d = to_dt(exp)
        if await pm(bot, uid, f"⚠️ {title or 'Group'} ki membership {left(d) if d else '24 ghante'} me khatam hogi.\n"
                              "Renew ke liye admin se contact karo."):
            q("UPDATE members SET last_reminded=%s WHERE chat_id=%s AND user_id=%s", (t.isoformat(), cid, uid))
        await asyncio.sleep(0.3)


async def worker(app: Application) -> None:
    n = 0
    while True:
        try:
            await sweep(app.bot)
            if n % REMIND_EVERY == 0:
                await remind(app.bot)
        except Exception:
            log.exception("sweep failed")
        n += 1
        await asyncio.sleep(SWEEP_INTERVAL)


async def _post_init(app: Application) -> None:
    app.bot_data["worker"] = asyncio.get_running_loop().create_task(worker(app))
    log.info("Bot started. Sweep every %ss.", SWEEP_INTERVAL)


async def _post_shutdown(app: Application) -> None:
    t = app.bot_data.get("worker")
    if t:
        t.cancel()
    if _pool:
        _pool.closeall()


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Error: %s", context.error)


# --------------------------------------------------------------------------
# Tiny health server (only when PORT is set, e.g. Render)
# --------------------------------------------------------------------------
class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):  # silence
        pass


def _serve_health(port: int) -> None:
    HTTPServer(("0.0.0.0", port), _Health).serve_forever()


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main() -> None:
    if not BOT_TOKEN or not DATABASE_URL:
        log.critical("BOT_TOKEN aur DATABASE_URL env me set karo.")
        sys.exit(1)

    init_db()

    if PORT.isdigit():
        threading.Thread(target=_serve_health, args=(int(PORT),), daemon=True).start()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )

    private = filters.ChatType.PRIVATE
    for name, fn in (("start", cmd_start), ("help", cmd_start), ("add", cmd_add), ("remove", cmd_remove),
                     ("check", cmd_check), ("list", cmd_list), ("groups", cmd_groups)):
        app.add_handler(CommandHandler(name, fn, filters=private))
    app.add_handler(CommandHandler(["start", "help", "add", "remove", "check", "list", "groups"],
                                   cmd_in_group, filters=~private))

    app.add_handler(ChatMemberHandler(on_my_status, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(ChatMemberHandler(on_member, ChatMemberHandler.CHAT_MEMBER))
    app.add_error_handler(on_error)

    app.run_polling(
        allowed_updates=["message", "chat_member", "my_chat_member"],
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
