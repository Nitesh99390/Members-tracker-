"""
Telegram Member Tracker Bot
============================
- Group/Channel mein members ko track karta hai
- 1 month baad auto-remove karta hai
- MongoDB mein data store karta hai
- Render.com pe run karne ke liye ready hai

Environment Variables:
  BOT_TOKEN    -> Telegram Bot Token (BotFather se milega)
  MONGO_URL    -> MongoDB Connection String
  CHECK_INTERVAL -> (Optional) Check interval in seconds (default: 3600 = 1 hour)
"""

import os
import logging
import asyncio
from datetime import datetime, timedelta, timezone

from telegram import Bot, Update, ChatMember
from telegram.ext import (
    Application,
    CommandHandler,
    ChatMemberHandler,
    ContextTypes,
)
from telegram.error import TelegramError, Forbidden, BadRequest
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("MemberBot")

# ─────────────────────────────────────────────
# Environment Variables
# ─────────────────────────────────────────────
BOT_TOKEN      = os.environ.get("BOT_TOKEN", "")
MONGO_URL      = os.environ.get("MONGO_URL", "")
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "3600"))  # seconds
REMOVAL_DAYS   = int(os.environ.get("REMOVAL_DAYS", "30"))       # default 30 days

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN environment variable set nahi hai!")
if not MONGO_URL:
    raise ValueError("❌ MONGO_URL environment variable set nahi hai!")

# ─────────────────────────────────────────────
# MongoDB Connection
# ─────────────────────────────────────────────
try:
    mongo_client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
    mongo_client.admin.command("ping")
    logger.info("✅ MongoDB se connection successful!")
except ConnectionFailure as e:
    raise ConnectionFailure(f"❌ MongoDB connect nahi ho saka: {e}")

db              = mongo_client["telegram_member_tracker"]
members_col     = db["members"]        # Active members ka record
removed_col     = db["removed_members"] # Removed members ka history
chats_col       = db["registered_chats"] # Bot kahan-kahan add hai

# MongoDB Indexes (fast queries ke liye)
members_col.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)], unique=True)
members_col.create_index([("join_date", ASCENDING)])
removed_col.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)])
chats_col.create_index([("chat_id", ASCENDING)], unique=True)

logger.info("✅ MongoDB indexes create ho gaye!")

# ─────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────

def now_utc() -> datetime:
    """Current UTC time return karta hai"""
    return datetime.now(timezone.utc)


def register_chat(chat_id: int, chat_title: str, chat_type: str):
    """Chat ko database mein register karta hai"""
    chats_col.update_one(
        {"chat_id": chat_id},
        {
            "$set": {
                "chat_id": chat_id,
                "chat_title": chat_title,
                "chat_type": chat_type,
                "registered_at": now_utc(),
            }
        },
        upsert=True,
    )


def add_member(chat_id: int, chat_title: str, user_id: int,
               username: str, full_name: str):
    """Member ko database mein add karta hai (pehle se hai to update karta hai)"""
    existing = members_col.find_one({"chat_id": chat_id, "user_id": user_id})
    if existing:
        # Already hai, join_date change mat karo
        members_col.update_one(
            {"chat_id": chat_id, "user_id": user_id},
            {"$set": {"username": username, "full_name": full_name, "is_active": True}},
        )
        logger.info(f"🔄 Member update: {full_name} ({user_id}) in chat {chat_title}")
    else:
        members_col.insert_one({
            "chat_id":    chat_id,
            "chat_title": chat_title,
            "user_id":    user_id,
            "username":   username,
            "full_name":  full_name,
            "join_date":  now_utc(),
            "is_active":  True,
        })
        logger.info(f"➕ New member added: {full_name} ({user_id}) in chat {chat_title}")


def remove_member_record(chat_id: int, user_id: int, reason: str = "left"):
    """Member ka record members_col se hata ke removed_col mein move karta hai"""
    doc = members_col.find_one({"chat_id": chat_id, "user_id": user_id})
    if doc:
        doc["removed_at"] = now_utc()
        doc["remove_reason"] = reason
        doc.pop("_id", None)
        removed_col.insert_one(doc)
        members_col.delete_one({"chat_id": chat_id, "user_id": user_id})
        logger.info(f"🗑️ Record removed: user {user_id} from chat {chat_id} | reason: {reason}")


def get_expired_members(days: int = REMOVAL_DAYS):
    """Aise members return karta hai jinhe 1 month (ya REMOVAL_DAYS) ho gaya"""
    cutoff = now_utc() - timedelta(days=days)
    return list(members_col.find({
        "join_date": {"$lte": cutoff},
        "is_active": True,
    }))


def get_chat_stats(chat_id: int) -> dict:
    """Ek chat ki stats return karta hai"""
    active_count  = members_col.count_documents({"chat_id": chat_id})
    removed_count = removed_col.count_documents({"chat_id": chat_id})
    oldest = members_col.find_one(
        {"chat_id": chat_id},
        sort=[("join_date", ASCENDING)]
    )
    return {
        "active":  active_count,
        "removed": removed_count,
        "oldest":  oldest,
    }


# ─────────────────────────────────────────────
# Auto-Remove Job (Scheduler)
# ─────────────────────────────────────────────

async def auto_remove_expired_members(bot: Bot):
    """
    Scheduled job:
    - REMOVAL_DAYS se purane members dhundta hai
    - Unhe group/channel se kick karta hai
    - Database record update karta hai
    """
    logger.info("🔍 Auto-remove check shuru hua...")
    expired = get_expired_members(REMOVAL_DAYS)

    if not expired:
        logger.info("✅ Koi expired member nahi mila.")
        return

    logger.info(f"⚠️ {len(expired)} expired members mile.")

    for member in expired:
        chat_id   = member["chat_id"]
        user_id   = member["user_id"]
        full_name = member.get("full_name", "Unknown")
        join_date = member.get("join_date")

        try:
            # Telegram se ban karo (kick)
            await bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
            # Immediately unban (taaki future mein join kar sake, sirf nikala hai)
            await asyncio.sleep(1)
            await bot.unban_chat_member(chat_id=chat_id, user_id=user_id,
                                        only_if_banned=True)

            join_str = join_date.strftime("%d-%m-%Y") if join_date else "N/A"
            logger.info(
                f"✅ Removed: {full_name} ({user_id}) from chat {chat_id} "
                f"| Joined: {join_str}"
            )

            # Database update
            remove_member_record(chat_id, user_id, reason="1_month_expired")

            # Admin ko notify karo (optional - errors ignore karo)
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🕐 <b>Auto-Remove:</b>\n"
                        f"👤 <b>{full_name}</b> ko group se hata diya gaya.\n"
                        f"📅 Join Date: <code>{join_str}</code>\n"
                        f"⏳ Reason: 1 mahina complete ho gaya."
                    ),
                    parse_mode="HTML",
                )
            except TelegramError:
                pass  # Message send na ho to ignore karo

        except Forbidden:
            logger.warning(
                f"⛔ Permission nahi: {full_name} ({user_id}) ko "
                f"chat {chat_id} se remove nahi kar saka."
            )
            # Bot ko admin banana padega, record rakhte hain
        except BadRequest as e:
            logger.warning(f"⚠️ BadRequest for {user_id} in {chat_id}: {e}")
            if "user not found" in str(e).lower() or "chat not found" in str(e).lower():
                remove_member_record(chat_id, user_id, reason="user_not_found")
        except TelegramError as e:
            logger.error(f"❌ TelegramError for {user_id} in {chat_id}: {e}")


# ─────────────────────────────────────────────
# Event Handlers
# ─────────────────────────────────────────────

async def track_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Jab bhi koi member join kare ya leave kare - yeh handle karta hai.
    ChatMemberHandler se trigger hota hai.
    """
    result = update.chat_member or update.my_chat_member
    if not result:
        return

    chat      = result.chat
    new_member = result.new_chat_member
    old_member = result.old_chat_member

    # Bot khud join hua
    if new_member.user.id == context.bot.id:
        if new_member.status in (ChatMember.MEMBER, ChatMember.ADMINISTRATOR):
            register_chat(chat.id, chat.title or "Unknown", chat.type)
            logger.info(f"🤖 Bot add hua chat: {chat.title} ({chat.id})")
        return

    user      = new_member.user
    user_id   = user.id
    username  = user.username or ""
    full_name = user.full_name or "Unknown"

    # Member join hua
    joined_statuses = (ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER)
    left_statuses   = (ChatMember.LEFT, ChatMember.BANNED)

    if old_member.status in left_statuses and new_member.status in joined_statuses:
        register_chat(chat.id, chat.title or "Unknown", chat.type)
        add_member(chat.id, chat.title or "Unknown", user_id, username, full_name)

    # Member chala gaya
    elif old_member.status in joined_statuses and new_member.status in left_statuses:
        remove_member_record(chat.id, user_id, reason="voluntarily_left_or_kicked")


# ─────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Bot start karo"""
    user = update.effective_user
    await update.message.reply_text(
        f"👋 Assalam o Alaikum, <b>{user.first_name}</b>!\n\n"
        "🤖 <b>Member Tracker Bot</b> active hai!\n\n"
        "📋 <b>Commands:</b>\n"
        "• /stats — Is chat ki stats dekho\n"
        "• /members — Active members list\n"
        "• /check — Abhi expired members check karo\n"
        "• /help — Help dekho\n\n"
        "⚙️ Mujhe group/channel mein <b>Admin</b> banao aur "
        "<b>Members Remove</b> permission do.",
        parse_mode="HTML",
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help message"""
    await update.message.reply_text(
        "📖 <b>Member Tracker Bot - Help</b>\n\n"
        "🔧 <b>Setup kaise karein:</b>\n"
        "1. Mujhe group/channel mein add karo\n"
        "2. Mujhe Admin banao\n"
        "3. 'Ban Users' ya 'Remove Members' permission do\n\n"
        "⏰ <b>Kaise kaam karta hai:</b>\n"
        "• Jab koi join karta hai, main uski date note kar leta hoon\n"
        f"• {REMOVAL_DAYS} din baad main use automatically remove karta hoon\n"
        "• Removed member ko unban bhi kar deta hoon (future mein rejoin ho sake)\n\n"
        "📊 <b>Commands:</b>\n"
        "• /stats — Chat ki member statistics\n"
        "• /members — Active members aur unki join date\n"
        "• /check — Manually expired members check karo\n\n"
        f"🔄 Auto-check interval: Har <b>{CHECK_INTERVAL // 60} minutes</b>\n"
        f"⏳ Removal after: <b>{REMOVAL_DAYS} din</b>",
        parse_mode="HTML",
    )


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Chat ki stats dikhao"""
    chat = update.effective_chat
    if chat.type == "private":
        await update.message.reply_text(
            "⚠️ Yeh command sirf group/channel mein use karein!"
        )
        return

    stats = get_chat_stats(chat.id)
    oldest = stats["oldest"]
    oldest_str = "N/A"
    if oldest and oldest.get("join_date"):
        oldest_str = (
            f"{oldest['full_name']} "
            f"({oldest['join_date'].strftime('%d-%m-%Y')})"
        )

    await update.message.reply_text(
        f"📊 <b>Stats: {chat.title}</b>\n\n"
        f"👥 Active Members: <b>{stats['active']}</b>\n"
        f"🗑️ Total Removed: <b>{stats['removed']}</b>\n"
        f"🏅 Purana Member: <b>{oldest_str}</b>\n\n"
        f"⏳ Removal Period: <b>{REMOVAL_DAYS} din</b>",
        parse_mode="HTML",
    )


async def cmd_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Active members ki list dikhao"""
    chat = update.effective_chat
    if chat.type == "private":
        await update.message.reply_text(
            "⚠️ Yeh command sirf group/channel mein use karein!"
        )
        return

    members = list(
        members_col.find({"chat_id": chat.id}).sort("join_date", ASCENDING).limit(50)
    )

    if not members:
        await update.message.reply_text("📭 Koi tracked member nahi mila.")
        return

    cutoff = now_utc() - timedelta(days=REMOVAL_DAYS)
    lines  = [f"👥 <b>Members List ({chat.title}):</b>\n"]

    for i, m in enumerate(members, 1):
        join_date  = m.get("join_date")
        join_str   = join_date.strftime("%d-%m-%Y") if join_date else "N/A"
        days_in    = (now_utc() - join_date).days if join_date else 0
        warning    = " ⚠️" if join_date and join_date <= cutoff else ""
        name       = m.get("full_name", "Unknown")
        lines.append(f"{i}. {name} | Join: {join_str} | {days_in}d{warning}")

    total = members_col.count_documents({"chat_id": chat.id})
    if total > 50:
        lines.append(f"\n... aur {total - 50} members hain")

    lines.append("\n⚠️ = 1 mahina ya zyada ho gaya (jald remove hoga)")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually expired members check karo aur remove karo"""
    chat   = update.effective_chat
    member = await chat.get_member(update.effective_user.id)

    # Sirf admin run kar sake
    if member.status not in (ChatMember.ADMINISTRATOR, ChatMember.OWNER):
        await update.message.reply_text("⛔ Sirf admins yeh command use kar sakte hain!")
        return

    msg = await update.message.reply_text("🔍 Check ho raha hai...")
    await auto_remove_expired_members(context.bot)
    await msg.edit_text("✅ Check complete! Expired members process ho gaye.")


# ─────────────────────────────────────────────
# Error Handler
# ─────────────────────────────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Global error handler"""
    logger.error(f"❌ Error: {context.error}", exc_info=context.error)


# ─────────────────────────────────────────────
# Main Entry Point
# ─────────────────────────────────────────────

def main():
    logger.info("🚀 Member Tracker Bot start ho raha hai...")

    # Application build karo
    app = Application.builder().token(BOT_TOKEN).build()

    # Commands register karo
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("help",    cmd_help))
    app.add_handler(CommandHandler("stats",   cmd_stats))
    app.add_handler(CommandHandler("members", cmd_members))
    app.add_handler(CommandHandler("check",   cmd_check))

    # Chat member events (join/leave track karo)
    app.add_handler(ChatMemberHandler(track_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(ChatMemberHandler(track_chat_member, ChatMemberHandler.CHAT_MEMBER))

    # Error handler
    app.add_error_handler(error_handler)

    # Scheduler setup (auto-remove ke liye)
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        auto_remove_expired_members,
        trigger="interval",
        seconds=CHECK_INTERVAL,
        args=[app.bot],
        id="auto_remove_job",
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=10),
    )
    scheduler.start()
    logger.info(
        f"⏰ Scheduler started | Check har {CHECK_INTERVAL}s baad | "
        f"Removal after {REMOVAL_DAYS} days"
    )

    # Bot run karo
    logger.info("✅ Bot polling shuru kar raha hai...")
    app.run_polling(
        allowed_updates=[
            Update.MESSAGE,
            Update.CHAT_MEMBER,
            Update.MY_CHAT_MEMBER,
        ],
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
