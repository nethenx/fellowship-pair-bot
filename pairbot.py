import json
import random
import asyncio
import threading
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from datetime import datetime, timezone
import logging
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# File to store user data
DATA_FILE = "church_pairings.json"

class ChurchPairingBot:
    def __init__(self, token):
        self.token = token
        self.app = Application.builder().token(token).build()
        self.scheduler = AsyncIOScheduler(timezone=timezone.utc)
        self.data = self.load_data()
        
        # Add handlers
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("pairme", self.pairme))
        self.app.add_handler(CommandHandler("status", self.status))
        self.app.add_handler(CommandHandler("leave", self.leave))
        self.app.add_handler(CommandHandler("mypair", self.mypair))
        
        # Schedule weekly pairings for Sunday 7 PM Ethiopian time (4 PM UTC)
        # Ethiopian time is UTC+3, so 7 PM ET = 4 PM UTC
        self.scheduler.add_job(
            self.send_weekly_pairings,
            CronTrigger(day_of_week=6, hour=16, minute=0),  # Sunday = 6, 4 PM UTC = 7 PM Ethiopian
            id='weekly_pairings'
        )

    def load_data(self):
        """Load user data from JSON file"""
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                logger.warning("Could not load data file, starting fresh")
        return {}

    def save_data(self):
        """Save user data to JSON file"""
        try:
            with open(DATA_FILE, 'w') as f:
                json.dump(self.data, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save data: {e}")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        chat_type = update.effective_chat.type
        
        if chat_type == 'private':
            await update.message.reply_text(
                "👋 Hi! I'm the Church Pairing Bot.\n\n"
                "Add me to your church group chat and use /pairme there to join weekly pairings!"
            )
        else:
            await update.message.reply_text(
                "🎉 Church Pairing Bot is now active in this group!\n\n"
                "📝 Use /pairme to join the weekly pairing\n"
                "📊 Use /status to see current participants\n"
                "👥 Use /mypair to see your pairing status\n"
                "🚪 Use /leave to remove yourself from pairings\n\n"
                "💫 Every Sunday at 7 PM Ethiopian time (dinner time), I'll randomly pair members and post the results here!"
            )

    async def pairme(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /pairme command - automatically detects group from chat_id"""
        chat_id = str(update.effective_chat.id)
        user_id = str(update.effective_user.id)
        username = update.effective_user.username or update.effective_user.first_name or "Unknown"
        chat_title = update.effective_chat.title or "Private Chat"
        
        # Only allow in group chats
        if update.effective_chat.type == 'private':
            await update.message.reply_text(
                "❌ Please use this command in your church group chat, not in private messages."
            )
            return
        
        # Initialize group data if it doesn't exist
        if chat_id not in self.data:
            self.data[chat_id] = {
                "chat_title": chat_title,
                "users": {},
                "created_at": datetime.now(timezone.utc).isoformat(),
                "pairing_history": []
            }
        
        # Update chat title in case it changed
        self.data[chat_id]["chat_title"] = chat_title
        
        # Check if user is already registered
        if user_id in self.data[chat_id]["users"]:
            await update.message.reply_text(
                f"✅ {username}, you're already registered for weekly pairings in this group!"
            )
            return
        
        # Add user to the group
        self.data[chat_id]["users"][user_id] = {
            "username": username,
            "joined_at": datetime.now(timezone.utc).isoformat(),
            "last_partner": None,
            "last_pairing_date": None
        }
        
        self.save_data()
        
        participant_count = len(self.data[chat_id]["users"])
        await update.message.reply_text(
            f"🎯 {username}, you've been added to the weekly pairing list!\n"
            f"👥 Total participants in this group: {participant_count}\n\n"
            f"📅 Next pairing: Sunday 7 PM Ethiopian time"
        )

    async def leave(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /leave command - remove user from pairings"""
        chat_id = str(update.effective_chat.id)
        user_id = str(update.effective_user.id)
        username = update.effective_user.username or update.effective_user.first_name or "Unknown"
        
        if update.effective_chat.type == 'private':
            await update.message.reply_text(
                "❌ Please use this command in your church group chat."
            )
            return
        
        if chat_id not in self.data or user_id not in self.data[chat_id]["users"]:
            await update.message.reply_text(
                f"❌ {username}, you're not currently registered for pairings in this group."
            )
            return
        
        # Remove user
        del self.data[chat_id]["users"][user_id]
        self.save_data()
        
        await update.message.reply_text(
            f"👋 {username}, you've been removed from the weekly pairing list."
        )

    async def mypair(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /mypair command - show user's pairing status and last partner"""
        chat_id = str(update.effective_chat.id)
        user_id = str(update.effective_user.id)
        username = update.effective_user.username or update.effective_user.first_name or "Unknown"
        
        if update.effective_chat.type == 'private':
            await update.message.reply_text(
                "❌ Please use this command in your church group chat."
            )
            return
        
        if chat_id not in self.data:
            await update.message.reply_text(
                f"❌ No pairing data found for this group. Use /pairme to join!"
            )
            return
        
        # Check current registration status
        is_registered = user_id in self.data[chat_id]["users"]
        
        # Get pairing history from group data
        group_data = self.data[chat_id]
        last_partner = None
        last_date = None
        
        # Look for user's last pairing in history
        if "pairing_history" in group_data:
            for pairing_record in reversed(group_data["pairing_history"]):  # Most recent first
                for pair in pairing_record["pairs"]:
                    if user_id in [pair["user1_id"], pair["user2_id"]]:
                        if pair["user1_id"] == user_id:
                            last_partner = pair["user2_name"]
                        else:
                            last_partner = pair["user1_name"]
                        last_date = pairing_record["date"]
                        break
                if last_partner:
                    break
        
        # Build status message
        status_lines = [f"👤 **Pairing Status for {username}**", ""]
        
        # Current registration status
        if is_registered:
            status_lines.append("✅ **Current Status:** Registered for next pairing")
            participant_count = len(self.data[chat_id]["users"])
            status_lines.append(f"👥 Total participants: {participant_count}")
        else:
            status_lines.append("❌ **Current Status:** Not registered")
            status_lines.append("💡 Use /pairme to join next week's pairing!")
        
        status_lines.append("")
        
        # Last pairing info
        if last_partner and last_date:
            formatted_date = datetime.fromisoformat(last_date).strftime("%B %d, %Y")
            status_lines.append(f"👥 **Last Partner:** @{last_partner}")
            status_lines.append(f"📅 **Last Pairing:** {formatted_date}")
        else:
            status_lines.append("👥 **Last Partner:** None (first time or no history)")
        
        status_lines.extend([
            "",
            "📅 **Next Pairing:** Sunday 7 PM Ethiopian time"
        ])
        
        await update.message.reply_text("\n".join(status_lines))

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command - show current participants"""
        chat_id = str(update.effective_chat.id)
        
        if update.effective_chat.type == 'private':
            await update.message.reply_text(
                "❌ Please use this command in your church group chat."
            )
            return
        
        if chat_id not in self.data or not self.data[chat_id]["users"]:
            await update.message.reply_text(
                "📝 No participants registered for weekly pairings yet.\n"
                "Use /pairme to join!"
            )
            return
        
        users = self.data[chat_id]["users"]
        participant_list = "\n".join([f"• {user_data['username']}" for user_data in users.values()])
        
        await update.message.reply_text(
            f"👥 **Current Participants ({len(users)}):**\n\n"
            f"{participant_list}\n\n"
                            f"📅 Next pairing: Sunday 7 PM Ethiopian time"
        )

    def generate_pairs(self, users_dict):
        """Generate random pairs from users dictionary"""
        user_items = list(users_dict.items())  # List of (user_id, user_data)
        random.shuffle(user_items)
        
        pairs = []
        for i in range(0, len(user_items), 2):
            if i + 1 < len(user_items):
                # Perfect pair
                pairs.append((user_items[i], user_items[i + 1]))
            else:
                # Odd person out
                pairs.append((user_items[i], None))
        
        return pairs

    async def send_weekly_pairings(self):
        """Send weekly pairings to all groups and clear the lists"""
        logger.info("Starting weekly pairing process...")
        
        if not self.data:
            logger.info("No groups registered, skipping pairings.")
            return
        
        for chat_id, group_data in self.data.items():
            try:
                users = group_data["users"]
                chat_title = group_data.get("chat_title", "Unknown Group")
                
                if not users:
                    logger.info(f"No users in group {chat_title}, skipping.")
                    continue
                
                if len(users) < 2:
                    # Send message about needing more participants
                    await self.app.bot.send_message(
                        chat_id=int(chat_id),
                        text=f"👥 Only 1 person registered for pairings this week.\n"
                             f"Need at least 2 people to create pairs.\n"
                             f"Use /pairme to join next week!"
                    )
                    continue
                
                # Generate pairs
                pairs = self.generate_pairs(users)
                current_date = datetime.now(timezone.utc).isoformat()
                
                # Store pairing history
                if "pairing_history" not in self.data[chat_id]:
                    self.data[chat_id]["pairing_history"] = []
                
                pairing_record = {
                    "date": current_date,
                    "pairs": []
                }
                
                # Create pairing message
                message_lines = [
                    f"🎯 **Weekly Pairings for {chat_title}**",
                    f"📅 {datetime.now(timezone.utc).strftime('%B %d, %Y')}",
                    "",
                ]
                
                for i, pair in enumerate(pairs, 1):
                    if pair[1] is not None:  # Normal pair
                        user1_id, user1_data = pair[0]
                        user2_id, user2_data = pair[1]
                        user1_name = user1_data['username']
                        user2_name = user2_data['username']
                        
                        message_lines.append(f"{i}. @{user1_name} ↔ @{user2_name}")
                        
                        # Store in history
                        pairing_record["pairs"].append({
                            "user1_id": user1_id,
                            "user1_name": user1_name,
                            "user2_id": user2_id,
                            "user2_name": user2_name
                        })
                    else:  # Odd person out
                        user1_id, user1_data = pair[0]
                        user1_name = user1_data['username']
                        message_lines.append(f"{i}. @{user1_name} (will join a group of 3 or meet someone new)")
                        
                        # Store in history (unpaired)
                        pairing_record["pairs"].append({
                            "user1_id": user1_id,
                            "user1_name": user1_name,
                            "user2_id": None,
                            "user2_name": None
                        })
                
                # Add pairing record to history
                self.data[chat_id]["pairing_history"].append(pairing_record)
                
                # Keep only last 10 pairing records to prevent data bloat
                if len(self.data[chat_id]["pairing_history"]) > 10:
                    self.data[chat_id]["pairing_history"] = self.data[chat_id]["pairing_history"][-10:]
                
                message_lines.extend([
                    "",
                    "💬 Reach out to your pair this week!",
                    "📝 Use /pairme to join next week's pairings"
                ])
                
                message = "\n".join(message_lines)
                
                # Send the pairing message
                await self.app.bot.send_message(chat_id=int(chat_id), text=message)
                
                # Clear the user list for this group after sending pairings
                self.data[chat_id]["users"] = {}
                logger.info(f"Sent pairings to {chat_title} and cleared user list.")
                
            except Exception as e:
                logger.error(f"Failed to send pairings to chat {chat_id}: {e}")
        
        # Save the cleared data
        self.save_data()
        logger.info("Weekly pairing process completed.")

    async def run(self):
        """Start the bot and scheduler"""
        try:
            # Start the scheduler
            self.scheduler.start()
            logger.info("Scheduler started - weekly pairings set for Sunday 9 AM UTC")
            
            # Start the bot
            logger.info("Starting bot...")
            await self.app.initialize()
            await self.app.start()
            await self.app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            
            # Keep the bot running
            while True:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error running bot: {e}")
        finally:
            # Cleanup
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
            self.scheduler.shutdown()

async def main():
    # Replace with your actual bot token from BotFather
    TOKEN = "8189938490:AAEZrsafKGy06N3QYSePHu7siZDHOto28JA"
    
    bot = ChurchPairingBot(TOKEN)
    await bot.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")