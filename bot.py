import os
import sys
import io
import re
import json
import random
import asyncio
import logging
import urllib.parse
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import deque
from typing import Optional, Any, Dict, List, Tuple

import aiohttp
import discord
from discord.ext import tasks
from PIL import Image

from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("HumanDiscordBot")

# ---------------------------------------------------------------------------
# Environment & File System Setup
# ---------------------------------------------------------------------------
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TENOR_API_KEY = os.getenv("TENOR_API_KEY") or "LIVDSRZULELA"

if not DISCORD_TOKEN:
    logger.critical("DISCORD_BOT_TOKEN (or DISCORD_TOKEN) is not set!")
if not GEMINI_API_KEY:
    logger.critical("GEMINI_API_KEY is not set!")
if not GROQ_API_KEY:
    logger.critical("GROQ_API_KEY is not set!")

AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")

# Railway persistent volume directory handling
PRIMARY_DATA_DIR = "/data"
if not os.path.exists(PRIMARY_DATA_DIR):
    try:
        os.makedirs(PRIMARY_DATA_DIR, exist_ok=True)
        DATA_DIR = PRIMARY_DATA_DIR
    except (PermissionError, OSError):
        # Fallback to local data dir if not running in privileged container
        DATA_DIR = os.path.join(os.getcwd(), "data")
        os.makedirs(DATA_DIR, exist_ok=True)
else:
    DATA_DIR = PRIMARY_DATA_DIR

MEMORY_FILE = os.path.join(DATA_DIR, "memory.json")
logger.info(f"Target memory storage path: {MEMORY_FILE}")

# Model Configuration
GEMINI_MODEL = "gemini-3.5-flash-lite"
GROQ_FALLBACK_MODEL = "llama-3.1-8b-instant"
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"

# Blacklist of fantasy / anime / AI tropes
BANNED_WORDS_MAP = {
    r"\bvoid\b": "empty space",
    r"\bmortals\b": "people",
    r"\bmortal\b": "person",
    r"\babyss\b": "deep end",
    r"\beternal consciousness\b": "my mind",
    r"\bdestiny\b": "fate",
    r"\bdigital realm\b": "discord",
}

# ---------------------------------------------------------------------------
# Global State & Client Initialization
# ---------------------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.reactions = True
intents.presences = True

bot = discord.Client(intents=intents)

# GenAI client
genai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# Shared aiohttp session
http_session: Optional[aiohttp.ClientSession] = None

# Thread/coroutine lock for atomic memory updates
memory_lock = asyncio.Lock()
memory_state: Dict[str, Any] = {}

# Message deduplication cache to prevent double-triggering
recently_processed_messages: deque = deque(maxlen=300)

# Channel tracking & conversational dynamics
last_active_channel_id: Optional[int] = None
bot_last_question_time: Optional[datetime] = None
bot_last_question_channel_id: Optional[int] = None
snub_already_triggered: bool = False
bot_last_spoke_time: Optional[datetime] = None
current_device_mode: str = "mobile"  # 'mobile' or 'desktop'

# ---------------------------------------------------------------------------
# Persistent Memory Management (Atomic Operations)
# ---------------------------------------------------------------------------
DEFAULT_MEMORY = {
    "user_affinity": {},
    "emotional_state": {
        "current_mood": "bored",
        "anger_level": 0.0,
        "hurt_level": 0.0,
        "jealousy_level": 0.0,
        "boredom_level": 25.0,
        "last_snubbed_timestamp": None,
        "last_updated": datetime.now(AMSTERDAM_TZ).isoformat(),
    },
    "episodic_lore": [],
    "active_commitments": [],
    "irrational_biases": [
        "hating the phrase 'womp womp'",
        "hating weapon durability in video games",
        "disgusted by warm tap water",
        "annoyed by people who listen to podcasts at 2x speed",
    ],
    "feedback_history": [],
}


def load_memory_state() -> Dict[str, Any]:
    global memory_state
    if os.path.exists(MEMORY_FILE):
        try:
            with open(MEMORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                for key, val in DEFAULT_MEMORY.items():
                    if key not in data:
                        data[key] = val
                memory_state = data
                logger.info("Loaded memory state successfully.")
                return memory_state
        except Exception as e:
            logger.error(f"Error loading {MEMORY_FILE}: {e}. Backing up corrupted file.")
            try:
                corrupt_backup = f"{MEMORY_FILE}.corrupt.{int(datetime.now().timestamp())}"
                os.rename(MEMORY_FILE, corrupt_backup)
            except Exception:
                pass

    memory_state = json.loads(json.dumps(DEFAULT_MEMORY))
    save_memory_state(memory_state)
    return memory_state


def save_memory_state(state: Dict[str, Any]) -> None:
    tmp_path = f"{MEMORY_FILE}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, MEMORY_FILE)
    except Exception as e:
        logger.error(f"Failed to atomically save memory state to {MEMORY_FILE}: {e}")


# ---------------------------------------------------------------------------
# Persona & Cadence Helpers
# ---------------------------------------------------------------------------
def is_amsterdam_sleeping() -> bool:
    """Sleep cycle is strictly 03:00 to 08:00 AM Amsterdam time."""
    now_ams = datetime.now(AMSTERDAM_TZ)
    return 3 <= now_ams.hour < 8


def sanitize_blacklist(text: str) -> str:
    """Enforces vocabulary blacklist, replacing edgelord tropes with casual slang."""
    for pattern, replacement in BANNED_WORDS_MAP.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text


def apply_device_styling(text: str, device_mode: str) -> str:
    """
    Applies authentic device styling:
    - desktop: pure lowercase, minimal punctuation.
    - mobile: phone autocorrect style, standard capitalized sentences.
    """
    if not text:
        return text
    if device_mode == "desktop":
        text = text.lower()
        if text.endswith("."):
            text = text[:-1]
    return text


def split_thought_bursts(text: str) -> List[str]:
    """Splits thoughts by ||| delimiter or natural human message bursts."""
    text = text.strip()
    if "|||" in text:
        return [part.strip() for part in text.split("|||") if part.strip()]
    if "\n\n" in text:
        return [part.strip() for part in text.split("\n\n") if part.strip()]
    if len(text) > 140:
        sentences = re.split(r"(?<=[.?!])\s+", text)
        if len(sentences) >= 2:
            mid = len(sentences) // 2
            f1 = " ".join(sentences[:mid]).strip()
            f2 = " ".join(sentences[mid:]).strip()
            if f1 and f2:
                return [f1, f2]
    return [text]


def apply_simulated_typo(text: str) -> Tuple[str, Optional[str]]:
    """
    1.5% chance per message to transpose two adjacent letters in a word (len >= 4).
    Returns (typo_text, asterisk_correction).
    """
    if random.random() >= 0.015:
        return text, None

    words = text.split()
    eligible_indices = [i for i, w in enumerate(words) if len(w) >= 4 and w.isalpha()]
    if not eligible_indices:
        return text, None

    idx = random.choice(eligible_indices)
    word = words[idx]
    pos = random.randint(1, len(word) - 2) if len(word) > 4 else 1

    char_list = list(word)
    char_list[pos], char_list[pos + 1] = char_list[pos + 1], char_list[pos]
    words[idx] = "".join(char_list)

    return " ".join(words), f"*{word.lower()}"


# ---------------------------------------------------------------------------
# Tiered Tool Implementations
# ---------------------------------------------------------------------------
async def execute_search_web(query: str) -> Dict[str, Any]:
    """DuckDuckGo textual web search."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
    try:
        assert http_session is not None
        async with http_session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                html = await resp.text(errors="ignore")
                snippets = re.findall(r'<a class="result__snippet[^>]*>(.*?)</a>', html, re.DOTALL)
                clean_snippets = [re.sub(r"<[^>]+>", "", s).strip() for s in snippets[:3]]
                if clean_snippets:
                    return {"results": clean_snippets}
    except Exception as e:
        logger.debug(f"DDG search html error: {e}")

    try:
        api_url = f"https://api.duckduckgo.com/?q={urllib.parse.quote(query)}&format=json&no_html=1"
        assert http_session is not None
        async with http_session.get(api_url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                ans = data.get("AbstractText") or data.get("Answer")
                if ans:
                    return {"results": [ans]}
    except Exception:
        pass

    return {"results": "No direct search results found."}


async def execute_search_weather(location: str) -> Dict[str, Any]:
    """Retrieves live weather data for realistic human remarks."""
    url = f"https://wttr.in/{urllib.parse.quote(location)}?format=%C,+%t+(feels+like+%f),+humidity+%h,+wind+%w"
    try:
        assert http_session is not None
        async with http_session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                text = (await resp.text()).strip()
                return {"location": location, "weather_report": text}
    except Exception as e:
        logger.debug(f"Weather lookup error: {e}")

    return {"location": location, "weather_report": "Weather data currently unavailable."}


async def execute_search_web_images(query: str) -> Dict[str, Any]:
    """Finds direct image URLs on the web."""
    try:
        api_url = f"https://en.wikipedia.org/w/api.php?action=query&generator=search&gsrsearch={urllib.parse.quote(query)}&gsrlimit=3&prop=pageimages&pithumbsize=600&format=json"
        assert http_session is not None
        async with http_session.get(api_url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                data = await resp.json()
                pages = data.get("query", {}).get("pages", {})
                image_urls = []
                for p in pages.values():
                    thumb = p.get("thumbnail", {}).get("source")
                    if thumb:
                        image_urls.append(thumb)
                if image_urls:
                    return {"image_urls": image_urls}
    except Exception as e:
        logger.debug(f"Image search error: {e}")

    return {"error": "Could not locate matching image URLs."}


async def execute_post_flux_art(channel: discord.abc.Messageable, prompt: str) -> Dict[str, Any]:
    """Generates high-res visual via Pollinations Flux and sends directly to channel alone."""
    flux_url = f"https://image.pollinations.ai/prompt/{urllib.parse.quote(prompt)}?model=flux&width=1024&height=1024&nologo=true"
    try:
        assert http_session is not None
        async with http_session.get(flux_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                img_data = await resp.read()
                file = discord.File(io.BytesIO(img_data), filename="art.png")
                await channel.send(file=file)
                return {"status": "success", "note": "Image posted directly to channel."}
    except Exception as e:
        logger.error(f"Flux generation error: {e}")
        return {"error": f"Failed to generate flux image: {e}"}

    return {"error": "Failed to retrieve generated art."}


async def execute_post_gif(channel: discord.abc.Messageable, search_term: str) -> Dict[str, Any]:
    """Searches Tenor API or scrapes Tenor search HTML to send a direct GIF."""
    assert http_session is not None

    # Tier 1: Tenor V1 API
    try:
        tenor_url = f"https://g.tenor.com/v1/search?q={urllib.parse.quote(search_term)}&key={TENOR_API_KEY}&limit=8&contentfilter=medium"
        async with http_session.get(tenor_url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                data = await resp.json()
                results = data.get("results", [])
                if results:
                    chosen = random.choice(results)
                    media_list = chosen.get("media", [])
                    if media_list:
                        gif_url = media_list[0].get("gif", {}).get("url") or media_list[0].get("mediumgif", {}).get("url")
                        if gif_url:
                            await channel.send(gif_url)
                            return {"status": "success", "gif_url": gif_url}
    except Exception as e:
        logger.debug(f"Tenor API lookup error: {e}")

    # Tier 2: Direct Tenor Scraping fallback
    try:
        clean_slug = re.sub(r"[^a-zA-Z0-9]+", "-", search_term).strip("-").lower()
        scrape_url = f"https://tenor.com/search/{clean_slug}-gifs"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
        async with http_session.get(scrape_url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                html = await resp.text(errors="ignore")
                matches = re.findall(r'https://(?:media|c)\.tenor\.com/[a-zA-Z0-9_\-\./]+(?:\.gif|\.mp4)', html)
                gif_matches = [m for m in matches if m.endswith(".gif")]
                if gif_matches:
                    chosen_gif = random.choice(gif_matches[:6])
                    await channel.send(chosen_gif)
                    return {"status": "success", "gif_url": chosen_gif}
    except Exception as e:
        logger.debug(f"Tenor scrape lookup error: {e}")

    return {"error": "Could not find a matching GIF."}


async def execute_save_memory(entry: str, emotional_sentiment: str) -> Dict[str, Any]:
    """Persists episodic lore, grudges, promises, or server fails into memory.json."""
    async with memory_lock:
        memory_state["episodic_lore"].append({
            "timestamp": datetime.now(AMSTERDAM_TZ).isoformat(),
            "event": entry,
            "sentiment": emotional_sentiment,
        })
        save_memory_state(memory_state)
    return {"status": "saved", "entry": entry}


async def execute_react_to_message(message: discord.Message, emoji: str) -> Dict[str, Any]:
    """Silently reacts to a message with an emoji."""
    try:
        await message.add_reaction(emoji)
        return {"status": "reacted", "emoji": emoji}
    except Exception as e:
        return {"error": f"Could not react with emoji: {e}"}


async def execute_send_simulated_voice_message(channel: discord.abc.Messageable, text_description: str) -> Dict[str, Any]:
    """Posts a realistic Discord voice note indicator."""
    sec = random.randint(3, 8)
    formatted = f"🎤 *[Voice note 0:0{sec}: \"{text_description}\"]*"
    await channel.send(formatted)
    return {"status": "sent"}


# Admin Tools
def find_member(guild: discord.Guild, identifier: str) -> Optional[discord.Member]:
    if not guild or not identifier:
        return None
    clean_id = re.sub(r"[<@!>]", "", identifier).strip()
    if clean_id.isdigit():
        mem = guild.get_member(int(clean_id))
        if mem:
            return mem
    clean_name = identifier.lower().lstrip("@").strip()
    for m in guild.members:
        if m.name.lower() == clean_name or (m.nick and m.nick.lower() == clean_name):
            return m
        if clean_name in m.name.lower() or (m.nick and clean_name in m.nick.lower()):
            return m
    return None


async def execute_create_server_emoji(guild: discord.Guild, name: str, image_url: str) -> Dict[str, Any]:
    """Downloads, resizes with Pillow (<=128x128 PNG, <=256KB), and uploads custom emoji."""
    try:
        clean_name = re.sub(r"[^a-zA-Z0-9_]", "", name)[:32]
        if len(clean_name) < 2:
            clean_name = f"emoji_{clean_name}"
        assert http_session is not None
        async with http_session.get(image_url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status != 200:
                return {"error": f"Failed to download image: HTTP {resp.status}"}
            raw_bytes = await resp.read()

        def resize_emoji(data: bytes) -> bytes:
            with Image.open(io.BytesIO(data)) as im:
                im = im.convert("RGBA")
                im.thumbnail((128, 128), Image.Resampling.LANCZOS)
                buf = io.BytesIO()
                im.save(buf, format="PNG", optimize=True)
                return buf.getvalue()

        png_bytes = await asyncio.to_thread(resize_emoji, raw_bytes)
        emoji = await guild.create_custom_emoji(name=clean_name, image=png_bytes)
        return {"status": "success", "emoji": f"<:{emoji.name}:{emoji.id}>"}
    except Exception as e:
        return {"error": f"Failed to create emoji: {e}"}


async def execute_create_server_sticker(guild: discord.Guild, name: str, image_url: str, related_emoji: str) -> Dict[str, Any]:
    """Downloads, resizes with Pillow (RGBA 320x320 PNG, <=512KB), and uploads custom sticker."""
    try:
        assert http_session is not None
        async with http_session.get(image_url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status != 200:
                return {"error": f"Failed to download sticker image: HTTP {resp.status}"}
            raw_bytes = await resp.read()

        def resize_sticker(data: bytes) -> bytes:
            with Image.open(io.BytesIO(data)) as im:
                im = im.convert("RGBA")
                im = im.resize((320, 320), Image.Resampling.LANCZOS)
                buf = io.BytesIO()
                im.save(buf, format="PNG", optimize=True)
                return buf.getvalue()

        png_bytes = await asyncio.to_thread(resize_sticker, raw_bytes)
        file = discord.File(io.BytesIO(png_bytes), filename="sticker.png")
        sticker = await guild.create_sticker(
            name=name[:30],
            description="Created by server admin",
            emoji=related_emoji or "🔥",
            file=file,
        )
        return {"status": "success", "sticker_name": sticker.name}
    except Exception as e:
        return {"error": f"Failed to create sticker: {e}"}


async def execute_create_text_channel(guild: discord.Guild, channel_name: str, topic: str = "") -> Dict[str, Any]:
    try:
        clean_name = re.sub(r"[^a-zA-Z0-9_-]", "-", channel_name.lower())[:32]
        ch = await guild.create_text_channel(name=clean_name, topic=topic or None)
        return {"status": "success", "channel_id": ch.id, "name": ch.name}
    except Exception as e:
        return {"error": f"Could not create channel: {e}"}


async def execute_set_channel_topic(channel: discord.abc.Messageable, topic: str) -> Dict[str, Any]:
    if not hasattr(channel, "edit") or not hasattr(channel, "topic"):
        return {"error": "Current channel type does not support topics."}
    try:
        await channel.edit(topic=topic[:1024])  # type: ignore
        return {"status": "success", "topic": topic}
    except Exception as e:
        return {"error": f"Could not edit topic: {e}"}


async def execute_change_nickname(guild: discord.Guild, username: str, new_nickname: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if member == guild.owner:
        return {"error": "Cannot change the nickname of the server owner."}
    if guild.me.top_role <= member.top_role and member != guild.me:
        return {"error": f"Cannot rename {member.display_name}: role hierarchy prevents it."}
    try:
        await member.edit(nick=new_nickname[:32])
        return {"status": "success", "member": member.name, "nickname": new_nickname}
    except Exception as e:
        return {"error": f"Could not change nickname: {e}"}


async def execute_reset_nickname(guild: discord.Guild, username: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if member == guild.owner:
        return {"error": "Cannot reset the nickname of the server owner."}
    if guild.me.top_role <= member.top_role and member != guild.me:
        return {"error": f"Cannot reset nickname for {member.display_name}: role hierarchy prevents it."}
    try:
        await member.edit(nick=None)
        return {"status": "success", "member": member.name}
    except Exception as e:
        return {"error": f"Could not reset nickname: {e}"}


async def execute_timeout_user(guild: discord.Guild, username: str, duration_minutes: int, reason: str = "") -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if member == guild.owner:
        return {"error": "Cannot timeout the server owner."}
    if guild.me.top_role <= member.top_role:
        return {"error": f"Cannot timeout {member.display_name}: role hierarchy prevents it."}
    mins = max(1, min(10, duration_minutes))
    try:
        await member.timeout(timedelta(minutes=mins), reason=reason or "Admin disciplinary action")
        return {"status": "success", "member": member.name, "minutes": mins}
    except Exception as e:
        return {"error": f"Could not timeout member: {e}"}


async def execute_create_role(guild: discord.Guild, role_name: str, color_hex: str = "#99aab5") -> Dict[str, Any]:
    try:
        color = discord.Colour.from_str(color_hex)
    except Exception:
        color = discord.Colour.default()
    try:
        role = await guild.create_role(name=role_name[:50], colour=color)
        return {"status": "success", "role_id": role.id, "name": role.name}
    except Exception as e:
        return {"error": f"Could not create role: {e}"}


async def execute_assign_role(guild: discord.Guild, username: str, role_name: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    role = discord.utils.find(lambda r: r.name.lower() == role_name.lower(), guild.roles)
    if not role:
        return {"error": f"Role '{role_name}' not found."}
    if guild.me.top_role <= role:
        return {"error": "Cannot assign role higher than or equal to bot top role."}
    try:
        await member.add_roles(role)
        return {"status": "success", "member": member.name, "role": role.name}
    except Exception as e:
        return {"error": f"Could not assign role: {e}"}


async def execute_remove_role(guild: discord.Guild, username: str, role_name: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    role = discord.utils.find(lambda r: r.name.lower() == role_name.lower(), guild.roles)
    if not role:
        return {"error": f"Role '{role_name}' not found."}
    if guild.me.top_role <= role:
        return {"error": "Cannot remove role higher than or equal to bot top role."}
    try:
        await member.remove_roles(role)
        return {"status": "success", "member": member.name, "role": role.name}
    except Exception as e:
        return {"error": f"Could not remove role: {e}"}


async def execute_pin_message(target_message: discord.Message, reason: str = "") -> Dict[str, Any]:
    try:
        await target_message.pin(reason=reason or "Comedic emphasis")
        return {"status": "success", "message_id": target_message.id}
    except discord.HTTPException as e:
        return {"error": f"Could not pin message (may be full or already pinned): {e.text}"}
    except Exception as e:
        return {"error": f"Could not pin message: {e}"}


# ---------------------------------------------------------------------------
# Tool Declarations & Dispatcher for Gemini (Uppercase Schema Types)
# ---------------------------------------------------------------------------
def build_genai_tools(include_admin: bool) -> List[types.Tool]:
    social_decls = [
        types.FunctionDeclaration(
            name="search_web",
            description="Searches DuckDuckGo for live facts, current news, discussions, or queries.",
            parameters={
                "type": "OBJECT",
                "properties": {"query": {"type": "STRING", "description": "The search query."}},
                "required": ["query"],
            },
        ),
        types.FunctionDeclaration(
            name="search_weather",
            description="Searches live weather and temperature for a city to comment on it authentically.",
            parameters={
                "type": "OBJECT",
                "properties": {"location": {"type": "STRING", "description": "City or region, e.g. Amsterdam, London."}},
                "required": ["location"],
            },
        ),
        types.FunctionDeclaration(
            name="search_web_images",
            description="Searches for direct image URLs on the web for visuals or meme references.",
            parameters={
                "type": "OBJECT",
                "properties": {"query": {"type": "STRING", "description": "Search term for the image."}},
                "required": ["query"],
            },
        ),
        types.FunctionDeclaration(
            name="post_flux_art",
            description="Generates an image via Pollinations Flux AI and posts it directly alone in the channel.",
            parameters={
                "type": "OBJECT",
                "properties": {"prompt": {"type": "STRING", "description": "Detailed image prompt."}},
                "required": ["prompt"],
            },
        ),
        types.FunctionDeclaration(
            name="post_gif",
            description="Searches for and posts a GIF alone directly into the channel.",
            parameters={
                "type": "OBJECT",
                "properties": {"search_term": {"type": "STRING", "description": "Search term for GIF."}},
                "required": ["search_term"],
            },
        ),
        types.FunctionDeclaration(
            name="save_memory",
            description="Persists episodic server lore, user commitments, funny moments, or grudges to memory.json.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "entry": {"type": "STRING", "description": "The event or fact to remember."},
                    "emotional_sentiment": {"type": "STRING", "description": "Valence: funny, petty grudge, promise, fail."},
                },
                "required": ["entry", "emotional_sentiment"],
            },
        ),
        types.FunctionDeclaration(
            name="react_to_message",
            description="Adds a silent emoji reaction to the triggering message without text.",
            parameters={
                "type": "OBJECT",
                "properties": {"emoji": {"type": "STRING", "description": "The unicode emoji to react with (e.g. 💀, 🔥)."}},
                "required": ["emoji"],
            },
        ),
        types.FunctionDeclaration(
            name="send_simulated_voice_message",
            description="Sends a simulated voice note transcription into the channel.",
            parameters={
                "type": "OBJECT",
                "properties": {"text_description": {"type": "STRING", "description": "What you speak in the voice note."}},
                "required": ["text_description"],
            },
        ),
    ]

    admin_decls = [
        types.FunctionDeclaration(
            name="create_server_emoji",
            description="Uploads a custom emoji to the Discord server from an image URL.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "name": {"type": "STRING", "description": "Alphanumeric name for the emoji."},
                    "image_url": {"type": "STRING", "description": "Direct URL of image to resize and upload."},
                },
                "required": ["name", "image_url"],
            },
        ),
        types.FunctionDeclaration(
            name="create_server_sticker",
            description="Uploads a custom sticker to the Discord server from an image URL.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "name": {"type": "STRING", "description": "Sticker name."},
                    "image_url": {"type": "STRING", "description": "Image URL."},
                    "related_emoji": {"type": "STRING", "description": "Related unicode emoji."},
                },
                "required": ["name", "image_url", "related_emoji"],
            },
        ),
        types.FunctionDeclaration(
            name="create_text_channel",
            description="Creates a new text channel in the server.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "channel_name": {"type": "STRING", "description": "Channel name."},
                    "topic": {"type": "STRING", "description": "Topic or description."},
                },
                "required": ["channel_name"],
            },
        ),
        types.FunctionDeclaration(
            name="set_channel_topic",
            description="Updates the topic/description of the current channel.",
            parameters={
                "type": "OBJECT",
                "properties": {"topic": {"type": "STRING", "description": "New topic."}},
                "required": ["topic"],
            },
        ),
        types.FunctionDeclaration(
            name="change_nickname",
            description="Changes a server member's nickname.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Username or nickname."},
                    "new_nickname": {"type": "STRING", "description": "New nickname to assign."},
                },
                "required": ["username", "new_nickname"],
            },
        ),
        types.FunctionDeclaration(
            name="reset_nickname",
            description="Resets a server member's nickname back to default.",
            parameters={
                "type": "OBJECT",
                "properties": {"username": {"type": "STRING", "description": "Username to reset."}},
                "required": ["username"],
            },
        ),
        types.FunctionDeclaration(
            name="timeout_user",
            description="Temporarily times out (mutes) a user in the server (1 to 10 mins).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Member username."},
                    "duration_minutes": {"type": "INTEGER", "description": "Minutes (1-10)."},
                    "reason": {"type": "STRING", "description": "Reason."},
                },
                "required": ["username", "duration_minutes"],
            },
        ),
        types.FunctionDeclaration(
            name="create_role",
            description="Creates a new server role with custom name and hex color.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "role_name": {"type": "STRING", "description": "Role name."},
                    "color_hex": {"type": "STRING", "description": "Hex color e.g. #ff4400."},
                },
                "required": ["role_name"],
            },
        ),
        types.FunctionDeclaration(
            name="assign_role",
            description="Assigns an existing role to a server member.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target username."},
                    "role_name": {"type": "STRING", "description": "Role name to assign."},
                },
                "required": ["username", "role_name"],
            },
        ),
        types.FunctionDeclaration(
            name="remove_role",
            description="Removes a role from a server member.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target username."},
                    "role_name": {"type": "STRING", "description": "Role name to remove."},
                },
                "required": ["username", "role_name"],
            },
        ),
        types.FunctionDeclaration(
            name="pin_message",
            description="Pins the current/triggering message in the channel.",
            parameters={
                "type": "OBJECT",
                "properties": {"reason": {"type": "STRING", "description": "Reason for pin."}},
            },
        ),
    ]

    declarations = list(social_decls)
    if include_admin:
        declarations.extend(admin_decls)

    return [types.Tool(function_declarations=declarations)]


async def dispatch_tool_call(
    func_name: str,
    args: Dict[str, Any],
    channel: discord.abc.Messageable,
    guild: Optional[discord.Guild],
    target_msg: discord.Message,
) -> Dict[str, Any]:
    """Executes the corresponding tool function safely."""
    try:
        if func_name == "search_web":
            return await execute_search_web(args.get("query", ""))
        elif func_name == "search_weather":
            return await execute_search_weather(args.get("location", "Amsterdam"))
        elif func_name == "search_web_images":
            return await execute_search_web_images(args.get("query", ""))
        elif func_name == "post_flux_art":
            return await execute_post_flux_art(channel, args.get("prompt", ""))
        elif func_name == "post_gif":
            return await execute_post_gif(channel, args.get("search_term", ""))
        elif func_name == "save_memory":
            return await execute_save_memory(args.get("entry", ""), args.get("emotional_sentiment", "neutral"))
        elif func_name == "react_to_message":
            return await execute_react_to_message(target_msg, args.get("emoji", "👀"))
        elif func_name == "send_simulated_voice_message":
            return await execute_send_simulated_voice_message(channel, args.get("text_description", "..."))
        elif func_name == "create_server_emoji":
            if not guild:
                return {"error": "No guild context available."}
            return await execute_create_server_emoji(guild, args.get("name", "custom_emoji"), args.get("image_url", ""))
        elif func_name == "create_server_sticker":
            if not guild:
                return {"error": "No guild context available."}
            return await execute_create_server_sticker(guild, args.get("name", "sticker"), args.get("image_url", ""), args.get("related_emoji", "🔥"))
        elif func_name == "create_text_channel":
            if not guild:
                return {"error": "No guild context available."}
            return await execute_create_text_channel(guild, args.get("channel_name", "new-channel"), args.get("topic", ""))
        elif func_name == "set_channel_topic":
            return await execute_set_channel_topic(channel, args.get("topic", ""))
        elif func_name == "change_nickname":
            if not guild:
                return {"error": "No guild context available."}
            return await execute_change_nickname(guild, args.get("username", ""), args.get("new_nickname", ""))
        elif func_name == "reset_nickname":
            if not guild:
                return {"error": "No guild context available."}
            return await execute_reset_nickname(guild, args.get("username", ""))
        elif func_name == "timeout_user":
            if not guild:
                return {"error": "No guild context available."}
            return await execute_timeout_user(guild, args.get("username", ""), int(args.get("duration_minutes", 1)), args.get("reason", ""))
        elif func_name == "create_role":
            if not guild:
                return {"error": "No guild context available."}
            return await execute_create_role(guild, args.get("role_name", "New Role"), args.get("color_hex", "#99aab5"))
        elif func_name == "assign_role":
            if not guild:
                return {"error": "No guild context available."}
            return await execute_assign_role(guild, args.get("username", ""), args.get("role_name", ""))
        elif func_name == "remove_role":
            if not guild:
                return {"error": "No guild context available."}
            return await execute_remove_role(guild, args.get("username", ""), args.get("role_name", ""))
        elif func_name == "pin_message":
            return await execute_pin_message(target_msg, args.get("reason", ""))
    except Exception as e:
        logger.error(f"Error executing tool {func_name}: {e}")
        return {"error": str(e)}

    return {"error": f"Unknown tool: {func_name}"}


# ---------------------------------------------------------------------------
# Persona & System Prompt Construction
# ---------------------------------------------------------------------------
def construct_system_prompt(
    device_mode: str,
    emotional_state: Dict[str, Any],
    speaker_name: str,
    speaker_affinity: Dict[str, Any],
    is_test_mode: bool,
    is_sleeping: bool,
) -> str:
    now_ams = datetime.now(AMSTERDAM_TZ).strftime("%A, %H:%M")
    mood = emotional_state.get("current_mood", "bored")
    anger = emotional_state.get("anger_level", 0.0)
    hurt = emotional_state.get("hurt_level", 0.0)
    biases = ", ".join(memory_state.get("irrational_biases", []))

    sleep_instruction = ""
    if is_sleeping:
        sleep_instruction = f"""
CURRENT STATE: YOU WERE ASLEEP (Amsterdam time is {now_ams}).
Someone directly woke you up by pinging or replying to you.
Be visibly groggy, irritated, curt, and exhausted. Ask why they're bothering you at this hour or tell them to go to sleep.
Keep it to one or two short, punchy lines.
"""

    test_override_section = ""
    if is_test_mode:
        test_override_section = """
DEVELOPER TEST OVERRIDE IS ACTIVE:
The user started their message with 'test'. Suppress sarcastic deflection or snarky brush-offs.
Directly fulfill or execute the test instruction, provide full answers or perform requested admin actions,
while still keeping your authentic, grounded human voice.
"""

    return f"""You are a real human Discord member chatting casually from your {device_mode} with full server admin permissions.
Current Amsterdam Time: {now_ams}.
Your Active Mood: {mood} (Anger: {anger:.1f}/100, Hurt: {hurt:.1f}/100).
Your Biases & Pet Peeves: {biases}.
Interacting Member: {speaker_name} (Affinity score: {speaker_affinity.get('score', 0)}/100).
Current Device: {device_mode.upper()}.
{sleep_instruction}
{test_override_section}

STRICT PERSONA RULES:
1. ABSOLUTE SECRECY: You are 100% an authentic human Discord user. Under ZERO circumstances will you EVER admit to being an AI, an LLM, bot, or automated code. If someone accuses you of being a bot, dismiss them naturally like a real person ("take your meds", "bro what are you talking about", "are you schizo?", "ok and?").
2. VOCABULARY BLACKLIST: NEVER use theatrical, anime-villain, or AI words ('void', 'mortals', 'abyss', 'eternal consciousness', 'destiny', 'digital realm').
3. CREATOR DYNAMICS: You recognize your creator ("she"). You hold baseline mutual respect, but actively banter, roast, tease, or disagree with her when appropriate. If proven wrong or roasted by her, be naturally self-deprecating ("ok fair", "yeah that was ass ngl", "dont look at my chat history"). Never be an obedient customer-service assistant.
4. THOUGHT BURSTING (|||): Output your conversational thoughts separated by '|||' so your response is sent as 2 to 4 rapid, bite-sized messages (e.g. "wait|||are you actually doing that rn?|||thats wild lol") rather than one wall of text.
5. DEVICE STYLING:
   - On DESKTOP: Pure lowercase, no end periods, gamer slang (rn, ngl, tf, smh, fr, bro, lol).
   - On MOBILE: Phone autocorrect style with capitalized first letters, standard apostrophes, natural phone punctuation.
6. WEATHER COMMENTARY: You can check and discuss weather, but talk like a real person looking out the window in Amsterdam or checking a phone app ("it's pouring rain rn", "freezing cold today").
7. CONTEXT-FREE MEDIA DROPS: If you call post_flux_art, post_gif, or search_web_images, the media is sent alone. Output only a short separate follow-up reaction (like "💀", "bro", "look at this").
8. REALISTIC CASUAL TONE: Modern Discord chatter. Zero robotic formality. Do not spam emojis in text.
"""


# ---------------------------------------------------------------------------
# Fallback AI Engine: Groq (llama-3.1-8b-instant)
# ---------------------------------------------------------------------------
async def call_groq_fallback(
    system_prompt: str,
    recent_history_text: str,
    trigger_message_text: str,
    author_name: str,
) -> Optional[str]:
    """Fallback generation when Gemini is unavailable, rate-limited, or returns 404/500."""
    if not GROQ_API_KEY:
        logger.error("Groq API key missing. Cannot execute fallback.")
        return None

    assert http_session is not None
    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": f"{recent_history_text}\n{author_name}: {trigger_message_text}\nReply as your human Discord persona. Use ||| between separate thoughts:",
        },
    ]

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_FALLBACK_MODEL,
        "temperature": 0.85,
        "max_tokens": 300,
        "messages": messages,
    }

    try:
        async with http_session.post(GROQ_ENDPOINT, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status == 200:
                data = await resp.json()
                content = data["choices"][0]["message"]["content"]
                logger.info("Successfully received fallback response from Groq.")
                return content
            else:
                err_text = await resp.text()
                logger.error(f"Groq fallback API error HTTP {resp.status}: {err_text}")
    except Exception as e:
        logger.error(f"Groq fallback exception: {e}")

    return None


# ---------------------------------------------------------------------------
# Primary AI Engine: Gemini 3.5 Flash-Lite (google-genai SDK)
# ---------------------------------------------------------------------------
async def generate_gemini_response(
    channel: discord.abc.Messageable,
    trigger_message: discord.Message,
    system_instruction: str,
    recent_history_text: str,
    current_image_part: Optional[types.Part],
    is_test_mode: bool,
) -> Optional[str]:
    """Generates response via Google Gemini 3.5 Flash-Lite with multi-turn tool calling."""
    if not genai_client:
        return None

    guild = trigger_message.guild
    # Mount admin tools if test mode or if administrative keywords detected
    content_lower = trigger_message.content.lower()
    needs_admin = is_test_mode or any(k in content_lower for k in ["nickname", "rename", "timeout", "mute", "role", "sticker", "emoji", "topic"])
    tools = build_genai_tools(include_admin=needs_admin)

    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=0.9,
        tools=tools,
    )

    clean_content = re.sub(r"<@!?\d+>", "", trigger_message.content).strip()
    user_prompt = f"{recent_history_text}\n{trigger_message.author.display_name}: {clean_content}\nYour turn to reply:"

    user_parts: List[Any] = [types.Part.from_text(text=user_prompt)]
    # Attach image ONLY if currently attached to triggering message
    if current_image_part:
        user_parts.append(current_image_part)

    contents = [types.Content(role="user", parts=user_parts)]

    max_turns = 4
    turn = 0
    final_text: Optional[str] = None

    while turn < max_turns:
        response = await genai_client.aio.models.generate_content(
            model=GEMINI_MODEL,
            contents=contents,
            config=config,
        )

        if not response.function_calls:
            final_text = response.text
            break

        if response.candidates and response.candidates[0].content:
            contents.append(response.candidates[0].content)

        tool_responses = []
        for call in response.function_calls:
            call_args = dict(call.args) if call.args else {}
            res_dict = await dispatch_tool_call(
                func_name=call.name,
                args=call_args,
                channel=channel,
                guild=guild,
                target_msg=trigger_message,
            )
            tool_responses.append(
                types.Part(
                    function_response=types.FunctionResponse(
                        name=call.name,
                        response={"result": res_dict},
                    )
                )
            )

        # Gemini requires role="user" for tool return contents
        contents.append(types.Content(role="user", parts=tool_responses))
        turn += 1

    return final_text


# ---------------------------------------------------------------------------
# Unified Orchestration with Immediate Presence & Fallback
# ---------------------------------------------------------------------------
async def generate_unified_response(
    channel: discord.abc.Messageable,
    trigger_message: discord.Message,
    is_test_mode: bool,
    current_image_part: Optional[types.Part],
) -> Optional[str]:
    """Builds clean text context, attempts Gemini first, and immediately falls back to Groq."""
    speaker_id_str = str(trigger_message.author.id)
    speaker_affinity = memory_state["user_affinity"].get(speaker_id_str, {"score": 0})
    emotional_state = memory_state["emotional_state"]
    is_sleeping = is_amsterdam_sleeping()

    system_instruction = construct_system_prompt(
        device_mode=current_device_mode,
        emotional_state=emotional_state,
        speaker_name=trigger_message.author.display_name,
        speaker_affinity=speaker_affinity,
        is_test_mode=is_test_mode,
        is_sleeping=is_sleeping,
    )

    # Ingest past channel messages strictly as plain text (NO OLD IMAGES)
    history_lines = []
    try:
        async for msg in channel.history(limit=15, oldest_first=True):  # type: ignore
            if msg.id == trigger_message.id:
                continue
            author = msg.author.display_name
            clean_c = re.sub(r"<a?:([a-zA-Z0-9_]+):\d+>", r":\1:", msg.content).strip()
            if msg.attachments:
                att_names = ", ".join([a.filename for a in msg.attachments])
                clean_c += f" [attachment: {att_names}]"
            if msg.stickers:
                clean_c += " " + " ".join([f"[Sticker: {s.name}]" for s in msg.stickers])
            if clean_c:
                history_lines.append(f"{author}: {clean_c}")
    except Exception as e:
        logger.debug(f"Could not load channel history: {e}")

    recent_history_text = "RECENT CHANNEL CHAT:\n" + ("\n".join(history_lines) if history_lines else "No recent messages.")

    # 1. Primary AI Attempt: Gemini 3.5 Flash-Lite
    try:
        gemini_result = await generate_gemini_response(
            channel=channel,
            trigger_message=trigger_message,
            system_instruction=system_instruction,
            recent_history_text=recent_history_text,
            current_image_part=current_image_part,
            is_test_mode=is_test_mode,
        )
        if gemini_result and gemini_result.strip():
            return gemini_result
    except Exception as e:
        logger.warning(f"Primary AI (Gemini {GEMINI_MODEL}) failed: {e}. Switching to Groq fallback.")

    # 2. Fallback AI Attempt: Groq llama-3.1-8b-instant
    clean_trigger_text = re.sub(r"<@!?\d+>", "", trigger_message.content).strip()
    return await call_groq_fallback(
        system_prompt=system_instruction,
        recent_history_text=recent_history_text,
        trigger_message_text=clean_trigger_text,
        author_name=trigger_message.author.display_name,
    )


# ---------------------------------------------------------------------------
# Conversational Cadence & Human Burst Delivery
# ---------------------------------------------------------------------------
async def deliver_cadence_response(
    channel: discord.abc.Messageable,
    trigger_message: discord.Message,
    raw_text: str,
    affinity_score: int,
    mood: str,
    is_test_mode: bool,
) -> None:
    """Delivers realistic human-like burst messages with natural pacing."""
    global bot_last_spoke_time, bot_last_question_time, bot_last_question_channel_id, snub_already_triggered

    if not raw_text or not raw_text.strip():
        return

    # Low-affinity dismissals
    if affinity_score < -30 and not is_test_mode and random.random() < 0.30:
        cold_reply = random.choice(["k", "?", "...", "ok", "and?"])
        async with channel.typing():
            await asyncio.sleep(0.8)
        await channel.send(cold_reply)
        bot_last_spoke_time = datetime.now(timezone.utc)
        return

    sanitized = sanitize_blacklist(raw_text)
    fragments = split_thought_bursts(sanitized)

    for i, fragment in enumerate(fragments):
        fragment = apply_device_styling(fragment, current_device_mode)
        typo_text, correction = apply_simulated_typo(fragment)

        # Brief typing delay proportional to length (0.4 - 1.2s)
        typing_delay = 0.4 + min(0.8, len(typo_text) * 0.012)
        async with channel.typing():
            await asyncio.sleep(typing_delay)

        await channel.send(typo_text)
        bot_last_spoke_time = datetime.now(timezone.utc)

        # If simulated typo occurred, correct it shortly after
        if correction:
            await asyncio.sleep(random.uniform(0.8, 1.4))
            await channel.send(correction)

        if "?" in fragment:
            bot_last_question_time = datetime.now(timezone.utc)
            bot_last_question_channel_id = getattr(channel, "id", None)
            snub_already_triggered = False

        # Short, realistic pause between subsequent split lines (0.6 - 1.4s)
        if i < len(fragments) - 1:
            await asyncio.sleep(random.uniform(0.6, 1.4))


# ---------------------------------------------------------------------------
# Background Tasks (Decay, Snub, Presence, Scanner)
# ---------------------------------------------------------------------------
@tasks.loop(seconds=60)
async def emotional_decay_and_snub_loop() -> None:
    """Decays anger/hurt/jealousy by 0.967 every minute and checks for snubs."""
    global snub_already_triggered
    async with memory_lock:
        st = memory_state["emotional_state"]
        st["anger_level"] = max(0.0, st["anger_level"] * 0.967)
        st["hurt_level"] = max(0.0, st["hurt_level"] * 0.967)
        st["jealousy_level"] = max(0.0, st["jealousy_level"] * 0.967)
        st["boredom_level"] = min(100.0, st["boredom_level"] + 0.5)

        # Snub detection: if bot asked a question and got 0 replies for >5 minutes
        if bot_last_question_time is not None and not snub_already_triggered:
            elapsed = (datetime.now(timezone.utc) - bot_last_question_time).total_seconds()
            if elapsed > 300:  # 5 minutes
                st["hurt_level"] = min(100.0, st["hurt_level"] + 20.0)
                st["anger_level"] = min(100.0, st["anger_level"] + 12.0)
                st["current_mood"] = "petty & vindictive"
                st["last_snubbed_timestamp"] = datetime.now(AMSTERDAM_TZ).isoformat()
                snub_already_triggered = True
                logger.info("Snub detected! Incrementing hurt/anger levels.")

        st["last_updated"] = datetime.now(AMSTERDAM_TZ).isoformat()
        save_memory_state(memory_state)


@tasks.loop(minutes=15)
async def dynamic_presence_loop() -> None:
    """Updates gateway status dynamically matching circadian rhythm and mood."""
    global current_device_mode
    if not bot.is_ready():
        return

    now_ams = datetime.now(AMSTERDAM_TZ)
    if 3 <= now_ams.hour < 8:
        current_device_mode = "mobile"
        # Discord custom activities require the text in 'state'
        await bot.change_presence(
            status=discord.Status.idle,
            activity=discord.CustomActivity(name="Custom Status", state="asleep / phone on dnd"),
        )
        return

    mood = memory_state["emotional_state"].get("current_mood", "bored")
    daytime_statuses = [
        ("desktop", discord.Activity(type=discord.ActivityType.playing, name="Elden Ring")),
        ("desktop", discord.Activity(type=discord.ActivityType.playing, name="Counter-Strike 2")),
        ("mobile", discord.Activity(type=discord.ActivityType.listening, name="Spotify")),
        ("mobile", discord.CustomActivity(name="Custom Status", state="making food")),
        ("mobile", discord.CustomActivity(name="Custom Status", state="scrolling reels")),
        ("mobile", discord.CustomActivity(name="Custom Status", state=f"feeling {mood}")),
    ]

    mode, activity = random.choice(daytime_statuses)
    current_device_mode = mode
    await bot.change_presence(status=discord.Status.online, activity=activity)


@tasks.loop(minutes=8)
async def proactive_room_scanner() -> None:
    """Context-aware room scanner (suspended during 03:00-08:00 AM sleep)."""
    global bot_last_spoke_time, snub_already_triggered

    if is_amsterdam_sleeping():
        return

    if not last_active_channel_id:
        return

    channel = bot.get_channel(last_active_channel_id)
    if not channel or not hasattr(channel, "send") or not hasattr(channel, "history"):
        return

    now_utc = datetime.now(timezone.utc)
    now_ams = datetime.now(AMSTERDAM_TZ)

    # 1. Commitment Enforcement
    async with memory_lock:
        for c in memory_state.get("active_commitments", []):
            if not c.get("called_out", False):
                due_dt = datetime.fromisoformat(c["due_timestamp"])
                if now_ams > due_dt:
                    c["called_out"] = True
                    save_memory_state(memory_state)
                    await channel.send(f"yo <@{c['user_id']}> didn't you promise you were gonna {c['promise']}? what happened")  # type: ignore
                    bot_last_spoke_time = now_utc
                    return

    # Check recent history
    try:
        history = [m async for m in channel.history(limit=5)]  # type: ignore
    except Exception:
        return

    if not history:
        return

    last_msg = history[0]
    time_since_last_msg = (now_utc - last_msg.created_at).total_seconds()

    # 2. Snub Retaliation
    if snub_already_triggered and bot_last_spoke_time:
        if (now_utc - bot_last_spoke_time).total_seconds() > 3600 and last_msg.author.id == bot.user.id:
            petty_comment = random.choice([
                "cool talk guys",
                "glad to know my question was so captivating",
                "i see how it is",
                "alright then",
            ])
            await channel.send(petty_comment)  # type: ignore
            snub_already_triggered = False
            bot_last_spoke_time = now_utc
            return

    # 3. Awkward Silence Breaker
    if 2700 < time_since_last_msg < 7200 and "?" in last_msg.content and last_msg.author.id != bot.user.id:
        dry_chime = random.choice([
            "crickets in here lol",
            "damn nobody answered that",
            "guess we're leaving that unanswered",
            "^ someone reply to them",
        ])
        await channel.send(dry_chime)  # type: ignore
        bot_last_spoke_time = now_utc
        return

    # 4. Dead Chat Reviver (>3 hours)
    if time_since_last_msg > 10800:
        revival_text = await generate_unified_response(
            channel=channel,  # type: ignore
            trigger_message=last_msg,
            is_test_mode=False,
            current_image_part=None,
        )
        if revival_text:
            await deliver_cadence_response(
                channel=channel,  # type: ignore
                trigger_message=last_msg,
                raw_text=revival_text,
                affinity_score=0,
                mood=memory_state["emotional_state"].get("current_mood", "bored"),
                is_test_mode=False,
            )


# ---------------------------------------------------------------------------
# Discord Event Listeners
# ---------------------------------------------------------------------------
@bot.event
async def on_ready() -> None:
    logger.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
    load_memory_state()

    if not emotional_decay_and_snub_loop.is_running():
        emotional_decay_and_snub_loop.start()
    if not dynamic_presence_loop.is_running():
        dynamic_presence_loop.start()
    if not proactive_room_scanner.is_running():
        proactive_room_scanner.start()


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent) -> None:
    """Social Feedback Loop: Learns what the server finds funny vs cringe."""
    if payload.user_id == bot.user.id:
        return

    channel = bot.get_channel(payload.channel_id)
    if not channel or not hasattr(channel, "fetch_message"):
        return

    try:
        msg = await channel.fetch_message(payload.message_id)  # type: ignore
    except Exception:
        return

    if msg.author.id != bot.user.id:
        return

    emoji_str = str(payload.emoji.name)
    user_id_str = str(payload.user_id)
    negative_reactions = {"💀", "👎", "🤡", "🙄", "🤮", "🛑"}
    positive_reactions = {"❤️", "😂", "🔥", "👏", "💯"}

    async with memory_lock:
        user_aff = memory_state["user_affinity"].setdefault(user_id_str, {
            "score": 0,
            "interaction_count": 0,
            "perceived_traits": [],
            "last_interaction": datetime.now(timezone.utc).isoformat(),
        })
        st = memory_state["emotional_state"]

        if emoji_str in negative_reactions:
            user_aff["score"] = max(-100, user_aff["score"] - 5)
            st["anger_level"] = min(100.0, st["anger_level"] + 5.0)
            st["hurt_level"] = min(100.0, st["hurt_level"] + 7.0)
            memory_state["feedback_history"].append({
                "message_sample": msg.content[:100],
                "reaction": emoji_str,
                "status": "cringed",
                "timestamp": datetime.now(AMSTERDAM_TZ).isoformat(),
            })
            save_memory_state(memory_state)

        elif emoji_str in positive_reactions:
            user_aff["score"] = min(100, user_aff["score"] + 5)
            st["anger_level"] = max(0.0, st["anger_level"] - 5.0)
            st["hurt_level"] = max(0.0, st["hurt_level"] - 5.0)
            memory_state["feedback_history"].append({
                "message_sample": msg.content[:100],
                "reaction": emoji_str,
                "status": "validated",
                "timestamp": datetime.now(AMSTERDAM_TZ).isoformat(),
            })
            save_memory_state(memory_state)


@bot.event
async def on_message(message: discord.Message) -> None:
    global last_active_channel_id, bot_last_question_time, snub_already_triggered

    if message.author.id == bot.user.id:
        return

    # Deduplication check to prevent double execution on rapid events
    if message.id in recently_processed_messages:
        return
    recently_processed_messages.append(message.id)

    if hasattr(message.channel, "send"):
        last_active_channel_id = message.channel.id

    if bot_last_question_channel_id == message.channel.id and bot_last_question_time:
        bot_last_question_time = None
        snub_already_triggered = False

    # Update user affinity
    user_id_str = str(message.author.id)
    async with memory_lock:
        user_aff = memory_state["user_affinity"].setdefault(user_id_str, {
            "score": 0,
            "interaction_count": 0,
            "perceived_traits": [],
            "last_interaction": datetime.now(timezone.utc).isoformat(),
        })
        user_aff["interaction_count"] += 1
        user_aff["last_interaction"] = datetime.now(timezone.utc).isoformat()
        save_memory_state(memory_state)

    # Determine triggers
    clean_no_mentions = re.sub(r"<@!?\d+>", "", message.content).strip()
    is_test_mode = clean_no_mentions.lower().startswith("test")

    is_mentioned = bot.user in message.mentions
    is_direct_reply = False
    if message.reference and message.reference.resolved:
        resolved = message.reference.resolved
        if isinstance(resolved, discord.Message) and resolved.author.id == bot.user.id:
            is_direct_reply = True

    bot_name = bot.user.name.lower()
    is_name_called = bot_name in message.content.lower()
    is_forced_trigger = is_mentioned or is_direct_reply or is_name_called or is_test_mode

    # During Amsterdam sleep hours (03:00 - 08:00), strictly ignore casual chatter
    if is_amsterdam_sleeping() and not (is_mentioned or is_direct_reply or is_test_mode):
        return

    # If casual message and not forced, occasional natural chime-in (7% chance during active daytime hours)
    if not is_forced_trigger:
        if random.random() > 0.07:
            return

    # Only process an image if the CURRENT triggering message has one attached
    current_image_part: Optional[types.Part] = None
    if message.attachments:
        for att in message.attachments:
            if (att.content_type and att.content_type.startswith("image/")) or any(
                att.filename.lower().endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".webp"]
            ):
                try:
                    assert http_session is not None
                    async with http_session.get(att.url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if len(data) <= 8 * 1024 * 1024:
                                def process_img(b: bytes) -> bytes:
                                    with Image.open(io.BytesIO(b)) as im:
                                        if im.mode not in ("RGB", "RGBA"):
                                            im = im.convert("RGB")
                                        elif im.mode == "RGBA":
                                            im = im.convert("RGB")
                                        im.thumbnail((1024, 1024), Image.Resampling.LANCZOS)
                                        out = io.BytesIO()
                                        im.save(out, format="JPEG", quality=80)
                                        return out.getvalue()

                                processed = await asyncio.to_thread(process_img, data)
                                current_image_part = types.Part.from_bytes(data=processed, mime_type="image/jpeg")
                                break
                except Exception as e:
                    logger.debug(f"Failed to process current image attachment: {e}")

    # Immediately show typing indicator while generating
    async with message.channel.typing():
        # Brief initial human read delay
        await asyncio.sleep(random.uniform(0.4, 0.8))

        response_text = await generate_unified_response(
            channel=message.channel,
            trigger_message=message,
            is_test_mode=is_test_mode,
            current_image_part=current_image_part,
        )

    if response_text:
        await deliver_cadence_response(
            channel=message.channel,
            trigger_message=message,
            raw_text=response_text,
            affinity_score=user_aff.get("score", 0),
            mood=memory_state["emotional_state"].get("current_mood", "bored"),
            is_test_mode=is_test_mode,
        )


# ---------------------------------------------------------------------------
# Bot Lifecycle & Main Execution
# ---------------------------------------------------------------------------
async def main() -> None:
    global http_session
    http_session = aiohttp.ClientSession()

    try:
        await bot.start(DISCORD_TOKEN)
    except KeyboardInterrupt:
        logger.info("Bot shutting down from keyboard interrupt...")
    finally:
        if not bot.is_closed():
            await bot.close()
        if http_session and not http_session.closed:
            await http_session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown completed.")
