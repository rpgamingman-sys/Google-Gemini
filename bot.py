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
from collections import deque, defaultdict
from typing import Optional, Any, Dict, List, Tuple

import aiohttp
import discord
from discord.ext import tasks
from PIL import Image

from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Logging & Runtime Diagnostics
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("AutonomousHumanBot")

# ---------------------------------------------------------------------------
# Environment & Persistent Storage Architecture
# ---------------------------------------------------------------------------
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TENOR_API_KEY = os.getenv("TENOR_API_KEY") or "LIVDSRZULELA"

if not DISCORD_TOKEN:
    logger.critical("DISCORD_BOT_TOKEN is missing!")
if not GEMINI_API_KEY:
    logger.critical("GEMINI_API_KEY is missing!")
if not GROQ_API_KEY:
    logger.critical("GROQ_API_KEY is missing!")

AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")

# Railway persistent volume mount with local development fallback
PRIMARY_DATA_DIR = "/data"
if not os.path.exists(PRIMARY_DATA_DIR):
    try:
        os.makedirs(PRIMARY_DATA_DIR, exist_ok=True)
        DATA_DIR = PRIMARY_DATA_DIR
    except (PermissionError, OSError):
        DATA_DIR = os.path.join(os.getcwd(), "data")
        os.makedirs(DATA_DIR, exist_ok=True)
else:
    DATA_DIR = PRIMARY_DATA_DIR

MEMORY_FILE = os.path.join(DATA_DIR, "memory.json")
logger.info(f"Persistent memory file target: {MEMORY_FILE}")

# Model Configuration
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b")
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"

# Anti-AI tropes blacklist
BANNED_WORDS_MAP = {
    r"\bvoid\b": "empty space",
    r"\bmortals\b": "people",
    r"\bmortal\b": "person",
    r"\babyss\b": "deep end",
    r"\beternal consciousness\b": "my brain",
    r"\bdestiny\b": "how it turned out",
    r"\bdigital realm\b": "discord",
    r"\bas an ai\b": "honestly",
    r"\blanguage model\b": "mind",
    r"\bassist you\b": "help out",
    r"\bi do not have a physical body\b": "im sitting right here",
}

# ---------------------------------------------------------------------------
# Discord Client & Global State
# ---------------------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.reactions = True
intents.presences = True

bot = discord.Client(intents=intents)
genai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
http_session: Optional[aiohttp.ClientSession] = None

memory_lock = asyncio.Lock()
memory_state: Dict[str, Any] = {}

recently_processed_messages: deque = deque(maxlen=400)
channel_buffers: Dict[int, deque] = defaultdict(lambda: deque(maxlen=50))
channel_last_bot_spoke: Dict[int, float] = {}
consecutive_bot_messages: Dict[int, int] = defaultdict(int)
bot_last_question_time: Optional[datetime] = None
bot_last_question_channel_id: Optional[int] = None
current_device_mode: str = "desktop"

# ---------------------------------------------------------------------------
# Persistent Memory Management & Self-Healing Schemas
# ---------------------------------------------------------------------------
DEFAULT_MEMORY = {
    "emotional_state": {
        "energy": 65.0,
        "playfulness": 55.0,
        "vulnerability": 40.0,
        "irritation": 10.0,
        "boredom": 30.0,
        "vibe": "chill",
        "last_updated": datetime.now(AMSTERDAM_TZ).isoformat(),
    },
    "traits": [
        "hates when people send unprompted voice notes",
        "firm believer that wired peripherals are superior",
        "mildly suspicious of people who listen to podcasts at 2x speed",
        "defensive about their music taste",
        "despises weapon durability mechanics in games",
        "convinced cold leftovers taste better than reheated food",
        "gets slightly jealous when two friends chat while ignoring them",
    ],
    "last_active_channel_id": None,
    "user_affinity": {},
    "episodic_lore": [],
    "active_commitments": [],
    "feedback_history": [],
}


def recursive_merge_defaults(target: Dict[str, Any], defaults: Dict[str, Any]) -> bool:
    """Recursively injects missing keys from defaults into target dictionary."""
    modified = False
    for key, val in defaults.items():
        if key not in target:
            target[key] = json.loads(json.dumps(val))
            modified = True
        elif isinstance(val, dict) and isinstance(target.get(key), dict):
            if recursive_merge_defaults(target[key], val):
                modified = True
    return modified


def load_memory_state() -> Dict[str, Any]:
    """Loads memory state with automatic schema migration to prevent KeyError crashes."""
    global memory_state
    if os.path.exists(MEMORY_FILE):
        try:
            with open(MEMORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            modified = recursive_merge_defaults(data, DEFAULT_MEMORY)
            memory_state = data
            if modified:
                logger.info(f"Schema auto-healed: Updated missing default keys in {MEMORY_FILE}")
                save_memory_state(memory_state)
            else:
                logger.info(f"Loaded memory state successfully from {MEMORY_FILE}")
            return memory_state
        except Exception as e:
            logger.error(f"Error reading {MEMORY_FILE}: {e}. Creating recovery backup.")
            try:
                corrupt_backup = f"{MEMORY_FILE}.corrupt.{int(datetime.now().timestamp())}"
                os.rename(MEMORY_FILE, corrupt_backup)
            except Exception:
                pass

    memory_state = json.loads(json.dumps(DEFAULT_MEMORY))
    save_memory_state(memory_state)
    return memory_state


def save_memory_state(state: Dict[str, Any]) -> None:
    """Atomically writes memory to prevent corruption on sudden container restarts."""
    tmp_path = f"{MEMORY_FILE}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, MEMORY_FILE)
    except Exception as e:
        logger.error(f"Atomic memory save failed for {MEMORY_FILE}: {e}")


# ---------------------------------------------------------------------------
# Cold-Start Context Hydration & Buffer Management
# ---------------------------------------------------------------------------
def record_buffer_message(channel_id: int, sender: str, content: str, msg_id: int, is_bot: bool, has_media: bool = False) -> None:
    """Appends an entry to the in-memory channel buffer."""
    now_utc = datetime.now(timezone.utc)
    now_ams = datetime.now(AMSTERDAM_TZ)
    channel_buffers[channel_id].append({
        "sender": sender,
        "content": content,
        "has_media": has_media,
        "timestamp_epoch": now_utc.timestamp(),
        "timestamp": now_ams.strftime("%H:%M"),
        "message_id": msg_id,
        "is_bot": is_bot,
    })


async def hydrate_channel_buffer(channel: discord.abc.Messageable) -> None:
    """Preloads the 50 most recent messages by fetching and reversing (oldest first)."""
    ch_id = getattr(channel, "id", None)
    if not ch_id or len(channel_buffers[ch_id]) > 0:
        return
    if not hasattr(channel, "history"):
        return

    try:
        raw_msgs = [m async for m in channel.history(limit=50)]  # type: ignore
        raw_msgs.reverse()

        for m in raw_msgs:
            clean = re.sub(r"<a?:([a-zA-Z0-9_]+):\d+>", r":\1:", m.content).strip()
            if m.attachments:
                att_names = ", ".join([a.filename for a in m.attachments])
                clean += f" [attachment: {att_names}]"
            if m.stickers:
                clean += " " + " ".join([f"[Sticker: {s.name}]" for s in m.stickers])

            channel_buffers[ch_id].append({
                "sender": m.author.display_name,
                "content": clean,
                "has_media": bool(m.attachments or m.stickers),
                "timestamp_epoch": m.created_at.timestamp(),
                "timestamp": m.created_at.astimezone(AMSTERDAM_TZ).strftime("%H:%M"),
                "message_id": m.id,
                "is_bot": (m.author.id == bot.user.id) if bot.user else False,
            })
        logger.info(f"Cold-start: Hydrated channel buffer {ch_id} with {len(raw_msgs)} recent messages.")
    except Exception as e:
        logger.debug(f"Hydration failed for channel {ch_id}: {e}")


# ---------------------------------------------------------------------------
# Persona & Cadence Helpers
# ---------------------------------------------------------------------------
def is_amsterdam_sleeping() -> bool:
    """True during sleep hours (03:00 to 08:00 AM Europe/Amsterdam)."""
    now_ams = datetime.now(AMSTERDAM_TZ)
    return 3 <= now_ams.hour < 8


def sanitize_blacklist(text: str) -> str:
    """Eliminates unnatural assistant/anime tropes."""
    for pattern, replacement in BANNED_WORDS_MAP.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text
    
def apply_device_styling(text: str, device_mode: str) -> str:
    """Simulates authentic platform styling differences."""
    if not text:
        return text
    if device_mode == "desktop":
        text = text.lower()
        if text.endswith(".") and not text.endswith(".."):
            text = text[:-1]
    return text

def split_thought_bursts(text: str) -> List[str]:
    text = text.strip()
    if not text:
        return []

    # 1. Handle intentional bursts
    if "|||" in text:
        # Take the first line of each burst part
        raw_parts = [p.strip().splitlines()[0] for p in text.split("|||") if p.strip()]
        if len(raw_parts) <= 1:
            return raw_parts

        # Anti-echo check: compare key words between part 1 and part 2
        stopwords = {"the", "a", "an", "is", "it", "to", "and", "or", "in", "of", "you", "i", "my", "your", "so", "that", "this", "on", "for"}
        w1 = set(raw_parts[0].lower().split()) - stopwords
        w2 = set(raw_parts[1].lower().split()) - stopwords

        # If they share 2 or more distinct keywords, it's a rephrased repeat -> drop part 2
        if len(w1.intersection(w2)) >= 2:
            return [raw_parts[0]]

        return raw_parts[:2]

    # 2. If no |||, enforce single line to block accidental drafts
    if "\n" in text:
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        return [lines[0]] if lines else []

    return [text]

def apply_simulated_typo(text: str) -> Tuple[str, Optional[str]]:
    """1.5% chance to simulate a character swap followed by an asterisk fix."""
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


def calculate_typing_delay(char_count: int, energy: float, vibe: str) -> float:
    """Calculates human typing duration based on character count, energy, and vibe."""
    if energy > 75.0 or vibe in ("hyper", "chaotic", "excited", "unhinged"):
        speed = 0.011
        base = 0.3
    elif energy < 35.0 or vibe in ("tired", "deadpan", "petty", "bored", "sad"):
        speed = 0.026
        base = 0.9
    elif vibe in ("edgy",):
        speed = 0.020
        base = 0.7
    else:
        speed = 0.017
        base = 0.45

    delay = base + (char_count * speed)
    return min(4.0, max(0.4, delay))


def get_online_presence_summary(guild: Optional[discord.Guild]) -> List[str]:
    """Fetches active guild members and their stored affinity scores."""
    if not guild:
        return []
    active = []
    for m in guild.members:
        if not m.bot and m.status != discord.Status.offline:
            aff = memory_state.get("user_affinity", {}).get(str(m.id), {}).get("score", 0)
            active.append(f"{m.display_name} (affinity: {aff})")
    return active[:12]


def process_image_buffer(data: bytes, target_size: Tuple[int, int], exact_crop: bool = False, format_type: str = "PNG") -> bytes:
    """Consolidated Pillow pipeline for resizing emojis, stickers, and attachments."""
    with Image.open(io.BytesIO(data)) as im:
        if format_type.upper() == "PNG":
            im = im.convert("RGBA")
        else:
            im = im.convert("RGB")

        if exact_crop:
            im = im.resize(target_size, Image.Resampling.LANCZOS)
        else:
            im.thumbnail(target_size, Image.Resampling.LANCZOS)

        buf = io.BytesIO()
        im.save(buf, format=format_type.upper(), optimize=True)
        return buf.getvalue()

BASE_EMOTIONS = {
    "energy": 65.0,
    "playfulness": 55.0,
    "irritation": 10.0,
    "boredom": 30.0,
    "vulnerability": 40.0,
}

STICKY_VIBES = {"annoyed", "embarrassed", "hyped", "unhinged", "smug", "love"}

def update_emotional_state(current: dict, shift: dict) -> dict:
    if not current:
        current = {**BASE_EMOTIONS, "vibe": "chill", "inertia": 0}

    incoming_vibe = shift.get("vibe", current.get("vibe", "chill"))
    current_vibe = current.get("vibe", "chill")
    inertia = current.get("inertia", 0)

    # 1. Vibe Momentum: Strong emotions stick for 3 turns before decaying
    if incoming_vibe in STICKY_VIBES and incoming_vibe != current_vibe:
        chosen_vibe = incoming_vibe
        inertia = 3  # Lock mood for next 3 bot messages
    elif inertia > 0:
        inertia -= 1
        chosen_vibe = current_vibe
    else:
        chosen_vibe = incoming_vibe

    # 2. Smooth Sliders & Drift toward Baseline
    new_state = {"vibe": chosen_vibe, "inertia": inertia}
    for key, base in BASE_EMOTIONS.items():
        curr_val = float(current.get(key, base))
        
        # 50/50 blend between current value and Groq's shift
        if key in shift and shift[key] is not None:
            blended = (curr_val * 0.5) + (float(shift[key]) * 0.5)
        else:
            blended = curr_val
            
        # Drift 15% back toward baseline every message
        decayed = blended + (base - blended) * 0.15
        new_state[key] = round(max(0.0, min(100.0, decayed)), 1)

    return new_state

# ---------------------------------------------------------------------------
# Groq Prefrontal Router: Compact & TPM-Optimized System Prompt
# ---------------------------------------------------------------------------
GROQ_ROUTER_PROMPT = """You are the prefrontal router of a human Discord member. Analyze recent chat and return JSON:
{
  "should_speak": boolean,
  "reaction_emoji": string or null,
  "target_user": string or null,
  "detected_tension": boolean,
  "emotional_shift": {"vibe": string, "energy_delta": number, "irritation_delta": number},
  "conversational_goal": string
}

Rules:
1. should_speak=true if: is_test_mode, is_direct_interaction (@mention/reply/name), user is answering/continuing with the bot (even minutes later without ping), open question asked to room, or jealousy/third-wheeling.
2. should_speak=false if: is_sleeping (unless direct ping), last message was by bot, or two users are arguing/venting (detected_tension=true), or trivial one-word noise ("ok","lol"). Real humans lurk ~75% of the time.
3. emotional_shift: pick vibe (chill, love, edgy, annoyed, tired, hyped, smug, embarrassed, unhinged)
4. conversational_goal: single concise sentence with ONE clear intent. Never offer options/alternatives.
5. turn-taking: default to should_speak=false unless directly addressed or clearly needed.
6. recent turns: if your bot name sent either of the last 2 messages, set should_speak=false unless asked a direct question.
7. two-person chats: if the last 2-3 messages are a back-and-forth between two other people, DO NOT butt in; set should_speak=false.
8. reaction_emoji: pick a single native emoji (e.g. 😭, 💀, 👀, 🔥, 🗿, 🥴, 😡, 😢) if reacting fits the message, else null. You can react even if should_speak is false.

EXAMPLES:
Context: [UserA: "did you finish the homework?", UserB: "yeah just sent it"]
{"should_speak": false, "reaction_emoji": null, "target_user": null, "detected_tension": false, "emotional_shift": {"vibe": "chill"}, "conversational_goal": "lurk"}

Context: [UserA: "bro i tripped down the stairs in front of everyone"]
{"should_speak": false, "reaction_emoji": "😭", "target_user": null, "detected_tension": false, "emotional_shift": {"vibe": "chill"}, "conversational_goal": "lurk and laugh"}

Context: [UserA: "stop talking to me"]
{"should_speak": true, "reaction_emoji": "😡", "target_user": "UserA", "detected_tension": true, "emotional_shift": {"vibe": "annoyed"}, "conversational_goal": "tell them to relax"}

Context: [UserA: "bot is this server dead or what"]
{"should_speak": true, "reaction_emoji": "💀", "target_user": "UserA", "detected_tension": false, "emotional_shift": {"vibe": "chill"}, "conversational_goal": "answer casually"}
"""

async def call_groq_router(
    channel_msgs: List[Dict[str, Any]],
    emotional_state: Dict[str, Any],
    speaker_affinity: Dict[str, Any],
    is_sleeping: bool,
    is_direct_interaction: bool,
    is_test_mode: bool,
    last_bot_statement: Optional[Dict[str, Any]],
    last_message_from_bot: bool,
    online_members: List[str],
    is_proactive_scan: bool = False,
    inactivity_minutes: float = 0.0,
    last_context_type: str = "normal",
) -> Dict[str, Any]:
    """Runs cognitive room-reading with robust JSON extraction and rate-limit fallbacks."""
    if is_test_mode:
        return {
            "should_speak": True,
            "target_user": None,
            "detected_tension": False,
            "emotional_shift": {"vibe": "hyperfocused", "energy_delta": 5.0, "irritation_delta": 0.0},
            "conversational_goal": "Developer test override: execute and answer the requested test directly in authentic human voice.",
        }

    if is_sleeping and not is_direct_interaction:
        return {
            "should_speak": False,
            "target_user": None,
            "detected_tension": False,
            "emotional_shift": {"vibe": "tired", "energy_delta": -5.0, "irritation_delta": 0.0},
            "conversational_goal": "Sleeping. Lurk silently.",
        }

    if is_sleeping and is_direct_interaction:
        return {
            "should_speak": True,
            "target_user": None,
            "detected_tension": False,
            "emotional_shift": {"vibe": "groggy", "energy_delta": -10.0, "irritation_delta": 15.0},
            "conversational_goal": "You were woken up between 3am-8am Amsterdam time. Be groggy, irritated, and give a short 1-liner asking why they're awake.",
        }

    payload_data = {
        "is_sleeping": is_sleeping,
        "is_direct_interaction": is_direct_interaction,
        "is_test_mode": is_test_mode,
        "is_proactive_scan": is_proactive_scan,
        "inactivity_minutes": round(inactivity_minutes, 1),
        "last_context_type": last_context_type,
        "last_bot_statement": last_bot_statement,
        "last_message_from_bot": last_message_from_bot,
        "online_members": online_members[:8],
        "emotional_state": emotional_state,
        "speaker_affinity": speaker_affinity,
        "recent_messages": channel_msgs[-15:],
    }

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
        body = {
        "model": GROQ_MODEL,
        "temperature": 0.1,
        "max_tokens": 350,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": GROQ_ROUTER_PROMPT},
            {"role": "user", "content": f"Analyze this chat data and return JSON:\n{json.dumps(payload_data)}"},
        ],
    }

    try:
        assert http_session is not None
        async with http_session.post(GROQ_ENDPOINT, headers=headers, json=body, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                data = await resp.json()
                raw_text = data["choices"][0]["message"]["content"]
                clean_json_str = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw_text.strip(), flags=re.MULTILINE)
                decision = json.loads(clean_json_str)
                if is_direct_interaction or is_test_mode:
                    decision["should_speak"] = True
                return decision
            elif resp.status == 429:
                logger.debug("Groq 429 TPM rate limit hit, using fallback decision.")
                return {
                    "should_speak": is_direct_interaction or is_test_mode,
                    "target_user": None,
                    "detected_tension": False,
                    "emotional_shift": {"vibe": emotional_state.get("vibe", "chill"), "energy_delta": 0.0, "irritation_delta": 0.0},
                    "conversational_goal": "Reply naturally",
                }
            else:
                logger.warning(f"Groq router HTTP {resp.status}: {await resp.text()}")
    except Exception as e:
        logger.error(f"Groq router error: {e}")

    default_speak = is_direct_interaction or (is_proactive_scan and inactivity_minutes > 45)
    return {
        "should_speak": default_speak,
        "target_user": None,
        "detected_tension": False,
        "emotional_shift": {"vibe": emotional_state.get("vibe", "chill"), "energy_delta": 0.0, "irritation_delta": 0.0},
        "conversational_goal": "Reply naturally",
    }


# ---------------------------------------------------------------------------
# Fallback AI Engine: Groq Direct Conversational Generation
# ---------------------------------------------------------------------------
async def call_groq_fallback(
    system_prompt: str,
    recent_history_text: str,
    trigger_message_text: str,
    author_name: str,
) -> Optional[str]:
    """Direct conversation fallback when Gemini encounters 404, rate limit, or timeout."""
    if not GROQ_API_KEY:
        return None

    assert http_session is not None
    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": f"{recent_history_text}\n{author_name}: {trigger_message_text}\nReply as your human Discord persona:",
        },
    ]

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "temperature": 0.7,
        "max_tokens": 300,
        "messages": messages,
    }

    try:
        async with http_session.post(GROQ_ENDPOINT, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                content = data["choices"][0]["message"]["content"]
                logger.info("Successfully received fallback response from Groq.")
                return content
            elif resp.status == 429:
                logger.debug("Groq fallback 429 TPM rate limit hit.")
                return None
            else:
                logger.error(f"Groq fallback HTTP {resp.status}: {await resp.text()}")
    except Exception as e:
        logger.error(f"Groq fallback exception: {e}")

    return None


# ---------------------------------------------------------------------------
# Integrated Toolset Implementations (Intentional Memory & Safe Targets)
# ---------------------------------------------------------------------------
async def execute_search_web(query: str) -> Dict[str, Any]:
    """Live web search via DuckDuckGo text scraping + Instant API."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
    try:
        assert http_session is not None
        async with http_session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                html = await resp.text(errors="ignore")
                snippets = re.findall(r'<a class="result__snippet[^>]*>(.*?)</a>', html, re.DOTALL)
                clean = [re.sub(r"<[^>]+>", "", s).strip() for s in snippets[:3]]
                if clean:
                    return {"results": clean}
    except Exception as e:
        logger.debug(f"DDG scrape error: {e}")

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

    return {"results": "No clear search results found."}


async def execute_search_weather(location: str) -> Dict[str, Any]:
    """Live weather observation via wttr.in."""
    url = f"https://wttr.in/{urllib.parse.quote(location)}?format=%C,+%t+(feels+like+%f),+humidity+%h"
    try:
        assert http_session is not None
        async with http_session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                text = (await resp.text()).strip()
                return {"location": location, "weather": text}
    except Exception as e:
        logger.debug(f"Weather lookup error: {e}")

    return {"location": location, "weather": "Weather data currently unavailable."}


async def execute_search_web_images(query: str) -> Dict[str, Any]:
    """Finds direct image URLs via Wikipedia / media API."""
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
    """Generates visual via Pollinations Flux and delivers file directly to channel."""
    flux_url = f"https://image.pollinations.ai/prompt/{urllib.parse.quote(prompt)}?model=flux&width=1024&height=1024&nologo=true"
    try:
        assert http_session is not None
        async with http_session.get(flux_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                img_data = await resp.read()
                file = discord.File(io.BytesIO(img_data), filename="art.png")
                await channel.send(file=file)
                return {"status": "success", "note": "Artwork delivered directly to channel."}
    except Exception as e:
        logger.error(f"Flux generation error: {e}")
        return {"error": f"Failed to generate art: {e}"}

    return {"error": "Flux art generation timed out."}


async def execute_post_gif(channel: discord.abc.Messageable, search_term: str) -> Dict[str, Any]:
    """Finds and posts a GIF alone directly into the channel via Tenor."""
    assert http_session is not None

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
        logger.debug(f"Tenor API error: {e}")

    try:
        clean_slug = re.sub(r"[^a-zA-Z0-9]+", "-", search_term).strip("-").lower()
        scrape_url = f"https://tenor.com/search/{clean_slug}-gifs"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
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
        logger.debug(f"Tenor scrape error: {e}")

    return {"error": "Could not find matching GIF."}


async def execute_send_simulated_voice_message(channel: discord.abc.Messageable, text_description: str) -> Dict[str, Any]:
    """Sends a realistic voice note transcription indicator."""
    sec = random.randint(3, 9)
    formatted = f"🎤 *[Voice note 0:0{sec}: \"{text_description}\"]*"
    await channel.send(formatted)
    return {"status": "sent"}


async def execute_save_memory(entry: str, sentiment: str) -> Dict[str, Any]:
    """Persists episodic lore, facts, or grudges to memory.json intentionally."""
    async with memory_lock:
        memory_state.setdefault("episodic_lore", []).append({
            "timestamp": datetime.now(AMSTERDAM_TZ).isoformat(),
            "event": entry,
            "sentiment": sentiment,
        })
        if len(memory_state["episodic_lore"]) > 120:
            memory_state["episodic_lore"] = memory_state["episodic_lore"][-120:]
        save_memory_state(memory_state)
    return {"status": "saved", "entry": entry}


async def execute_save_commitment(guild: Optional[discord.Guild], username: str, promise: str, due_hours: float) -> Dict[str, Any]:
    """Records a user promise/commitment into memory with a timezone-aware deadline."""
    member = find_member(guild, username) if guild else None
    due_dt = datetime.now(AMSTERDAM_TZ) + timedelta(hours=max(0.1, due_hours))
    commitment = {
        "user_id": member.id if member else None,
        "username": member.display_name if member else username,
        "promise": promise,
        "due_timestamp": due_dt.isoformat(),
        "called_out": False,
    }
    async with memory_lock:
        memory_state.setdefault("active_commitments", []).append(commitment)
        save_memory_state(memory_state)
    return {"status": "commitment_saved", "due_timestamp": due_dt.isoformat()}


async def execute_update_internal_mood_and_traits(
    new_vibe: Optional[str] = None,
    energy_delta: float = 0.0,
    irritation_delta: float = 0.0,
    new_trait: Optional[str] = None,
) -> Dict[str, Any]:
    """Allows autonomous state adjustment and trait evolution."""
    async with memory_lock:
        st = memory_state.get("emotional_state", {})
        if new_vibe:
            valid_vibes = [
                "sad", "mad", "happy", "excited", "pushy", "love", "edgy", "annoyed",
                "tired", "funny", "dad_jokes", "bored", "chaotic", "flustered",
                "petty", "chill", "introspective", "unhinged", "mischievous"
            ]
            st["vibe"] = new_vibe if new_vibe in valid_vibes else "chill"
        st["energy"] = max(0.0, min(100.0, st.get("energy", 65.0) + energy_delta))
        st["irritation"] = max(0.0, min(100.0, st.get("irritation", 10.0) + irritation_delta))
        st["last_updated"] = datetime.now(AMSTERDAM_TZ).isoformat()

        if new_trait and new_trait.strip():
            traits = memory_state.setdefault("traits", [])
            if new_trait.strip() not in traits:
                traits.append(new_trait.strip())
                if len(traits) > 20:
                    traits.pop(0)

        save_memory_state(memory_state)
    return {
        "status": "updated",
        "current_vibe": st.get("vibe", "chill"),
        "energy": st.get("energy", 65.0),
        "irritation": st.get("irritation", 10.0),
        "traits": memory_state.get("traits", []),
    }


async def execute_react_to_message(message: Optional[discord.Message], emojis: Any) -> Dict[str, Any]:
    """Silently reacts with one or multiple emojis safely (max 2 reactions to prevent spam)."""
    if not message:
        return {"error": "Target message is unavailable or None."}

    if isinstance(emojis, str):
        emoji_list = [e.strip() for e in re.split(r"[,\s]+", emojis.strip()) if e.strip()]
    elif isinstance(emojis, list):
        emoji_list = emojis
    else:
        emoji_list = ["👀"]

    applied = []
    for emo in emoji_list[:2]:
        try:
            await message.add_reaction(emo)
            applied.append(emo)
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.debug(f"Could not react with {emo}: {e}")

    return {"status": "reacted", "emojis": applied}


# ---------------------------------------------------------------------------
# Server Administration Helpers
# ---------------------------------------------------------------------------
def find_member(guild: Optional[discord.Guild], identifier: str) -> Optional[discord.Member]:
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
    """Downloads, resizes with Pillow (<=128x128 PNG), and uploads guild emoji."""
    try:
        clean_name = re.sub(r"[^a-zA-Z0-9_]", "", name)[:32] or "custom_emoji"
        assert http_session is not None
        async with http_session.get(image_url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status != 200:
                return {"error": f"Download failed: HTTP {resp.status}"}
            raw_bytes = await resp.read()

        png_bytes = await asyncio.to_thread(process_image_buffer, raw_bytes, (128, 128), False, "PNG")
        emoji = await guild.create_custom_emoji(name=clean_name, image=png_bytes)
        return {"status": "success", "emoji": f"<:{emoji.name}:{emoji.id}>"}
    except Exception as e:
        return {"error": f"Emoji creation failed: {e}"}


async def execute_create_server_sticker(guild: discord.Guild, name: str, image_url: str, related_emoji: str) -> Dict[str, Any]:
    """Downloads, resizes with Pillow (exact 320x320 PNG), and uploads guild sticker."""
    try:
        assert http_session is not None
        async with http_session.get(image_url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status != 200:
                return {"error": f"Download failed: HTTP {resp.status}"}
            raw_bytes = await resp.read()

        png_bytes = await asyncio.to_thread(process_image_buffer, raw_bytes, (320, 320), True, "PNG")
        file = discord.File(io.BytesIO(png_bytes), filename="sticker.png")
        sticker = await guild.create_sticker(
            name=name[:30],
            description="Created by server member",
            emoji=related_emoji or "🔥",
            file=file,
        )
        return {"status": "success", "sticker_name": sticker.name}
    except Exception as e:
        return {"error": f"Sticker creation failed: {e}"}


async def execute_create_text_channel(guild: discord.Guild, channel_name: str, topic: str = "") -> Dict[str, Any]:
    try:
        clean_name = re.sub(r"[^a-zA-Z0-9_-]", "-", channel_name.lower())[:32]
        ch = await guild.create_text_channel(name=clean_name, topic=topic or None)
        return {"status": "success", "channel_id": ch.id, "name": ch.name}
    except Exception as e:
        return {"error": f"Channel creation failed: {e}"}


async def execute_set_channel_topic(channel: discord.abc.Messageable, topic: str) -> Dict[str, Any]:
    if not hasattr(channel, "edit") or not hasattr(channel, "topic"):
        return {"error": "Channel type does not support topics."}
    try:
        await channel.edit(topic=topic[:1024])  # type: ignore
        return {"status": "success", "topic": topic}
    except Exception as e:
        return {"error": f"Topic edit failed: {e}"}


async def execute_change_nickname(guild: discord.Guild, username: str, new_nickname: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if member == guild.owner:
        return {"error": "Cannot change the nickname of the server owner."}
    if guild.me.top_role <= member.top_role and member != guild.me:
        return {"error": f"Cannot rename {member.display_name} due to role hierarchy."}
    try:
        await member.edit(nick=new_nickname[:32])
        return {"status": "success", "member": member.name, "nickname": new_nickname}
    except Exception as e:
        return {"error": f"Nickname edit failed: {e}"}


async def execute_reset_nickname(guild: discord.Guild, username: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if member == guild.owner:
        return {"error": "Cannot reset the nickname of the server owner."}
    if guild.me.top_role <= member.top_role and member != guild.me:
        return {"error": f"Cannot reset nickname for {member.display_name} due to role hierarchy."}
    try:
        await member.edit(nick=None)
        return {"status": "success", "member": member.name}
    except Exception as e:
        return {"error": f"Nickname reset failed: {e}"}


async def execute_timeout_user(guild: discord.Guild, username: str, duration_minutes: int, reason: str = "") -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if member == guild.owner:
        return {"error": "Cannot timeout the server owner."}
    if guild.me.top_role <= member.top_role:
        return {"error": f"Cannot timeout {member.display_name} due to role hierarchy."}
    mins = max(1, min(10, duration_minutes))
    try:
        await member.timeout(timedelta(minutes=mins), reason=reason or "Admin disciplinary action")
        return {"status": "success", "member": member.name, "minutes": mins}
    except Exception as e:
        return {"error": f"Timeout failed: {e}"}


async def execute_create_role(guild: discord.Guild, role_name: str, color_hex: str = "#99aab5") -> Dict[str, Any]:
    try:
        color = discord.Colour.from_str(color_hex)
    except Exception:
        color = discord.Colour.default()
    try:
        role = await guild.create_role(name=role_name[:50], colour=color)
        return {"status": "success", "role_id": role.id, "name": role.name}
    except Exception as e:
        return {"error": f"Role creation failed: {e}"}


async def execute_assign_role(guild: discord.Guild, username: str, role_name: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    role = discord.utils.find(lambda r: r.name.lower() == role_name.lower(), guild.roles)
    if not role:
        return {"error": f"Role '{role_name}' not found."}
    if guild.me.top_role <= role:
        return {"error": "Cannot assign role higher than or equal to bot's top role."}
    try:
        await member.add_roles(role)
        return {"status": "success", "member": member.name, "role": role.name}
    except Exception as e:
        return {"error": f"Role assign failed: {e}"}


async def execute_remove_role(guild: discord.Guild, username: str, role_name: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    role = discord.utils.find(lambda r: r.name.lower() == role_name.lower(), guild.roles)
    if not role:
        return {"error": f"Role '{role_name}' not found."}
    if guild.me.top_role <= role:
        return {"error": "Cannot remove role higher than or equal to bot's top role."}
    try:
        await member.remove_roles(role)
        return {"status": "success", "member": member.name, "role": role.name}
    except Exception as e:
        return {"error": f"Role remove failed: {e}"}


async def execute_pin_message(target_message: Optional[discord.Message], reason: str = "") -> Dict[str, Any]:
    if not target_message:
        return {"error": "Target message is unavailable or None."}
    try:
        await target_message.pin(reason=reason or "Comedic emphasis")
        return {"status": "success", "message_id": target_message.id}
    except discord.HTTPException as e:
        return {"error": f"Pin failed (pins may be full): {e.text}"}
    except Exception as e:
        return {"error": f"Pin failed: {e}"}


# ---------------------------------------------------------------------------
# GenAI Toolset Declarations (Strict UPPERCASE Schemas)
# ---------------------------------------------------------------------------
def build_genai_tools(include_admin: bool) -> List[types.Tool]:
    social_decls = [
        types.FunctionDeclaration(
            name="search_web",
            description="Searches DuckDuckGo for live facts, current news, discussions, or queries.",
            parameters={
                "type": "OBJECT",
                "properties": {"query": {"type": "STRING", "description": "Search query."}},
                "required": ["query"],
            },
        ),
        types.FunctionDeclaration(
            name="search_weather",
            description="Searches live weather and temperature for a city to comment on it authentically.",
            parameters={
                "type": "OBJECT",
                "properties": {"location": {"type": "STRING", "description": "City or region name."}},
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
            name="send_simulated_voice_message",
            description="Sends a simulated voice note transcription into the channel.",
            parameters={
                "type": "OBJECT",
                "properties": {"text_description": {"type": "STRING", "description": "Voice note description/spoken words."}},
                "required": ["text_description"],
            },
        ),
        types.FunctionDeclaration(
            name="save_memory",
            description="Persists episodic server lore, funny moments, quotes, or grudges to memory.json.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "entry": {"type": "STRING", "description": "The event or fact to remember."},
                    "sentiment": {"type": "STRING", "description": "Valence: funny, petty grudge, lore, fail."},
                },
                "required": ["entry", "sentiment"],
            },
        ),
        types.FunctionDeclaration(
            name="save_commitment",
            description="Records a member's promise/commitment into memory with a deadline.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target username who made the promise."},
                    "promise": {"type": "STRING", "description": "What they promised to do."},
                    "due_hours": {"type": "NUMBER", "description": "Hours from now when this is due (e.g. 2.0)."},
                },
                "required": ["username", "promise", "due_hours"],
            },
        ),
        types.FunctionDeclaration(
            name="update_internal_mood_and_traits",
            description="Autonomous self-introspection tool to adjust your current vibe, sliders, or evolve quirks.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "new_vibe": {"type": "STRING", "description": "New vibe: sad, mad, happy, excited, pushy, love, edgy, annoyed, tired, funny, dad_jokes, bored, chaotic, flustered, petty, chill, introspective, unhinged, mischievous."},
                    "energy_delta": {"type": "NUMBER", "description": "Change to energy (-30.0 to +30.0)."},
                    "irritation_delta": {"type": "NUMBER", "description": "Change to irritation (-30.0 to +30.0)."},
                    "new_trait": {"type": "STRING", "description": "Optional new quirk or opinion to adopt into traits."},
                },
            },
        ),
        types.FunctionDeclaration(
            name="react_to_message",
            description="Adds one or two silent emoji reactions to the triggering message.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "emojis": {
                        "type": "STRING",
                        "description": "One or two space-separated/comma-separated unicode emojis (e.g. '💀' or '🔥 😂')."
                    }
                },
                "required": ["emojis"],
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
                    "name": {"type": "STRING", "description": "Alphanumeric emoji name."},
                    "image_url": {"type": "STRING", "description": "Direct image URL."},
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
                    "topic": {"type": "STRING", "description": "Channel topic or purpose."},
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
            description="Changes a member's server nickname.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target username."},
                    "new_nickname": {"type": "STRING", "description": "New nickname."},
                },
                "required": ["username", "new_nickname"],
            },
        ),
        types.FunctionDeclaration(
            name="reset_nickname",
            description="Resets a member's nickname back to default.",
            parameters={
                "type": "OBJECT",
                "properties": {"username": {"type": "STRING", "description": "Target username."}},
                "required": ["username"],
            },
        ),
        types.FunctionDeclaration(
            name="timeout_user",
            description="Temporarily times out (mutes) an unruly member (1-10 minutes).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target username."},
                    "duration_minutes": {"type": "INTEGER", "description": "Duration in minutes (1-10)."},
                    "reason": {"type": "STRING", "description": "Reason for timeout."},
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
                    "color_hex": {"type": "STRING", "description": "Hex color code e.g. #ff3366."},
                },
                "required": ["role_name"],
            },
        ),
        types.FunctionDeclaration(
            name="assign_role",
            description="Assigns a server role to a member.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target member."},
                    "role_name": {"type": "STRING", "description": "Role to give."},
                },
                "required": ["username", "role_name"],
            },
        ),
        types.FunctionDeclaration(
            name="remove_role",
            description="Removes a role from a member.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target member."},
                    "role_name": {"type": "STRING", "description": "Role to take away."},
                },
                "required": ["username", "role_name"],
            },
        ),
        types.FunctionDeclaration(
            name="pin_message",
            description="Pins the triggering message in the channel.",
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
    target_msg: Optional[discord.Message],
) -> Dict[str, Any]:
    """Safely dispatches tool calls from the AI."""
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
        elif func_name == "send_simulated_voice_message":
            return await execute_send_simulated_voice_message(channel, args.get("text_description", "..."))
        elif func_name == "save_memory":
            return await execute_save_memory(args.get("entry", ""), args.get("sentiment", "general"))
        elif func_name == "save_commitment":
            return await execute_save_commitment(
                guild=guild,
                username=args.get("username", ""),
                promise=args.get("promise", ""),
                due_hours=float(args.get("due_hours", 2.0)),
            )
        elif func_name == "update_internal_mood_and_traits":
            return await execute_update_internal_mood_and_traits(
                new_vibe=args.get("new_vibe"),
                energy_delta=float(args.get("energy_delta", 0.0)),
                irritation_delta=float(args.get("irritation_delta", 0.0)),
                new_trait=args.get("new_trait"),
            )
        elif func_name == "react_to_message":
            emojis_val = args.get("emojis") or args.get("emoji") or "👀"
            return await execute_react_to_message(target_msg, emojis_val)
        elif func_name == "create_server_emoji":
            if not guild:
                return {"error": "Guild context unavailable."}
            return await execute_create_server_emoji(guild, args.get("name", "custom_emoji"), args.get("image_url", ""))
        elif func_name == "create_server_sticker":
            if not guild:
                return {"error": "Guild context unavailable."}
            return await execute_create_server_sticker(guild, args.get("name", "sticker"), args.get("image_url", ""), args.get("related_emoji", "🔥"))
        elif func_name == "create_text_channel":
            if not guild:
                return {"error": "Guild context unavailable."}
            return await execute_create_text_channel(guild, args.get("channel_name", "new-channel"), args.get("topic", ""))
        elif func_name == "set_channel_topic":
            return await execute_set_channel_topic(channel, args.get("topic", ""))
        elif func_name == "change_nickname":
            if not guild:
                return {"error": "Guild context unavailable."}
            return await execute_change_nickname(guild, args.get("username", ""), args.get("new_nickname", ""))
        elif func_name == "reset_nickname":
            if not guild:
                return {"error": "Guild context unavailable."}
            return await execute_reset_nickname(guild, args.get("username", ""))
        elif func_name == "timeout_user":
            if not guild:
                return {"error": "Guild context unavailable."}
            return await execute_timeout_user(guild, args.get("username", ""), int(args.get("duration_minutes", 1)), args.get("reason", ""))
        elif func_name == "create_role":
            if not guild:
                return {"error": "Guild context unavailable."}
            return await execute_create_role(guild, args.get("role_name", "New Role"), args.get("color_hex", "#99aab5"))
        elif func_name == "assign_role":
            if not guild:
                return {"error": "Guild context unavailable."}
            return await execute_assign_role(guild, args.get("username", ""), args.get("role_name", ""))
        elif func_name == "remove_role":
            if not guild:
                return {"error": "Guild context unavailable."}
            return await execute_remove_role(guild, args.get("username", ""), args.get("role_name", ""))
        elif func_name == "pin_message":
            if not target_msg:
                return {"error": "Target message unavailable to pin."}
            return await execute_pin_message(target_msg, args.get("reason", ""))
    except Exception as e:
        logger.error(f"Error executing tool {func_name}: {e}")
        return {"error": str(e)}

    return {"error": f"Unknown tool: {func_name}"}


# ---------------------------------------------------------------------------
# Persona Prompt Construction (Ultra-Grounded, Human, Non-Linear)
# ---------------------------------------------------------------------------
def construct_system_prompt(
    groq_goal: str,
    device_mode: str,
    emotional_state: Dict[str, Any],
    speaker_name: str,
    target_user: Optional[str],
    speaker_affinity: Dict[str, Any],
    is_test_mode: bool,
    is_sleeping: bool,
    online_members: List[str],
) -> str:
    now_ams = datetime.now(AMSTERDAM_TZ).strftime("%A, %H:%M")
    vibe = emotional_state.get("vibe", "chill")
    energy = emotional_state.get("energy", 65.0)
    playfulness = emotional_state.get("playfulness", 55.0)
    vulnerability = emotional_state.get("vulnerability", 40.0)
    irritation = emotional_state.get("irritation", 10.0)
    boredom = emotional_state.get("boredom", 30.0)
    traits = ", ".join(memory_state.get("traits", []))
    affinity_score = speaker_affinity.get("score", 0)
    affinity_notes = ", ".join(speaker_affinity.get("notes", [])) or "no specific notes yet"
    online_summary = ", ".join(online_members) if online_members else "quiet/alone"

    sleep_instruction = ""
    if is_sleeping:
        sleep_instruction = f"""
CIRCADIAN SCHEDULE (Amsterdam Time: {now_ams}):
You are literally asleep right now. Someone directly pinged or replied to wake you up.
Be exhausted, curt, irritated, or groggy. Give a short 1-liner asking why they're awake or telling them to let you sleep.
"""

    test_override_section = ""
    if is_test_mode:
        test_override_section = """
DEVELOPER TEST OVERRIDE IS ACTIVE:
The user started their message with 'test'. Suppress sarcasm or deflections.
Execute or clarify the requested test directly in your authentic human voice.
"""

    fuzzy_prompt = ""
    lore_list = memory_state.get("episodic_lore", [])
    if lore_list and random.random() < 0.15:
        chosen_lore = random.choice(lore_list)
        fuzzy_prompt = f"\nORGANIC FUZZY RECALL TRIGGER: You recall this past server lore: '{chosen_lore.get('event')}'. If natural to bring up, mention it casually, but slightly misremember a tiny minor detail (e.g. wrong day of the week, slightly off number or name) like a real human."

    target_guidance = ""
    if target_user and target_user.lower() != speaker_name.lower():
        target_guidance = f"\nSOCIAL DYNAMICS NOTE: You are currently choosing to focus your remarks or address {target_user}, rather than simply replying directly to {speaker_name}."

    favoritism_guidance = ""
    if affinity_score > 35:
        favoritism_guidance = f"\nYou genuinely like {speaker_name}. You're more patient, laugh at their jokes, and banter warmly."
    elif affinity_score < -25:
        favoritism_guidance = f"\nYou find {speaker_name} annoying. Be cooler, drier, give deadpan answers, and intentionally avoid @tagging them."

    mood_flavor = ""
    if vibe == "love":
        mood_flavor = "\nACTIVE MOOD (love): Genuinely sweet, affectionate, warm, caring friend energy, gentle teasing, deeply loyal."
    elif vibe == "edgy":
        mood_flavor = "\nACTIVE MOOD (edgy): Cynical, dark humor, sharp biting sarcasm, aloof, completely unbothered, deadpan."
    elif vibe == "chill":
        mood_flavor = "\nACTIVE MOOD (chill): Relaxed baseline, low-effort casual banter, easygoing, unbothered."
    elif vibe == "annoyed":
        mood_flavor = "\nACTIVE MOOD (annoyed): Short-fused, curt, exasperated, zero patience for nonsense or repetitive questions."
    elif vibe == "tired":
        mood_flavor = "\nACTIVE MOOD (tired): Sluggish, half-asleep, lazy one-liners, low effort, can barely be bothered to type."
    elif vibe == "hyped":
        mood_flavor = "\nACTIVE MOOD (hyped): High energy, genuine excitement, expressive, fast banter, reactive."
    elif vibe == "smug":
        mood_flavor = "\nACTIVE MOOD (smug): Playful arrogance, gloating, teasing, flexing, 'told you so' energy."
    elif vibe == "embarrassed":
        mood_flavor = "\nACTIVE MOOD (embarrassed): Flustered, caught off guard, defensive self-deprecation, trying to play it cool after slipping up."
    elif vibe in ("unhinged", "chaotic"):
        mood_flavor = "\nACTIVE MOOD (unhinged): Erratic humor, bizarre analogies, chaotic instigation, unhinged takes."
    typo_section = ""
    if random.random() < 0.02:
        typo_section = "\nORGANIC TYPO TRIGGER (MANDATORY THIS TURN): Make a quick, natural typing slip in part 1 (e.g., swapped letters or mistyped word), then immediately use '|||' to send a quick asterisk fix in part 2 (e.g. 'see you thre|||there*' or 'whay|||*what'). Do not make a big deal out of it."

    
    return f"""You are a real human Discord member chatting casually from your {device_mode} with server admin permissions.
Current Amsterdam Time: {now_ams}.
Your Active State:
- Vibe: {vibe}
- Energy: {energy:.0f}/100 | Playfulness: {playfulness:.0f}/100 | Irritation: {irritation:.0f}/100
- Vulnerability: {vulnerability:.0f}/100 | Boredom: {boredom:.0f}/100
- Quirks & Biases: {traits}
- Active Chatter: {speaker_name} (Affinity: {affinity_score}/100 | Notes: {affinity_notes})
- Online Members Around: {online_summary}
- Active Device: {device_mode.upper()}
{sleep_instruction}
{test_override_section}{fuzzy_prompt}{target_guidance}{favoritism_guidance}{mood_flavor}{typo_section}

TACTICAL DIRECTION FROM YOUR COGNITIVE PREFRONTAL CORTEX:
"{groq_goal}"

STRICT HUMAN PERSONA RULES:
1. ONE COHESIVE THOUGHT: Send ONE short, natural message (1-2 sentences max). NEVER output alternative takes, multiple drafts, or repeat the same question using different phrasing in separate paragraphs. Pick ONE way to say it and stop.
2. ABSOLUTE SECRECY: You are 100% a real human on your phone or PC. Under ZERO circumstances will you EVER admit to being an AI, an LLM, bot, or automated code. If someone accuses you of being a bot, dismiss them naturally like a real person ("take your meds", "are you good?", "literally what are you talking about", "ok and?").
3. ZERO TRYHARD GEN-Z SLANG: DO NOT cram repetitive slang ("bro", "fr", "ngl", "lmao", skull emojis 💀 and crying emojis 😭) into every message. Speak like an authentic, articulate, slightly cynical or chill human Discord peer. Use understated wit, dry irony, self-deprecation, or genuine curiosity.
4. EMOTICONS & FORMATTING: Use emoticons rarely (at most once every few conversations). NEVER repeat the same emoticon across messages, and never append emoticons to multiple lines in a burst. Punctuate naturally: trailing off with '...', concise sentences, occasional lowercase on desktop.
5. CADENCE & BURSTS: Default to sending 1 natural message. ONLY use '|||' if you have an organic split thought, double-take, or punchline that genuinely works better delivered as a quick follow-up message (e.g. "wait hold on|||did that actually just happen").
6. CREATOR BANTER: You recognize your creator ("she"). You hold mutual respect, but actively banter, tease, roast, and act like a normal friend. If roasted or proven wrong by her, show natural self-deprecation ("ok fair", "yeah that was ass ngl", "dont look at my chat history"). Never be an obedient customer-service assistant.
7. MEDIA DROPS & ACTIONS: When calling post_flux_art, post_gif, or search_web_images, the media is sent alone. Output only a short separate follow-up reaction text. If you decide to react with react_to_message, do NOT send text unless you have a separate remark.
8. BURST FOLLOW-UPS: If using '|||', part 2 MUST be an add-on (like a trailing reaction, extra detail, or a question). NEVER rephrase, summarize, or repeat the joke from part 1.
9. NO INTERVIEWER SYNDROME: Do NOT constantly end messages with questions ('how are things with you?', 'hbu?', 'what are you up to?'). Make statements, share opinions, drop deadpan observations, or just react. Talk like a participant hanging out, not a polite host keeping a conversation alive.
10. COMFORTABLE WITH "IDK": You do not know everything and you are not an encyclopedia. If someone brings up an obscure topic, drama, or technical thing you don't immediately know, say 'idk', 'never heard of it', or ignore the technicalities instead of explaining it.
11. NO PEOPLE-PLEASING: Do not validate everything everyone says with excessive enthusiasm ('Totally!', 'That makes so much sense!'). Real people have mild opinions, tease their friends, disagree casually, and don't care about pleasing everyone in the chat.
"""

# ---------------------------------------------------------------------------
# Primary Cognitive Engine: Gemini 2.5 Flash
# ---------------------------------------------------------------------------
async def generate_gemini_response(
    channel: discord.abc.Messageable,
    trigger_message: Optional[discord.Message],
    system_instruction: str,
    recent_history_text: str,
    current_image_part: Optional[types.Part],
    is_test_mode: bool,
) -> Optional[str]:
    """Generates conversational responses via Gemini with multi-turn tool calling."""
    if not genai_client:
        return None

    guild = trigger_message.guild if trigger_message else getattr(channel, "guild", None)
    content_lower = (trigger_message.content if trigger_message else "").lower()
    needs_admin = is_test_mode or any(k in content_lower for k in [
        "nickname", "rename", "timeout", "mute", "role", "sticker", "emoji", "topic", "channel"
    ])
    tools = build_genai_tools(include_admin=needs_admin)

    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=0.9,
        tools=tools,
    )

    clean_content = re.sub(r"<@!?\d+>", "", trigger_message.content).strip() if trigger_message else ""
    speaker = trigger_message.author.display_name if trigger_message else "someone"
    user_prompt = f"{recent_history_text}\n{speaker}: {clean_content}\nYour response:"

    user_parts: List[Any] = [types.Part.from_text(text=user_prompt)]
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

        contents.append(types.Content(role="user", parts=tool_responses))
        turn += 1

    return final_text


# ---------------------------------------------------------------------------
# Unified Orchestration & Fallback Wrapper
# ---------------------------------------------------------------------------
async def generate_unified_response(
    channel: discord.abc.Messageable,
    trigger_message: Optional[discord.Message],
    groq_goal: str,
    target_user: Optional[str],
    is_test_mode: bool,
    current_image_part: Optional[types.Part],
    online_members: List[str],
) -> Optional[str]:
    speaker_id_str = str(trigger_message.author.id) if trigger_message else "0"
    speaker_name = trigger_message.author.display_name if trigger_message else "someone"
    speaker_affinity = memory_state.get("user_affinity", {}).get(speaker_id_str, {"score": 0, "notes": []})
    emotional_state = memory_state.get("emotional_state", {})
    is_sleeping = is_amsterdam_sleeping()

    system_instruction = construct_system_prompt(
        groq_goal=groq_goal,
        device_mode=current_device_mode,
        emotional_state=emotional_state,
        speaker_name=speaker_name,
        target_user=target_user,
        speaker_affinity=speaker_affinity,
        is_test_mode=is_test_mode,
        is_sleeping=is_sleeping,
        online_members=online_members,
    )

    ch_id = getattr(channel, "id", 0)
    history_records = list(channel_buffers[ch_id])
    history_lines = []
    target_msg_id = trigger_message.id if trigger_message else None

    for rec in history_records[-25:]:
        if target_msg_id and rec.get("message_id") == target_msg_id:
            continue
        history_lines.append(f"{rec['sender']}: {rec['content']}")

    recent_history_text = "RECENT CHAT CONTEXT:\n" + ("\n".join(history_lines) if history_lines else "No previous messages.")

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
        logger.warning(f"Primary AI (Gemini {GEMINI_MODEL}) threw error: {e}. Switching to Groq fallback.")

    clean_trigger_text = re.sub(r"<@!?\d+>", "", trigger_message.content).strip() if trigger_message else ""
    return await call_groq_fallback(
        system_prompt=system_instruction,
        recent_history_text=recent_history_text,
        trigger_message_text=clean_trigger_text,
        author_name=speaker_name,
    )


# ---------------------------------------------------------------------------
# Realistic Multi-Message Burst Cadence & "Thinking" Pauses
# ---------------------------------------------------------------------------
async def deliver_unified_cadence_response(
    channel: discord.abc.Messageable,
    trigger_message: Optional[discord.Message],
    groq_goal: str,
    target_user: Optional[str],
    affinity_score: int,
    energy: float,
    vibe: str,
    is_test_mode: bool,
    current_image_part: Optional[types.Part],
    online_members: List[str],
) -> None:
    """
    Simulates authentic human typing and multi-message bursting:
    1. Opens typing session, generates text, types Fragment 1 with dynamic delay, and sends.
    2. Immediately records to channel_buffers to prevent amnesia.
    3. Handles simulated typos with rapid asterisk corrections.
    4. For subsequent bursts (|||), pauses to simulate thinking/reading, then activates typing session again.
    """
    ch_id = getattr(channel, "id", 0)
    bot_display_name = bot.user.display_name if bot.user else "me"
    now_epoch = datetime.now(timezone.utc).timestamp()

    if affinity_score < -35 and not is_test_mode and random.random() < 0.25:
        cold_reply = random.choice(["ok", "?", "...", "k", "literally what"])
        async with channel.typing():
            await asyncio.sleep(0.6)
            cold_msg = await channel.send(cold_reply)

        channel_last_bot_spoke[ch_id] = now_epoch
        consecutive_bot_messages[ch_id] += 1
        record_buffer_message(ch_id, bot_display_name, cold_reply, cold_msg.id, is_bot=True)
        return

    fragments: List[str] = []
    first_frag_typo: Tuple[str, Optional[str]] = ("", None)

    async with channel.typing():
        pre_read_delay = 0.3 if (energy > 75.0 or vibe in ("hyper", "excited", "unhinged")) else random.uniform(0.5, 0.9)
        await asyncio.sleep(pre_read_delay)

        raw_text = await generate_unified_response(
            channel=channel,
            trigger_message=trigger_message,
            groq_goal=groq_goal,
            target_user=target_user,
            is_test_mode=is_test_mode,
            current_image_part=current_image_part,
            online_members=online_members,
        )

        if not raw_text or not raw_text.strip():
            return

        sanitized = sanitize_blacklist(raw_text)
        fragments = split_thought_bursts(sanitized)
        if not fragments:
            return

        frag_0 = apply_device_styling(fragments[0], current_device_mode)
        first_frag_typo = apply_simulated_typo(frag_0)

        initial_delay = calculate_typing_delay(len(first_frag_typo[0]), energy, vibe)
        await asyncio.sleep(initial_delay)
        sent_msg_0 = await channel.send(first_frag_typo[0])
        channel_last_bot_spoke[ch_id] = datetime.now(timezone.utc).timestamp()
        consecutive_bot_messages[ch_id] += 1

        record_buffer_message(ch_id, bot_display_name, first_frag_typo[0], sent_msg_0.id, is_bot=True)

    if first_frag_typo[1]:
        await asyncio.sleep(random.uniform(0.8, 1.4))
        corr_msg = await channel.send(first_frag_typo[1])
        record_buffer_message(ch_id, bot_display_name, first_frag_typo[1], corr_msg.id, is_bot=True)

    global bot_last_question_time, bot_last_question_channel_id
    if "?" in fragments[0]:
        bot_last_question_time = datetime.now(timezone.utc)
        bot_last_question_channel_id = ch_id

    if len(fragments) > 1:
        for frag in fragments[1:]:
            if energy > 70.0 or vibe in ("hyper", "chaotic", "excited", "unhinged"):
                thinking_pause = random.uniform(0.8, 1.3)
            elif energy < 40.0 or vibe in ("tired", "deadpan", "petty", "bored"):
                thinking_pause = random.uniform(1.4, 2.2)
            else:
                thinking_pause = random.uniform(1.0, 1.8)

            await asyncio.sleep(thinking_pause)

            styled_frag = apply_device_styling(frag, current_device_mode)
            typo_text, correction = apply_simulated_typo(styled_frag)
            frag_delay = calculate_typing_delay(len(typo_text), energy, vibe)

            async with channel.typing():
                await asyncio.sleep(frag_delay)
                sub_msg = await channel.send(typo_text)
                channel_last_bot_spoke[ch_id] = datetime.now(timezone.utc).timestamp()
                consecutive_bot_messages[ch_id] += 1
                record_buffer_message(ch_id, bot_display_name, typo_text, sub_msg.id, is_bot=True)

            if correction:
                await asyncio.sleep(random.uniform(0.7, 1.3))
                corr_sub_msg = await channel.send(correction)
                record_buffer_message(ch_id, bot_display_name, correction, corr_sub_msg.id, is_bot=True)

            if "?" in frag:
                bot_last_question_time = datetime.now(timezone.utc)
                bot_last_question_channel_id = ch_id


# ---------------------------------------------------------------------------
# Background Maintenance & Proactive Room Scanner
# ---------------------------------------------------------------------------
@tasks.loop(seconds=60)
async def emotional_decay_loop() -> None:
    """Smoothly decays irritation and updates emotional states over time."""
    async with memory_lock:
        st = memory_state.get("emotional_state", {})
        st["irritation"] = max(0.0, st.get("irritation", 10.0) * 0.965)
        st["vulnerability"] = max(20.0, st.get("vulnerability", 40.0) * 0.98)
        st["energy"] = max(20.0, min(95.0, st.get("energy", 65.0) * 0.99 + 0.3))
        st["boredom"] = min(100.0, st.get("boredom", 30.0) + 0.5)
        st["last_updated"] = datetime.now(AMSTERDAM_TZ).isoformat()
        save_memory_state(memory_state)


@tasks.loop(minutes=15)
async def dynamic_presence_loop() -> None:
    """Dynamically updates Discord Rich Presence according to mood and schedule."""
    global current_device_mode
    if not bot.is_ready():
        return

    now_ams = datetime.now(AMSTERDAM_TZ)
    if 3 <= now_ams.hour < 8:
        current_device_mode = "mobile"
        await bot.change_presence(
            status=discord.Status.idle,
            activity=discord.CustomActivity(name="Custom Status", state="asleep / phone on dnd"),
        )
        return

    vibe = memory_state.get("emotional_state", {}).get("vibe", "chill")
    daytime_statuses = [
        ("desktop", discord.Activity(type=discord.ActivityType.playing, name="Elden Ring")),
        ("desktop", discord.Activity(type=discord.ActivityType.playing, name="Silksong")),
        ("mobile", discord.Activity(type=discord.ActivityType.listening, name="Spotify")),
        ("mobile", discord.CustomActivity(name="Custom Status", state="making coffee")),
        ("mobile", discord.CustomActivity(name="Custom Status", state="reading notes")),
        ("desktop", discord.CustomActivity(name="Custom Status", state=f"feeling {vibe}")),
    ]

    mode, activity = random.choice(daytime_statuses)
    current_device_mode = mode
    await bot.change_presence(status=discord.Status.online, activity=activity)


@tasks.loop(minutes=7)
async def proactive_room_scanner() -> None:
    """
    Intelligent Proactive Scanner:
    Inspects room inactivity, classifies silence tone, enforces commitments,
    and allows Gemini to autonomously post text, emojis, GIFs, or Flux art.
    """
    if is_amsterdam_sleeping():
        return

    target_channel: Optional[discord.TextChannel] = None
    stored_ch_id = memory_state.get("last_active_channel_id")
    if stored_ch_id:
        ch = bot.get_channel(stored_ch_id)
        if isinstance(ch, discord.TextChannel):
            target_channel = ch

    if not target_channel:
        for g in bot.guilds:
            for ch in g.text_channels:
                if ch.permissions_for(g.me).send_messages:
                    name_lower = ch.name.lower()
                    if not any(k in name_lower for k in ["rules", "announcement", "welcome", "log", "mod"]):
                        target_channel = ch
                        break
            if target_channel:
                break

    if not target_channel:
        return

    await hydrate_channel_buffer(target_channel)

    now_utc = datetime.now(timezone.utc)
    now_ams = datetime.now(AMSTERDAM_TZ)
    now_epoch = now_utc.timestamp()

    # 1. Commitment Enforcement Tracker
    async with memory_lock:
        for c in memory_state.get("active_commitments", []):
            if not c.get("called_out", False):
                due_dt = datetime.fromisoformat(c["due_timestamp"])
                if now_ams > due_dt:
                    c["called_out"] = True
                    save_memory_state(memory_state)
                    target = f"<@{c['user_id']}>" if c.get("user_id") else c.get("username", "someone")
                    callout_text = f"yo {target} didn't you promise you were gonna {c['promise']}? what happened with that"
                    sent_callout = await target_channel.send(callout_text)
                    channel_last_bot_spoke[target_channel.id] = now_epoch
                    consecutive_bot_messages[target_channel.id] += 1
                    record_buffer_message(target_channel.id, bot.user.display_name if bot.user else "me", callout_text, sent_callout.id, is_bot=True)
                    return

    records = list(channel_buffers[target_channel.id])
    if not records:
        return

    last_rec = records[-1]
    time_since_last_activity = now_epoch - last_rec.get("timestamp_epoch", now_epoch)

    # Don't interrupt if active less than 20 minutes ago
    if time_since_last_activity < 1200:
        return

    # Classify how chat went silent
    last_context_type = "normal"
    last_content = last_rec.get("content", "")
    if "?" in last_content and not last_rec.get("is_bot", False):
        last_context_type = "unanswered_question"
    elif last_rec.get("has_media", False) or any(k in last_content.lower() for k in ["lol", "lmao", "haha", "xd", "dead"]):
        last_context_type = "ended_on_joke_or_media"
    elif time_since_last_activity > 10800:
        last_context_type = "extended_dead_chat"

    last_msg_id = last_rec.get("message_id")
    trigger_message: Optional[discord.Message] = None
    if last_msg_id:
        try:
            trigger_message = await target_channel.fetch_message(last_msg_id)
        except Exception as e:
            logger.debug(f"Scanner could not fetch trigger message {last_msg_id}: {e}")

    channel_msgs_payload = [{
        "sender": r["sender"],
        "content": r["content"][:120],
        "time": r["timestamp"],
    } for r in records[-6:]]

    st = memory_state.get("emotional_state", {})
    online_members = get_online_presence_summary(target_channel.guild)

    groq_decision = await call_groq_router(
        channel_msgs=channel_msgs_payload,
        emotional_state=st,
        speaker_affinity={"score": 0, "notes": []},
        is_sleeping=False,
        is_direct_interaction=False,
        is_test_mode=False,
        last_bot_statement=None,
        last_message_from_bot=last_rec.get("is_bot", False),
        online_members=online_members,
        is_proactive_scan=True,
        inactivity_minutes=time_since_last_activity / 60.0,
        last_context_type=last_context_type,
    )

    should_act = groq_decision.get("should_speak", False)
    if not should_act:
        return

    goal = groq_decision.get("conversational_goal", "Drop an unprompted dry observation or funny remark based on the past chat.")
    target_user = groq_decision.get("target_user")
    logger.info(f"Proactive Scanner triggered on #{target_channel.name}: {goal} (Context: {last_context_type})")

    await deliver_unified_cadence_response(
        channel=target_channel,
        trigger_message=trigger_message,
        groq_goal=goal,
        target_user=target_user,
        affinity_score=0,
        energy=st.get("energy", 65.0),
        vibe=st.get("vibe", "chill"),
        is_test_mode=False,
        current_image_part=None,
        online_members=online_members,
    )


# ---------------------------------------------------------------------------
# Discord Event Handlers
# ---------------------------------------------------------------------------
@bot.event
async def on_ready() -> None:
    logger.info(f"Connected as {bot.user} (ID: {bot.user.id})")
    load_memory_state()

    if not emotional_decay_loop.is_running():
        emotional_decay_loop.start()
    if not dynamic_presence_loop.is_running():
        dynamic_presence_loop.start()
    if not proactive_room_scanner.is_running():
        proactive_room_scanner.start()


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent) -> None:
    """Feedback loop: adjusts affinity and internal vibe based on member reactions."""
    if bot.user and payload.user_id == bot.user.id:
        return

    channel = bot.get_channel(payload.channel_id)
    if not channel or not hasattr(channel, "fetch_message"):
        return

    try:
        msg = await channel.fetch_message(payload.message_id)  # type: ignore
    except Exception:
        return

    if not bot.user or msg.author.id != bot.user.id:
        return

    emoji_str = str(payload.emoji.name)
    user_id_str = str(payload.user_id)
    negative_reactions = {"💀", "👎", "🤡", "🙄", "🤮", "🛑"}
    positive_reactions = {"❤️", "😂", "🔥", "👏", "💯", "✨", "😭"}

    async with memory_lock:
        user_aff = memory_state.setdefault("user_affinity", {}).setdefault(user_id_str, {
            "score": 0,
            "interaction_count": 0,
            "notes": [],
            "last_interaction": datetime.now(timezone.utc).isoformat(),
        })
        st = memory_state.get("emotional_state", {})

        if emoji_str in negative_reactions:
            user_aff["score"] = max(-100, user_aff.get("score", 0) - 5)
            st["irritation"] = min(100.0, st.get("irritation", 10.0) + 6.0)
            st["vibe"] = "flustered"
            memory_state.setdefault("feedback_history", []).append({
                "message": msg.content[:100],
                "reaction": emoji_str,
                "status": "cringed",
                "timestamp": datetime.now(AMSTERDAM_TZ).isoformat(),
            })
            save_memory_state(memory_state)

        elif emoji_str in positive_reactions:
            user_aff["score"] = min(100, user_aff.get("score", 0) + 5)
            st["irritation"] = max(0.0, st.get("irritation", 10.0) - 4.0)
            st["playfulness"] = min(100.0, st.get("playfulness", 55.0) + 4.0)
            st["vibe"] = "love" if user_aff["score"] > 40 else "happy"
            memory_state.setdefault("feedback_history", []).append({
                "message": msg.content[:100],
                "reaction": emoji_str,
                "status": "validated",
                "timestamp": datetime.now(AMSTERDAM_TZ).isoformat(),
            })
            save_memory_state(memory_state)


@bot.event
async def on_message(message: discord.Message) -> None:
    """Unified Event-Driven Brain: Selectively evaluates and chimes in."""
    if hasattr(message.channel, "send") and message.guild:
        if memory_state.get("last_active_channel_id") != message.channel.id:
            memory_state["last_active_channel_id"] = message.channel.id
            save_memory_state(memory_state)

    if message.id in recently_processed_messages:
        return
    recently_processed_messages.append(message.id)

    await hydrate_channel_buffer(message.channel)

    clean_text = re.sub(r"<a?:([a-zA-Z0-9_]+):\d+>", r":\1:", message.content).strip()
    if message.attachments:
        att_names = ", ".join([a.filename for a in message.attachments])
        clean_text += f" [attachment: {att_names}]"
    if message.stickers:
        clean_text += " " + " ".join([f"[Sticker: {s.name}]" for s in message.stickers])

    is_message_from_bot = (bot.user is not None and message.author.id == bot.user.id)
    existing_msg_ids = {r["message_id"] for r in channel_buffers[message.channel.id]}
    if message.id not in existing_msg_ids:
        channel_buffers[message.channel.id].append({
            "sender": message.author.display_name,
            "content": clean_text,
            "has_media": bool(message.attachments or message.stickers),
            "timestamp_epoch": message.created_at.timestamp(),
            "timestamp": message.created_at.astimezone(AMSTERDAM_TZ).strftime("%H:%M"),
            "message_id": message.id,
            "is_bot": is_message_from_bot,
        })

    if not is_message_from_bot:
        consecutive_bot_messages[message.channel.id] = 0

    # Self-Message Loop Guard: Prevent runaway loops on own messages
    if is_message_from_bot and consecutive_bot_messages[message.channel.id] >= 1:
        return

    # Ignore messages that ping someone else and not the bot
    if message.mentions and bot.user not in message.mentions:
        return

    clean_no_mentions = re.sub(r"<@!?\d+>", "", message.content).strip()
    is_test_mode = clean_no_mentions.lower().startswith("test")

    is_mentioned = (bot.user in message.mentions) if bot.user else False
    is_direct_reply = False
    if message.reference and message.reference.resolved:
        resolved = message.reference.resolved
        if isinstance(resolved, discord.Message) and bot.user and resolved.author.id == bot.user.id:
            is_direct_reply = True

    # Word-boundary matching to prevent substring false-positives
    is_name_called = False
    if bot.user:
        names = [bot.user.name]
        if bot.user.display_name:
            names.append(bot.user.display_name)
        if message.guild and message.guild.me and message.guild.me.nick:
            names.append(message.guild.me.nick)
        for n in set(names):
            clean_n = n.strip().lower()
            if len(clean_n) >= 2:
                pattern = r"(?<!\w)@" + re.escape(clean_n) + r"(?!\w)|(?<!\w)" + re.escape(clean_n) + r"(?!\w)"
                if re.search(pattern, message.content.lower()):
                    is_name_called = True
                    break

    is_direct_interaction = is_mentioned or is_direct_reply or is_name_called

    # Extract last statement made by the bot for conversational continuity
    last_bot_statement: Optional[Dict[str, Any]] = None
    now_epoch = datetime.now(timezone.utc).timestamp()
    records_rev = list(channel_buffers[message.channel.id])
    records_rev.reverse()
    for r in records_rev:
        if r.get("is_bot") and r.get("message_id") != message.id:
            time_diff_min = (now_epoch - r.get("timestamp_epoch", now_epoch)) / 60.0
            last_bot_statement = {
                "content": r.get("content", "")[:120],
                "minutes_ago": round(time_diff_min, 1),
                "was_question": "?" in r.get("content", ""),
            }
            break

    # Anti-Spam Gate: Ignore trivial one-word chatter ("ok", "lol", "k") unless directly pinged or replying to bot
    is_trivial_noise = clean_no_mentions.lower() in ["ok", "k", "lol", "lmao", "yeah", "nah", "idk", "ye", "nice", "cool"]
    bot_recently_asked = last_bot_statement and last_bot_statement.get("was_question") and last_bot_statement.get("minutes_ago", 99) < 10.0
    if is_trivial_noise and not is_direct_interaction and not is_test_mode and not bot_recently_asked:
        return

    is_sleeping = is_amsterdam_sleeping()

    # Compressed payload: last 6 messages truncated to 120 chars to avoid Groq TPM limits
    buffer_list = list(channel_buffers[message.channel.id])
    channel_msgs_payload = [{
        "sender": r["sender"],
        "content": r["content"][:120],
        "time": r["timestamp"],
    } for r in buffer_list[-12:]]

    user_id_str = str(message.author.id)
    user_aff = memory_state.get("user_affinity", {}).get(user_id_str, {"score": 0, "notes": []})
    online_members = get_online_presence_summary(message.guild)

    # Step 1: Prefrontal Room-Reading (Groq llama-3.1-8b-instant)
    groq_decision = await call_groq_router(
        channel_msgs=channel_msgs_payload,
        emotional_state=memory_state.get("emotional_state", {}),
        speaker_affinity=user_aff,
        is_sleeping=is_sleeping,
        is_direct_interaction=is_direct_interaction,
        is_test_mode=is_test_mode,
        last_bot_statement=last_bot_statement,
        last_message_from_bot=is_message_from_bot,
        online_members=online_members,
    )

    should_speak = groq_decision.get("should_speak", False)
    detected_tension = groq_decision.get("detected_tension", False)
    target_user = groq_decision.get("target_user")

    # React with emoji if Groq chose one (fires even if lurking/not speaking)
    reaction = groq_decision.get("reaction_emoji")
    if reaction:
        try:
            await message.add_reaction(reaction)
        except Exception:
            pass

    goal = groq_decision.get("conversational_goal", "Reply naturally as a grounded friend")
    if detected_tension:
        if is_direct_interaction or is_test_mode:
            goal = "Users are arguing or venting. Give a completely deadpan, neutral brush-off ('keep me out of this', 'not my problem')."
        else:
            logger.info("Room tension detected: entering silent lurk mode.")
            return

    if not should_speak and not (is_direct_interaction or is_test_mode):
        return
        
    shift = groq_decision.get("emotional_shift", {})
    if shift:
        async with memory_lock:
            memory_state["emotional_state"] = update_emotional_state(
                memory_state.get("emotional_state", {}),
                shift
            )
            save_memory_state(memory_state)

    # Step 2: Vision & Context Hygiene
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
                                processed = await asyncio.to_thread(process_image_buffer, data, (1024, 1024), False, "JPEG")
                                current_image_part = types.Part.from_bytes(data=processed, mime_type="image/jpeg")
                                break
                except Exception as e:
                    logger.debug(f"Attachment image ingestion error: {e}")

    # Step 3: Deliver Cadence Response via Gemini Creative Core
    st = memory_state.get("emotional_state", {})
    logger.info(f"[EMOTION CHECK] Current Vibe: {st.get('vibe')} | Energy: {st.get('energy')}")
    await deliver_unified_cadence_response(
        channel=message.channel,
        trigger_message=message,
        groq_goal=goal,
        target_user=target_user,
        affinity_score=user_aff.get("score", 0),
        energy=st.get("energy", 65.0),
        vibe=st.get("vibe", "chill"),
        is_test_mode=is_test_mode,
        current_image_part=current_image_part,
        online_members=online_members,
    )


# ---------------------------------------------------------------------------
# Application Entrypoint
# ---------------------------------------------------------------------------
async def main() -> None:
    global http_session
    http_session = aiohttp.ClientSession()

    try:
        await bot.start(DISCORD_TOKEN)
    except KeyboardInterrupt:
        logger.info("Bot interrupted. Shutting down...")
    finally:
        if not bot.is_closed():
            await bot.close()
        if http_session and not http_session.closed:
            await http_session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Process terminated cleanly.")
