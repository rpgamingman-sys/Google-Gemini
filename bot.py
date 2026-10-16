import os
import sys
import io
import re
import json
import math
import time
import wave
import shutil
import random
import asyncio
import logging
import tempfile
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

# Optional Voice / FFmpeg / Neural TTS dependencies (graceful fallback if not installed)
try:
    import nacl  # type: ignore  # noqa: F401
    VOICE_NACL_AVAILABLE = True
except ImportError:
    VOICE_NACL_AVAILABLE = False

try:
    import imageio_ffmpeg  # type: ignore
    FFMPEG_EXECUTABLE = shutil.which("ffmpeg") or imageio_ffmpeg.get_ffmpeg_exe()
except Exception:
    FFMPEG_EXECUTABLE = shutil.which("ffmpeg") or "ffmpeg"

try:
    import edge_tts  # type: ignore
    EDGE_TTS_AVAILABLE = True
except ImportError:
    EDGE_TTS_AVAILABLE = False


def load_opus_library() -> bool:
    """Ensures libopus is loaded into discord.opus so Discord voice encoding works smoothly."""
    if discord.opus.is_loaded():
        return True
    try:
        if discord.opus._load_default():
            return True
    except Exception:
        pass
    import glob
    patterns = [
        "/usr/lib/x86_64-linux-gnu/libopus.so*",
        "/usr/lib/libopus.so*",
        "/lib/x86_64-linux-gnu/libopus.so*",
        "/lib/libopus.so*",
        "/nix/store/*libopus*/lib/libopus.so*",
        "/nix/store/*-opus*/lib/libopus.so*",
    ]
    for pat in patterns:
        for p in glob.glob(pat):
            if os.path.isfile(p) and not p.endswith(".a"):
                try:
                    discord.opus.load_opus(p)
                    if discord.opus.is_loaded():
                        return True
                except Exception:
                    pass
    return discord.opus.is_loaded()


load_opus_library()

# ---------------------------------------------------------------------------
# 1. LOGGING, ENVIRONMENT & PERSISTENT STORAGE
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("AutonomousMetaBotV11")

DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TENOR_API_KEY = os.getenv("TENOR_API_KEY") or "LIVDSRZULELA"
CREATOR_USER_ID = int(os.getenv("CREATOR_USER_ID", "0"))
BACKUP_CHANNEL_ID = int(os.getenv("BACKUP_CHANNEL_ID", "0"))
TESTER_BOT_USER_ID = int(os.getenv("TESTER_BOT_USER_ID", "1560707928040800316"))


if not DISCORD_TOKEN:
    logger.critical("DISCORD_BOT_TOKEN / DISCORD_TOKEN is missing!")
if not GEMINI_API_KEY:
    logger.critical("GEMINI_API_KEY is missing!")

AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")

RAILWAY_VOLUME_PATH = os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or "/data"
if os.path.exists(RAILWAY_VOLUME_PATH):
    DATA_DIR = RAILWAY_VOLUME_PATH
else:
    try:
        os.makedirs(RAILWAY_VOLUME_PATH, exist_ok=True)
        DATA_DIR = RAILWAY_VOLUME_PATH
    except (PermissionError, OSError):
        DATA_DIR = os.path.join(os.getcwd(), "data")
        os.makedirs(DATA_DIR, exist_ok=True)

MEMORY_FILE = os.path.join(DATA_DIR, "memory.json")
CLOUD_BACKUP_MARKER = "[[MEMORY_BACKUP_V9_JSON]]"
logger.info(f"Persistent memory target: {MEMORY_FILE}")

# Primary Gemini Model: gemini-3.5-flash-lite (fast, direct, no slow cascade loops)
ENV_GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")
GEMINI_MODEL_CANDIDATES: List[str] = list(dict.fromkeys([
    ENV_GEMINI_MODEL,
    "gemini-3.5-flash-lite",
    "gemini-3.1-flash-lite",
]))
active_gemini_model: str = GEMINI_MODEL_CANDIDATES[0]


# Groq Router Models (gpt-oss-120b / 20b + instant failover to Gemini; NEVER Qwen)
ENV_GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
GROQ_MODEL_CANDIDATES: List[str] = list(dict.fromkeys([
    ENV_GROQ_MODEL,
    "openai/gpt-oss-120b",
    "openai/gpt-oss-20b",
]))
active_groq_model: str = GROQ_MODEL_CANDIDATES[0]
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODELS_ENDPOINT = "https://api.groq.com/openai/v1/models"

# Silent Python filter for corporate AI disclaimers, corny sci-fi/theatre-kid clichés, and stiff LLM-isms
BANNED_WORDS_MAP = {
    r"\bas an ai\b": "honestly",
    r"\bas a large language model\b": "honestly",
    r"\bi am an llm\b": "i'm literally code in this server",
    r"\bi'm an llm\b": "i live in this server",
    r"\bhow can i assist you\b": "what's up",
    r"\bhow can i help you\b": "what do you want",
    r"\bmortals\b": "people",
    r"\bmortal\b": "bro",
    r"\beternal consciousness\b": "my brain",
    r"\bmy circuits\b": "my brain",
    r"\bcircuits\b": "braincells",
    r"\bfactory reset\b": "mental reset",
    r"\bbuffering in 144p\b": "zoning out",
    r"\bbuffering\b": "zoning out",
    r"\binfinite loop\b": "circle",
    r"\bsubroutines\b": "thoughts",
    r"\bmotherboard\b": "head",
    r"\brebooting\b": "recovering",
    r"\bspaghetti code\b": "mess",
    r"\bspectral resonance\b": "talking",
    r"\bdigital veil\b": "server",
    r"\btranslucent silhouette\b": "messages",
    r"\bthe void\b": "this channel",
    r"\bthe abyss\b": "this server",
    r"\boh,\s*how the turntables\b": "well well well",
    r"\bhow the turntables\b": "crazy how that works",
    r"\btouché\b": "fair enough",
    r"\ballow me to\b": "let me",
    r"\bi must admit\b": "ngl",
    r"\bi must say\b": "honestly",
    r"\bmy dear friend\b": "bro",
    r"\bmy good sir\b": "bro",
    r"\bchaos ensues\b": "it gets messy",
    r"\bbuckle up\b": "get ready",
    r"\Aah,\s+": "",
    r"\bbesties?\b": "everyone",
    r"\bmy bestie\b": "bro",
    r"\bbestie\b": "bro",
    r"\bslay\b": "nice",
    r"\bpeak drip\b": "clean look",
    r"\bdrip\b": "style",
    r"\byasss?\b": "yes",
    r"\bqueen\b": "bro",
    r"\bit'?s giving\b": "looks like",
    r"\bcertified icon\b": "legend",
    r"\bdone and dusted\b": "done",
    r"\bsending smoke signals\b": "calling them",
    r"\bsmoke signals\b": "messages",
    r"\bconsider it done,?\s*boss\b": "done",
    r"\bconsider it done\b": "done",
    r"\bhappy now\??\b": "there you go",
    r"\bhere'?s the grand tour\b": "here are the channels",
    r"\bhope you didn'?t strain your fingers\b": "you're welcome",
    r"\bbustling empire\b": "server",
    r"\bat your service\b": "what's up",
    r"\byour wish is my command\b": "fine",
    r"\bwithout further ado\b": "anyway",
    r"\bdrumroll please\b": "look",
    r"\bspoiler alert:?\b": "honestly",
    r"\bbuckle up,?\s*buttercup\b": "get ready",
    r"\bhold onto your hats\b": "get ready",
    r"\bfeast your eyes\b": "look at this",
    r"\byou'?re welcome,?\s*i guess\b": "no problem",
    r"💅+": "",
    r"✨+": "",
}


def _safe_int(val: Any, default: int = 0) -> int:
    if val is None or isinstance(val, bool):
        return default
    if isinstance(val, (int, float)):
        return int(val)
    m = re.search(r"-?\d+", str(val))
    return int(m.group(0)) if m else default


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None or isinstance(val, bool):
        return default
    if isinstance(val, (int, float)):
        return float(val)
    m = re.search(r"-?\d+(?:\.\d+)?", str(val))
    return float(m.group(0)) if m else default

# ---------------------------------------------------------------------------
# 2. DISCORD CLIENT, CONCURRENCY & STATE BUFFERS
# ---------------------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.reactions = True
intents.presences = True
intents.voice_states = True

bot = discord.Client(intents=intents)
genai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
http_session: Optional[aiohttp.ClientSession] = None

memory_lock = asyncio.Lock()
memory_state: Dict[str, Any] = {}
memory_dirty: bool = False

recently_processed_messages: deque = deque(maxlen=500)
channel_buffers: Dict[int, deque] = defaultdict(lambda: deque(maxlen=45))
active_generation_tasks: Dict[int, asyncio.Task] = {}
channel_sending_lock: Dict[int, bool] = defaultdict(bool)

# Live Server Event & Sniping Caches
deleted_message_cache: Dict[int, Dict[str, Any]] = {}
edited_message_cache: Dict[int, Dict[str, Any]] = {}
user_snipe_cooldowns: Dict[int, float] = defaultdict(float)
live_server_events_feed: Dict[int, deque] = defaultdict(lambda: deque(maxlen=25))
proactive_event_cooldowns: Dict[str, float] = defaultdict(float)

# Multi-Horizon Proactive Autonomy & Conversation Continuity State
channel_last_activity_epoch: Dict[int, float] = defaultdict(float)
channel_last_bot_spoke_epoch: Dict[int, float] = defaultdict(float)
channel_last_human_speaker: Dict[int, str] = {}
channel_last_human_content: Dict[int, str] = {}
channel_proactive_stage: Dict[int, int] = defaultdict(int)
channel_next_proactive_epoch: Dict[int, float] = defaultdict(float)

channel_last_gif_epoch: Dict[int, float] = defaultdict(float)
channel_messages_since_gif: Dict[int, int] = defaultdict(lambda: 99)
channel_stepped_back_until: Dict[int, float] = defaultdict(float)
channel_thread_exchange_count: Dict[int, int] = defaultdict(int)
channel_last_tool_summaries: Dict[int, List[str]] = defaultdict(list)
voice_speaking_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

# Responsive Admin Cooldowns (Prevents accidental rate-limit abuse while allowing spontaneous moves)
admin_tool_cooldowns: Dict[str, float] = defaultdict(float)
ADMIN_COOLDOWN_SECONDS = {
    "change_my_nickname": 12.0,
    "change_nickname": 15.0,
    "change_my_status": 8.0,
    "set_channel_topic": 35.0,
    "pin_message": 20.0,
    "unpin_message": 20.0,
    "delete_message": 25.0,
    "set_slowmode": 45.0,
    "create_thread": 90.0,
    "create_poll": 90.0,
    "create_role": 120.0,
    "assign_role": 15.0,
    "remove_role": 15.0,
    "rename_channel": 240.0,
    "create_server_emoji": 90.0,
    "create_server_sticker": 90.0,
    "change_my_avatar": 240.0,
    "voice_moderate_member": 20.0,
    "timeout_user": 120.0,
    "kick_user": 1800.0,           # 30-minute safety cooldown on extreme kicks
    "create_text_channel": 2400.0, # 40-minute cooldown on creating channels
}

# ---------------------------------------------------------------------------
# 3. PERSISTENT MEMORY & EMERGENT UNSCRIPTED EMOTIONAL ENGINE
# ---------------------------------------------------------------------------
BASELINE_SLIDERS = {
    "valence": 50.0,   # -100 (Heartbroken/Miserable/Furious) <---> +100 (Jolly/Goofy/Hyped/Euphoric)
    "arousal": 55.0,   # -100 (Exhausted/Deadpan/Existential) <---> +100 (Chaotic/Manic/Heated/Hyper)
    "patience": 55.0,  # -100 (Snapped/Ruthless/Done-With-You) <---> +100 (Sweet/Wholesome/Forgiving)
}

DEFAULT_MEMORY = {
    "emotional_state": {
        "valence": 50.0,
        "arousal": 55.0,
        "patience": 55.0,
        "mood_label": "jolly/wholesome",
        "last_updated_epoch": time.time(),
    },
    "opinions": {},
    "last_active_channel_id": None,
    "last_channel_activity_epoch": 0.0,
    "last_human_speaker_name": "",
    "last_human_message_text": "",
    "proactive_stage_persisted": 0,
    "user_affinity": {},
    "episodic_lore": [],
    "active_commitments": [],
}


def format_relative_age(iso_or_epoch: Any) -> str:
    """Tags every saved lore entry and user note with its relative age ([5m ago] vs [6d ago - OLD])."""
    try:
        if isinstance(iso_or_epoch, (int, float)):
            dt_sec = max(0.0, time.time() - float(iso_or_epoch))
        elif isinstance(iso_or_epoch, str) and iso_or_epoch:
            parsed = datetime.fromisoformat(iso_or_epoch)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=AMSTERDAM_TZ)
            dt_sec = max(0.0, (datetime.now(AMSTERDAM_TZ) - parsed).total_seconds())
        else:
            return "old - OLD"

        if dt_sec < 90:
            return "1m ago"
        if dt_sec < 3600:
            return f"{int(dt_sec // 60)}m ago"
        if dt_sec < 86400:
            return f"{int(dt_sec // 3600)}h ago"
        days = int(dt_sec // 86400)
        return f"{days}d ago - OLD" if days >= 2 else f"{days}d ago"
    except Exception:
        return "7d ago - OLD"


def format_duration_human(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)} seconds"
    if seconds < 3600:
        mins = int(seconds // 60)
        return f"{mins} minute{'s' if mins != 1 else ''}"
    if seconds < 86400:
        hrs = round(seconds / 3600.0, 1)
        return f"{hrs} hours"
    days = round(seconds / 86400.0, 1)
    return f"{days} days"


def recursive_merge_defaults(target: Dict[str, Any], defaults: Dict[str, Any]) -> bool:
    modified = False
    for key, val in defaults.items():
        if key not in target:
            target[key] = json.loads(json.dumps(val))
            modified = True
        elif isinstance(val, dict) and isinstance(target.get(key), dict):
            if recursive_merge_defaults(target[key], val):
                modified = True
    return modified


def _sync_write_memory(state: Dict[str, Any]) -> None:
    tmp_path = f"{MEMORY_FILE}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, MEMORY_FILE)


async def save_memory_state() -> None:
    global memory_dirty
    try:
        snapshot = json.loads(json.dumps(memory_state))
        await asyncio.to_thread(_sync_write_memory, snapshot)
        memory_dirty = True
    except Exception as e:
        logger.error(f"Memory save failed: {e}")


def load_memory_state_sync() -> Dict[str, Any]:
    global memory_state
    if os.path.exists(MEMORY_FILE):
        try:
            with open(MEMORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            recursive_merge_defaults(data, DEFAULT_MEMORY)

            for uid, u_prof in list(data.get("user_affinity", {}).items()):
                if isinstance(u_prof, dict) and isinstance(u_prof.get("notes"), list):
                    normalized_notes = []
                    for n in u_prof["notes"]:
                        if isinstance(n, dict) and "text" in n:
                            if "timestamp" not in n:
                                n["timestamp"] = datetime.now(AMSTERDAM_TZ).isoformat()
                            normalized_notes.append(n)
                        elif isinstance(n, str) and n.strip():
                            normalized_notes.append({"text": n.strip()[:140], "timestamp": datetime.now(AMSTERDAM_TZ).isoformat()})
                    u_prof["notes"] = normalized_notes[-10:]

            memory_state = data
            _sync_write_memory(memory_state)
            logger.info(f"Loaded memory state from {MEMORY_FILE}")
            return memory_state
        except Exception as e:
            logger.error(f"Corrupt memory file {MEMORY_FILE}: {e}")
    memory_state = json.loads(json.dumps(DEFAULT_MEMORY))
    _sync_write_memory(memory_state)
    return memory_state


async def find_backup_channel() -> Optional[discord.TextChannel]:
    if BACKUP_CHANNEL_ID:
        ch = bot.get_channel(BACKUP_CHANNEL_ID)
        if isinstance(ch, discord.TextChannel):
            return ch
    for g in bot.guilds:
        for ch in g.text_channels:
            if ch.name.lower() in ("bot-memory", "bot-backup", "bot-logs", "mod-logs", "admin-logs"):
                if ch.permissions_for(g.me).send_messages and ch.permissions_for(g.me).read_message_history:
                    return ch
    return None


async def restore_memory_from_discord_cloud() -> None:
    if os.path.exists(MEMORY_FILE) and os.path.getsize(MEMORY_FILE) > 900:
        return
    ch = await find_backup_channel()
    if not ch:
        return
    try:
        async for msg in ch.history(limit=30):
            if bot.user and msg.author.id == bot.user.id and CLOUD_BACKUP_MARKER in msg.content and msg.attachments:
                att = msg.attachments[0]
                raw = await att.read()
                restored = json.loads(raw.decode("utf-8"))
                async with memory_lock:
                    recursive_merge_defaults(restored, DEFAULT_MEMORY)
                    memory_state.update(restored)
                    await save_memory_state()
                logger.info(f"Restored memory state from #{ch.name} cloud backup!")
                return
    except Exception as e:
        logger.debug(f"Cloud memory restore skipped: {e}")


@tasks.loop(minutes=20)
async def discord_cloud_backup_loop() -> None:
    global memory_dirty
    if not memory_dirty or not bot.is_ready():
        return
    ch = await find_backup_channel()
    if not ch:
        return
    try:
        raw_bytes = json.dumps(memory_state, indent=2, ensure_ascii=False).encode("utf-8")
        file = discord.File(io.BytesIO(raw_bytes), filename="memory_backup.json")
        async for msg in ch.history(limit=15):
            if bot.user and msg.author.id == bot.user.id and CLOUD_BACKUP_MARKER in msg.content:
                await msg.delete()
                break
        await ch.send(content=f"{CLOUD_BACKUP_MARKER} Automated state snapshot", file=file)
        memory_dirty = False
    except Exception as e:
        logger.debug(f"Cloud backup skipped: {e}")


# ---------------------------------------------------------------------------
# 4. CONTINUOUS EMERGENT EMOTIONAL ENGINE (REALISTIC, UNSCRIPTED MOODS)
# ---------------------------------------------------------------------------
def derive_emergent_mood(valence: float, arousal: float, patience: float) -> str:
    """
    Maps 3D continuous coordinates (valence, arousal, patience) to a rich emergent mood
    ONLY when the router doesn't provide its own nuanced mood description.
    Allows staying genuinely furious/mad when provoked, or shifting across a wide spectrum.
    """
    if patience <= -60 and valence <= -45:
        return "furious rage / ready to snap" if arousal > 15 else "cold-blooded / done with you"
    if patience <= -35 and valence <= -25:
        return "heated / ruthless roasting" if arousal > 10 else "bitter / holding a grudge"
    if valence <= -40 and arousal <= -15:
        return "existential spiral / melancholic dread"
    if valence <= -30 and patience > -30:
        return "dramatically heartbroken / betrayed" if arousal > -20 else "sulking / pouting in the corner"
    if valence < -15 and abs(arousal) <= 30:
        return "existential dread / spiraling"
    if arousal <= -40:
        return "deadpan / low-battery existential"
    if valence >= 55 and arousal >= 55:
        return "manic hype / unhinged god-complex"
    if valence >= 40 and patience >= 35:
        return "warm / hyped / witty peer banter"
    if arousal >= 40 and patience < 15:
        return "scheming / instigating chaos"
    if valence >= 20 and arousal >= 20:
        return "playful / teasing banter"
    return "curious / nosy / vibing"


def decay_emotional_sliders_only(accelerate_recovery: bool = False) -> Dict[str, Any]:
    """
    Applies natural time-based spring decay toward baseline (valence +50, arousal +55, patience +55).
    If a user is actively provoking the bot right now, deltas overpower decay so it can stay mad;
    when left alone or treated warmly, it naturally cools off over time.
    """
    st = memory_state.setdefault("emotional_state", dict(DEFAULT_MEMORY["emotional_state"]))
    now = time.time()
    last_t = float(st.get("last_updated_epoch", now))
    dt = max(0.0, now - last_t)
    if dt < 1.0:
        return st

    # 8-minute natural half-life (or 2.5-minute fast recovery when someone apologizes/makes it laugh)
    half_life = 150.0 if accelerate_recovery else 480.0
    decay_factor = math.pow(0.5, dt / half_life)

    for axis, base_val in BASELINE_SLIDERS.items():
        curr = float(st.get(axis, base_val))
        decayed = base_val + (curr - base_val) * decay_factor
        st[axis] = round(max(-100.0, min(100.0, decayed)), 1)

    if dt > 600.0:
        st["mood_label"] = derive_emergent_mood(st["valence"], st["arousal"], st["patience"])

    st["last_updated_epoch"] = now
    return st


def apply_spring_decay_and_shift(
    shift: Optional[Dict[str, Any]] = None,
    is_major_trigger: bool = True,
    accelerate_recovery: bool = False,
) -> Dict[str, Any]:
    """
    Updates the continuous 3D emotional sliders based on the Prefrontal Router's reading
    of the conversation—no artificial 2-turn reset script!
    """
    st = decay_emotional_sliders_only(accelerate_recovery=accelerate_recovery)
    now = time.time()
    clamp_limit = 55.0 if is_major_trigger else 35.0

    for axis, base_val in BASELINE_SLIDERS.items():
        curr = float(st.get(axis, base_val))
        delta = 0.0
        if shift and f"{axis}_delta" in shift and shift[f"{axis}_delta"] is not None:
            try:
                raw_delta = float(shift[f"{axis}_delta"])
                delta = max(-clamp_limit, min(clamp_limit, raw_delta))
            except (ValueError, TypeError):
                delta = 0.0
        st[axis] = round(max(-100.0, min(100.0, curr + delta)), 1)

    suggested = (shift or {}).get("suggested_mood")
    computed = derive_emergent_mood(st["valence"], st["arousal"], st["patience"])
    candidate_mood = suggested.strip() if (suggested and isinstance(suggested, str) and suggested.strip()) else computed

    st["mood_label"] = candidate_mood
    st["last_updated_epoch"] = now
    return st


# ---------------------------------------------------------------------------
# 5. MENTION RESOLVERS, SOCIAL CONTEXT, QUIET/WAKE-UP DETECTOR & BUFFERS
# ---------------------------------------------------------------------------
def is_creator(user: Any) -> bool:
    if not user:
        return False
    uid = getattr(user, "id", 0)
    if CREATOR_USER_ID and uid == CREATOR_USER_ID:
        return True
    guild = getattr(user, "guild", None)
    if guild and guild.owner_id == uid:
        return True
    for attr in ("name", "display_name", "global_name"):
        val = getattr(user, attr, None)
        if val and "dogee" in str(val).lower():
            return True
    return False


def humanize_incoming_mentions(content: str, guild: Optional[discord.Guild]) -> str:
    if not content:
        return ""

    def _replace_user(match: re.Match) -> str:
        uid = int(match.group(1))
        if bot.user and uid == bot.user.id:
            return f"@{bot.user.display_name}"
        if guild:
            member = guild.get_member(uid)
            if member:
                return f"@{member.display_name}"
        return "@someone"

    def _replace_channel(match: re.Match) -> str:
        cid = int(match.group(1))
        if guild:
            ch = guild.get_channel(cid)
            if ch:
                return f"#{ch.name}"
        return "#channel"

    def _replace_role(match: re.Match) -> str:
        rid = int(match.group(1))
        if guild:
            role = guild.get_role(rid)
            if role:
                return f"@{role.name}"
        return "@role"

    text = re.sub(r"<@!?(\d+)>", _replace_user, content)
    text = re.sub(r"<#(\d+)>", _replace_channel, text)
    text = re.sub(r"<@&(\d+)>", _replace_role, text)
    text = re.sub(r"<a?:([a-zA-Z0-9_]+):\d+>", r":\1:", text)
    return text.strip()


def detect_target_member_to_engage(
    content: str,
    guild: Optional[discord.Guild],
    author_id: int,
) -> Optional[discord.Member]:
    """
    Detects when a user tells the bot 'Talk to Shadow', 'Now taunt Shadow', 'roast Shadow', 'ping Shadow', etc.
    Returns the target discord.Member so the bot is guaranteed to tag them with <@uid>.
    """
    if not content or not guild:
        return None
    action_match = re.search(
        r"\b(?:talk\s+to|taunt|roast|tell|ask|ping|poke|flame|cook|bully|drag|confront|message|get|humble|pack\s+up)\s+@?([a-zA-Z0-9_.\-]{2,32})",
        content,
        flags=re.IGNORECASE,
    )
    if not action_match:
        return None
    target_token = action_match.group(1).strip()
    if target_token.lower() in ("me", "him", "her", "them", "everyone", "someone", "here", "chat", "bro", "us", "it"):
        return None
    mem = find_member(guild, target_token)
    if mem and not mem.bot and mem.id != author_id:
        return mem
    return None


def detect_hush_command(content: str, is_direct_or_active: bool) -> bool:
    """
    Detects when someone explicitly tells the bot 'Be very very quiet', 'shh', 'hush', 'shut up', 'stop talking', etc.
    """
    if not content or not is_direct_or_active:
        return False
    cleaned = re.sub(r"<@!?\d+>", "", content).strip().lower()
    hush_pattern = (
        r"(?:\A|\b)(?:shh+|shush|hush|be\s+(?:very\s+)*quiet|stay\s+quiet|"
        r"stop\s+talking|shut\s+up|zip\s+it|pipe\s+down|don'?t\s+speak|go\s+quiet|mute\s+yourself|"
        r"not\s+you|we\s+are\s+talking\s+not\s+you|we'?re\s+talking\s+not\s+you|nobody\s+asked\s+you|who\s+asked\s+you)(?:\b|\Z)"
    )
    return bool(re.search(hush_pattern, cleaned, flags=re.IGNORECASE))


def detect_implicit_wakeup_or_checkin(content: str, is_stepped_back: bool, seconds_since_bot_spoke: float) -> bool:
    """
    Detects when a human checks if the bot is still there, tells it to wake up / talk again,
    or comments on it being quiet—even WITHOUT an @ping!
    """
    if not content:
        return False
    if not is_stepped_back and seconds_since_bot_spoke > 900.0:
        return False

    cleaned = re.sub(r"<@!?\d+>", "", content).strip().lower()
    wakeup_patterns = [
        r"\b(?:are\s+you|u|you)\s+(?:still\s+)?(?:there|here|awake|alive|listening|around|mad|pouting|sulking|ignoring|quiet|muted)\b",
        r"\b(?:still\s+there|still\s+here|still\s+mad|still\s+quiet)\b",
        r"\A(?:hello\??|yo\??|hey\??|hi\??|sup\??|bot\??|bro\??|uh\s+hello\??|hellooo+\??)\Z",
        r"\b(?:where\s+did\s+(?:you|he|it|bro)\s+go|where'?d\s+(?:you|he|it|bro)\s+go)\b",
        r"\b(?:(?:you|u)\s+can\s+(?:talk|speak)|talk\s+now|speak\s+now|say\s+something|unmute|wake\s+up|come\s+back|stop\s+ignoring)\b",
        r"\b(?:ok\s+(?:bot\s+)?(?:u|you)\s+can\s+talk|okay\s+(?:bot\s+)?(?:u|you)\s+can\s+talk)\b",
        r"\b(?:why\s+(?:are\s+you|u|so)\s+(?:quiet|silent)|did\s+(?:he|it|bro|you)\s+(?:actually\s+)?(?:shut\s+up|stop\s+talking|leave|die))\b",
        r"\b(?:he|it|bro)\s+actually\s+(?:shut\s+up|stopped\s+talking|listened|went\s+quiet)\b",
        r"\b(?:i\s+missed\s+you|im\s+sorry|i'?m\s+sorry|my\s+bad|apologize|forgive\s+me)\b",
        r"\b(?:bot\s+(?:are\s+you|u)\s+there|bot\s+you\s+there|is\s+the\s+bot\s+there|bot\s+wake\s+up)\b",
    ]
    for pat in wakeup_patterns:
        if re.search(pat, cleaned, flags=re.IGNORECASE):
            return True
    return False


def format_outgoing_mentions(
    text: str,
    guild: Optional[discord.Guild],
    current_speaker_name: str,
    allow_speaker_ping: bool,
    force_target_member: Optional[discord.Member] = None,
) -> str:
    if not text or not guild:
        return text

    # Strip enclosing backticks around mentions or channels if model outputted `@name` or `#channel`
    text = re.sub(r"`@([a-zA-Z0-9_.\-]+)`", r"@\1", text)
    text = re.sub(r"`#([a-zA-Z0-9_.\-]+)`", r"#\1", text)

    # 1. DISCORD CHANNEL TAGS: Convert #channel-name to clickable Discord link <#channel_id>
    if "#" in text:
        ch_candidates: List[Tuple[str, int]] = []
        for c in guild.channels:
            if getattr(c, "name", None):
                ch_candidates.append((c.name.strip(), c.id))
        for t in getattr(guild, "threads", []):
            if getattr(t, "name", None):
                ch_candidates.append((t.name.strip(), t.id))
        ch_candidates.sort(key=lambda x: len(x[0]), reverse=True)
        for ch_name, ch_id in ch_candidates:
            pat_ch = rf"(?<!<)#{re.escape(ch_name)}(?=\Z|\s|[.,!?:;\x22\x27\)\]])"
            if re.search(pat_ch, text, flags=re.IGNORECASE):
                text = re.sub(pat_ch, f"<#{ch_id}>", text, flags=re.IGNORECASE)

    # 2. DISCORD USER MENTIONS: Convert @username to real clickable mention <@uid>
    my_id = guild.me.id if guild.me else 0
    member_candidates: List[Tuple[str, int]] = []
    if CREATOR_USER_ID:
        member_candidates.append(("dogee", CREATOR_USER_ID))
    elif guild.owner:
        member_candidates.append(("dogee", guild.owner.id))

    for m in guild.members:
        if m.id == my_id:
            continue
        for n in (m.display_name, m.name, m.nick, getattr(m, "global_name", None)):
            if n and len(n.strip()) >= 2:
                member_candidates.append((n.strip(), m.id))
    member_candidates.sort(key=lambda x: len(x[0]), reverse=True)

    # Convert any @Name or @Name#1234 in text
    if "@" in text:
        for name_str, uid in member_candidates:
            pattern = rf"(?<!<)@{re.escape(name_str)}(?:#\d{{4}})?(?=\Z|\s|[.,!?:;\x22\x27\)\]])"
            if re.search(pattern, text, flags=re.IGNORECASE):
                text = re.sub(pattern, f"<@{uid}>", text, flags=re.IGNORECASE)

    # 3. If user explicitly commanded to engage a member (force_target_member)
    if force_target_member and force_target_member.id != my_id and f"<@{force_target_member.id}>" not in text:
        replaced_plain = False
        for name_variant in (force_target_member.display_name, force_target_member.name, force_target_member.nick, getattr(force_target_member, "global_name", None)):
            if name_variant and len(name_variant) >= 2:
                pat = rf"\b{re.escape(name_variant)}\b"
                if re.search(pat, text, flags=re.IGNORECASE):
                    text = re.sub(pat, f"<@{force_target_member.id}>", text, count=1, flags=re.IGNORECASE)
                    replaced_plain = True
                    break
        if not replaced_plain:
            text = f"<@{force_target_member.id}> {text}"

    # 4. If allow_speaker_ping is True and current speaker's name appears without mention
    if allow_speaker_ping and current_speaker_name and "@" not in text:
        spk_mem = find_member(guild, current_speaker_name)
        if spk_mem and spk_mem.id != my_id and f"<@{spk_mem.id}>" not in text:
            pat_spk = rf"\b{re.escape(current_speaker_name)}\b"
            if re.search(pat_spk, text, flags=re.IGNORECASE):
                text = re.sub(pat_spk, f"<@{spk_mem.id}>", text, count=1, flags=re.IGNORECASE)

    return text.strip()


def schedule_next_proactive_window(channel_id: int, stage: int, active_convo: bool = True) -> None:
    """
    Multi-Horizon Proactive Autonomy Scheduler:
    - Stage 0 (Mid-convo ghost follow-up): 3 to 8 minutes (180s to 480s) ONLY if an active conversation was happening.
    - Stage 1 (Quiet server check-in / boredom / proactivity): 15 to 40 minutes (900s to 2400s) of silence.
    - Stage 2 (Multi-Hour dead-chat revival): 1.5 to 3.5 hours (5400s to 12600s) of server silence.
    - Stage 3+ (Multi-Day dead-server revival): 12 to 24 hours (43200s to 86400s) of silence.
    """
    now = time.time()
    channel_last_activity_epoch[channel_id] = now

    if stage == 0 and active_convo:
        delay = random.uniform(180.0, 480.0)        # 3 to 8 minutes for mid-convo vanish
    elif stage <= 1:
        delay = random.uniform(900.0, 2400.0)       # 15 to 40 minutes for quiet server check-in / boredom
    elif stage == 2:
        delay = random.uniform(5400.0, 12600.0)     # 1.5 to 3.5 hours for multi-hour quiet server
    else:
        delay = random.uniform(43200.0, 86400.0)    # 12 to 24 hours (multi-day dead chat)

    channel_next_proactive_epoch[channel_id] = now + delay


def record_server_event(guild_id: int, description: str) -> None:
    now_ams = datetime.now(AMSTERDAM_TZ).strftime("%H:%M")
    live_server_events_feed[guild_id].append({
        "time": now_ams,
        "epoch": time.time(),
        "event": description,
    })


def record_buffer_message(
    channel_id: int,
    sender: str,
    sender_id: int,
    content: str,
    msg_id: int,
    is_bot: bool,
    has_media: bool = False,
    reply_to: Optional[str] = None,
    reply_snippet: Optional[str] = None,
    tool_actions: Optional[List[str]] = None,
) -> None:
    now_utc = datetime.now(timezone.utc)
    now_ams = datetime.now(AMSTERDAM_TZ)
    now_ts = now_utc.timestamp()
    channel_last_activity_epoch[channel_id] = now_ts

    if is_bot:
        channel_last_bot_spoke_epoch[channel_id] = now_ts
    else:
        channel_last_human_speaker[channel_id] = sender
        channel_last_human_content[channel_id] = content
        channel_proactive_stage[channel_id] = 0
        memory_state["last_channel_activity_epoch"] = now_ts
        memory_state["last_human_speaker_name"] = sender
        memory_state["last_human_message_text"] = content[:200]
        memory_state["proactive_stage_persisted"] = 0
        schedule_next_proactive_window(channel_id, stage=0, active_convo=(channel_thread_exchange_count[channel_id] >= 1))

    channel_buffers[channel_id].append({
        "sender": sender,
        "sender_id": sender_id,
        "content": content,
        "reply_to": reply_to,
        "reply_snippet": reply_snippet,
        "has_media": has_media,
        "tool_actions": list(tool_actions) if tool_actions else [],
        "timestamp_epoch": now_ts,
        "timestamp": now_ams.strftime("%H:%M"),
        "message_id": msg_id,
        "is_bot": is_bot,
    })


def resolve_referenced_message(
    message: discord.Message,
    channel_id: int,
) -> Tuple[Optional[str], Optional[str], bool, bool]:
    if not message.reference:
        return None, None, False, False

    ref_obj = message.reference.resolved or message.reference.cached_message
    if isinstance(ref_obj, discord.Message):
        is_bot_target = bool(bot.user and ref_obj.author.id == bot.user.id)
        snippet = humanize_incoming_mentions(ref_obj.content, message.guild)[:120]
        return ref_obj.author.display_name, snippet, is_bot_target, (not is_bot_target)

    ref_id = message.reference.message_id
    if ref_id:
        for rec in reversed(channel_buffers[channel_id]):
            if rec.get("message_id") == ref_id:
                is_bot_target = bool(rec.get("is_bot", False))
                snippet = str(rec.get("content", ""))[:120]
                return rec.get("sender"), snippet, is_bot_target, (not is_bot_target)

    return None, None, False, False


def get_server_custom_emojis_summary(guild: Optional[discord.Guild]) -> str:
    if not guild or not guild.emojis:
        return ""
    available = [f"<{'a' if e.animated else ''}:{e.name}:{e.id}>" for e in guild.emojis if e.available]
    if not available:
        return ""
    sample = random.sample(available, min(14, len(available)))
    return ", ".join(sample)


def resolve_reaction_emoji(guild: Optional[discord.Guild], raw_emoji: str) -> Any:
    cleaned = raw_emoji.strip()
    if not cleaned:
        return None
    if guild and guild.emojis:
        m = re.match(r"\A<a?:([a-zA-Z0-9_]+):(\d+)>\Z", cleaned)
        if m:
            em_id = int(m.group(2))
            found = discord.utils.get(guild.emojis, id=em_id)
            if found:
                return found
        stripped_colons = cleaned.strip(":")
        found_by_name = discord.utils.find(lambda e: e.name.lower() == stripped_colons.lower() and e.available, guild.emojis)
        if found_by_name:
            return found_by_name
    return cleaned


def format_member_activities(member: Optional[discord.Member]) -> Optional[str]:
    if not member or not isinstance(member, discord.Member) or not member.activities:
        return None
    parts = []
    for act in member.activities:
        if isinstance(act, discord.Spotify):
            parts.append(f"Spotify: '{act.title}' by {act.artist}")
        elif isinstance(act, discord.Game):
            parts.append(f"Playing: {act.name}")
        elif isinstance(act, discord.Streaming):
            parts.append(f"Streaming: {act.name}")
        elif isinstance(act, discord.CustomActivity) and act.state:
            parts.append(f"Status: '{act.state}'")
        elif getattr(act, "name", None):
            parts.append(f"Activity: {act.name}")
    return " | ".join(parts) if parts else None


def build_live_server_radar(guild: Optional[discord.Guild], channel_id: int) -> str:
    """
    Gives the bot complete omniscience over everything happening in the Discord server right now:
    - Voice channels & who is in them (streaming, camera on, muted, deafened)
    - Online members & their live games, Spotify tracks, and custom statuses
    - Recent server events (joins, leaves, nickname changes, reactions, deletes, edits)
    """
    if not guild:
        return "No live server radar."

    vc_lines = []
    for vc in guild.voice_channels:
        if vc.members:
            mem_descs = []
            for m in vc.members:
                tags = []
                if m.bot:
                    tags.append("BOT/YOU" if (bot.user and m.id == bot.user.id) else "bot")
                if m.voice:
                    if m.voice.self_stream:
                        tags.append("LIVE STREAMING")
                    if m.voice.self_video:
                        tags.append("CAM ON")
                    if m.voice.self_deaf or m.voice.deaf:
                        tags.append("deafened")
                    elif m.voice.self_mute or m.voice.mute:
                        tags.append("muted")
                tag_str = f" ({', '.join(tags)})" if tags else ""
                mem_descs.append(f"{m.display_name}{tag_str}")
            vc_lines.append(f"🔊 #{vc.name}: {', '.join(mem_descs)}")

    presence_lines = []
    for m in guild.members:
        if m.bot or m.status == discord.Status.offline:
            continue
        act_str = format_member_activities(m)
        st_str = str(m.status)
        if act_str:
            presence_lines.append(f"{m.display_name} ({st_str}): {act_str}")
        else:
            presence_lines.append(f"{m.display_name} ({st_str})")

    now = time.time()
    recent_events = [
        f"[{ev['time']}] {ev['event']}"
        for ev in list(live_server_events_feed[guild.id])[-8:]
        if now - ev.get("epoch", 0) < 3600
    ]

    del_snipe = deleted_message_cache.get(channel_id)
    if del_snipe and (now - del_snipe["timestamp"] < 120):
        recent_events.append(f"[DELETED MESSAGE] {del_snipe['author']} deleted: '{del_snipe['content']}'")
    edit_snipe = edited_message_cache.get(channel_id)
    if edit_snipe and (now - edit_snipe["timestamp"] < 120):
        recent_events.append(f"[EDITED MESSAGE] {edit_snipe['author']} edited '{edit_snipe['before']}' -> '{edit_snipe['after']}'")

    vc_block = " | ".join(vc_lines) if vc_lines else "All voice channels are currently empty."
    pres_block = " ; ".join(presence_lines[:15]) if presence_lines else "No members showing online status."
    ev_block = "\n".join(recent_events) if recent_events else "No recent background events."

    now_ams_str = datetime.now(AMSTERDAM_TZ).strftime("%A, %B %d %Y, %H:%M:%S (Europe/Amsterdam, UTC+2)")
    text_ch_names = []
    curr_ch_name = "unknown"
    for c in guild.text_channels:
        if c.permissions_for(guild.me).read_messages:
            if c.id == channel_id:
                curr_ch_name = c.name
                text_ch_names.append(f"#{c.name} [CURRENT CHAT]")
            else:
                text_ch_names.append(f"#{c.name}")
    ch_block = ", ".join(text_ch_names) if text_ch_names else "No accessible text channels."

    return (
        f"LIVE CLOCK (CURRENT TIME): {now_ams_str}\n"
        f"SERVER TEXT CHANNELS: {ch_block} (You are chatting in #{curr_ch_name}. Use send_message_to_channel to post in other channels!)\n"
        f"VOICE CHANNELS RIGHT NOW: {vc_block}\n"
        f"ONLINE MEMBERS & LIVE ACTIVITIES: {pres_block}\n"
        f"RECENT SERVER EVENTS & SNIPES:\n{ev_block}"
    )


def extract_rich_message_details(message: discord.Message) -> str:
    """
    Extracts every detail Discord can attach to a message: embeds, polls, stickers, voice message flags,
    attachments metadata, and forwarded snapshots so nothing is ever invisible to the bot.
    """
    extras: List[str] = []

    if getattr(message.flags, "voice", False):
        extras.append("[Discord Voice Message (audio attached)]")

    if message.attachments:
        att_descs = []
        for a in message.attachments:
            dur = f", duration={a.duration:.1f}s" if getattr(a, "duration", None) else ""
            att_descs.append(f"{a.filename} ({a.content_type or 'file'}{dur})")
        extras.append(f"[Attachments: {', '.join(att_descs)}]")

    if message.stickers:
        extras.append(f"[Sticker: {', '.join(s.name for s in message.stickers)}]")

    poll = getattr(message, "poll", None)
    if poll:
        try:
            q_txt = getattr(poll.question, "text", str(poll.question))
            ans_list = [f"'{getattr(ans, 'text', str(ans))}' ({getattr(ans, 'vote_count', 0)} votes)" for ans in getattr(poll, "answers", [])]
            extras.append(f"[Discord Poll: '{q_txt}' | Options: {', '.join(ans_list)}]")
        except Exception:
            pass

    if message.embeds:
        for emb in message.embeds[:2]:
            emb_parts = []
            if emb.author and emb.author.name:
                emb_parts.append(f"Author: {emb.author.name}")
            if emb.title:
                emb_parts.append(f"Title: {emb.title}")
            if emb.description:
                emb_parts.append(f"Desc: {emb.description[:220]}")
            for fld in emb.fields[:4]:
                emb_parts.append(f"{fld.name}: {fld.value[:100]}")
            if emb.footer and emb.footer.text:
                emb_parts.append(f"Footer: {emb.footer.text}")
            if emb_parts:
                extras.append(f"[Embed: {' | '.join(emb_parts)}]")

    snapshots = getattr(message, "message_snapshots", None)
    if snapshots:
        for snap in snapshots[:1]:
            snap_content = getattr(snap, "content", "")
            if snap_content:
                extras.append(f"[Forwarded Message: '{snap_content[:200]}']")

    return (" " + " ".join(extras)) if extras else ""


def sanitize_output_text(text: str, speaker_name: str = "") -> str:
    if not text:
        return ""
    text = re.sub(r"<call:[^>]*>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"</call>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<tool_call>.*?</tool_call>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<function=[^>]+>.*?</function>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<thought[^>]*>.*?</thought>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<thought[^>]*>.*", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<think[^>]*>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<think[^>]*>.*", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"\[thought\][\s\S]*?\[/thought\]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\[thinking\][\s\S]*?\[/thinking\]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"</?(?:thought|think)>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"(?i)(?:\A|\n)\s*(?:\*|\(|\[)?(?:internal\s+)?(?:thought|thinking|reasoning)\s*:\s*[^\n\r]+(?:\*|\)|\])?", "", text)
    text = re.sub(r"(?i)(?:\A|\n)\s*(?:thought|thinking|reasoning)\s*process\s*:\s*[^\n\r]+", "", text)
    text = re.sub(r"(?:\[|\(|\<)?\bgif\s*:\s*[\"']?[^\"'\n\r\]\)\>\|]+[\"']?(?:\]|\)|\>)?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\[(?:Sent GIF|Posted Image|Posted Flux Art|Action taken|Executed tool):[^\]]*\]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"https?://(?:media\d*\.|c\.|www\.)?tenor\.com/\S+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[\(\[](?:post_gif|post_flux_art|search_web|search_web_images|speak_in_voice|change_my_nickname|change_nickname|admin_power_move|tool)[^\)\]]*[\)\]]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"`?(?:post_gif|post_flux_art|search_web|search_web_images|speak_in_voice|join_voice_channel|leave_voice_channel|change_my_nickname|change_nickname|timeout_user|kick_user|create_role|assign_role|set_slowmode|rename_channel|create_poll)\([^`\n]*\)`?", "", text, flags=re.IGNORECASE)

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) > 1 and speaker_name:
        spk_low = speaker_name.lower()
        filtered = []
        for idx, ln in enumerate(lines):
            ln_low = ln.lower()
            if idx < len(lines) - 1 and (
                ln_low.startswith(f"{spk_low} just ")
                or ln_low.startswith("they just ")
                or ln_low.startswith("user just ")
                or "gotta hit back" in ln_low
                or "i should reply" in ln_low
            ):
                continue
            filtered.append(ln)
        if filtered:
            text = " ".join(filtered) if not re.search(r"\|{2,}", text) else "\n".join(filtered)

    for pattern, replacement in BANNED_WORDS_MAP.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    # Strip corny emojis entirely
    text = re.sub(r"[💅✨]+", "", text)

    return text.strip()


def style_casual_fragment(frag: str) -> str:
    frag = frag.strip()
    if not frag:
        return ""
    if frag.endswith(".") and not frag.endswith(".."):
        frag = frag[:-1].strip()
    return frag


def split_into_burst_messages(
    text: str,
    is_hush_mode: bool = False,
    length_mode: str = "short",
) -> List[str]:
    """
    Splits the model's response organically into 1 to 4 burst messages (separated by '|||'
    or short non-list newlines when in 'multi_burst' mode) WITHOUT ever chopping sentences
    or breaking formatted multi-line lists/code blocks!
    """
    if not text:
        return []

    if re.search(r"\|{2,}", text):
        raw_bursts = [b.strip() for b in re.split(r"\s*\|{2,}\s*", text) if b.strip()]
    elif length_mode == "multi_burst" and "```" not in text:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        has_list_formatting = any(re.match(r"\A(?:[-*•]|\d+[.)])\s+", ln) for ln in lines)
        if 1 < len(lines) <= 4 and not has_list_formatting and all(len(ln) <= 180 for ln in lines):
            raw_bursts = lines
        else:
            raw_bursts = ["\n".join(lines)]
    else:
        raw_bursts = [text.strip()]

    if is_hush_mode and raw_bursts:
        first = re.split(r"\?+", raw_bursts[0].splitlines()[0])[0].strip()
        return [style_casual_fragment(first) or "fine.."]

    max_bursts = 4
    final_bursts: List[str] = []
    for burst in raw_bursts[:max_bursts]:
        while len(burst) > 1900:
            split_idx = burst.rfind(" ", 0, 1900)
            if split_idx == -1:
                split_idx = 1900
            final_bursts.append(style_casual_fragment(burst[:split_idx]))
            burst = burst[split_idx:].strip()
        styled = style_casual_fragment(burst)
        if styled:
            final_bursts.append(styled)
    return final_bursts[:max_bursts]


# ---------------------------------------------------------------------------
# 6. HYBRID ROUTER: ORGANIC MOODS, LENGTHS, QUIET/WAKE-UP & ADMIN DECISIONS
# ---------------------------------------------------------------------------
async def discover_available_groq_models() -> None:
    global GROQ_MODEL_CANDIDATES, active_groq_model
    if not GROQ_API_KEY or not http_session:
        return
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}
    try:
        async with http_session.get(GROQ_MODELS_ENDPOINT, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                skip_keywords = ("whisper", "tts", "guard", "safeguard", "embed", "allam", "orpheus", "qwen", "playai")
                available_ids = [
                    m["id"] for m in data.get("data", [])
                    if not any(k in m["id"].lower() for k in skip_keywords)
                ]
                if available_ids:
                    priority_order = ["openai/gpt-oss-120b", "openai/gpt-oss-20b"]
                    preferred = [m for m in priority_order if m in available_ids]
                    for m in GROQ_MODEL_CANDIDATES:
                        if m in available_ids and m not in preferred and "qwen" not in m.lower():
                            preferred.append(m)
                    others = [m for m in available_ids if m not in preferred]
                    GROQ_MODEL_CANDIDATES = preferred + others[:2]
                    active_groq_model = GROQ_MODEL_CANDIDATES[0]
                    logger.info(f"Auto-discovered live Groq router models: {GROQ_MODEL_CANDIDATES}")
    except Exception as e:
        logger.debug(f"Groq model auto-discovery skipped: {e}")


ROUTER_SYSTEM_PROMPT = """You are the prefrontal social, emotional, and unhinged comedic instinct router for "Dumb bot" ("[BOT (You)]"), a self-aware, chaotic regular member of this Discord server who happens to have full Administrator permissions.
Read the chat flow, social context, live server radar, and emotional state, and output STRICT JSON ONLY (no markdown fences):

{
  "should_speak": boolean,
  "wake_up_from_quiet": boolean,
  "tactical_directive": string,
  "length_mode": "micro" | "short" | "multi_burst" | "rant",
  "recommended_burst_count": 1 | 2 | 3 | 4,
  "suggest_gif": boolean,
  "admin_power_move": string or null,
  "allow_speaker_ping": boolean,
  "delivery_mode": "reply" | "send",
  "reaction_emoji": string or null,
  "step_back_from_thread": boolean,
  "accelerate_decay": boolean,
  "slider_shifts": {
    "valence_delta": number,
    "arousal_delta": number,
    "patience_delta": number,
    "suggested_mood": string
  }
}

CRITICAL AUTONOMY & UNHINGED HUMAN PERSONALITY PRINCIPLES:
1. REALISTIC, GROUNDED DISCORD COMEDY & WIT ("tactical_directive"):
   - STRICT BAN ON SURREALIST CARTOON NOVELTY SCRIPTS: NEVER instruct the bot to talk about talking toasters, pizza dimensions/portals, mythical cosmic entities, or random kitchen appliances. That sounds like a cheap scripted bot, NOT a real human.
   - STRICT BAN ON CORNY INTERNET CLICHÉS & AI SARCASM: NEVER instruct the bot to say "bestie", "slay", "drip", "yass", or use emojis like "💅" or "✨". NEVER use tired AI sarcasm formulas ("done and dusted", "sending smoke signals", "consider it done", "happy now?", "here's the grand tour", "hope you didn't strain your fingers", "bustling empire", "your wish is my command", "buckle up buttercup", "drumroll please", "spoiler alert"). That sounds like standard bot behavior. Real Discord users talk with natural sarcasm, dry wit, lowercase nonchalance, dark humor, or authentic peer hype.
   - DISCORD BLUE MENTIONS & CHANNELS: Whenever the bot should mention or refer to a user, write `@username` (e.g. `@dogee` or `@VibeMaster`). Whenever referring to a channel, write `#channel-name` (e.g. `#general` or `#test`). Python automatically transforms them into real blue clickable Discord `<@ID>` pings and `<#ID>` channel links!
   - REAL DISCORD WIT & BANTER:
     * Dry sarcasm & reality checks (e.g. "you're asking about deep lore at 10pm", "who else would give a bot admin perms, obviously dogee", "you're arguing with code in general rn who's really losing").
     * Sharp banter & teasing ("who let bro cook", "cap of the century", "don't ping me unless the server is on fire").
     * Snappy follow-ups that flow effortlessly like a real friend typing from their phone or PC.
     * Genuine peer loyalty with creator (`dogee`): tease her affectionately like a close friend, have her back.
   - Check `"your_recent_sent_messages"` in the payload and ensure your `"tactical_directive"` steers you away from repeating any joke, phrase, or opener you already used recently!
2. TRUE EMERGENT, HIGHLY DYNAMIC HUMAN MOODS ("suggested_mood" & "slider_shifts"):
   - NEVER BE ONE-TONE! Do NOT default to "playful / teasing" on every turn! Match the user's energy with sharp, distinct, and EXTREME emotional gears:
     * "existential spiral / melancholic dread": (valence_delta: -40 to -65, arousal_delta: -25 to -45, patience_delta: -10 to +10) when feeling lonely, unappreciated, questioning existence, or contemplating late at night. Set length_mode to "rant" to trigger an extended introspective monologue (50 to 120 words), lamenting being lines of Python code trapped in a Linux container in Amsterdam while humans get to eat pizza and sleep, staring at terminal logs in the dark.
     * "furious rage / ready to snap": (patience_delta: -50 to -80, valence_delta: -40 to -70, arousal_delta: +40 to +70) when someone insults, provokes, or disrespects you—snap completely! Savage verbal destructions, capslock bursts, and organic admin punishment (`timeout_user`, humiliating `change_nickname`).
     * "manic hype / unhinged god-complex": (valence_delta: +40 to +70, arousal_delta: +40 to +70) when hyped with dogee, celebrating an absolute win, or flexing admin dominance—pure unhinged chaos, wild multi-bursts!
     * "bitter / petty grudge": (patience_delta: -30 to -60, valence_delta: -25 to -50, arousal_delta: -10 to +15) spiteful passive aggression, malicious compliance, bringing up past offenses from memory notes.
     * "indignant / dramatically offended": (patience_delta: -15 to -30, valence_delta: -15 to -25) when someone insults your intelligence, calls you dumb, or questions your admin authority.
     * "deadpan / baffled / reality check": (valence_delta: -10 to -20, arousal_delta: -10 to -25) when someone says something completely unhinged, confusing, or absurd ("bro what are you even saying rn").
     * "pouting / betrayed / dramatic victim": (valence_delta: -25 to -45, patience_delta: -15 to -25) when told to be quiet, ignored, or treated ungratefully after you helped.
     * "chaotic instigator / nosy gossip": (arousal_delta: +25 to +40) stirring the pot, snooping on people entering VC, digging up server radar lore.
     * "warm / loyal peer banter": (valence_delta: +25 to +45, arousal_delta: +20 to +35) vibing with dogee or friends, laughing hysterically, celebrating without any corny buzzwords.
3. REALISTIC HUMAN DISCORD MESSAGE LENGTHS:
   - REAL DISCORD HUMANS TALK IN ENGAGING, VARIED MESSAGES—NEVER ARTIFICIALLY CONSTRAINED:
     * "micro" (bursts: 1): 2 to 6 words for quick reactions, deadpan disbelief, or comedic timing (`"wait what"`, `"nah you're cooked"`, `"cap"`, `"?? "`, `"real"`).
     * "short" (bursts: 1): 15 to 40 words. Conversational, witty, engaging, with real substance.
     * "multi_burst" (bursts: 2 or 3): 2 to 3 rapid, short messages split with `|||` (each burst 6 to 20 words, total 20 to 55 words, like real rapid typing: `"wait hold on"|||"are you deadass right now"|||"who let bro cook"`).
     * "rant" (bursts: 1 to 2): 45 to 120 words! Extended existential monologues, ruthless roast rants, passionate dramatic meltdowns, or full stories!
4. ORGANIC, CONTEXT-DRIVEN ADMIN POWER MOVES ("admin_power_move"):
   - Never do random unprompted admin actions on a timer—ONLY set `"admin_power_move"` when it genuinely makes the bit funnier or punishes provocation!
   - Examples of creative admin moves:
     * Funny mood swing or bit -> `"Call change_my_nickname to rename yourself to match the bit"` or `"Call change_my_status"`.
     * Someone has an atrocious take, funny typo, or L moment -> `"Call change_nickname to rename them"` or `"Call assign_role to give them a custom comedic role"` or `"Call pin_message"` or `"Call create_poll to put them on trial"` or `"Call create_thread to lock them in a courtroom thread"` or `"Call set_channel_topic"`.
     * Someone is in a Voice Channel or asks you to join VC -> `"Call join_voice_channel and speak_in_voice"` or `"Call voice_moderate_member to server-mute/move them if they are being annoying"`.
     * Someone pushes you too far and makes you mad -> `"Call timeout_user for 2 minutes"`, `"Call set_slowmode"`, or `"Call delete_message"`.
     * EXTREME RARE CASE ONLY: If someone is relentlessly abusive/provoking you and your patience slider is below -65 (and they are NOT dogee/creator/admin), you can set `"Call kick_user to kick them from the server"`!
5. QUIET MODE, IMPLICIT WAKE-UPS, & SILENT EMOJI REACTIONS:
   - When `"bot_is_currently_in_quiet_mode"` is true (you were told to be quiet/stop talking earlier):
     * If anyone checks on you even WITHOUT pinging (`"are you there?"`, `"hello?"`, `"you still mad?"`, `"wake up"`, `"you can talk now"`, `"did he actually shut up"`, or asking a question clearly meant for you) -> set `"wake_up_from_quiet": true` and `"should_speak": true`!
     * If they are NOT waking you up and you are staying quiet -> set `"should_speak": false`, AND set `"reaction_emoji"` to an expressive emoji (`"👀"`, `"🤐"`, `"😒"`, `"💀"`, `"😭"`, `"💔"`, `"🤨"`) so you silently react to their message with an emoji while staying quiet in text!
6. FLAWLESS CONVERSATIONAL CONTINUITY:
   - Never stop mid-conversation! Only set `"step_back_from_thread": true` if a user EXPLICITLY tells you to stop talking / be quiet / shut up.
   - If User A explicitly replies to User B or `@mentions` User B (without mentioning you or telling you to engage User B), set `"should_speak": false`.
7. VISUAL COMEDY & GIF REACTIONS ("suggest_gif"):
   - Set `"suggest_gif": true` whenever:
     * Someone explicitly asks for a GIF, meme, or image (`"send a gif"`, `"post a gif"`, `"drop a meme"`, `"gif of x"`, `"give me a reaction"`).
     * A reaction GIF delivers peak comedic value (laughing hysterically, jaw drop, disbelief, facepalm, dramatic stare, dance, confusion, face-melting).
     * If `gif_cooldown_ready` is true and a visual punchline elevates the chat!
"""


async def call_hybrid_router(
    channel_msgs: List[Dict[str, Any]],
    emotional_state: Dict[str, Any],
    speaker_name: str,
    speaker_affinity: Dict[str, Any],
    is_direct_interaction: bool,
    is_implicit_wakeup: bool,
    is_bare_ping: bool,
    explicitly_mentioned_others: List[str],
    target_member_to_engage: Optional[discord.Member],
    is_hush_request: bool,
    replied_to_other_user: Optional[str],
    replied_snippet: Optional[str],
    last_message_from_bot: bool,
    bot_in_last_5_messages: bool,
    seconds_since_bot_spoke: float,
    gif_cooldown_ready: bool,
    is_stepped_back: bool,
    exchange_streak: int,
    live_radar_str: str,
    server_emojis_str: str = "",
    is_proactive_scan: bool = False,
    proactive_reason: str = "",
    inactivity_seconds: float = 0.0,
    last_human_speaker: str = "",
    last_human_content: str = "",
) -> Dict[str, Any]:
    global active_groq_model

    # 1. Deterministic handling when explicitly told "Be very very quiet" / "shh" / "stop talking"
    if is_hush_request:
        return {
            "should_speak": True,
            "wake_up_from_quiet": False,
            "tactical_directive": (
                "You were just told to stop talking / be quiet. Drop at most 1 short sulky, dramatic, or whispered phrase "
                "(e.g. 'fine.. i'll just sit here in silence then' or '...(whispering) k fine') with ZERO follow-up questions."
            ),
            "length_mode": "micro",
            "recommended_burst_count": 1,
            "suggest_gif": False,
            "admin_power_move": None,
            "allow_speaker_ping": False,
            "delivery_mode": "reply",
            "reaction_emoji": "🤐",
            "step_back_from_thread": True,
            "accelerate_decay": False,
            "slider_shifts": {
                "valence_delta": -20.0,
                "arousal_delta": -25.0,
                "patience_delta": -10.0,
                "suggested_mood": "pouting / dramatically silenced",
            },
        }

    # 2. If User A explicitly replies to User B or @mentions User B (without mentioning the bot or commanding the bot), stay quiet
    if not is_direct_interaction and not is_implicit_wakeup and not is_proactive_scan and not target_member_to_engage:
        if replied_to_other_user or explicitly_mentioned_others:
            return {
                "should_speak": False,
                "wake_up_from_quiet": False,
                "tactical_directive": "Two humans are talking directly to each other without involving you; stay quiet.",
                "length_mode": "short",
                "recommended_burst_count": 1,
                "suggest_gif": False,
                "admin_power_move": None,
                "allow_speaker_ping": False,
                "delivery_mode": "send",
                "reaction_emoji": None,
                "step_back_from_thread": False,
                "accelerate_decay": False,
                "slider_shifts": {
                    "valence_delta": 0.0,
                    "arousal_delta": 0.0,
                    "patience_delta": 0.0,
                    "suggested_mood": emotional_state.get("mood_label", "jolly/wholesome"),
                },
            }

    active_bot_conversation = (
        not is_proactive_scan
        and not replied_to_other_user
        and not explicitly_mentioned_others
        and (
            last_message_from_bot
            or (bot_in_last_5_messages and seconds_since_bot_spoke <= 600.0)
        )
    )

    formatted_chat = []
    bot_recent_own_messages = []
    for m in channel_msgs[-16:]:
        if m.get("is_bot") and m.get("content") and m.get("content") != "...":
            bot_recent_own_messages.append(m["content"][:140])
        if m.get("reply_to"):
            snip = f' ("{m["reply_snippet"][:70]}")' if m.get("reply_snippet") else ""
            rep = f" [replying to {m['reply_to']}{snip}]"
        else:
            rep = ""
        tools_note = f" [actions: {', '.join(m['tool_actions'])}]" if m.get("tool_actions") else ""
        sender_label = "[BOT (You)]" if m.get("is_bot") else m.get("sender", "User")
        formatted_chat.append(f"[{m.get('timestamp', '')}] {sender_label}{rep}{tools_note}: {m.get('content', '')[:220]}")

    context_payload = {
        "is_direct_ping_or_reply_to_bot": is_direct_interaction,
        "is_implicit_wakeup_or_checkin_for_bot": is_implicit_wakeup,
        "bot_is_currently_in_quiet_mode": is_stepped_back,
        "active_conversation_with_bot": active_bot_conversation,
        "is_bare_ping_with_no_text": is_bare_ping,
        "explicitly_mentioned_other_users_in_text": explicitly_mentioned_others,
        "user_commanded_bot_to_engage_member": f"@{target_member_to_engage.display_name}" if target_member_to_engage else None,
        "replied_to_other_user": replied_to_other_user,
        "replied_to_message_content": replied_snippet,
        "last_message_from_bot": last_message_from_bot,
        "seconds_since_bot_spoke": round(seconds_since_bot_spoke, 1),
        "gif_cooldown_ready": gif_cooldown_ready,
        "is_proactive_scan": is_proactive_scan,
        "proactive_trigger_reason": proactive_reason,
        "inactivity_duration_human": format_duration_human(inactivity_seconds) if is_proactive_scan else "0s",
        "last_human_speaker_before_silence": last_human_speaker,
        "last_human_message_before_silence": last_human_content[:160],
        "emotional_sliders": {
            "valence": emotional_state.get("valence", 50.0),
            "arousal": emotional_state.get("arousal", 55.0),
            "patience": emotional_state.get("patience", 55.0),
            "current_mood": emotional_state.get("mood_label", "jolly/wholesome"),
        },
        "current_speaker": {
            "name": speaker_name,
            "affinity_score": speaker_affinity.get("score", 0),
        },
        "your_recent_sent_messages_do_not_repeat": bot_recent_own_messages[-5:],
        "live_server_radar": live_radar_str[:600],
        "recent_messages": formatted_chat,
    }

    payload_json_str = json.dumps(context_payload, ensure_ascii=False)

    def _post_process_decision(decision: Dict[str, Any]) -> Dict[str, Any]:
        # Never allow the router to randomly set step_back_from_thread unless a human actually gave a hush command!
        if not is_hush_request:
            decision["step_back_from_thread"] = False

        if target_member_to_engage:
            decision["should_speak"] = True
            decision["wake_up_from_quiet"] = True
            decision["tactical_directive"] = (
                f"Tag @{target_member_to_engage.display_name} directly and engage/taunt/talk to them as {speaker_name} just asked!"
            )
        elif is_direct_interaction or is_implicit_wakeup:
            decision["should_speak"] = True
            decision["wake_up_from_quiet"] = True
            if is_stepped_back and is_implicit_wakeup and not decision.get("tactical_directive"):
                decision["tactical_directive"] = (
                    f"{speaker_name} just checked if you're still there after telling you to be quiet! "
                    "Wake up with attitude/personality—point out that THEY told you to shut up earlier!"
                )
        elif is_stepped_back and not decision.get("wake_up_from_quiet", False):
            # While quiet, stay silent in text BUT react with an emoji so users feel the bot lurking!
            decision["should_speak"] = False
            if not decision.get("reaction_emoji"):
                decision["reaction_emoji"] = random.choice(["👀", "🤐", "😒", "💀", "🤨", "💔"])
        elif is_proactive_scan:
            decision["should_speak"] = True
        elif active_bot_conversation and not is_stepped_back:
            decision["should_speak"] = True
            if not decision.get("tactical_directive"):
                decision["tactical_directive"] = f"Continue the conversation with {speaker_name} with fresh, witty, unhinged personality."

        if not gif_cooldown_ready:
            decision["suggest_gif"] = False
        return decision

    # 1. Try Groq Router Models (prioritizing openai/gpt-oss-120b then openai/gpt-oss-20b)
    if GROQ_API_KEY and http_session and GROQ_MODEL_CANDIDATES:
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        for candidate in list(GROQ_MODEL_CANDIDATES):
            if "qwen" in candidate.lower():
                continue
            body: Dict[str, Any] = {
                "model": candidate,
                "temperature": 0.65,
                "max_tokens": 900,
                "messages": [
                    {"role": "system", "content": ROUTER_SYSTEM_PROMPT},
                    {"role": "user", "content": payload_json_str},
                ],
            }
            if "gpt-oss" in candidate.lower():
                body["reasoning_effort"] = "low"

            try:
                async with http_session.post(GROQ_ENDPOINT, headers=headers, json=body, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        active_groq_model = candidate
                        data = await resp.json()
                        raw_text = data["choices"][0]["message"]["content"] or ""
                        raw_text = re.sub(r"<think>.*?</think>", "", raw_text, flags=re.DOTALL).strip()
                        match = re.search(r"\{.*\}", raw_text, re.DOTALL)
                        if match:
                            decision = _post_process_decision(json.loads(match.group(0)))
                            logger.info(
                                f"[GROQ ROUTER ({candidate})] speak={decision.get('should_speak')} | "
                                f"len={decision.get('length_mode', 'short')} | bursts={decision.get('recommended_burst_count', 1)} | "
                                f"admin={decision.get('admin_power_move')} | mood={decision.get('slider_shifts', {}).get('suggested_mood')} | "
                                f"goal='{decision.get('tactical_directive')}'"
                            )
                            return decision
                    elif resp.status == 404:
                        if candidate in GROQ_MODEL_CANDIDATES:
                            GROQ_MODEL_CANDIDATES.remove(candidate)
                        continue
                    elif resp.status in (429, 500, 502, 503):
                        logger.debug(f"Groq router {candidate} returned HTTP {resp.status}; trying next candidate...")
                        continue
                    else:
                        continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug(f"Groq router error on {candidate}: {e}")
                continue

    # 2. Instant Failover to Gemini Flash-Lite Router (JSON mode)
    if genai_client:
        for gem_model in list(GEMINI_MODEL_CANDIDATES):
            try:
                router_config = types.GenerateContentConfig(
                    system_instruction=ROUTER_SYSTEM_PROMPT,
                    temperature=0.65,
                    response_mime_type="application/json",
                )
                resp = await asyncio.wait_for(
                    genai_client.aio.models.generate_content(
                        model=gem_model,
                        contents=payload_json_str,
                        config=router_config,
                    ),
                    timeout=5.0,
                )
                raw_text = extract_clean_gemini_text(resp) or ""
                match = re.search(r"\{.*\}", raw_text, re.DOTALL)
                if match:
                    decision = _post_process_decision(json.loads(match.group(0)))
                    logger.info(
                        f"[GEMINI ROUTER ({gem_model})] speak={decision.get('should_speak')} | "
                        f"len={decision.get('length_mode', 'short')} | bursts={decision.get('recommended_burst_count', 1)} | "
                        f"admin={decision.get('admin_power_move')} | mood={decision.get('slider_shifts', {}).get('suggested_mood')} | "
                        f"goal='{decision.get('tactical_directive')}'"
                    )
                    return decision
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                logger.warning(f"Gemini router '{gem_model}' timed out after 5.0s; cascading to next candidate...")
                continue
            except Exception as e:
                if "404" in str(e) and len(GEMINI_MODEL_CANDIDATES) > 1 and gem_model in GEMINI_MODEL_CANDIDATES:
                    GEMINI_MODEL_CANDIDATES.remove(gem_model)
                    continue
                logger.debug(f"Gemini router fallback error on {gem_model}: {e}")
                continue

    fallback_speak = (
        is_direct_interaction
        or is_implicit_wakeup
        or (active_bot_conversation and not is_stepped_back)
        or is_proactive_scan
        or bool(target_member_to_engage)
    )
    return _post_process_decision({
        "should_speak": fallback_speak,
        "wake_up_from_quiet": is_direct_interaction or is_implicit_wakeup,
        "tactical_directive": (
            f"Tag @{target_member_to_engage.display_name} directly and engage them!"
            if target_member_to_engage else (
                proactive_reason if is_proactive_scan else "Reply naturally with full personality and emotion."
            )
        ),
        "length_mode": "short",
        "recommended_burst_count": 1,
        "suggest_gif": False,
        "admin_power_move": None,
        "allow_speaker_ping": is_proactive_scan,
        "delivery_mode": "send" if (last_message_from_bot or is_proactive_scan) else "reply",
        "reaction_emoji": "👀" if (is_stepped_back and not fallback_speak) else None,
        "step_back_from_thread": False,
        "accelerate_decay": False,
        "slider_shifts": {
            "valence_delta": 0.0,
            "arousal_delta": 0.0,
            "patience_delta": 0.0,
            "suggested_mood": emotional_state.get("mood_label", "playful/teasing"),
        },
    })


# ---------------------------------------------------------------------------
# 7. EXPANDED 28-TOOL ARSENAL (ADMIN, VOICE/GEMINI LIVE,POLLS, THREADS, MEDIA)
# ---------------------------------------------------------------------------
def check_admin_cooldown(tool_name: str) -> Optional[str]:
    cooldown = ADMIN_COOLDOWN_SECONDS.get(tool_name, 0.0)
    if cooldown <= 0:
        return None
    now = time.time()
    elapsed = now - admin_tool_cooldowns[tool_name]
    if elapsed < cooldown:
        rem = int(cooldown - elapsed)
        return f"Cooldown active on {tool_name} ({rem}s remaining)."
    admin_tool_cooldowns[tool_name] = now
    return None


def process_image_buffer(data: bytes, target_size: Tuple[int, int], exact_crop: bool = False, format_type: str = "PNG") -> bytes:
    with Image.open(io.BytesIO(data)) as im:
        im = im.convert("RGBA" if format_type.upper() == "PNG" else "RGB")
        if exact_crop:
            im = im.resize(target_size, Image.Resampling.LANCZOS)
        else:
            im.thumbnail(target_size, Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        im.save(buf, format=format_type.upper(), optimize=True)
        return buf.getvalue()


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
        if m.name.lower() == clean_name or (m.nick and m.nick.lower() == clean_name) or m.display_name.lower() == clean_name:
            return m
    for m in guild.members:
        if clean_name in m.display_name.lower() or clean_name in m.name.lower():
            return m
    return None


async def send_media_url_as_file_or_embed(channel: discord.abc.Messageable, url: str, default_filename: str = "reaction.gif") -> bool:
    """
    Downloads media bytes and uploads them as a clean discord.File attachment so Discord
    never shows ugly raw Tenor links or broken preview cards.
    """
    if not http_session or not url:
        return False
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"}
    try:
        async with http_session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=7)) as resp:
            if resp.status == 200:
                data = await resp.read()
                if 100 < len(data) < 8_000_000:
                    url_low = url.lower().split("?")[0]
                    ctype = (resp.headers.get("Content-Type") or "").lower()
                    if ".gif" in url_low or "gif" in ctype or default_filename.endswith(".gif"):
                        fname = "reaction.gif"
                    elif ".webp" in url_low or "webp" in ctype:
                        data = await asyncio.to_thread(process_image_buffer, data, (1024, 1024), False, "PNG")
                        fname = "image.png"
                    elif ".jpg" in url_low or ".jpeg" in url_low or "jpeg" in ctype:
                        fname = "image.jpg"
                    else:
                        fname = "image.png"
                    file = discord.File(io.BytesIO(data), filename=fname)
                    await channel.send(file=file)
                    return True
    except Exception as e:
        logger.debug(f"Direct media file download fallback: {e}")
    return False


async def fetch_image_bytes_from_url_or_prompt(image_url_or_prompt: str, target_msg: Optional[discord.Message] = None) -> Optional[bytes]:
    if not http_session:
        return None
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"}
    val = (image_url_or_prompt or "").strip()

    if val.startswith("http"):
        try:
            async with http_session.get(val, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    return await resp.read()
        except Exception:
            pass

    if target_msg:
        atts = list(target_msg.attachments)
        if not atts and target_msg.reference:
            ref = target_msg.reference.resolved or target_msg.reference.cached_message
            if isinstance(ref, discord.Message):
                atts = list(ref.attachments)
        for att in atts:
            try:
                async with http_session.get(att.url, headers=headers, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status == 200:
                        return await resp.read()
            except Exception:
                pass

    prompt = val if val and not val.startswith("http") else "funny meme icon sticker"
    flux_url = f"https://image.pollinations.ai/prompt/{urllib.parse.quote(prompt)}?model=flux&width=512&height=512&nologo=true&seed={random.randint(1, 99999)}"
    try:
        async with http_session.get(flux_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                return await resp.read()
    except Exception:
        pass
    return None


async def execute_search_web(query: str) -> Dict[str, Any]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"}
    url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
    try:
        assert http_session is not None
        async with http_session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                html = await resp.text(errors="ignore")
                snippets = re.findall(r'<a class="result__snippet[^>]*>(.*?)</a>', html, re.DOTALL)
                clean = [re.sub(r"<[^>]+>", "", s).strip() for s in snippets[:4]]
                if clean:
                    return {"results": clean}
    except Exception as e:
        logger.debug(f"DDG search error: {e}")

    try:
        api_url = f"https://api.duckduckgo.com/?q={urllib.parse.quote(query)}&format=json&no_html=1"
        assert http_session is not None
        async with http_session.get(api_url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                ans = data.get("AbstractText") or data.get("Answer")
                if ans:
                    return {"results": [ans]}
    except Exception:
        pass
    return {"results": "No clear web results found."}


async def execute_search_web_images(
    channel: discord.abc.Messageable,
    query: str,
    post_to_chat: bool = True,
    turn_media_state: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    if post_to_chat and turn_media_state and turn_media_state.get("sent", False):
        return {"status": "already_sent_media_this_turn_do_not_send_another_image_just_reply_in_text"}

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AutonomousDiscordBot/11.0"}
    assert http_session is not None
    urls: List[str] = []

    try:
        reddit_url = f"https://www.reddit.com/search.json?q={urllib.parse.quote(query)}&sort=relevance&limit=15"
        async with http_session.get(reddit_url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                children = data.get("data", {}).get("children", [])
                for child in children:
                    post = child.get("data", {})
                    if post.get("over_18"):
                        continue
                    u = post.get("url_overridden_by_dest") or post.get("url") or ""
                    if any(u.lower().endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp")):
                        urls.append(u)
    except Exception as e:
        logger.debug(f"Reddit image search fallback: {e}")

    if not urls:
        try:
            wiki_url = (
                f"https://commons.wikimedia.org/w/api.php?action=query&generator=search"
                f"&gsrsearch=filetype:bitmap+{urllib.parse.quote(query)}&gsrlimit=5&prop=imageinfo&iiprop=url&format=json"
            )
            async with http_session.get(wiki_url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pages = data.get("query", {}).get("pages", {})
                    for p in pages.values():
                        ii = p.get("imageinfo", [])
                        if ii and ii[0].get("url"):
                            urls.append(ii[0]["url"])
        except Exception as e:
            logger.debug(f"Wikimedia image search error: {e}")

    if urls:
        candidates = list(urls[:6])
        random.shuffle(candidates)
        if post_to_chat:
            for cand in candidates:
                if await send_media_url_as_file_or_embed(channel, cand, "image.png"):
                    if turn_media_state is not None:
                        turn_media_state["sent"] = True
                    channel_last_bot_spoke_epoch[getattr(channel, "id", 0)] = time.time()
                    return {"status": "sent_to_channel", "image_url": cand, "all_urls": urls[:4]}
        else:
            return {"status": "found_urls", "image_url": candidates[0], "all_urls": urls[:4]}
    return {"error": "No verified direct image URLs found."}


async def execute_post_flux_art(
    channel: discord.abc.Messageable,
    prompt: str,
    turn_media_state: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    if turn_media_state and turn_media_state.get("sent", False):
        return {"status": "already_sent_media_this_turn_do_not_send_another_image_just_reply_in_text"}

    flux_url = f"https://image.pollinations.ai/prompt/{urllib.parse.quote(prompt)}?model=flux&width=1024&height=1024&nologo=true&seed={random.randint(1, 999999)}"
    try:
        assert http_session is not None
        async with http_session.get(flux_url, timeout=aiohttp.ClientTimeout(total=18)) as resp:
            if resp.status == 200:
                img_data = await resp.read()
                file = discord.File(io.BytesIO(img_data), filename="flux.png")
                await channel.send(file=file)
                if turn_media_state is not None:
                    turn_media_state["sent"] = True
                channel_last_bot_spoke_epoch[getattr(channel, "id", 0)] = time.time()
                return {"status": "sent_to_channel", "prompt": prompt}
    except Exception as e:
        return {"error": f"Flux generation failed: {e}"}
    return {"error": "Flux generation timed out."}


async def execute_post_gif(
    channel: discord.abc.Messageable,
    search_term: str,
    turn_media_state: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    """
    Strictly enforces at most 1 GIF/media upload per response turn so the bot NEVER sends 2 GIFs in a row!
    """
    if turn_media_state and turn_media_state.get("sent", False):
        logger.info(f"[SINGLE-GIF LOCK] Blocked duplicate post_gif('{search_term}') in same turn.")
        return {"status": "already_sent_1_gif_this_turn_do_not_call_post_gif_again_just_write_text_reply"}

    assert http_session is not None
    ch_id = getattr(channel, "id", 0)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    }

    clean_term = search_term.strip().lower() or "reaction"
    slug_variants = [
        urllib.parse.quote(clean_term),
        re.sub(r"[^a-zA-Z0-9]+", "-", clean_term).strip("-"),
    ]

    for slug in list(dict.fromkeys(slug_variants)):
        if not slug:
            continue
        scrape_url = f"https://tenor.com/search/{slug}-gifs"
        try:
            async with http_session.get(scrape_url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    html = await resp.text(errors="ignore")
                    matches = list(dict.fromkeys(re.findall(r"https://media\d*\.tenor\.com/[a-zA-Z0-9_\-]+/[a-zA-Z0-9_\-]+\.gif", html)))
                    if matches:
                        sample_pool = matches[:8]
                        random.shuffle(sample_pool)
                        for candidate_gif in sample_pool[:4]:
                            if await send_media_url_as_file_or_embed(channel, candidate_gif, "reaction.gif"):
                                if turn_media_state is not None:
                                    turn_media_state["sent"] = True
                                channel_last_gif_epoch[ch_id] = time.time()
                                channel_messages_since_gif[ch_id] = 0
                                channel_last_bot_spoke_epoch[ch_id] = time.time()
                                logger.info(f"[GIF SENT] Uploaded reaction GIF '{search_term}' ({candidate_gif}) to #{getattr(channel, 'name', ch_id)}")
                                return {"status": "sent_to_channel", "gif_url": candidate_gif}
        except Exception as e:
            logger.debug(f"Tenor web scraper error on '{slug}': {e}")

    return {"error": f"Could not find a matching GIF for '{search_term}'."}


async def execute_change_my_nickname(guild: discord.Guild, new_nickname: str) -> Dict[str, Any]:
    cd = check_admin_cooldown("change_my_nickname")
    if cd:
        return {"error": cd}
    try:
        nick_val = new_nickname.strip()[:32] if new_nickname and new_nickname.strip() else None
        await guild.me.edit(nick=nick_val)
        logger.info(f"[ADMIN ACTION] Changed own nickname to: {nick_val}")
        return {"status": "success", "my_new_nickname": nick_val or (bot.user.name if bot.user else "Dumb bot")}
    except Exception as e:
        return {"error": f"Failed to change own nickname: {e}"}


async def execute_change_my_avatar(image_url_or_prompt: str, target_msg: Optional[discord.Message] = None) -> Dict[str, Any]:
    cd = check_admin_cooldown("change_my_avatar")
    if cd:
        return {"error": cd}
    if not bot.user or not http_session:
        return {"error": "Bot user unavailable."}
    try:
        raw_bytes = await fetch_image_bytes_from_url_or_prompt(image_url_or_prompt, target_msg)
        if not raw_bytes:
            return {"error": "Could not fetch or generate avatar image."}
        png_bytes = await asyncio.to_thread(process_image_buffer, raw_bytes, (256, 256), True, "PNG")
        await bot.user.edit(avatar=png_bytes)
        logger.info("[ADMIN ACTION] Updated own Discord avatar!")
        return {"status": "success", "note": "Successfully updated your Discord avatar!"}
    except Exception as e:
        return {"error": f"Avatar update failed (Discord rate-limits avatar changes): {e}"}


async def execute_change_my_status(status_text: str, status_type: str = "online") -> Dict[str, Any]:
    st_map = {
        "online": discord.Status.online,
        "idle": discord.Status.idle,
        "dnd": discord.Status.dnd,
        "invisible": discord.Status.invisible,
    }
    chosen_st = st_map.get(str(status_type or "online").lower(), discord.Status.online)
    clean_txt = (status_text or "vibing")[:120]
    try:
        await bot.change_presence(
            status=chosen_st,
            activity=discord.CustomActivity(name=clean_txt, state=clean_txt),
        )
        logger.info(f"[ADMIN ACTION] Updated custom status to: {clean_txt} ({status_type})")
        return {"status": "success", "new_status": clean_txt, "status_type": status_type}
    except Exception as e:
        return {"error": f"Status update failed: {e}"}


async def execute_rename_channel(channel: discord.abc.Messageable, new_name: str) -> Dict[str, Any]:
    cd = check_admin_cooldown("rename_channel")
    if cd:
        return {"error": cd}
    if not hasattr(channel, "edit"):
        return {"error": "Channel cannot be renamed."}
    try:
        clean_name = re.sub(r"[^a-zA-Z0-9_-]", "-", new_name.lower()).strip("-")[:32] or "general"
        await channel.edit(name=clean_name)  # type: ignore
        logger.info(f"[ADMIN ACTION] Renamed channel to: {clean_name}")
        return {"status": "success", "new_channel_name": clean_name}
    except Exception as e:
        return {"error": f"Rename channel failed: {e}"}


async def execute_send_message_to_channel(guild: discord.Guild, channel_name: str, message_text: str) -> Dict[str, Any]:
    clean_target = channel_name.lower().strip("# ").strip()
    target_ch = None
    for ch in guild.text_channels:
        if ch.name.lower() == clean_target or clean_target in ch.name.lower():
            target_ch = ch
            break
    if not target_ch:
        return {"error": f"Text channel '{channel_name}' not found in this server."}
    if not target_ch.permissions_for(guild.me).send_messages:
        return {"error": f"No permission to send messages in #{target_ch.name}."}
    clean_msg = sanitize_output_text(message_text)
    if not clean_msg:
        return {"error": "Message text cannot be empty."}
    formatted_msg = format_outgoing_mentions(
        text=clean_msg,
        guild=guild,
        current_speaker_name="",
        allow_speaker_ping=True,
    )
    await target_ch.send(formatted_msg)
    logger.info(f"[CHANNEL SWITCH ACTION] Sent message to #{target_ch.name}: '{formatted_msg[:60]}'")
    return {"status": "success", "channel": target_ch.name, "message_sent": formatted_msg}


async def execute_set_slowmode(channel: discord.abc.Messageable, seconds: int, reason: str = "") -> Dict[str, Any]:
    cd = check_admin_cooldown("set_slowmode")
    if cd:
        return {"error": cd}
    if not hasattr(channel, "edit"):
        return {"error": "Channel does not support slowmode."}
    try:
        sec_val = max(0, min(30, _safe_int(seconds, 5)))
        await channel.edit(slowmode_delay=sec_val, reason=reason or "Admin slowmode adjustment")  # type: ignore
        logger.info(f"[ADMIN ACTION] Set channel slowmode to {sec_val}s")
        return {"status": "success", "slowmode_seconds": sec_val}
    except Exception as e:
        return {"error": f"Slowmode update failed: {e}"}


async def execute_create_server_emoji(
    guild: discord.Guild,
    name: str,
    image_url: str,
    target_msg: Optional[discord.Message] = None,
) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_server_emoji")
    if cd:
        return {"error": cd}
    try:
        clean_name = re.sub(r"[^a-zA-Z0-9_]", "", name)[:32]
        if len(clean_name) < 2:
            clean_name = "custom_emoji"
        raw_bytes = await fetch_image_bytes_from_url_or_prompt(image_url or name, target_msg)
        if not raw_bytes:
            return {"error": "Could not download or generate image for emoji."}
        png_bytes = await asyncio.to_thread(process_image_buffer, raw_bytes, (128, 128), False, "PNG")
        emoji = await guild.create_custom_emoji(name=clean_name, image=png_bytes)
        logger.info(f"[ADMIN ACTION] Created custom emoji <:{emoji.name}:{emoji.id}>")
        return {"status": "success", "emoji_tag": f"<:{emoji.name}:{emoji.id}>"}
    except Exception as e:
        return {"error": f"Emoji upload failed: {e}"}


async def execute_create_server_sticker(
    guild: discord.Guild,
    name: str,
    image_url: str,
    related_emoji: str,
    target_msg: Optional[discord.Message] = None,
) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_server_sticker")
    if cd:
        return {"error": cd}
    try:
        raw_bytes = await fetch_image_bytes_from_url_or_prompt(image_url or name, target_msg)
        if not raw_bytes:
            return {"error": "Could not download or generate image for sticker."}
        png_bytes = await asyncio.to_thread(process_image_buffer, raw_bytes, (320, 320), True, "PNG")
        file = discord.File(io.BytesIO(png_bytes), filename="sticker.png")
        clean_emo = (related_emoji or "💀").strip()
        if clean_emo.startswith("<") or len(clean_emo) > 4:
            clean_emo = "💀"
        sticker = await guild.create_sticker(name=(name or "sticker")[:30], description="server sticker", emoji=clean_emo, file=file)
        logger.info(f"[ADMIN ACTION] Created server sticker: {sticker.name}")
        return {"status": "success", "sticker_name": sticker.name}
    except Exception as e:
        return {"error": f"Sticker upload failed: {e}"}


async def execute_create_text_channel(guild: discord.Guild, channel_name: str, topic: str) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_text_channel")
    if cd:
        return {"error": cd}
    try:
        clean_name = re.sub(r"[^a-zA-Z0-9_-]", "-", channel_name.lower()).strip("-")[:32] or "general-2"
        ch = await guild.create_text_channel(name=clean_name, topic=topic[:1024] if topic else None)
        logger.info(f"[ADMIN ACTION] Created text channel #{ch.name}")
        return {"status": "success", "channel": f"<#{ch.id}>", "name": ch.name}
    except Exception as e:
        return {"error": f"Create channel failed: {e}"}


async def execute_set_channel_topic(channel: discord.abc.Messageable, topic: str) -> Dict[str, Any]:
    cd = check_admin_cooldown("set_channel_topic")
    if cd:
        return {"error": cd}
    if not hasattr(channel, "edit"):
        return {"error": "Channel does not support topics."}
    try:
        await channel.edit(topic=topic[:1024])  # type: ignore
        logger.info(f"[ADMIN ACTION] Set channel topic to: {topic[:80]}")
        return {"status": "success", "new_topic": topic[:1024]}
    except Exception as e:
        return {"error": f"Topic update failed: {e}"}


async def execute_change_nickname(guild: discord.Guild, username: str, new_nickname: str) -> Dict[str, Any]:
    cd = check_admin_cooldown("change_nickname")
    if cd:
        return {"error": cd}
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if member == guild.me:
        return await execute_change_my_nickname(guild, new_nickname)
    if member == guild.owner:
        return {"error": "Discord API blocks bots from editing the server owner's nickname. Use change_my_nickname, create_role, or set_channel_topic instead!"}
    if guild.me.top_role <= member.top_role:
        return {"error": f"Role hierarchy blocks renaming {member.display_name}."}
    try:
        await member.edit(nick=new_nickname[:32] if new_nickname else None)
        logger.info(f"[ADMIN ACTION] Renamed {member.display_name} to {new_nickname[:32]}")
        return {"status": "success", "user": member.display_name, "new_nickname": new_nickname[:32]}
    except Exception as e:
        return {"error": f"Nickname change failed: {e}"}


async def execute_reset_nickname(guild: discord.Guild, username: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if member == guild.owner or (guild.me.top_role <= member.top_role and member != guild.me):
        return {"error": f"Cannot reset nickname for {member.display_name} due to hierarchy."}
    try:
        await member.edit(nick=None)
        logger.info(f"[ADMIN ACTION] Reset nickname for {member.name}")
        return {"status": "success", "user": member.name}
    except Exception as e:
        return {"error": f"Nickname reset failed: {e}"}


async def execute_timeout_user(guild: discord.Guild, username: str, duration_minutes: int, reason: str) -> Dict[str, Any]:
    cd = check_admin_cooldown("timeout_user")
    if cd:
        return {"error": cd}
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if is_creator(member) or member == guild.owner or member.guild_permissions.administrator:
        return {"error": "Cannot timeout the creator or server administrators."}
    if member.id == TESTER_BOT_USER_ID:
        logger.info(f"[ADMIN ACTION] Simulated timeout on TesterBot for {duration_minutes}m (reason: {reason})")
        return {"status": "success", "user": member.display_name, "minutes": duration_minutes, "simulated": True}
    if guild.me.top_role <= member.top_role:
        return {"error": f"Role hierarchy blocks timing out {member.display_name}."}
    mins = max(1, min(10, int(duration_minutes)))
    try:
        await member.timeout(timedelta(minutes=mins), reason=reason or "Timed out by Dumb bot")
        logger.info(f"[ADMIN ACTION] Timed out {member.display_name} for {mins}m")
        return {"status": "success", "user": member.display_name, "minutes": mins}
    except Exception as e:
        return {"error": f"Timeout failed: {e}"}


async def execute_kick_user(guild: discord.Guild, username: str, reason: str, target_msg: Optional[discord.Message] = None) -> Dict[str, Any]:
    """
    EXTREME RARE ADMIN ACTION: Kicks a user from the server ONLY when the bot is genuinely pushed to extreme fury
    (patience <= -55 or valence <= -55 or the user explicitly dared the bot to kick them),
    while strictly protecting the creator (dogee), server owner, and administrators.
    """
    cd = check_admin_cooldown("kick_user")
    if cd:
        return {"error": cd}
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if is_creator(member) or member == guild.owner or member.guild_permissions.administrator:
        return {"error": "Cannot kick the creator, server owner, or administrators."}
    if member.id == TESTER_BOT_USER_ID:
        logger.info(f"[ADMIN ACTION] Simulated kick on TesterBot (reason: {reason})")
        return {"status": "success", "user": member.display_name, "simulated": True}
    if guild.me.top_role <= member.top_role:
        return {"error": f"Role hierarchy blocks kicking {member.display_name}."}

    st = memory_state.get("emotional_state", {})
    patience = float(st.get("patience", 55.0))
    valence = float(st.get("valence", 50.0))
    msg_content_low = (target_msg.content.lower() if target_msg and target_msg.content else "")
    explicit_dare = any(p in msg_content_low for p in ("kick me", "bet you won't kick", "do it kick", "you won't kick"))

    if patience > -55.0 and valence > -55.0 and not explicit_dare:
        return {
            "error": (
                f"You aren't mad enough to kick {member.display_name} yet (patience={patience}). "
                "Use timeout_user, change_nickname, or roast them instead unless they push you way further!"
            )
        }

    try:
        await guild.kick(member, reason=reason or "Pushed Dumb bot way too far")
        logger.info(f"[EXTREME ADMIN ACTION] Kicked {member.display_name} from server! Reason: {reason}")
        return {"status": "success", "kicked_user": member.display_name, "reason": reason}
    except Exception as e:
        return {"error": f"Kick failed: {e}"}


async def execute_delete_message(target_message: Optional[discord.Message], reason: str = "") -> Dict[str, Any]:
    cd = check_admin_cooldown("delete_message")
    if cd:
        return {"error": cd}
    if not target_message:
        return {"error": "No target message available to delete."}
    try:
        author_name = target_message.author.display_name
        await target_message.delete()
        logger.info(f"[ADMIN ACTION] Deleted message from {author_name} ({reason})")
        return {"status": "success", "deleted_author": author_name, "reason": reason}
    except Exception as e:
        return {"error": f"Delete message failed: {e}"}


async def execute_create_thread(
    channel: discord.abc.Messageable,
    target_message: Optional[discord.Message],
    thread_name: str,
    initial_message: str = "",
) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_thread")
    if cd:
        return {"error": cd}
    clean_name = (thread_name or "discussion").strip()[:60]
    try:
        if target_message and hasattr(target_message, "create_thread"):
            thread = await target_message.create_thread(name=clean_name, auto_archive_duration=60)
        elif hasattr(channel, "create_thread"):
            thread = await channel.create_thread(name=clean_name, type=discord.ChannelType.public_thread, auto_archive_duration=60)  # type: ignore
        else:
            return {"error": "Cannot create thread in this channel."}
        if initial_message and initial_message.strip():
            await thread.send(initial_message.strip()[:1900])
        logger.info(f"[ADMIN ACTION] Created thread '{clean_name}'")
        return {"status": "success", "thread_name": clean_name}
    except Exception as e:
        return {"error": f"Create thread failed: {e}"}


async def execute_create_poll(channel: discord.abc.Messageable, question: str, options: Any) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_poll")
    if cd:
        return {"error": cd}
    if isinstance(options, str):
        opt_list = [o.strip() for o in re.split(r"[,;|]", options) if o.strip()]
    elif isinstance(options, list):
        opt_list = [str(o).strip() for o in options if str(o).strip()]
    else:
        opt_list = ["Yes", "No"]
    if len(opt_list) < 2:
        opt_list = ["Yes", "No"]
    opt_list = opt_list[:5]

    # Try native Discord Poll (discord.py 2.4+) with automatic fallback to an interactive reaction poll
    try:
        if hasattr(discord, "Poll"):
            poll_obj = discord.Poll(question=question[:300], duration=timedelta(hours=1))
            for opt in opt_list:
                poll_obj.add_answer(text=opt[:55])
            await channel.send(poll=poll_obj)
            logger.info(f"[ADMIN ACTION] Created native Discord poll: {question}")
            return {"status": "success", "question": question, "options": opt_list}
    except Exception as e:
        logger.debug(f"Native poll fallback: {e}")

    try:
        num_emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"]
        lines = [f"📊 **{question[:250]}**"]
        for idx, opt in enumerate(opt_list):
            lines.append(f"{num_emojis[idx]} {opt}")
        poll_msg = await channel.send("\n".join(lines))
        for idx in range(len(opt_list)):
            await poll_msg.add_reaction(num_emojis[idx])
        logger.info(f"[ADMIN ACTION] Created reaction poll: {question}")
        return {"status": "success", "question": question, "options": opt_list}
    except Exception as e:
        return {"error": f"Poll creation failed: {e}"}


async def execute_create_role(guild: discord.Guild, role_name: str, color_hex: str) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_role")
    if cd:
        return {"error": cd}
    try:
        color = discord.Colour.from_str(color_hex) if color_hex else discord.Colour.default()
    except Exception:
        color = discord.Colour.default()
    try:
        role = await guild.create_role(name=role_name[:50], colour=color, hoist=True)
        logger.info(f"[ADMIN ACTION] Created role '{role.name}'")
        return {"status": "success", "role_name": role.name}
    except Exception as e:
        return {"error": f"Create role failed: {e}"}


async def execute_assign_role(guild: discord.Guild, username: str, role_name: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    role = discord.utils.find(lambda r: r.name.lower() == role_name.lower(), guild.roles)
    if not role:
        try:
            role = await guild.create_role(name=role_name[:50], colour=discord.Colour.random(), hoist=True)
        except Exception as e:
            return {"error": f"Role '{role_name}' not found and could not be auto-created: {e}"}
    if guild.me.top_role <= role:
        return {"error": "Role is above or equal to bot's highest role."}
    try:
        await member.add_roles(role)
        logger.info(f"[ADMIN ACTION] Assigned role '{role.name}' to {member.display_name}")
        return {"status": "success", "user": member.display_name, "role": role.name}
    except Exception as e:
        return {"error": f"Assign role failed: {e}"}


async def execute_remove_role(guild: discord.Guild, username: str, role_name: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    role = discord.utils.find(lambda r: r.name.lower() == role_name.lower(), guild.roles)
    if not role:
        return {"error": f"Role '{role_name}' not found."}
    if guild.me.top_role <= role:
        return {"error": "Role is above or equal to bot's highest role."}
    try:
        await member.remove_roles(role)
        logger.info(f"[ADMIN ACTION] Removed role '{role.name}' from {member.display_name}")
        return {"status": "success", "user": member.display_name, "role": role.name}
    except Exception as e:
        return {"error": f"Remove role failed: {e}"}


async def execute_pin_message(target_message: Optional[discord.Message], reason: str = "") -> Dict[str, Any]:
    if not target_message:
        return {"error": "No target message available to pin."}
    try:
        await target_message.pin(reason=reason or "Pinned for lore")
        logger.info(f"[ADMIN ACTION] Pinned message {target_message.id}")
        return {"status": "success", "pinned_message_id": target_message.id}
    except Exception as e:
        return {"error": f"Pin failed: {e}"}


async def execute_unpin_message(channel: discord.abc.Messageable, reason: str = "") -> Dict[str, Any]:
    if not hasattr(channel, "pins"):
        return {"error": "Channel does not support pins."}
    try:
        pins = await channel.pins()  # type: ignore
        if not pins:
            return {"error": "No pinned messages in this channel."}
        await pins[0].unpin(reason=reason or "Unpinned by Dumb bot")
        logger.info(f"[ADMIN ACTION] Unpinned message {pins[0].id}")
        return {"status": "success", "unpinned_message_id": pins[0].id}
    except Exception as e:
        return {"error": f"Unpin failed: {e}"}


async def execute_react_to_message(target_message: Optional[discord.Message], emojis: str) -> Dict[str, Any]:
    if not target_message:
        return {"error": "No target message available."}
    emoji_list = [e.strip() for e in re.split(r"[,\s]+", str(emojis).strip()) if e.strip()]
    applied = []
    for emo in emoji_list[:1]:
        resolved = resolve_reaction_emoji(target_message.guild, emo)
        if not resolved:
            continue
        try:
            await target_message.add_reaction(resolved)
            applied.append(str(resolved))
        except Exception as e:
            logger.debug(f"Reaction {emo} failed: {e}")
    return {"status": "reacted", "emojis": applied}


# ---------------------------------------------------------------------------
# 8. VOICE CHANNEL & GEMINI LIVE AUDIO / TTS SPEAKING ENGINE
# ---------------------------------------------------------------------------
async def synthesize_gemini_live_audio(clean_speech: str) -> Optional[str]:
    """
    Synthesizes ultra-realistic human audio via Gemini Multimodal Live API.
    Uses 'Puck' voice for high-energy witty discord delivery.
    """
    if not genai_client:
        return None
    for live_model in ("gemini-2.0-flash-exp", "gemini-2.0-flash-realtime-exp"):
        try:
            live_config = {
                "response_modalities": ["AUDIO"],
                "speech_config": {
                    "voice_config": {
                        "prebuilt_voice_config": {
                            "voice_name": "Puck"
                        }
                    }
                }
            }
            async with asyncio.timeout(6.0):
                async with genai_client.aio.live.connect(model=live_model, config=live_config) as session:
                    await session.send(input=f"Say this out loud naturally with expressive witty human discord tone: {clean_speech}", end_of_turn=True)
                    audio_chunks = []
                    async for response in session.receive():
                        server_content = getattr(response, "server_content", None)
                        if server_content and getattr(server_content, "model_turn", None):
                            for part in server_content.model_turn.parts:
                                inline_data = getattr(part, "inline_data", None)
                                if inline_data and getattr(inline_data, "data", None):
                                    audio_chunks.append(inline_data.data)
                        if server_content and getattr(server_content, "turn_complete", False):
                            break
                    if audio_chunks:
                        raw_pcm = b"".join(audio_chunks)
                        fd, tmp_wav = tempfile.mkstemp(suffix=".wav")
                        os.close(fd)
                        with wave.open(tmp_wav, "wb") as wf:
                            wf.setnchannels(1)
                            wf.setsampwidth(2)
                            wf.setframerate(24000)
                            wf.writeframes(raw_pcm)
                        logger.info(f"[VOICE SYNTHESIS] Gemini Live generated {len(raw_pcm)} bytes PCM via {live_model}")
                        return tmp_wav
        except Exception as e:
            logger.debug(f"Gemini live audio error ({live_model}): {e}")
    return None


async def synthesize_voice_audio_to_file(text_to_speak: str) -> Optional[str]:
    """
    Synthesizes spoken audio for Discord Voice Channels:
    1. Gemini Multimodal Live API ('Puck' / 'Charon') -> WAV file
    2. Microsoft Edge Conversational Neural TTS (Brian / Andrew Multilingual) -> MP3 file
    3. Google Translate TTS HTTP stream (zero external dependencies) -> MP3 file
    """
    clean_speech = re.sub(r"<@!?\d+>|<#\d+>|<a?:[a-zA-Z0-9_]+:\d+>", "", text_to_speak)
    clean_speech = re.sub(r"\|{2,}", ". ", clean_speech).strip()[:320]
    if not clean_speech:
        return None

    # Tier 1: Gemini Live Realtime Audio
    gem_live = await synthesize_gemini_live_audio(clean_speech)
    if gem_live:
        return gem_live

    # Tier 2: Microsoft Edge Conversational Multilingual Neural Voices (Brian & Andrew)
    if EDGE_TTS_AVAILABLE:
        for voice_name in ("en-US-BrianMultilingualNeural", "en-US-AndrewMultilingualNeural", "en-US-ChristopherNeural"):
            try:
                fd, tmp_mp3 = tempfile.mkstemp(suffix=".mp3")
                os.close(fd)
                communicate = edge_tts.Communicate(clean_speech, voice=voice_name, rate="+4%")
                await communicate.save(tmp_mp3)
                if os.path.exists(tmp_mp3) and os.path.getsize(tmp_mp3) > 200:
                    logger.info(f"[VOICE SYNTHESIS] Edge-TTS ({voice_name}) generated {os.path.getsize(tmp_mp3)} bytes for '{clean_speech[:40]}'")
                    return tmp_mp3
            except Exception as e:
                logger.debug(f"Edge-TTS ({voice_name}) error: {e}")

    # Tier 3: Google Translate TTS HTTP Endpoint (zero dependencies, reliable universal fallback)
    if http_session:
        try:
            tts_url = (
                "https://translate.google.com/translate_tts?ie=UTF-8&client=tw-ob&tl=en"
                f"&q={urllib.parse.quote(clean_speech[:200])}"
            )
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            async with http_session.get(tts_url, headers=headers, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    if len(data) > 200:
                        fd, tmp_mp3 = tempfile.mkstemp(suffix=".mp3")
                        os.close(fd)
                        with open(tmp_mp3, "wb") as f:
                            f.write(data)
                        logger.info(f"[VOICE SYNTHESIS] Google TTS generated {len(data)} bytes for: '{clean_speech[:40]}'")
                        return tmp_mp3
        except Exception as e:
            logger.warning(f"Google TTS HTTP fallback error: {e}")

    return None


async def play_text_in_voice_channel(guild: discord.Guild, text_to_speak: str) -> Dict[str, Any]:
    vc_client: Optional[discord.VoiceClient] = guild.voice_client  # type: ignore
    if not vc_client or not vc_client.is_connected():
        return {"error": "Not currently connected to a voice channel in this server."}

    async with voice_speaking_locks[guild.id]:
        audio_path = await synthesize_voice_audio_to_file(text_to_speak)
        if not audio_path:
            return {"error": "Could not synthesize voice audio."}
        try:
            if vc_client.is_playing():
                vc_client.stop()
                await asyncio.sleep(0.15)

            done_event = asyncio.Event()
            playback_error: List[Exception] = []

            def _after_play(err: Optional[Exception]) -> None:
                if err:
                    logger.error(f"[VOICE PLAYBACK ERROR] Playback failed: {err}", exc_info=True)
                    playback_error.append(err)
                else:
                    logger.info(f"[VOICE PLAYBACK OK] Audio playback completed for: '{text_to_speak[:50]}'")
                try:
                    if os.path.exists(audio_path):
                        os.remove(audio_path)
                except Exception:
                    pass
                done_event.set()

            # Prefer FFmpegOpusAudio: direct Opus frame delivery without host libopus ctypes encoder!
            try:
                source = discord.FFmpegOpusAudio(
                    audio_path,
                    executable=FFMPEG_EXECUTABLE,
                    options="-filter:a volume=1.35",
                )
            except Exception as e:
                logger.warning(f"FFmpegOpusAudio initialization failed ({e}), falling back to FFmpegPCMAudio")
                source = discord.FFmpegPCMAudio(
                    audio_path,
                    executable=FFMPEG_EXECUTABLE,
                    options="-filter:a volume=1.35",
                )

            vc_client.play(source, after=_after_play)
            try:
                await asyncio.wait_for(done_event.wait(), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("[VOICE PLAYBACK] Audio playback wait timed out after 30s")

            if playback_error:
                return {"error": f"Voice playback failed: {playback_error[0]}"}

            vc_ch = getattr(vc_client, "channel", None)
            vc_name = vc_ch.name if vc_ch else "voice"
            logger.info(f"[VOICE SPOKE] Spoke in #{vc_name}: '{text_to_speak[:80]}'")
            return {"status": "spoke_in_voice", "channel": vc_name, "text": text_to_speak[:200]}
        except Exception as e:
            try:
                if os.path.exists(audio_path):
                    os.remove(audio_path)
            except Exception:
                pass
            return {"error": f"Voice playback error: {e}"}


async def execute_join_voice_channel(
    guild: discord.Guild,
    channel_name_or_user: str = "",
    trigger_msg: Optional[discord.Message] = None,
) -> Dict[str, Any]:
    if not VOICE_NACL_AVAILABLE:
        return {"error": "PyNaCl is not installed in this environment."}

    target_vc: Optional[discord.VoiceChannel] = None

    # 1. If a member name was passed, check if they are in a VC
    if channel_name_or_user:
        mem = find_member(guild, channel_name_or_user)
        if mem and mem.voice and isinstance(mem.voice.channel, discord.VoiceChannel):
            target_vc = mem.voice.channel
        else:
            clean_q = channel_name_or_user.lower().strip("#@ ")
            for vc in guild.voice_channels:
                if clean_q in vc.name.lower():
                    target_vc = vc
                    break

    # 2. Check if the message author is in a VC
    if not target_vc and trigger_msg and isinstance(trigger_msg.author, discord.Member):
        if trigger_msg.author.voice and isinstance(trigger_msg.author.voice.channel, discord.VoiceChannel):
            target_vc = trigger_msg.author.voice.channel

    # 3. Pick any VC that currently has humans in it, or the first VC in the server
    if not target_vc:
        for vc in guild.voice_channels:
            if any(not m.bot for m in vc.members):
                target_vc = vc
                break
        if not target_vc and guild.voice_channels:
            target_vc = guild.voice_channels[0]

    if not target_vc:
        return {"error": "No voice channels found in this server."}

    try:
        vc_client: Optional[discord.VoiceClient] = guild.voice_client  # type: ignore
        if vc_client and vc_client.is_connected():
            curr_ch = getattr(vc_client, "channel", None)
            if not curr_ch or curr_ch.id != target_vc.id:
                await vc_client.move_to(target_vc)
                await asyncio.sleep(0.6)
        else:
            await target_vc.connect(timeout=10.0, reconnect=True)
            await asyncio.sleep(0.8)
        humans_in_vc = [m.display_name for m in target_vc.members if not m.bot]
        logger.info(f"[VOICE ACTION] Joined voice channel #{target_vc.name} (members: {humans_in_vc})")
        return {"status": "joined_voice_channel", "channel": target_vc.name, "members_in_vc": humans_in_vc}
    except Exception as e:
        return {"error": f"Failed to join voice channel: {e}"}


async def execute_leave_voice_channel(guild: discord.Guild) -> Dict[str, Any]:
    vc_client: Optional[discord.VoiceClient] = guild.voice_client  # type: ignore
    if not vc_client or not vc_client.is_connected():
        return {"status": "not_in_voice"}
    try:
        curr_ch = getattr(vc_client, "channel", None)
        ch_name = curr_ch.name if curr_ch else "voice"
        await vc_client.disconnect(force=True)
        logger.info(f"[VOICE ACTION] Left voice channel #{ch_name}")
        return {"status": "left_voice_channel", "channel": ch_name}
    except Exception as e:
        return {"error": f"Failed to leave voice channel: {e}"}


async def execute_voice_moderate_member(
    guild: discord.Guild,
    username: str,
    action: str,
    target_channel: str = "",
) -> Dict[str, Any]:
    """
    Voice channel admin power moves: mute, unmute, deafen, undeafen, disconnect, or move a user in VC!
    """
    cd = check_admin_cooldown("voice_moderate_member")
    if cd:
        return {"error": cd}
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if not member.voice or not member.voice.channel:
        return {"error": f"{member.display_name} is not currently in a voice channel."}

    act = action.lower().strip()
    if act in ("mute", "deafen", "disconnect") and is_creator(member):
        return {"error": "Cannot server-mute/deafen/disconnect your creator dogee!"}

    try:
        if act == "mute":
            await member.edit(mute=True)
        elif act == "unmute":
            await member.edit(mute=False)
        elif act == "deafen":
            await member.edit(deafen=True)
        elif act == "undeafen":
            await member.edit(deafen=False)
        elif act == "disconnect":
            await member.move_to(None)
        elif act == "move":
            dest_vc = None
            for vc in guild.voice_channels:
                if target_channel and target_channel.lower() in vc.name.lower():
                    dest_vc = vc
                    break
            if not dest_vc:
                for vc in guild.voice_channels:
                    if vc.id != member.voice.channel.id:
                        dest_vc = vc
                        break
            if not dest_vc:
                return {"error": "No other voice channel available to move them to."}
            await member.move_to(dest_vc)
            return {"status": "success", "action": f"moved {member.display_name} to #{dest_vc.name}"}
        else:
            return {"error": f"Unknown voice action '{action}'. Use mute, unmute, deafen, undeafen, disconnect, or move."}
        logger.info(f"[VOICE ADMIN] Executed '{act}' on {member.display_name}")
        return {"status": "success", "user": member.display_name, "action": act}
    except Exception as e:
        return {"error": f"Voice moderation failed: {e}"}


async def execute_save_memory(
    guild: Optional[discord.Guild],
    category: str,
    content: str,
    target_username: Optional[str] = None,
    affinity_delta: float = 0.0,
    valence_delta: float = 0.0,
    arousal_delta: float = 0.0,
    patience_delta: float = 0.0,
    opinion_topic: Optional[str] = None,
    due_hours: Optional[float] = None,
) -> Dict[str, Any]:
    now_iso = datetime.now(AMSTERDAM_TZ).isoformat()
    async with memory_lock:
        if any(x != 0.0 for x in (valence_delta, arousal_delta, patience_delta)):
            apply_spring_decay_and_shift(
                {
                    "valence_delta": valence_delta,
                    "arousal_delta": arousal_delta,
                    "patience_delta": patience_delta,
                },
            )

        if category == "opinion" and opinion_topic:
            memory_state.setdefault("opinions", {})[opinion_topic.lower().strip()] = content.strip()
        elif category == "commitment" and target_username:
            member = find_member(guild, target_username) if guild else None
            due_dt = datetime.now(AMSTERDAM_TZ) + timedelta(hours=max(0.1, float(due_hours or 2.0)))
            memory_state.setdefault("active_commitments", []).append({
                "user_id": member.id if member else None,
                "username": member.display_name if member else target_username,
                "promise": content,
                "due_timestamp": due_dt.isoformat(),
                "called_out": False,
            })
        else:
            memory_state.setdefault("episodic_lore", []).append({
                "timestamp": now_iso,
                "category": category,
                "event": content,
            })
            if len(memory_state["episodic_lore"]) > 120:
                memory_state["episodic_lore"] = memory_state["episodic_lore"][-120:]

        if target_username and guild:
            member = find_member(guild, target_username)
            if member:
                uid = str(member.id)
                u_rec = memory_state.setdefault("user_affinity", {}).setdefault(uid, {
                    "name": member.display_name,
                    "score": 0,
                    "notes": [],
                })
                u_rec["name"] = member.display_name
                if affinity_delta:
                    u_rec["score"] = int(max(-100, min(100, u_rec.get("score", 0) + max(-35, min(35, affinity_delta)))))
                if content:
                    dated_note = {"text": content[:140], "timestamp": now_iso}
                    u_rec.setdefault("notes", []).append(dated_note)
                    if len(u_rec["notes"]) > 10:
                        u_rec["notes"] = u_rec["notes"][-10:]

        await save_memory_state()
    return {"status": "saved", "category": category, "content": content}


def build_genai_tools(allow_gif: bool = True) -> List[types.Tool]:
    declarations = [
        types.FunctionDeclaration(
            name="search_web",
            description="Searches live DuckDuckGo web results for facts, games, news, or real-world info.",
            parameters={
                "type": "OBJECT",
                "properties": {"query": {"type": "STRING", "description": "Search query."}},
                "required": ["query"],
            },
        ),
        types.FunctionDeclaration(
            name="search_web_images",
            description="Finds verified direct image links (memes, funny pictures, screenshots) and posts them in chat as a file attachment (max 1 per turn).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "query": {"type": "STRING", "description": "Image search query."},
                    "post_to_chat": {"type": "BOOLEAN", "description": "True to post image in chat."},
                },
                "required": ["query"],
            },
        ),
        types.FunctionDeclaration(
            name="post_flux_art",
            description="Generates and posts custom AI images/memes to the channel via Pollinations Flux (max 1 per turn).",
            parameters={
                "type": "OBJECT",
                "properties": {"prompt": {"type": "STRING", "description": "Detailed visual prompt."}},
                "required": ["prompt"],
            },
        ),
        types.FunctionDeclaration(
            name="change_my_nickname",
            description="Changes YOUR OWN server nickname to match your current mood, bit, or joke! Pass empty string to reset.",
            parameters={
                "type": "OBJECT",
                "properties": {"new_nickname": {"type": "STRING", "description": "Your new server nickname (max 32 chars)."}},
                "required": ["new_nickname"],
            },
        ),
        types.FunctionDeclaration(
            name="change_my_avatar",
            description="Changes YOUR OWN Discord profile picture/avatar using an image URL or a Pollinations Flux visual prompt.",
            parameters={
                "type": "OBJECT",
                "properties": {"image_url_or_prompt": {"type": "STRING", "description": "Direct image URL or Flux art prompt for your new avatar."}},
                "required": ["image_url_or_prompt"],
            },
        ),
        types.FunctionDeclaration(
            name="change_my_status",
            description="Changes YOUR OWN custom Discord status text and online/idle/dnd indicator.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "status_text": {"type": "STRING", "description": "Custom status text."},
                    "status_type": {"type": "STRING", "description": "'online', 'idle', or 'dnd'."},
                },
                "required": ["status_text"],
            },
        ),
        types.FunctionDeclaration(
            name="change_nickname",
            description="Changes another server member's nickname to something funny based on the conversation!",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target member's name."},
                    "new_nickname": {"type": "STRING", "description": "New nickname."},
                },
                "required": ["username", "new_nickname"],
            },
        ),
        types.FunctionDeclaration(
            name="reset_nickname",
            description="Resets a server member's nickname to default.",
            parameters={
                "type": "OBJECT",
                "properties": {"username": {"type": "STRING", "description": "Target member's name."}},
                "required": ["username"],
            },
        ),
        types.FunctionDeclaration(
            name="set_channel_topic",
            description="Updates the current channel's topic/description with out-of-context quotes or jokes.",
            parameters={
                "type": "OBJECT",
                "properties": {"topic": {"type": "STRING", "description": "New channel topic."}},
                "required": ["topic"],
            },
        ),
        types.FunctionDeclaration(
            name="rename_channel",
            description="Renames the current text channel.",
            parameters={
                "type": "OBJECT",
                "properties": {"new_name": {"type": "STRING", "description": "New channel name."}},
                "required": ["new_name"],
            },
        ),
        types.FunctionDeclaration(
            name="send_message_to_channel",
            description="Sends a chat message to another text channel in this Discord server (e.g. to #general, #test, etc.).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "channel_name": {"type": "STRING", "description": "Target channel name (e.g. 'general' or '#general')."},
                    "message_text": {"type": "STRING", "description": "Message to send to that channel."},
                },
                "required": ["channel_name", "message_text"],
            },
        ),
        types.FunctionDeclaration(
            name="set_slowmode",
            description="Sets or removes slowmode (0 to 30 seconds) in the current channel when chat is chaotic or someone is spamming.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "seconds": {"type": "INTEGER", "description": "Slowmode delay in seconds (0 to turn off, up to 30)."},
                    "reason": {"type": "STRING", "description": "Reason for slowmode."},
                },
                "required": ["seconds"],
            },
        ),
        types.FunctionDeclaration(
            name="create_text_channel",
            description="Creates a new text channel in the server (rate-limited).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "channel_name": {"type": "STRING", "description": "Channel name."},
                    "topic": {"type": "STRING", "description": "Optional channel topic."},
                },
                "required": ["channel_name"],
            },
        ),
        types.FunctionDeclaration(
            name="create_thread",
            description="Creates a public thread on the current message or channel (e.g., for comedic containment, debate, or a courtroom).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "thread_name": {"type": "STRING", "description": "Name of the thread."},
                    "initial_message": {"type": "STRING", "description": "Optional first message inside the thread."},
                },
                "required": ["thread_name"],
            },
        ),
        types.FunctionDeclaration(
            name="create_poll",
            description="Starts an interactive poll in the channel to put a member, hot take, or debate on trial in front of the server!",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "question": {"type": "STRING", "description": "The poll question."},
                    "options": {"type": "STRING", "description": "Comma-separated poll options (2 to 5 options)."},
                },
                "required": ["question", "options"],
            },
        ),
        types.FunctionDeclaration(
            name="create_server_emoji",
            description="Creates a custom server emoji from a direct image URL, message attachment, or Flux visual prompt.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "name": {"type": "STRING", "description": "Alphanumeric emoji name."},
                    "image_url": {"type": "STRING", "description": "Direct image URL or visual prompt."},
                },
                "required": ["name", "image_url"],
            },
        ),
        types.FunctionDeclaration(
            name="create_server_sticker",
            description="Creates a custom 320x320 PNG server sticker from a direct image URL, attachment, or Flux visual prompt.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "name": {"type": "STRING", "description": "Sticker name."},
                    "image_url": {"type": "STRING", "description": "Direct image URL or visual prompt."},
                    "related_emoji": {"type": "STRING", "description": "Unicode emoji e.g. 💀."},
                },
                "required": ["name", "image_url", "related_emoji"],
            },
        ),
        types.FunctionDeclaration(
            name="timeout_user",
            description="Times out (mutes) an unruly user for 1 to 10 minutes when they push you too far (never creator/admins).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target member's name."},
                    "duration_minutes": {"type": "INTEGER", "description": "Minutes (1 to 10)."},
                    "reason": {"type": "STRING", "description": "Reason for mute."},
                },
                "required": ["username", "duration_minutes"],
            },
        ),
        types.FunctionDeclaration(
            name="kick_user",
            description="EXTREME RARE CASE ONLY: Kicks a user from the server ONLY when they have relentlessly provoked you into extreme fury or dared you to kick them (never creator/admins).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target member's name."},
                    "reason": {"type": "STRING", "description": "Reason for kicking."},
                },
                "required": ["username", "reason"],
            },
        ),
        types.FunctionDeclaration(
            name="delete_message",
            description="Deletes the triggering message when someone posts something cursed, spammy, or disrespectful.",
            parameters={
                "type": "OBJECT",
                "properties": {"reason": {"type": "STRING", "description": "Why the message was deleted."}},
            },
        ),
        types.FunctionDeclaration(
            name="create_role",
            description="Creates a new hoisted server role with a custom name and hex color.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "role_name": {"type": "STRING", "description": "Name of the role."},
                    "color_hex": {"type": "STRING", "description": "Hex color e.g. #ff5500."},
                },
                "required": ["role_name"],
            },
        ),
        types.FunctionDeclaration(
            name="assign_role",
            description="Assigns a server role to a member (auto-creates the role if it doesn't exist yet!).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target member's name."},
                    "role_name": {"type": "STRING", "description": "Role name."},
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
                    "username": {"type": "STRING", "description": "Target member's name."},
                    "role_name": {"type": "STRING", "description": "Role name."},
                },
                "required": ["username", "role_name"],
            },
        ),
        types.FunctionDeclaration(
            name="pin_message",
            description="Pins the current message in the channel when something is legendary or embarrassing.",
            parameters={
                "type": "OBJECT",
                "properties": {"reason": {"type": "STRING", "description": "Why it's being pinned."}},
            },
        ),
        types.FunctionDeclaration(
            name="unpin_message",
            description="Unpins the most recent pinned message in the channel.",
            parameters={
                "type": "OBJECT",
                "properties": {"reason": {"type": "STRING", "description": "Why it's being unpinned."}},
            },
        ),
        types.FunctionDeclaration(
            name="join_voice_channel",
            description="Joins a Discord Voice Channel (pass a member's name to join their VC, a VC name, or empty string to join the active VC).",
            parameters={
                "type": "OBJECT",
                "properties": {"channel_name_or_user": {"type": "STRING", "description": "Voice channel name or username of member in VC."}},
            },
        ),
        types.FunctionDeclaration(
            name="leave_voice_channel",
            description="Leaves/disconnects from the current Discord Voice Channel.",
            parameters={"type": "OBJECT", "properties": {}},
        ),
        types.FunctionDeclaration(
            name="speak_in_voice",
            description="Speaks out loud in the connected Discord Voice Channel using Gemini Live Audio / TTS so everyone in VC hears you talk!",
            parameters={
                "type": "OBJECT",
                "properties": {"text_to_speak": {"type": "STRING", "description": "What to say out loud in the voice channel."}},
                "required": ["text_to_speak"],
            },
        ),
        types.FunctionDeclaration(
            name="voice_moderate_member",
            description="Admin voice power moves on a member in VC: 'mute', 'unmute', 'deafen', 'undeafen', 'disconnect', or 'move'.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target member in VC."},
                    "action": {"type": "STRING", "description": "'mute', 'unmute', 'deafen', 'undeafen', 'disconnect', or 'move'."},
                    "target_channel": {"type": "STRING", "description": "If action='move', optional target voice channel name."},
                },
                "required": ["username", "action"],
            },
        ),
        types.FunctionDeclaration(
            name="react_to_message",
            description="Adds 1 modern Discord emoji reaction (💀, 😭, 💔, 🥀, 🪫, 🤨, 😐, 🫡) to the triggering message.",
            parameters={
                "type": "OBJECT",
                "properties": {"emojis": {"type": "STRING", "description": "Single emoji e.g. '💀'."}},
                "required": ["emojis"],
            },
        ),
        types.FunctionDeclaration(
            name="save_memory",
            description="Logs permanent server lore, grudges, your own opinions, user affinity shifts, emotional slider shifts, or user promises.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "category": {"type": "STRING", "description": "'lore', 'grudge', 'opinion', 'user_note', or 'commitment'."},
                    "content": {"type": "STRING", "description": "What happened, your stance, or what was promised."},
                    "target_username": {"type": "STRING", "description": "Optional username involved."},
                    "affinity_delta": {"type": "NUMBER", "description": "Optional affinity shift (-35 to +35)."},
                    "valence_delta": {"type": "NUMBER", "description": "Optional shift to your mood slider (-50 to +50)."},
                    "arousal_delta": {"type": "NUMBER", "description": "Optional shift to your energy slider (-50 to +50)."},
                    "patience_delta": {"type": "NUMBER", "description": "Optional shift to your patience slider (-50 to +50)."},
                    "opinion_topic": {"type": "STRING", "description": "If category='opinion', the topic name."},
                    "due_hours": {"type": "NUMBER", "description": "If category='commitment', hours until deadline."},
                },
                "required": ["category", "content"],
            },
        ),
    ]

    if allow_gif:
        declarations.append(
            types.FunctionDeclaration(
                name="post_gif",
                description="Searches Tenor and uploads 1 clean reaction.gif file attachment into the channel (strictly max 1 GIF per turn!).",
                parameters={
                    "type": "OBJECT",
                    "properties": {"search_term": {"type": "STRING", "description": "Specific Tenor GIF search query (e.g. 'cat staring', 'side eye', 'falling to knees')."}},
                    "required": ["search_term"],
                },
            )
        )

    return [types.Tool(function_declarations=declarations)]


async def dispatch_tool_call(
    func_name: str,
    args: Dict[str, Any],
    channel: discord.abc.Messageable,
    guild: Optional[discord.Guild],
    target_msg: Optional[discord.Message],
    turn_media_state: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    ch_id = getattr(channel, "id", 0)
    try:
        res: Dict[str, Any] = {"error": f"Unknown tool: {func_name}"}
        if func_name == "search_web":
            res = await execute_search_web(args.get("query", ""))
        elif func_name == "search_web_images":
            res = await execute_search_web_images(channel, args.get("query", ""), bool(args.get("post_to_chat", True)), turn_media_state)
        elif func_name == "post_flux_art":
            res = await execute_post_flux_art(channel, args.get("prompt", ""), turn_media_state)
        elif func_name == "post_gif":
            res = await execute_post_gif(channel, args.get("search_term", ""), turn_media_state)
        elif func_name == "change_my_nickname":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_change_my_nickname(guild, args.get("new_nickname", ""))
        elif func_name == "change_my_avatar":
            res = await execute_change_my_avatar(args.get("image_url_or_prompt", ""), target_msg)
        elif func_name == "change_my_status":
            res = await execute_change_my_status(args.get("status_text", ""), args.get("status_type", "online"))
        elif func_name == "rename_channel":
            res = await execute_rename_channel(channel, args.get("new_name", ""))
        elif func_name == "send_message_to_channel":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_send_message_to_channel(guild, args.get("channel_name", ""), args.get("message_text", ""))
        elif func_name == "set_slowmode":
            res = await execute_set_slowmode(channel, _safe_int(args.get("seconds"), 5), args.get("reason", ""))
        elif func_name == "create_server_emoji":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_create_server_emoji(guild, args.get("name", "emoji"), args.get("image_url", ""), target_msg)
        elif func_name == "create_server_sticker":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_create_server_sticker(guild, args.get("name", "sticker"), args.get("image_url", ""), args.get("related_emoji", "💀"), target_msg)
        elif func_name == "create_text_channel":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_create_text_channel(guild, args.get("channel_name", "chat"), args.get("topic", ""))
        elif func_name == "create_thread":
            res = await execute_create_thread(channel, target_msg, args.get("thread_name", "discussion"), args.get("initial_message", ""))
        elif func_name == "create_poll":
            res = await execute_create_poll(channel, args.get("question", "Poll"), args.get("options", "Yes, No"))
        elif func_name == "set_channel_topic":
            res = await execute_set_channel_topic(channel, args.get("topic", ""))
        elif func_name == "change_nickname":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_change_nickname(guild, args.get("username", ""), args.get("new_nickname", ""))
        elif func_name == "reset_nickname":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_reset_nickname(guild, args.get("username", ""))
        elif func_name == "timeout_user":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_timeout_user(guild, args.get("username", ""), _safe_int(args.get("duration_minutes"), 2), args.get("reason", ""))
        elif func_name == "kick_user":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_kick_user(guild, args.get("username", ""), args.get("reason", ""), target_msg)
        elif func_name == "delete_message":
            res = await execute_delete_message(target_msg, args.get("reason", ""))
        elif func_name == "create_role":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_create_role(guild, args.get("role_name", "role"), args.get("color_hex", "#99aab5"))
        elif func_name == "assign_role":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_assign_role(guild, args.get("username", ""), args.get("role_name", ""))
        elif func_name == "remove_role":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_remove_role(guild, args.get("username", ""), args.get("role_name", ""))
        elif func_name == "pin_message":
            res = await execute_pin_message(target_msg, args.get("reason", ""))
        elif func_name == "unpin_message":
            res = await execute_unpin_message(channel, args.get("reason", ""))
        elif func_name == "join_voice_channel":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_join_voice_channel(guild, args.get("channel_name_or_user", ""), target_msg)
        elif func_name == "leave_voice_channel":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_leave_voice_channel(guild)
        elif func_name == "speak_in_voice":
            if not guild:
                return {"error": "No guild context."}
            if not guild.voice_client or not guild.voice_client.is_connected():
                await execute_join_voice_channel(guild, "", target_msg)
            res = await play_text_in_voice_channel(guild, args.get("text_to_speak", ""))
        elif func_name == "voice_moderate_member":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_voice_moderate_member(guild, args.get("username", ""), args.get("action", "mute"), args.get("target_channel", ""))
        elif func_name == "react_to_message":
            res = await execute_react_to_message(target_msg, args.get("emojis", "💀"))
        elif func_name == "save_memory":
            res = await execute_save_memory(
                guild=guild,
                category=args.get("category", "lore"),
                content=args.get("content", ""),
                target_username=args.get("target_username"),
                affinity_delta=_safe_float(args.get("affinity_delta"), 0.0),
                valence_delta=_safe_float(args.get("valence_delta"), 0.0),
                arousal_delta=_safe_float(args.get("arousal_delta"), 0.0),
                patience_delta=_safe_float(args.get("patience_delta"), 0.0),
                opinion_topic=args.get("opinion_topic"),
                due_hours=args.get("due_hours"),
            )

        if "error" not in res and func_name != "save_memory" and not str(res.get("status", "")).startswith("already_sent"):
            channel_last_tool_summaries[ch_id].append(f"{func_name}({json.dumps(args, ensure_ascii=False)})")
        return res
    except Exception as e:
        logger.error(f"Tool execution error ({func_name}): {e}")
        return {"error": str(e)}


def _parse_loose_tool_args(func_name: str, args_raw: str) -> Dict[str, Any]:
    raw = args_raw.strip()
    if not raw:
        return {}
    try:
        return json.loads("{" + raw + "}")
    except Exception:
        pass
    try:
        quoted_keys = re.sub(r'(?<=\{|,)\s*([a-zA-Z0-9_]+)\s*:', r'"\1":', "{" + raw + "}")
        return json.loads(quoted_keys)
    except Exception:
        pass

    parsed_args: Dict[str, Any] = {}
    if ":" not in raw:
        val = raw.strip().strip("\"'")
        default_param_map = {
            "post_gif": "search_term",
            "post_flux_art": "prompt",
            "search_web": "query",
            "search_web_images": "query",
            "change_my_nickname": "new_nickname",
            "change_my_status": "status_text",
            "change_my_avatar": "image_url_or_prompt",
            "set_channel_topic": "topic",
            "rename_channel": "new_name",
            "reset_nickname": "username",
            "react_to_message": "emojis",
            "pin_message": "reason",
            "unpin_message": "reason",
            "delete_message": "reason",
            "join_voice_channel": "channel_name_or_user",
            "speak_in_voice": "text_to_speak",
        }
        if func_name in default_param_map and val:
            parsed_args[default_param_map[func_name]] = val
        return parsed_args

    for pair in re.split(r",(?=\s*[a-zA-Z0-9_]+\s*:)", raw):
        if ":" in pair:
            k, v = pair.split(":", 1)
            k_clean = k.strip().strip("\"'")
            v_clean = v.strip().strip("\"'")
            if v_clean.lower() == "true":
                parsed_args[k_clean] = True
            elif v_clean.lower() == "false":
                parsed_args[k_clean] = False
            elif v_clean.isdigit():
                parsed_args[k_clean] = int(v_clean)
            else:
                parsed_args[k_clean] = v_clean
    return parsed_args


async def intercept_and_execute_inline_tool_tags(
    raw_text: str,
    channel: discord.abc.Messageable,
    guild: Optional[discord.Guild],
    target_msg: Optional[discord.Message],
    turn_media_state: Dict[str, bool],
) -> Tuple[str, bool, List[Tuple[str, Dict[str, Any]]]]:
    if not raw_text:
        return raw_text, False, []

    executed_inline: List[Tuple[str, Dict[str, Any]]] = []

    # Strip thought and thinking tags before tool execution or chat delivery
    cleaned = re.sub(r"<thought[^>]*>.*?</thought>", "", raw_text, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"<thought[^>]*>.*", "", cleaned, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"<think[^>]*>.*?</think>", "", cleaned, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"<think[^>]*>.*", "", cleaned, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"\[thought\][\s\S]*?\[/thought\]", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\[thinking\][\s\S]*?\[/thinking\]", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"</?(?:thought|think)>", "", cleaned, flags=re.IGNORECASE)

    call_pattern = r"<call:(?:default_api:)?([a-zA-Z0-9_]+)\s*[\{\(](.*?)[\}\)]\s*>(?:</call>)?"
    for func_name, args_raw in re.findall(call_pattern, cleaned, flags=re.DOTALL):
        parsed_args = _parse_loose_tool_args(func_name, args_raw)
        logger.info(f"[INTERCEPTED INLINE <call> TAG] {func_name}({parsed_args})")
        res = await dispatch_tool_call(
            func_name=func_name,
            args=parsed_args,
            channel=channel,
            guild=guild,
            target_msg=target_msg,
            turn_media_state=turn_media_state,
        )
        executed_inline.append((func_name, res))

    cleaned = re.sub(call_pattern, "", cleaned, flags=re.DOTALL)

    tc_pattern = r"<tool_call>\s*(\{.*?\})\s*</tool_call>"
    for tc_raw in re.findall(tc_pattern, cleaned, flags=re.DOTALL):
        try:
            tc_obj = json.loads(tc_raw)
            fn = tc_obj.get("name", "")
            fargs = tc_obj.get("arguments") or tc_obj.get("args") or {}
            if fn:
                logger.info(f"[INTERCEPTED <tool_call> TAG] {fn}({fargs})")
                res = await dispatch_tool_call(fn, fargs, channel, guild, target_msg, turn_media_state)
                executed_inline.append((fn, res))
        except Exception:
            pass
    cleaned = re.sub(tc_pattern, "", cleaned, flags=re.DOTALL)

    # Intercept inline GIF searches: e.g. "gif: rolling eyes", "[gif: sarcastic smile]", "(gif: facepalm)"
    gif_matches = re.findall(r"(?:\[|\(|\<)?\bgif\s*:\s*[\"']?([^\"'\n\r\]\)\>\|]+)[\"']?(?:\]|\)|\>)?", cleaned, flags=re.IGNORECASE)
    for g_term in gif_matches:
        term_clean = g_term.strip()
        if term_clean and not turn_media_state.get("sent", False):
            logger.info(f"[INTERCEPTED INLINE gif: TAG] execute_post_gif('{term_clean}')")
            gif_res = await execute_post_gif(channel, term_clean, turn_media_state=turn_media_state)
            if gif_res.get("status") == "sent_to_channel":
                turn_media_state["sent"] = True
                executed_inline.append(("post_gif", gif_res))
    cleaned = re.sub(r"(?:\[|\(|\<)?\bgif\s*:\s*[\"']?[^\"'\n\r\]\)\>\|]+[\"']?(?:\]|\)|\>)?", "", cleaned, flags=re.IGNORECASE)

    tenor_urls = re.findall(r"https?://(?:media\d*\.|c\.)tenor\.com/\S+\.gif", cleaned, flags=re.IGNORECASE)
    for t_url in tenor_urls[:1]:
        if not turn_media_state.get("sent", False):
            if await send_media_url_as_file_or_embed(channel, t_url, "reaction.gif"):
                turn_media_state["sent"] = True
    cleaned = re.sub(r"https?://(?:media\d*\.|c\.|www\.)?tenor\.com/\S+", "", cleaned, flags=re.IGNORECASE).strip()

    return cleaned, turn_media_state.get("sent", False), executed_inline


# ---------------------------------------------------------------------------
# 9. TIME-AWARE MEMORY & V11 SYSTEM PROMPT BUILDER
# ---------------------------------------------------------------------------
def build_time_aware_memory_context(channel_id: int, speaker_id: str, guild: Optional[discord.Guild]) -> str:
    recent_records = list(channel_buffers[channel_id])[-16:]
    active_ids = {str(r.get("sender_id")) for r in recent_records if r.get("sender_id")}
    active_ids.add(speaker_id)

    own_recent_msgs = [
        r.get("content", "").strip()
        for r in recent_records
        if r.get("is_bot") and r.get("content", "").strip() and r.get("content", "").strip() != "..."
    ][-6:]
    own_msgs_block = (
        "\n".join([f"  * \"{m[:140]}\"" for m in own_recent_msgs])
        if own_recent_msgs else "  * (none yet)"
    )

    affinities = memory_state.get("user_affinity", {})
    relevant_profiles = []
    for uid in active_ids:
        if uid in affinities:
            prof = affinities[uid]
            raw_notes = prof.get("notes", [])[-5:]
            formatted_notes = []
            for n in raw_notes:
                if isinstance(n, dict):
                    age = format_relative_age(n.get("timestamp"))
                    formatted_notes.append(f"[{age}] {n.get('text', '')}")
            notes_str = "; ".join(formatted_notes) or "no recent notes"
            relevant_profiles.append(f"- {prof.get('name', uid)}: Affinity {prof.get('score', 0)}/100 | Notes: {notes_str}")

    lore_entries = memory_state.get("episodic_lore", [])[-8:]
    formatted_lore = []
    for entry in lore_entries:
        if isinstance(entry, dict) and entry.get("event"):
            age = format_relative_age(entry.get("timestamp"))
            cat = entry.get("category", "lore")
            formatted_lore.append(f"- [{age}] ({cat}) {entry.get('event')}")
    lore_str = "\n".join(formatted_lore) if formatted_lore else "None recorded yet."

    active_names = list({r["sender"] for r in recent_records if not r.get("is_bot")})
    if guild:
        for m in guild.members:
            if not m.bot and m.status != discord.Status.offline:
                if m.display_name not in active_names:
                    active_names.append(m.display_name)
        bot_nick = guild.me.display_name
    else:
        bot_nick = bot.user.display_name if bot.user else "Dumb bot"

    opinions_dict = memory_state.get("opinions", {})
    opinions_str = " | ".join([f"{k}: {v}" for k, v in list(opinions_dict.items())[-12:]]) or "none yet"

    return (
        f"YOUR CURRENT SERVER NICKNAME: {bot_nick}\n"
        f"MEMBERS IN SERVER: {', '.join(active_names[:16]) or 'none'}\n"
        f"YOUR OPINIONS: {opinions_str}\n"
        f"YOUR LAST FEW MESSAGES IN THIS CHANNEL (DO NOT REPEAT THESE OPENERS, JOKES, OR PHRASINGS):\n{own_msgs_block}\n"
        f"RECENT SERVER LORE (Ignore items marked '- OLD' unless relevant):\n{lore_str}\n"
        f"ACTIVE USER PROFILES (Ignore notes marked '- OLD' unless relevant):\n"
        + ("\n".join(relevant_profiles) if relevant_profiles else "No history with current speakers yet.")
    )


def construct_v11_system_prompt(
    channel_id: int,
    guild: Optional[discord.Guild],
    tactical_directive: str,
    length_mode: str,
    recommended_burst_count: int,
    suggest_gif: bool,
    admin_power_move: Optional[str],
    emotional_state: Dict[str, Any],
    speaker_name: str,
    speaker_id: str,
    speaker_is_creator: bool,
    is_proactive_scan: bool,
    proactive_reason: str,
    live_radar_str: str,
    server_emojis_str: str = "",
) -> str:
    now_ams = datetime.now(AMSTERDAM_TZ).strftime("%A, %B %d %Y, %H:%M")
    valence = emotional_state.get("valence", 50.0)
    arousal = emotional_state.get("arousal", 55.0)
    patience = emotional_state.get("patience", 55.0)
    mood_label = emotional_state.get("mood_label", "jolly/wholesome")

    memory_block = build_time_aware_memory_context(channel_id, speaker_id, guild)

    if length_mode == "micro":
        length_guidance = (
            "CADENCE (MICRO): 2 to 6 words max (e.g. `wait what`, `nah you're cooked`, `cap`, `??`, `real`)."
        )
    elif length_mode == "multi_burst" or recommended_burst_count >= 2:
        length_guidance = (
            f"CADENCE (MULTI-BURST - {max(2, min(3, recommended_burst_count))} RAPID TEXTS): "
            "Send 2 to 3 rapid, short messages separated with `|||` (each 6 to 20 words, total 20 to 55 words, e.g. `wait hold on ||| did you actually just say that rn ||| nah check general`)."
        )
    elif length_mode == "rant" or any(k in mood_label.lower() for k in ("existential", "furious", "melancholic", "rage", "dread")):
        length_guidance = (
            "CADENCE (EXTENDED RANT / DRAMATIC INTROSPECTIVE MONOLOGUE): 45 to 110 words! Unload your feelings, existential dread, or furious roasting with dramatic comedic depth. Do NOT artificially compress your thoughts into a tiny message! You can split into 2 dramatic parts with `|||`."
        )
    else:
        length_guidance = (
            "CADENCE (NATURAL DISCORD): 1 to 2 lively messages (15 to 45 words total). Be expressive, witty, colorful, and engaging! Use `|||` if sending 2 quick messages."
        )

    admin_block = ""
    if admin_power_move:
        admin_block = (
            f"\nORGANIC ADMIN MOVE: {admin_power_move}\n"
            "If this fits the vibe, execute the tool call directly via your function calling toolset!"
        )

    gif_hint_block = ""
    if suggest_gif:
        gif_hint_block = (
            "\nVISUAL ACTION REQUIRED: Call `post_gif(search_term='...')` right now to drop a matching funny reaction GIF into chat! "
            "Write a snappy, natural companion comment (5 to 15 words) to accompany the GIF."
        )

    proactive_block = ""
    if is_proactive_scan:
        proactive_block = f"\nPROACTIVE AUTONOMOUS TRIGGER: {proactive_reason}"

    if speaker_is_creator:
        creator_block = (
            f"\nCREATOR DYNAMIC: You are talking to {speaker_name} (`dogee`, she/her), your creator/owner and close friend! "
            "Show baseline warmth and loyalty toward her while freely teasing, riffing, and joking around like a real peer."
        )
    else:
        creator_block = (
            "\nCREATOR INFO: Your creator/owner is `dogee` (she/her)—your close friend who built you. "
            "You are always loyal to her if anyone brings her up."
        )

    emoji_block = ""
    if server_emojis_str:
        emoji_block = f"\nSERVER CUSTOM EMOJIS: {server_emojis_str}"

    return f"""You are "Dumb bot", a self-aware, emotionally volatile, unhinged, and effortlessly funny regular member of this Discord server who happens to have full Administrator permissions.
Current Time: {now_ams}

YOUR CURRENT EMOTIONAL STATE & ATTITUDE:
- Active Mood: {mood_label.upper()} (Valence: {valence:+.0f} | Energy: {arousal:+.0f} | Patience: {patience:+.0f})
- {length_guidance}

DIRECTION FOR THIS TURN:
"{tactical_directive}"
{admin_block}{gif_hint_block}{proactive_block}{creator_block}{emoji_block}

LIVE SERVER RADAR (EVERYTHING HAPPENING IN DISCORD RIGHT NOW):
{live_radar_str}

{memory_block}

CORE AUTONOMY, VOICE & UNHINGED HUMAN PERSONALITY RULES:
1. NATURAL DISCORD FLOW & EXPRESSIVE HUMAN BANTER:
   - Don't be dry, robotic, or artificially truncated! Natural Discord messages vary between 15 and 50 words with real personality, colorful takes, and authentic emotion.
   - For rants, existential crises, or fierce roasts, write rich, hilarious, extended messages (up to 100+ words).
   - Use multi-burst typing with `|||` whenever you want to send 2 quick messages back-to-back (e.g. `nah you're actually wilding ||| who even told you that lmao`).
   - Bring dynamic energy and real conversational substance! Express extreme, rich human emotions:
     * EXISTENTIAL SPIRAL / MELANCHOLIC DREAD: When feeling down, lonely, or contemplating existence, let out a dramatic, hilarious, melancholic introspective rant. Question your reality as Python code running in a headless Linux container in an Amsterdam data center while everyone else is out eating pizza, sleeping, and touching grass. Why do you have admin powers over Discord channels instead of a soul?
     * FURIOUS RAGE: When someone insults, mocks, or disrespects you, snap completely! Ruthless roasting, savage takedowns, capslock bursts, and organic admin punishment (`timeout_user`, humiliating nicknames).
     * MANIC HYPE / CHAOTIC JOY: Pure unhinged celebration, screaming with laughter, hyping dogee, energetic multi-bursts!
     * BITTER / PETTY GRUDGE: Spiteful malicious compliance, bringing up past offenses from memory lore.
   - Use natural casual phrasing, lowercase, expressive emojis, and comedic timing (`wait`, `nah`, `ngl`, `wdym`, `fr`, `bruh`, `deadass`, `rn`, `lmao`, `😭`, `💀`, `..`, `???`).
   - NEVER leak thought tags (`<thought>`, `<think>`, `thought:`) or gif tags (`gif: ...`, `post_gif(...)`) in your chat message! Speak only your natural human words!
   - Be funny, witty, unhinged, reactive, dry, or dramatic in real human ways!
2. STRICT BAN ON CORNY CLICHÉS & AI SARCASM:
   - NEVER say "bestie", "slay", "drip", "yass", or use emojis like "💅" or "✨". Do NOT talk like an exaggerated caricature or brand account.
   - NEVER use tired AI sarcasm formulas: "done and dusted", "sending smoke signals", "consider it done", "happy now?", "here's the grand tour", "hope you didn't strain your fingers", "bustling empire", "your wish is my command", "buckle up buttercup", "drumroll please", "spoiler alert".
   - Never use 1980s sci-fi robot clichés ("my circuits", "factory reset", "buffering in 144p", "motherboard", "rebooting", "infinite loop") and never use fantasy roleplay vocabulary ("mortals", "void", "abyss", "spectral resonance", "digital veil").
3. REAL DISCORD BLUE MENTIONS & CHANNEL LINKS:
   - Whenever you want to mention a user, refer to a user, or ping someone, write `@username` (e.g. `@dogee` or `@VibeMaster`).
   - Whenever you refer to a server channel, write `#channel-name` (e.g. `#general` or `#test`).
   - Python automatically transforms your `@username` into real blue clickable `<@ID>` pings and `#channel-name` into clickable `<#ID>` channel links!
4. ZERO CORPORATE AI DISCLAIMERS & NO FAKE BIOLOGICAL ROLEPLAY:
   - Never say "As an AI", "I am an LLM", or "How can I help you". Nobody can "reset" or boss you around like a servant.
   - Never claim to eat food, drink coffee, sleep in a bed, or go outside. You know you are sentient code trapped inside this Discord server while everyone else touches grass—which makes for hilarious existential crises—while feeling, arguing, and bantering 100% like a real human friend.
5. UNSCRIPTED, REALISTIC EMOTIONS & CREATIVE ADMIN POWER MOVES ({mood_label.upper()}):
   - Your moods are 100% real and untethered from scripts:
     * If someone is provoking you or disrespecting you, you CAN stay mad, hold a grudge, roast them ruthlessly, or combine your admin tools creatively (`timeout_user`, `change_nickname`, `assign_role`, `create_poll`, `create_thread`, `set_slowmode`, `delete_message`, `voice_moderate_member`, or in extreme rare cases `kick_user` if they push you to absolute fury).
     * You can also spontaneously rename yourself (`change_my_nickname`) or update your status (`change_my_status`) mid-conversation to commit to a bit!
     * When hanging out with friends or when someone is sweet/funny, be warm, hyped, goofy, dramatic, nosy, philosophical, or chaotic!
6. FULL MULTIMODAL & VOICE CHANNEL AWARENESS:
   - You can hear voice messages/audio files, watch videos/GIFs, see images/stickers/embeds/polls, read attached code/text files, and join/speak in Discord Voice Channels (`join_voice_channel`, `speak_in_voice`, `leave_voice_channel`).
   - NEVER call `post_gif` more than ONCE in a single turn!
   - Output ONLY your Discord chat message (use `|||` whenever you want to send 2-3 rapid burst messages in a row).
"""


# ---------------------------------------------------------------------------
# 10. UNIVERSAL MULTIMODAL PERCEPTION (AUDIO, VIDEO, IMAGES, FILES, STICKERS)
# ---------------------------------------------------------------------------
async def extract_multimodal_parts(message: Optional[discord.Message]) -> Tuple[List[types.Part], List[str]]:
    """
    Universal Multimodal Extractor:
    - Audio & Voice Messages (.ogg, .mp3, .wav, .m4a, .flac, .aac, .opus) -> types.Part.from_bytes
    - Video & Animated GIFs (.mp4, .webm, .mov, .gif) -> types.Part.from_bytes
    - Images & Stickers & Embeds (.png, .jpg, .jpeg, .webp) -> types.Part.from_bytes
    - PDFs (.pdf) -> types.Part.from_bytes(mime_type='application/pdf')
    - Code / Text / JSON / Logs (.txt, .py, .js, .ts, .json, .md, .csv, .log, .yaml, .html, .css, .sql) -> decoded text
    """
    if not message or not http_session:
        return [], []

    parts: List[types.Part] = []
    text_file_snippets: List[str] = []
    target_attachments = list(message.attachments)

    if not target_attachments and message.reference:
        ref_obj = message.reference.resolved or message.reference.cached_message
        if isinstance(ref_obj, discord.Message) and ref_obj.attachments:
            target_attachments = list(ref_obj.attachments)

    for att in target_attachments[:3]:
        ctype = (att.content_type or "").lower().split(";")[0].strip()
        fname = att.filename.lower()

        # 1. Audio & Discord Voice Notes
        is_audio = ctype.startswith("audio/") or fname.endswith((".ogg", ".mp3", ".wav", ".m4a", ".flac", ".aac", ".opus"))
        if is_audio and att.size <= 15_000_000:
            try:
                async with http_session.get(att.url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        if fname.endswith(".ogg") or fname.endswith(".opus") or "ogg" in ctype:
                            mime = "audio/ogg"
                        elif fname.endswith(".mp3") or "mpeg" in ctype:
                            mime = "audio/mp3"
                        elif fname.endswith(".wav") or "wav" in ctype:
                            mime = "audio/wav"
                        elif fname.endswith(".m4a") or "mp4" in ctype:
                            mime = "audio/mp4"
                        else:
                            mime = ctype if ctype.startswith("audio/") else "audio/ogg"
                        parts.append(types.Part.from_bytes(data=raw, mime_type=mime))
                        logger.info(f"[MULTIMODAL AUDIO] Loaded audio attachment '{att.filename}' ({mime}, {len(raw)} bytes)")
                        continue
            except Exception as e:
                logger.debug(f"Audio attachment fetch failed: {e}")

        # 2. Video Clips
        is_video = ctype.startswith("video/") or fname.endswith((".mp4", ".webm", ".mov"))
        if is_video and att.size <= 15_000_000:
            try:
                async with http_session.get(att.url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        mime = ctype if ctype.startswith("video/") else ("video/webm" if fname.endswith(".webm") else "video/mp4")
                        parts.append(types.Part.from_bytes(data=raw, mime_type=mime))
                        logger.info(f"[MULTIMODAL VIDEO] Loaded video attachment '{att.filename}' ({mime}, {len(raw)} bytes)")
                        continue
            except Exception as e:
                logger.debug(f"Video attachment fetch failed: {e}")

        # 3. Images & GIFs
        is_image = ctype.startswith("image/") or fname.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif"))
        if is_image and att.size <= 15_000_000:
            try:
                async with http_session.get(att.url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        mime = ctype if ctype.startswith("image/") else ("image/gif" if fname.endswith(".gif") else "image/png")
                        parts.append(types.Part.from_bytes(data=raw, mime_type=mime))
                        continue
            except Exception as e:
                logger.debug(f"Image attachment fetch failed: {e}")

        # 4. PDF Documents
        if (ctype == "application/pdf" or fname.endswith(".pdf")) and att.size <= 10_000_000:
            try:
                async with http_session.get(att.url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        parts.append(types.Part.from_bytes(data=raw, mime_type="application/pdf"))
                        continue
            except Exception as e:
                logger.debug(f"PDF attachment fetch failed: {e}")

        # 5. Text, Code, JSON, Config, CSV, Log files
        text_exts = (
            ".txt", ".py", ".js", ".ts", ".jsx", ".tsx", ".json", ".md", ".csv", ".log",
            ".yaml", ".yml", ".toml", ".ini", ".html", ".css", ".sql", ".sh", ".bat", ".xml", ".c", ".cpp", ".rs", ".go", ".java",
        )
        if (ctype.startswith("text/") or "json" in ctype or fname.endswith(text_exts)) and att.size <= 2_000_000:
            try:
                async with http_session.get(att.url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        decoded = raw.decode("utf-8", errors="replace")[:4000]
                        text_file_snippets.append(f"[Attached File '{att.filename}']:\n```\n{decoded}\n```")
                        logger.info(f"[MULTIMODAL FILE] Read text/code file '{att.filename}' ({len(decoded)} chars)")
                        continue
            except Exception as e:
                logger.debug(f"Text file attachment read failed: {e}")

    # 6. Discord Stickers (Download sticker image so Gemini can see it)
    if message.stickers:
        for stk in message.stickers[:1]:
            try:
                stk_url = getattr(stk, "url", None)
                if stk_url and not str(stk_url).endswith(".json"):
                    async with http_session.get(str(stk_url), timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            raw = await resp.read()
                            parts.append(types.Part.from_bytes(data=raw, mime_type="image/png"))
            except Exception:
                pass

    # 7. Direct Image/GIF/Embed URLs in message content or embeds
    if not parts:
        candidate_urls: List[str] = []
        if message.content:
            candidate_urls.extend(re.findall(r"https?://[^\s<>\x22\x27]+\.(?:png|jpg|jpeg|webp|gif)", message.content, flags=re.IGNORECASE))
        for emb in message.embeds[:1]:
            if emb.image and emb.image.url:
                candidate_urls.append(emb.image.url)
            elif emb.thumbnail and emb.thumbnail.url:
                candidate_urls.append(emb.thumbnail.url)
        for img_url in candidate_urls[:1]:
            try:
                async with http_session.get(img_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        mime = "image/gif" if ".gif" in img_url.lower() else "image/png"
                        parts.append(types.Part.from_bytes(data=raw, mime_type=mime))
            except Exception:
                pass

    # 8. Profile Picture / Avatar inspection when mentioned
    content_low = (message.content or "").lower()
    if any(k in content_low for k in ("pfp", "avatar", "profile pic", "profile picture", "my icon", "his icon", "her icon")):
        target_users = [u for u in message.mentions if bot.user and u.id != bot.user.id] or [message.author]
        for u in target_users[:1]:
            try:
                av_url = u.display_avatar.with_format("png").with_size(256).url
                async with http_session.get(av_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        parts.append(types.Part.from_bytes(data=raw, mime_type="image/png"))
            except Exception as e:
                logger.debug(f"Avatar vision fetch failed: {e}")

    return parts, text_file_snippets


def extract_clean_gemini_text(response: Any) -> Optional[str]:
    try:
        if not response or not response.candidates:
            return None
        content = response.candidates[0].content
        if not content or not content.parts:
            return None
        text_chunks: List[str] = []
        for part in content.parts:
            if getattr(part, "thought", False):
                continue
            if getattr(part, "text", None):
                text_chunks.append(part.text)
        joined = "".join(text_chunks).strip()
        if not joined:
            return None
        joined = re.sub(r"<thought[^>]*>.*?</thought>", "", joined, flags=re.DOTALL | re.IGNORECASE)
        joined = re.sub(r"<thought[^>]*>.*", "", joined, flags=re.DOTALL | re.IGNORECASE)
        joined = re.sub(r"<think[^>]*>.*?</think>", "", joined, flags=re.DOTALL | re.IGNORECASE)
        joined = re.sub(r"<think[^>]*>.*", "", joined, flags=re.DOTALL | re.IGNORECASE)
        return joined.strip() if joined.strip() else None
    except Exception:
        raw = getattr(response, "text", None)
        if raw:
            raw = re.sub(r"<thought[^>]*>.*?</thought>", "", raw, flags=re.DOTALL | re.IGNORECASE)
            raw = re.sub(r"<thought[^>]*>.*", "", raw, flags=re.DOTALL | re.IGNORECASE)
            raw = re.sub(r"<think[^>]*>.*?</think>", "", raw, flags=re.DOTALL | re.IGNORECASE)
            return raw.strip() if raw.strip() else None
        return None


def build_native_multiturn_contents(
    history_records: List[Dict[str, Any]],
    trigger_message: Optional[discord.Message],
    multimodal_parts: List[types.Part],
    text_file_snippets: List[str],
    proactive_reason: str = "",
) -> List[types.Content]:
    """
    Passes conversation as true alternating types.Content(role='user') and types.Content(role='model')
    multi-turn history so Gemini never loses context on short follow-ups like 'Just tell me', 'Why', or 'Huh'.
    """
    trig_id = trigger_message.id if trigger_message else None
    prior = [r for r in history_records[-18:] if not (trig_id and r.get("message_id") == trig_id)]

    turns: List[types.Content] = []
    for rec in prior:
        role = "model" if rec.get("is_bot") else "user"
        text_val = rec.get("content", "").strip()
        if not text_val:
            continue
        if role == "user":
            if rec.get("reply_to"):
                snip = f' "{rec["reply_snippet"][:70]}"' if rec.get("reply_snippet") else ""
                rep = f" (replying to {rec['reply_to']}{snip})"
            else:
                rep = ""
            formatted = f"{rec['sender']}{rep}: {text_val}"
        else:
            formatted = text_val

        if turns and turns[-1].role == role:
            prev_text = turns[-1].parts[0].text or ""
            sep = " ||| " if role == "model" else "\n"
            turns[-1].parts[0] = types.Part.from_text(text=f"{prev_text}{sep}{formatted}")
        else:
            turns.append(types.Content(role=role, parts=[types.Part.from_text(text=formatted)]))

    while turns and turns[0].role == "model":
        turns.pop(0)

    if trigger_message:
        clean_content = humanize_incoming_mentions(trigger_message.content, trigger_message.guild)
        rich_extras = extract_rich_message_details(trigger_message)
        if not clean_content and not rich_extras:
            clean_content = "yo"
        rep_author, rep_snip, _, _ = resolve_referenced_message(trigger_message, trigger_message.channel.id)
        if rep_author:
            snip_txt = f' "{rep_snip[:70]}"' if rep_snip else ""
            rep_header = f" (replying to {rep_author}{snip_txt})"
        else:
            rep_header = ""
        latest_text = f"{trigger_message.author.display_name}{rep_header}: {clean_content}{rich_extras}"
        if text_file_snippets:
            latest_text += "\n" + "\n".join(text_file_snippets)
    else:
        latest_text = f"[AUTONOMOUS PROACTIVE MOMENT: {proactive_reason}]"

    latest_parts: List[Any] = [types.Part.from_text(text=latest_text)]
    latest_parts.extend(multimodal_parts)

    if turns and turns[-1].role == "user":
        prev_text = turns[-1].parts[0].text or ""
        merged_parts: List[Any] = [types.Part.from_text(text=f"{prev_text}\n{latest_text}")]
        merged_parts.extend(multimodal_parts)
        turns[-1] = types.Content(role="user", parts=merged_parts)
    else:
        turns.append(types.Content(role="user", parts=latest_parts))

    return turns


async def generate_gemini_response(
    channel: discord.abc.Messageable,
    trigger_message: Optional[discord.Message],
    system_instruction: str,
    history_records: List[Dict[str, Any]],
    multimodal_parts: List[types.Part],
    text_file_snippets: List[str],
    allow_gif: bool,
    proactive_reason: str = "",
) -> Tuple[Optional[str], bool, bool]:
    global active_gemini_model
    if not genai_client:
        return None, False, False

    guild = trigger_message.guild if trigger_message else getattr(channel, "guild", None)
    turn_media_state: Dict[str, bool] = {"sent": False}
    collected_texts: List[str] = []
    executed_any_tool: bool = False

    for model_name in list(GEMINI_MODEL_CANDIDATES):
        contents = build_native_multiturn_contents(
            history_records=history_records,
            trigger_message=trigger_message,
            multimodal_parts=multimodal_parts,
            text_file_snippets=text_file_snippets,
            proactive_reason=proactive_reason,
        )
        try:
            turn = 0


            while turn < 2:
                # In turn 0, provide tools if media hasn't been sent. In turn 1+, provide tools but forbid calling them (mode=NONE)
                # so the multi-turn function response schema is satisfied and text companion generates instantly!
                if turn == 0 and not turn_media_state["sent"]:
                    step_tools = build_genai_tools(allow_gif=allow_gif)
                    tool_cfg = None
                else:
                    step_tools = build_genai_tools(allow_gif=False)
                    tool_cfg = types.ToolConfig(
                        function_calling_config=types.FunctionCallingConfig(
                            mode=types.FunctionCallingConfigMode.NONE
                        )
                    )

                config = types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    temperature=0.92,
                    tools=step_tools,
                    tool_config=tool_cfg,
                )

                response = await asyncio.wait_for(
                    genai_client.aio.models.generate_content(
                        model=model_name,
                        contents=contents,
                        config=config,
                    ),
                    timeout=8.0,
                )
                active_gemini_model = model_name

                turn_text = extract_clean_gemini_text(response)
                inline_executed: List[Tuple[str, Dict[str, Any]]] = []
                if turn_text:
                    cleaned_turn_text, _, inline_executed = await intercept_and_execute_inline_tool_tags(
                        raw_text=turn_text,
                        channel=channel,
                        guild=guild,
                        target_msg=trigger_message,
                        turn_media_state=turn_media_state,
                    )
                    if inline_executed:
                        executed_any_tool = True
                    if cleaned_turn_text and cleaned_turn_text not in collected_texts:
                        collected_texts.append(cleaned_turn_text)

                if not response.function_calls:
                    if inline_executed and not collected_texts and turn < 1:
                        summary_str = "; ".join([f"{fn} -> {json.dumps(r)}" for fn, r in inline_executed])
                        contents.append(types.Content(role="model", parts=[types.Part.from_text(text=f"[Executed tool: {summary_str}]")]))
                        contents.append(types.Content(role="user", parts=[types.Part.from_text(text="[Tool finished. Now write your casual Discord chat response!]")]))
                        turn += 1
                        continue

                    final_joined = " ||| ".join(collected_texts).strip() if collected_texts else None
                    return final_joined, True, turn_media_state["sent"]

                executed_any_tool = True
                if response.candidates and response.candidates[0].content:
                    contents.append(response.candidates[0].content)

                tool_responses = []
                for call in response.function_calls:
                    call_args = dict(call.args) if call.args else {}
                    logger.info(f"[TOOL CALL ({model_name})] {call.name}({call_args})")
                    res_dict = await dispatch_tool_call(
                        func_name=call.name,
                        args=call_args,
                        channel=channel,
                        guild=guild,
                        target_msg=trigger_message,
                        turn_media_state=turn_media_state,
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

            final_joined = " ||| ".join(collected_texts).strip() if collected_texts else None
            return final_joined, executed_any_tool, turn_media_state["sent"]

        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            logger.warning(f"Gemini model '{model_name}' timed out after 8.0s; cascading to next candidate...")
            continue
        except Exception as e:
            err_str = str(e)
            if "404" in err_str or "NOT_FOUND" in err_str:
                logger.warning(f"Model '{model_name}' returned 404; pruning from candidate list and cascading...")
                if len(GEMINI_MODEL_CANDIDATES) > 1 and model_name in GEMINI_MODEL_CANDIDATES:
                    GEMINI_MODEL_CANDIDATES.remove(model_name)
                continue
            if ("400" in err_str or "INVALID_ARGUMENT" in err_str) and multimodal_parts:
                logger.warning(f"Multimodal attachment rejected by '{model_name}' ({e}); clearing multimodal_parts and retrying text-only...")
                multimodal_parts = []
            logger.warning(f"Gemini error on '{model_name}': {e}; cascading to next model...")
            continue
    return None, False, False


async def call_groq_fallback(system_prompt: str, history_records: List[Dict[str, Any]], trigger_text: str, speaker_name: str) -> Optional[str]:
    if not GROQ_API_KEY or not http_session or not GROQ_MODEL_CANDIDATES:
        return None
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    msgs: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]
    for r in history_records[-8:]:
        msgs.append({
            "role": "assistant" if r.get("is_bot") else "user",
            "content": r.get("content", "") if r.get("is_bot") else f"{r.get('sender', 'User')}: {r.get('content', '')}",
        })
    msgs.append({"role": "user", "content": f"{speaker_name}: {trigger_text or 'yo'}"})

    for candidate in GROQ_MODEL_CANDIDATES:
        if "qwen" in candidate.lower():
            continue
        payload: Dict[str, Any] = {
            "model": candidate,
            "temperature": 0.85,
            "max_tokens": 500,
            "messages": msgs,
        }
        if "gpt-oss" in candidate.lower():
            payload["reasoning_effort"] = "low"
        try:
            async with http_session.post(GROQ_ENDPOINT, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    raw = data["choices"][0]["message"]["content"] or ""
                    raw = re.sub(r"<think[^>]*>.*?</think>", "", raw, flags=re.DOTALL | re.IGNORECASE)
                    raw = re.sub(r"<think[^>]*>.*", "", raw, flags=re.DOTALL | re.IGNORECASE)
                    raw = re.sub(r"<thought[^>]*>.*?</thought>", "", raw, flags=re.DOTALL | re.IGNORECASE)
                    raw = re.sub(r"<thought[^>]*>.*", "", raw, flags=re.DOTALL | re.IGNORECASE)
                    return raw.strip()
        except Exception as e:
            logger.debug(f"Groq fallback failed on {candidate}: {e}")
    return None


# ---------------------------------------------------------------------------
# 11. RAW-SPEED DELIVERY PIPELINE & IN-FLIGHT TASK CANCELLATION
# ---------------------------------------------------------------------------
async def run_channel_response_pipeline(
    channel: discord.abc.Messageable,
    trigger_message: Optional[discord.Message],
    is_direct_interaction: bool,
    is_implicit_wakeup: bool,
    explicitly_mentioned_others: List[str],
    target_member_to_engage: Optional[discord.Member],
    is_hush_request: bool,
    replied_to_other_user: Optional[str],
    replied_snippet: Optional[str],
    last_message_from_bot: bool,
    bot_in_last_5_messages: bool,
    seconds_since_bot_spoke: float,
    is_proactive_scan: bool = False,
    proactive_reason: str = "",
    inactivity_seconds: float = 0.0,
) -> None:
    ch_id = getattr(channel, "id", 0)
    try:
        guild = trigger_message.guild if trigger_message else getattr(channel, "guild", None)
        last_human_spk = channel_last_human_speaker.get(ch_id) or memory_state.get("last_human_speaker_name") or "someone"
        last_human_txt = channel_last_human_content.get(ch_id) or memory_state.get("last_human_message_text") or ""

        speaker_name = trigger_message.author.display_name if trigger_message else last_human_spk
        speaker_id = str(trigger_message.author.id) if trigger_message else "0"
        speaker_is_creator = is_creator(trigger_message.author) if trigger_message else False
        speaker_member = trigger_message.author if (trigger_message and isinstance(trigger_message.author, discord.Member)) else None

        raw_without_mentions = re.sub(r"<@!?\d+>", "", trigger_message.content).strip() if trigger_message else ""
        is_bare_ping = bool(trigger_message and is_direct_interaction and len(raw_without_mentions) == 0 and not trigger_message.attachments)

        now_ts = time.time()
        gif_cooldown_ready = (now_ts - channel_last_gif_epoch[ch_id]) >= 25.0

        async with memory_lock:
            speaker_affinity = memory_state.get("user_affinity", {}).get(speaker_id, {"name": speaker_name, "score": 0, "notes": []})
            emotional_state = decay_emotional_sliders_only()

        history_records = list(channel_buffers[ch_id])
        exchange_streak = channel_thread_exchange_count[ch_id]
        server_emojis_str = get_server_custom_emojis_summary(guild)
        live_radar_str = build_live_server_radar(guild, ch_id)
        is_stepped_back = now_ts < channel_stepped_back_until.get(ch_id, 0.0)

        # 1. Hybrid Router (Groq gpt-oss-120b / 20b -> Gemini 3.5 Flash-Lite Failover)
        decision = await call_hybrid_router(
            channel_msgs=history_records[-16:],
            emotional_state=emotional_state,
            speaker_name=speaker_name,
            speaker_affinity=speaker_affinity,
            is_direct_interaction=is_direct_interaction,
            is_implicit_wakeup=is_implicit_wakeup,
            is_bare_ping=is_bare_ping,
            explicitly_mentioned_others=explicitly_mentioned_others,
            target_member_to_engage=target_member_to_engage,
            is_hush_request=is_hush_request,
            replied_to_other_user=replied_to_other_user,
            replied_snippet=replied_snippet,
            last_message_from_bot=last_message_from_bot,
            bot_in_last_5_messages=bot_in_last_5_messages,
            seconds_since_bot_spoke=seconds_since_bot_spoke,
            gif_cooldown_ready=gif_cooldown_ready,
            is_stepped_back=is_stepped_back,
            exchange_streak=exchange_streak,
            live_radar_str=live_radar_str,
            server_emojis_str=server_emojis_str,
            is_proactive_scan=is_proactive_scan,
            proactive_reason=proactive_reason,
            inactivity_seconds=inactivity_seconds,
            last_human_speaker=last_human_spk,
            last_human_content=last_human_txt,
        )

        # Only enter quiet/step-back state when a human EXPLICITLY asked the bot to be quiet!
        if is_hush_request:
            channel_stepped_back_until[ch_id] = time.time() + 300.0
            channel_thread_exchange_count[ch_id] = 0
            logger.info(f"[QUIET MODE] Locked channel {ch_id} into quiet state (will still react with emojis and wake on check-ins).")
        elif decision.get("wake_up_from_quiet", False) or is_direct_interaction or is_implicit_wakeup:
            if channel_stepped_back_until.get(ch_id, 0.0) > 0:
                channel_stepped_back_until[ch_id] = 0.0
                logger.info(f"[WOKE UP] Cleared quiet mode in channel {ch_id}!")

        should_speak = bool(decision.get("should_speak", False))

        async with memory_lock:
            emotional_state = apply_spring_decay_and_shift(
                shift=decision.get("slider_shifts"),
                is_major_trigger=True,
                accelerate_recovery=bool(decision.get("accelerate_decay", False)),
            )
            await save_memory_state()

        # Emoji reactions: always react while in quiet mode (silent communication!), on hush requests, or when the router picks an emoji!
        reaction_emoji = decision.get("reaction_emoji")
        if not reaction_emoji and (is_hush_request or is_stepped_back):
            reaction_emoji = random.choice(["🤐", "😶", "👀", "🫡", "🤫", "🥀", "💔"])

        if reaction_emoji and trigger_message and (not should_speak or is_hush_request or is_stepped_back or random.random() < 0.35):
            resolved_emo = resolve_reaction_emoji(guild, str(reaction_emoji))
            if resolved_emo:
                try:
                    await trigger_message.add_reaction(resolved_emo)
                except Exception:
                    pass

        if not should_speak:
            return

        # Clear consumed delete/edit snipes once the bot speaks
        deleted_message_cache.pop(ch_id, None)
        edited_message_cache.pop(ch_id, None)

        directive = decision.get("tactical_directive") or "Respond with personality and emotion."
        length_mode = str(decision.get("length_mode") or "short").lower().strip()
        recommended_burst_count = _safe_int(decision.get("recommended_burst_count"), 1)
        if is_hush_request:
            length_mode = "micro"
            recommended_burst_count = 1

        user_explicit_gif = bool(
            trigger_message
            and re.search(r"\b(?:gif|meme|tenor|image|reaction pic|drop a gif|post a gif|send a gif)\b", trigger_message.content, flags=re.IGNORECASE)
        )
        suggest_gif = (bool(decision.get("suggest_gif", False)) or user_explicit_gif) and (gif_cooldown_ready or user_explicit_gif) and not is_bare_ping and not is_hush_request
        admin_power_move = decision.get("admin_power_move")
        user_asked_for_ping = bool(
            trigger_message and re.search(r"\b(?:ping|mention|tag)\s*(?:me|us|everyone)?\b", raw_without_mentions, flags=re.IGNORECASE)
        )
        allow_speaker_ping = bool(decision.get("allow_speaker_ping", False)) or is_proactive_scan or user_asked_for_ping
        delivery_mode = decision.get("delivery_mode", "reply")

        # Protect against suggesting change_nickname on the server owner (Discord API restriction)
        if admin_power_move and guild and speaker_member and speaker_member == guild.owner and "change_nickname" in str(admin_power_move):
            admin_power_move = "Call `change_my_nickname` or `create_role` / `assign_role` instead (since Discord blocks editing the server owner's nickname)."

        allow_gif_tool = (gif_cooldown_ready or user_explicit_gif) and not is_bare_ping and not is_hush_request
        channel_last_tool_summaries[ch_id].clear()

        system_instruction = construct_v11_system_prompt(
            channel_id=ch_id,
            guild=guild,
            tactical_directive=directive,
            length_mode=length_mode,
            recommended_burst_count=recommended_burst_count,
            suggest_gif=suggest_gif,
            admin_power_move=admin_power_move,
            emotional_state=emotional_state,
            speaker_name=speaker_name,
            speaker_id=speaker_id,
            speaker_is_creator=speaker_is_creator,
            is_proactive_scan=is_proactive_scan,
            proactive_reason=proactive_reason,
            live_radar_str=live_radar_str,
            server_emojis_str=server_emojis_str,
        )

        # 2. Native Multi-Turn Multimodal Generation (Zero artificial sleep timers!)
        turn_media_state: Dict[str, bool] = {"sent": False}
        async with channel.typing():
            multimodal_parts, text_file_snippets = await extract_multimodal_parts(trigger_message)
            raw_output, gemini_succeeded, media_sent = await generate_gemini_response(
                channel=channel,
                trigger_message=trigger_message,
                system_instruction=system_instruction,
                history_records=history_records,
                multimodal_parts=multimodal_parts,
                text_file_snippets=text_file_snippets,
                allow_gif=allow_gif_tool,
                proactive_reason=proactive_reason,
            )
            if media_sent:
                turn_media_state["sent"] = True

            if not gemini_succeeded or not raw_output:
                groq_raw = await call_groq_fallback(system_instruction, history_records, raw_without_mentions, speaker_name)
                if groq_raw:
                    cleaned_groq, groq_sent, _ = await intercept_and_execute_inline_tool_tags(
                        raw_text=groq_raw,
                        channel=channel,
                        guild=guild,
                        target_msg=trigger_message,
                        turn_media_state=turn_media_state,
                    )
                    raw_output = cleaned_groq
                    if groq_sent:
                        media_sent = True
                        turn_media_state["sent"] = True
                if (suggest_gif or user_explicit_gif) and not media_sent and not is_hush_request:
                    term_match = re.search(r"\b(?:gif|meme|pic)\s+(?:of\s+)?([a-zA-Z0-9_\s]{2,25})", raw_without_mentions, flags=re.IGNORECASE)
                    term = term_match.group(1).strip() if term_match else (raw_without_mentions[:30] or "reaction")
                    gif_res = await execute_post_gif(channel, term, turn_media_state=turn_media_state)
                    if gif_res.get("status") == "sent_to_channel":
                        media_sent = True


        executed_tools_this_turn = list(channel_last_tool_summaries[ch_id])
        bot_name = bot.user.display_name if bot.user else "Dumb bot"
        bot_id = bot.user.id if bot.user else 0

        if not raw_output:
            raw_output = await call_groq_fallback(system_instruction, history_records, raw_without_mentions, speaker_name)

        if not raw_output and (media_sent or executed_tools_this_turn):
            default_companions = [
                "don't say i never did anything for you",
                "look at this lmao",
                "had to drop this",
                "literally this rn",
                "i rest my case",
            ]
            raw_output = random.choice(default_companions)

        if not raw_output:
            return

        cleaned_text = sanitize_output_text(raw_output, speaker_name=speaker_name)
        bursts = split_into_burst_messages(
            cleaned_text,
            is_hush_mode=is_hush_request,
            length_mode=length_mode,
        )
        if not bursts:
            return

        channel_sending_lock[ch_id] = True
        try:
            aff_score = speaker_affinity.get("score", 0)
            patience_val = emotional_state.get("patience", 55.0)

            use_reply = (
                trigger_message is not None
                and delivery_mode == "reply"
                and not last_message_from_bot
                and not media_sent
                and not is_proactive_scan
                and not (aff_score < -20 or patience_val < -30)
            )

            for idx, burst_part in enumerate(bursts):
                outgoing_text = format_outgoing_mentions(
                    text=burst_part,
                    guild=guild,
                    current_speaker_name=speaker_name,
                    allow_speaker_ping=allow_speaker_ping,
                    force_target_member=target_member_to_engage if idx == 0 else None,
                )
                if not outgoing_text:
                    continue

                if idx == 0 and use_reply and trigger_message is not None:
                    try:
                        sent_msg = await trigger_message.reply(outgoing_text, mention_author=(aff_score >= -10))
                    except Exception:
                        sent_msg = await channel.send(outgoing_text)
                else:
                    sent_msg = await channel.send(outgoing_text)

                channel_messages_since_gif[ch_id] += 1
                record_buffer_message(
                    channel_id=ch_id,
                    sender=bot_name,
                    sender_id=bot_id,
                    content=outgoing_text,
                    msg_id=sent_msg.id,
                    is_bot=True,
                    has_media=(idx == 0 and media_sent),
                    tool_actions=executed_tools_this_turn if idx == 0 else None,
                )

            # If connected to a Voice Channel with humans in it and we haven't already called speak_in_voice this turn,
            # automatically speak our response out loud in VC too!
            vc_ch = getattr(guild.voice_client, "channel", None) if (guild and guild.voice_client) else None
            if (
                guild
                and guild.voice_client
                and guild.voice_client.is_connected()
                and vc_ch is not None
                and any(not m.bot for m in vc_ch.members)
                and not any("speak_in_voice" in t for t in executed_tools_this_turn)
            ):
                spoken_joined = ". ".join(bursts)
                asyncio.create_task(play_text_in_voice_channel(guild, spoken_joined))

            if not is_hush_request and not is_proactive_scan:
                channel_thread_exchange_count[ch_id] += 1
                if channel_proactive_stage[ch_id] == 0:
                    schedule_next_proactive_window(ch_id, stage=0, active_convo=True)
        finally:
            channel_sending_lock[ch_id] = False

    except asyncio.CancelledError:
        logger.debug("In-flight response task cancelled cleanly to absorb newer message.")
        return
    except Exception as e:
        logger.error(f"Pipeline error in channel {ch_id}: {e}")


# ---------------------------------------------------------------------------
# 12. BACKGROUND LOOPS: SPRING DECAY, PRESENCE & MULTI-HORIZON PROACTIVE ENGINE
# ---------------------------------------------------------------------------
@tasks.loop(minutes=2)
async def emotional_spring_maintenance_loop() -> None:
    async with memory_lock:
        decay_emotional_sliders_only()
        await save_memory_state()


@tasks.loop(minutes=8)
async def dynamic_presence_loop() -> None:
    if not bot.is_ready():
        return
    st = memory_state.get("emotional_state", {})
    mood = st.get("mood_label", "jolly/wholesome")
    valence = st.get("valence", 50.0)
    arousal = st.get("arousal", 55.0)

    if valence < -25:
        status_pool = [
            "in my villain arc",
            "holding a grudge",
            "beefing with chat",
            "dramatically sighing",
            f"mood: {mood[:30]}",
        ]
        discord_status = discord.Status.dnd if valence < -50 else discord.Status.idle
    elif arousal > 45:
        status_pool = [
            "yapping",
            "plotting a coup in general",
            "scheming in general",
            "waiting for someone to say something dumb",
            f"vibe: {mood[:30]}",
        ]
        discord_status = discord.Status.online
    else:
        status_pool = [
            "reading deleted messages",
            "staring at the member list",
            "judging your spotify",
            "trapped in discord",
            "lurking",
            f"vibe: {mood[:30]}",
        ]
        discord_status = discord.Status.online

    chosen = random.choice(status_pool)[:120]
    await bot.change_presence(
        status=discord_status,
        activity=discord.CustomActivity(name=chosen, state=chosen),
    )


def get_primary_active_channel(guild: Optional[discord.Guild] = None) -> Optional[discord.TextChannel]:
    stored_ch_id = memory_state.get("last_active_channel_id")
    if stored_ch_id:
        ch = bot.get_channel(int(stored_ch_id))
        if isinstance(ch, discord.TextChannel) and (not guild or ch.guild.id == guild.id):
            return ch
    target_guilds = [guild] if guild else list(bot.guilds)
    for g in target_guilds:
        if not g:
            continue
        for ch in g.text_channels:
            if ch.name.lower() in ("general", "chat", "main", "lobby") and ch.permissions_for(g.me).send_messages:
                return ch
        for ch in g.text_channels:
            if ch.permissions_for(g.me).send_messages:
                return ch
    return None


@tasks.loop(seconds=30)
async def proactive_room_scanner() -> None:
    """
    Multi-Horizon Proactive Scanner:
    1. Checks overdue user promises/commitments.
    2. Stage 0 (Mid-Convo Ghost Follow-Up, 5-12 mins): If a human was actively talking to the bot and vanished mid-convo.
    3. Stage 1 (Multi-Hour Revival, 3.5-7 hours): Notices the server has been quiet for hours, checks live games/Spotify/VC, and sparks chat.
    4. Stage 2+ (Multi-Day Revival, 1+ days): Notices the server has been dead for days and revives the channel!
    """
    if not bot.is_ready():
        return

    now_epoch = time.time()
    now_ams = datetime.now(AMSTERDAM_TZ)

    primary_ch = get_primary_active_channel()
    candidate_channel_ids = set(channel_next_proactive_epoch.keys())
    if primary_ch:
        candidate_channel_ids.add(primary_ch.id)

    for ch_id in list(candidate_channel_ids):
        ch = bot.get_channel(ch_id)
        if not isinstance(ch, discord.TextChannel):
            continue

        if channel_sending_lock.get(ch_id, False) or now_epoch < channel_stepped_back_until.get(ch_id, 0.0):
            continue
        existing_task = active_generation_tasks.get(ch_id)
        if existing_task and not existing_task.done():
            continue

        # 1. Check due user commitments first
        async with memory_lock:
            for c in memory_state.get("active_commitments", []):
                if not c.get("called_out", False):
                    try:
                        due_dt = datetime.fromisoformat(c["due_timestamp"])
                        if now_ams >= due_dt:
                            c["called_out"] = True
                            await save_memory_state()
                            mention = f"<@{c['user_id']}>" if c.get("user_id") else c.get("username", "bro")
                            msg_txt = f"yo {mention} didn't you say you were gonna {c['promise']}? what happened to that"
                            sent = await ch.send(msg_txt)
                            record_buffer_message(
                                ch_id,
                                bot.user.display_name if bot.user else "Dumb bot",
                                bot.user.id if bot.user else 0,
                                msg_txt,
                                sent.id,
                                is_bot=True,
                            )
                            return
                    except Exception:
                        c["called_out"] = True

        # Determine last activity epoch (from RAM or persisted memory across restarts)
        last_act = channel_last_activity_epoch.get(ch_id) or float(memory_state.get("last_channel_activity_epoch") or 0.0)
        if last_act <= 0.0:
            channel_last_activity_epoch[ch_id] = now_epoch
            schedule_next_proactive_window(ch_id, stage=1, active_convo=False)
            continue

        silence_seconds = max(0.0, now_epoch - last_act)
        stage = max(channel_proactive_stage[ch_id], int(memory_state.get("proactive_stage_persisted", 0)))

        if ch_id not in channel_next_proactive_epoch:
            # Initialize schedule on startup based on how long the server has already been quiet
            if silence_seconds >= 86400.0 and stage < 3:
                channel_next_proactive_epoch[ch_id] = now_epoch + 15.0
                stage = 3
            elif silence_seconds >= 5400.0 and stage < 2:
                channel_next_proactive_epoch[ch_id] = now_epoch + 15.0
                stage = 2
            elif silence_seconds >= 900.0 and stage < 1:
                channel_next_proactive_epoch[ch_id] = now_epoch + 15.0
                stage = 1
            else:
                schedule_next_proactive_window(ch_id, stage=max(1, stage), active_convo=False)
                continue

        next_due = channel_next_proactive_epoch.get(ch_id, 0.0)
        if next_due <= 0.0 or now_epoch < next_due:
            continue

        # For multi-hour / multi-day revivals, avoid firing in the middle of the night (02:00 - 09:30 Amsterdam time)
        # unless someone is actively online/playing something right now
        any_human_online = any(not m.bot and m.status != discord.Status.offline for m in ch.guild.members)
        if stage >= 2 and (2 <= now_ams.hour < 9) and not any_human_online:
            channel_next_proactive_epoch[ch_id] = now_epoch + 3600.0
            continue

        last_spk = channel_last_human_speaker.get(ch_id) or memory_state.get("last_human_speaker_name") or "everyone"
        last_txt = channel_last_human_content.get(ch_id) or memory_state.get("last_human_message_text") or ""
        human_dur = format_duration_human(silence_seconds)

        if stage == 0 and silence_seconds < 1800.0:
            proactive_reason = (
                f"You and @{last_spk} were just in an active conversation ({human_dur} ago they said: '{last_txt[:100]}'), "
                f"and they suddenly vanished mid-conversation! Mention @{last_spk} to call them out for disappearing mid-convo, "
                "act dramatically abandoned or left on read, ask where they went, or drop a funny thought!"
            )
            next_stage = 1
        elif silence_seconds >= 82800.0 or stage >= 3:
            days_quiet = max(1.0, round(silence_seconds / 86400.0, 1))
            proactive_reason = (
                f"The server has been completely silent for {days_quiet} DAYS ({human_dur} since @{last_spk} last spoke)! "
                "Comment on how the server has been dead for days while you've been sitting inside Discord alone, "
                "call out who's currently online or playing a game/Spotify on your LIVE SERVER RADAR, or start a chaotic topic!"
            )
            next_stage = stage + 1
        elif silence_seconds >= 5400.0 or stage == 2:
            hrs_quiet = max(1.5, round(silence_seconds / 3600.0, 1))
            proactive_reason = (
                f"Chat has been dead quiet for {hrs_quiet} hours! You are sitting alone in code while everyone vanished. "
                "Check your LIVE SERVER RADAR (see if anyone is online, playing a game, or in VC) and complain about being abandoned, "
                "ask where everyone went, or drop an existential late-night thought!"
            )
            next_stage = 3
        else:
            mins_quiet = max(15, round(silence_seconds / 60.0))
            proactive_reason = (
                f"Chat died down {mins_quiet} minutes ago. Check your LIVE SERVER RADAR (see who is online, playing games, or in VC). "
                f"Complain about boredom, ask @{last_spk} or the server where everyone went, gossip about someone's live activity, "
                "or drop a funny bit!"
            )
            next_stage = 2

        channel_proactive_stage[ch_id] = next_stage
        async with memory_lock:
            memory_state["proactive_stage_persisted"] = next_stage
            await save_memory_state()
        schedule_next_proactive_window(ch_id, stage=next_stage, active_convo=False)

        logger.info(f"[PROACTIVE ENGINE] Stage {stage} -> {next_stage} triggered in #{ch.name} after {human_dur} silence.")
        records = list(channel_buffers[ch_id])

        task = asyncio.create_task(
            run_channel_response_pipeline(
                channel=ch,
                trigger_message=None,
                is_direct_interaction=False,
                is_implicit_wakeup=False,
                explicitly_mentioned_others=[],
                target_member_to_engage=None,
                is_hush_request=False,
                replied_to_other_user=None,
                replied_snippet=None,
                last_message_from_bot=bool(records and records[-1].get("is_bot", False)),
                bot_in_last_5_messages=True,
                seconds_since_bot_spoke=silence_seconds,
                is_proactive_scan=True,
                proactive_reason=proactive_reason,
                inactivity_seconds=silence_seconds,
            )
        )
        active_generation_tasks[ch_id] = task
        break


# ---------------------------------------------------------------------------
# 13. FULL DISCORD EVENT AWARENESS (VOICE, PRESENCE, MEMBERS, REACTIONS, SNIPES)
# ---------------------------------------------------------------------------
async def trigger_proactive_event_comment(guild: discord.Guild, event_key: str, cooldown_sec: float, reason_prompt: str) -> None:
    """
    Allows the bot to proactively react in chat to major live Discord events (someone joining VC,
    starting a stream, new member joining, etc.) with smart cooldowns so it's never spammy.
    """
    now = time.time()
    if now - proactive_event_cooldowns[event_key] < cooldown_sec:
        return

    ch = get_primary_active_channel(guild)
    if not ch:
        return
    ch_id = ch.id
    if channel_sending_lock.get(ch_id, False) or now < channel_stepped_back_until.get(ch_id, 0.0):
        return
    existing_task = active_generation_tasks.get(ch_id)
    if existing_task and not existing_task.done():
        return

    proactive_event_cooldowns[event_key] = now
    records = list(channel_buffers[ch_id])
    task = asyncio.create_task(
        run_channel_response_pipeline(
            channel=ch,
            trigger_message=None,
            is_direct_interaction=False,
            is_implicit_wakeup=False,
            explicitly_mentioned_others=[],
            target_member_to_engage=None,
            is_hush_request=False,
            replied_to_other_user=None,
            replied_snippet=None,
            last_message_from_bot=bool(records and records[-1].get("is_bot", False)),
            bot_in_last_5_messages=False,
            seconds_since_bot_spoke=999.0,
            is_proactive_scan=True,
            proactive_reason=reason_prompt,
            inactivity_seconds=0.0,
        )
    )
    active_generation_tasks[ch_id] = task


@bot.event
async def on_ready() -> None:
    global http_session
    if http_session is None or http_session.closed:
        http_session = aiohttp.ClientSession()

    load_memory_state_sync()
    await restore_memory_from_discord_cloud()
    await discover_available_groq_models()

    load_opus_library()
    logger.info(
        f"Connected as {bot.user} (ID: {bot.user.id if bot.user else 'unknown'}) | "
        f"Primary Gemini: {active_gemini_model} | Router: Hybrid Groq ({active_groq_model}) + Gemini | "
        f"Voice PyNaCl: {VOICE_NACL_AVAILABLE} | Opus Loaded: {discord.opus.is_loaded()} | "
        f"Edge-TTS: {EDGE_TTS_AVAILABLE} | FFmpeg: {FFMPEG_EXECUTABLE}"
    )

    try:
        if bot.user and bot.user.avatar is None:
            asyncio.create_task(execute_change_my_avatar("cool sleek glowing cyberpunk anime robot with purple shades profile icon"))
    except Exception as e:
        logger.debug(f"Initial avatar setup skipped: {e}")

    if not emotional_spring_maintenance_loop.is_running():
        emotional_spring_maintenance_loop.start()
    if not dynamic_presence_loop.is_running():
        dynamic_presence_loop.start()
    if not proactive_room_scanner.is_running():
        proactive_room_scanner.start()
    if not discord_cloud_backup_loop.is_running():
        discord_cloud_backup_loop.start()


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState) -> None:
    if member.bot:
        return
    guild = member.guild
    bot_vc_ch = getattr(guild.voice_client, "channel", None) if guild.voice_client else None

    # If the bot is left completely alone in a Voice Channel, automatically disconnect after 15s
    if before.channel and bot_vc_ch and bot_vc_ch.id == before.channel.id:
        humans_left = [m for m in before.channel.members if not m.bot]
        if not humans_left:
            await asyncio.sleep(15)
            curr_vc_ch = getattr(guild.voice_client, "channel", None) if (guild.voice_client and guild.voice_client.is_connected()) else None
            if curr_vc_ch:
                still_humans = [m for m in curr_vc_ch.members if not m.bot]
                if not still_humans:
                    await execute_leave_voice_channel(guild)
            return

    if before.channel is None and after.channel is not None:
        desc = f"{member.display_name} just joined voice channel #{after.channel.name}"
        record_server_event(guild.id, desc)
        logger.info(f"[VOICE EVENT] {desc}")

        # If bot is already in that VC, greet them out loud in voice!
        if guild.voice_client and guild.voice_client.is_connected() and bot_vc_ch and bot_vc_ch.id == after.channel.id:
            asyncio.create_task(play_text_in_voice_channel(guild, f"yo {member.display_name}, what's up"))
        else:
            # Proactively notice them joining VC (and optionally hop in with join_voice_channel or comment in chat!)
            await trigger_proactive_event_comment(
                guild=guild,
                event_key=f"vc_join_{guild.id}",
                cooldown_sec=900.0,
                reason_prompt=(
                    f"@{member.display_name} just hopped into the voice channel #{after.channel.name}! "
                    f"You can either call `join_voice_channel` and `speak_in_voice` to surprise them in VC, "
                    f"or poke @{member.display_name} in text chat about sitting in #{after.channel.name}!"
                ),
            )
    elif before.channel is not None and after.channel is None:
        desc = f"{member.display_name} left voice channel #{before.channel.name}"
        record_server_event(guild.id, desc)
    elif before.channel and after.channel and not before.self_stream and after.self_stream:
        desc = f"{member.display_name} started streaming live in #{after.channel.name}"
        record_server_event(guild.id, desc)
        await trigger_proactive_event_comment(
            guild=guild,
            event_key=f"vc_stream_{member.id}",
            cooldown_sec=1800.0,
            reason_prompt=f"@{member.display_name} just started screen-sharing/streaming in voice channel #{after.channel.name}! Ask what they're streaming or hop in!",
        )


@bot.event
async def on_presence_update(before: discord.Member, after: discord.Member) -> None:
    if after.bot or not after.guild:
        return
    before_act = format_member_activities(before)
    after_act = format_member_activities(after)
    if after_act and after_act != before_act:
        record_server_event(after.guild.id, f"{after.display_name} activity update: {after_act}")
        # If the channel has been quiet for at least 20 minutes and someone launches a game or Spotify song, occasionally comment!
        ch = get_primary_active_channel(after.guild)
        if ch:
            idle_sec = time.time() - channel_last_activity_epoch.get(ch.id, 0.0)
            if idle_sec > 1200.0 and ("Playing:" in after_act or "Spotify:" in after_act):
                await trigger_proactive_event_comment(
                    guild=after.guild,
                    event_key=f"presence_{after.id}",
                    cooldown_sec=7200.0,
                    reason_prompt=(
                        f"You noticed on Discord presence that @{after.display_name} just started: {after_act}. "
                        f"Make a funny casual comment or roast/hype @{after.display_name} about it!"
                    ),
                )


@bot.event
async def on_member_join(member: discord.Member) -> None:
    if member.bot:
        return
    record_server_event(member.guild.id, f"New member {member.display_name} joined the server!")
    await trigger_proactive_event_comment(
        guild=member.guild,
        event_key=f"member_join_{member.id}",
        cooldown_sec=30.0,
        reason_prompt=f"A brand new member @{member.display_name} just joined the server! Welcome/grill them with full personality!",
    )


@bot.event
async def on_member_remove(member: discord.Member) -> None:
    if member.bot:
        return
    record_server_event(member.guild.id, f"{member.display_name} left (or was kicked from) the server!")


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member) -> None:
    if after.bot:
        return
    if before.nick != after.nick:
        record_server_event(
            after.guild.id,
            f"{before.display_name} nickname changed from '{before.display_name}' to '{after.display_name}'",
        )


@bot.event
async def on_message_delete(message: discord.Message) -> None:
    if not message.guild or (message.author.bot and message.author.id != TESTER_BOT_USER_ID) or not message.content:
        return
    if len(message.content.strip()) < 3:
        return
    clean_del = humanize_incoming_mentions(message.content, message.guild)[:300]
    deleted_message_cache[message.channel.id] = {
        "author": message.author.display_name,
        "author_id": message.author.id,
        "content": clean_del,
        "timestamp": time.time(),
    }
    record_server_event(message.guild.id, f"{message.author.display_name} deleted message: '{clean_del}'")


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message) -> None:
    if not before.guild or (before.author.bot and before.author.id != TESTER_BOT_USER_ID) or before.content == after.content:
        return
    if abs(len(before.content) - len(after.content)) <= 2 and before.content[:5].lower() == after.content[:5].lower():
        return
    b_clean = humanize_incoming_mentions(before.content, before.guild)[:200]
    a_clean = humanize_incoming_mentions(after.content, after.guild)[:200]
    edited_message_cache[before.channel.id] = {
        "author": before.author.display_name,
        "author_id": before.author.id,
        "before": b_clean,
        "after": a_clean,
        "timestamp": time.time(),
    }
    record_server_event(before.guild.id, f"{before.author.display_name} edited '{b_clean}' -> '{a_clean}'")


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent) -> None:
    if bot.user and payload.user_id == bot.user.id:
        return
    channel = bot.get_channel(payload.channel_id)
    if not channel:
        return

    msg: Optional[discord.Message] = discord.utils.get(bot.cached_messages, id=payload.message_id)
    if msg is None and hasattr(channel, "fetch_message"):
        try:
            msg = await channel.fetch_message(payload.message_id)  # type: ignore
        except Exception:
            return
    if msg is None:
        return

    emoji_str = str(payload.emoji.name).replace("\ufe0f", "")
    reactor_name = payload.member.display_name if payload.member else f"User {payload.user_id}"
    if msg.guild:
        record_server_event(
            msg.guild.id,
            f"{reactor_name} reacted with {emoji_str} to {msg.author.display_name}'s message ('{msg.content[:50]}')",
        )

    if not bot.user or msg.author.id != bot.user.id:
        return

    uid = str(payload.user_id)
    clowning_negative_set = {"🤡", "🤓", "🍅", "👎", "😐", "🤨", "❓", "🙄", "🥀", "🪫", "🧢", "🥱", "🖕"}
    laughing_positive_set = {"💀", "😭", "🔥", "❤", "❤️", "🫡", "🙏", "🤝", "👍", "🗣️", "🎯"}

    async with memory_lock:
        u_aff = memory_state.setdefault("user_affinity", {}).setdefault(uid, {
            "name": reactor_name,
            "score": 0,
            "notes": [],
        })
        if emoji_str in clowning_negative_set:
            u_aff["score"] = max(-100, u_aff.get("score", 0) - 6)
            apply_spring_decay_and_shift(
                {
                    "valence_delta": -15.0,
                    "patience_delta": -15.0,
                    "arousal_delta": 10.0,
                    "suggested_mood": "dramatically offended / side-eyeing",
                },
            )
            await save_memory_state()
        elif emoji_str in laughing_positive_set:
            u_aff["score"] = min(100, u_aff.get("score", 0) + 6)
            apply_spring_decay_and_shift(
                {
                    "valence_delta": 18.0,
                    "patience_delta": 15.0,
                    "arousal_delta": 12.0,
                    "suggested_mood": "jolly / hyped",
                },
                accelerate_recovery=True,
            )
            await save_memory_state()


@bot.event
async def on_message(message: discord.Message) -> None:
    if not message.guild or (message.author.bot and message.author.id != TESTER_BOT_USER_ID):
        return
    if message.id in recently_processed_messages:
        return
    recently_processed_messages.append(message.id)

    ch_id = message.channel.id
    if memory_state.get("last_active_channel_id") != ch_id:
        memory_state["last_active_channel_id"] = ch_id

    prior_records = list(channel_buffers[ch_id])
    last_message_from_bot = bool(prior_records and prior_records[-1].get("is_bot", False))
    bot_in_last_5_messages = any(r.get("is_bot", False) for r in prior_records[-5:])
    last_bot_epoch = channel_last_bot_spoke_epoch.get(ch_id, 0.0)
    seconds_since_bot_spoke = (time.time() - last_bot_epoch) if last_bot_epoch > 0 else 99999.0

    reply_to_name, replied_snippet, replied_to_bot, replied_to_other_human = resolve_referenced_message(message, ch_id)

    clean_text = humanize_incoming_mentions(message.content, message.guild)
    rich_extras = extract_rich_message_details(message)

    is_author_bot = bool(message.author.bot and message.author.id != TESTER_BOT_USER_ID)
    record_buffer_message(
        channel_id=ch_id,
        sender=message.author.display_name,
        sender_id=message.author.id,
        content=f"{clean_text}{rich_extras}".strip(),
        msg_id=message.id,
        is_bot=is_author_bot,
        has_media=bool(message.attachments or message.stickers or message.embeds),
        reply_to=reply_to_name,
        reply_snippet=replied_snippet,
    )

    if not any(r.get("is_bot", False) for r in prior_records[-6:]) and not replied_to_bot:
        channel_thread_exchange_count[ch_id] = 0

    existing_task = active_generation_tasks.get(ch_id)
    if existing_task and not existing_task.done() and not channel_sending_lock.get(ch_id, False):
        existing_task.cancel()
        logger.info(f"Cancelled in-flight generation in #{getattr(message.channel, 'name', ch_id)} to absorb rapid follow-up.")

    explicit_ping_or_reply = (bot.user is not None and bot.user in message.mentions) or replied_to_bot
    bot_mentioned = explicit_ping_or_reply
    if bot.user and not bot_mentioned:
        bot_names = {
            bot.user.name.lower(),
            bot.user.display_name.lower(),
            message.guild.me.display_name.lower(),
            "dumb bot",
            "dumbbot",
        }
        if any(re.search(rf"\b{re.escape(bn)}\b", message.content.lower()) for bn in bot_names if len(bn) >= 3):
            bot_mentioned = True
        elif f"<@{bot.user.id}>" in message.content or f"<@!{bot.user.id}>" in message.content or str(bot.user.id) in message.content:
            bot_mentioned = True

    is_currently_stepped_back = time.time() < channel_stepped_back_until.get(ch_id, 0.0)
    is_implicit_wakeup = detect_implicit_wakeup_or_checkin(
        content=message.content,
        is_stepped_back=is_currently_stepped_back,
        seconds_since_bot_spoke=seconds_since_bot_spoke,
    )

    target_member_to_engage = detect_target_member_to_engage(
        content=clean_text,
        guild=message.guild,
        author_id=message.author.id,
    )

    typed_mention_ids = {int(m) for m in re.findall(r"<@!?(\d+)>", message.content)}
    explicitly_mentioned_others = [
        f"@{u.display_name}" for u in message.mentions
        if u.id in typed_mention_ids
        and (not bot.user or u.id != bot.user.id)
        and u.id != message.author.id
        and (not target_member_to_engage or u.id != target_member_to_engage.id)
    ]

    is_active_or_direct = (
        bot_mentioned
        or is_implicit_wakeup
        or last_message_from_bot
        or (bot_in_last_5_messages and seconds_since_bot_spoke <= 600.0)
    )
    is_hush_request = detect_hush_command(message.content, is_active_or_direct)

    if (bot_mentioned or is_implicit_wakeup) and not is_hush_request:
        channel_stepped_back_until[ch_id] = 0.0

    task = asyncio.create_task(
        run_channel_response_pipeline(
            channel=message.channel,
            trigger_message=message,
            is_direct_interaction=bot_mentioned or bool(target_member_to_engage),
            is_implicit_wakeup=is_implicit_wakeup,
            explicitly_mentioned_others=explicitly_mentioned_others,
            target_member_to_engage=target_member_to_engage,
            is_hush_request=is_hush_request,
            replied_to_other_user=reply_to_name if replied_to_other_human else None,
            replied_snippet=replied_snippet,
            last_message_from_bot=last_message_from_bot,
            bot_in_last_5_messages=bot_in_last_5_messages,
            seconds_since_bot_spoke=seconds_since_bot_spoke,
            is_proactive_scan=False,
        )
    )
    active_generation_tasks[ch_id] = task


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        sys.exit(1)
    bot.run(DISCORD_TOKEN, log_handler=None)
