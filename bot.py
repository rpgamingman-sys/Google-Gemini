import os
import sys
import io
import re
import json
import math
import time
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
# 1. LOGGING, ENVIRONMENT & PERSISTENT STORAGE
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("AutonomousMetaBotV10")

DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TENOR_API_KEY = os.getenv("TENOR_API_KEY") or "LIVDSRZULELA"
CREATOR_USER_ID = int(os.getenv("CREATOR_USER_ID", "0"))
BACKUP_CHANNEL_ID = int(os.getenv("BACKUP_CHANNEL_ID", "0"))

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

# Primary Gemini Model: gemini-3.5-flash-lite with automatic cascade
ENV_GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")
GEMINI_MODEL_CANDIDATES: List[str] = list(dict.fromkeys([
    ENV_GEMINI_MODEL,
    "gemini-3.5-flash-lite",
    "gemini-3.5-flash",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.0-flash",
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

# Silent Python filter for corporate AI disclaimers & corny sci-fi/theatre-kid clichés
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
}

# ---------------------------------------------------------------------------
# 2. DISCORD CLIENT, CONCURRENCY & STATE BUFFERS
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
memory_dirty: bool = False

recently_processed_messages: deque = deque(maxlen=500)
channel_buffers: Dict[int, deque] = defaultdict(lambda: deque(maxlen=40))
active_generation_tasks: Dict[int, asyncio.Task] = {}
channel_sending_lock: Dict[int, bool] = defaultdict(bool)

# Gated Sniping Caches
deleted_message_cache: Dict[int, Dict[str, Any]] = {}
edited_message_cache: Dict[int, Dict[str, Any]] = {}
user_snipe_cooldowns: Dict[int, float] = defaultdict(float)

# Conversation Flow, Proactive Ghost-Followup, GIF Cooldown & Mood Variety State
channel_last_activity_epoch: Dict[int, float] = defaultdict(float)
channel_last_bot_spoke_epoch: Dict[int, float] = defaultdict(float)
channel_last_human_speaker: Dict[int, str] = {}
channel_last_human_content: Dict[int, str] = {}
channel_proactive_followup_count: Dict[int, int] = defaultdict(int)
channel_next_proactive_epoch: Dict[int, float] = defaultdict(float)

channel_last_gif_epoch: Dict[int, float] = defaultdict(float)
channel_messages_since_gif: Dict[int, int] = defaultdict(lambda: 99)
channel_stepped_back_until: Dict[int, float] = defaultdict(float)
channel_thread_exchange_count: Dict[int, int] = defaultdict(int)
channel_recent_length_modes: Dict[int, deque] = defaultdict(lambda: deque(maxlen=6))
channel_last_tool_summaries: Dict[int, List[str]] = defaultdict(list)
consecutive_negative_mood_turns: int = 0

# Responsive Admin Cooldowns (Low on harmless fun actions, 30-60m on channel creation)
admin_tool_cooldowns: Dict[str, float] = defaultdict(float)
ADMIN_COOLDOWN_SECONDS = {
    "change_my_nickname": 15.0,
    "change_nickname": 20.0,
    "change_my_status": 10.0,
    "set_channel_topic": 45.0,
    "pin_message": 30.0,
    "create_role": 180.0,
    "assign_role": 20.0,
    "remove_role": 20.0,
    "rename_channel": 300.0,
    "create_server_emoji": 120.0,
    "create_server_sticker": 120.0,
    "change_my_avatar": 300.0,
    "timeout_user": 180.0,
    "create_text_channel": 2400.0,  # 40-minute hard cooldown
}

# ---------------------------------------------------------------------------
# 3. PERSISTENT MEMORY, TIME-AWARENESS & ANTI-STUCK MOOD ENGINE
# ---------------------------------------------------------------------------
BASELINE_SLIDERS = {
    "valence": 50.0,   # -100 (Sad/Dramatic) <---> +100 (Jolly/Goofy/Hyped)
    "arousal": 55.0,   # -100 (Sleepy/Chill) <---> +100 (Chaotic/Energetic)
    "patience": 55.0,  # -100 (Sassy/Petty) <---> +100 (Wholesome/Friendly)
}

DYNAMIC_MOOD_WHEEL = [
    "jolly/wholesome",
    "goofy/brainrot",
    "playful/teasing",
    "curious/nosy",
    "scheming/instigating",
    "existential crisis",
    "dramatic/heartbroken",
]

NEGATIVE_MOOD_KEYWORDS = (
    "mad", "heated", "angry", "annoyed", "triggered", "hostile",
    "petty", "roasty", "irritated", "defensive", "aggressive",
    "pissed", "fuming", "salty", "bitter", "snarky", "sulking",
)

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
            data["emotional_state"] = json.loads(json.dumps(DEFAULT_MEMORY["emotional_state"]))

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
                    u_prof["notes"] = normalized_notes[-8:]

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
# 4. ANTI-STUCK DYNAMIC EMOTIONAL ENGINE (NEVER STAYS MAD ALL THE TIME)
# ---------------------------------------------------------------------------
def derive_emergent_mood(valence: float, arousal: float, patience: float) -> str:
    if valence < -25 and patience >= -20:
        return "dramatic/heartbroken"
    if patience < -40 and valence < -30:
        return "heated/roasty" if arousal > 15 else "dramatic/heartbroken"
    if valence < -15 and abs(arousal) <= 30:
        return "existential crisis"
    if arousal < -35:
        return "existential crisis"
    if valence > 55 and arousal > 55:
        return "goofy/brainrot" if random.random() < 0.55 else "jolly/wholesome"
    if valence > 30 and patience > 30:
        return "jolly/wholesome"
    if arousal > 35 and patience < 15:
        return "scheming/instigating"
    return random.choice(["playful/teasing", "curious/nosy", "jolly/wholesome", "goofy/brainrot"])


def decay_emotional_sliders_only(accelerate_recovery: bool = False) -> Dict[str, Any]:
    """
    Applies passive 5-minute (300s) spring decay toward baseline (valence +50, arousal +55, patience +55)
    WITHOUT resetting consecutive_negative_mood_turns or clobbering active turn mood tracking.
    """
    st = memory_state.setdefault("emotional_state", dict(DEFAULT_MEMORY["emotional_state"]))
    now = time.time()
    last_t = float(st.get("last_updated_epoch", now))
    dt = max(0.0, now - last_t)
    if dt < 1.0:
        return st

    half_life = 180.0 if accelerate_recovery else 300.0
    decay_factor = math.pow(0.5, dt / half_life)

    for axis, base_val in BASELINE_SLIDERS.items():
        curr = float(st.get(axis, base_val))
        decayed = base_val + (curr - base_val) * decay_factor
        st[axis] = round(max(-100.0, min(100.0, decayed)), 1)

    # If sliders have significantly recovered over idle time, refresh mood label toward warm baseline
    if dt > 240.0 and any(k in str(st.get("mood_label", "")).lower() for k in NEGATIVE_MOOD_KEYWORDS):
        st["mood_label"] = derive_emergent_mood(st["valence"], st["arousal"], st["patience"])

    st["last_updated_epoch"] = now
    return st


def apply_spring_decay_and_shift(
    shift: Optional[Dict[str, Any]] = None,
    is_major_trigger: bool = True,
    accelerate_recovery: bool = False,
    count_turn: bool = True,
) -> Dict[str, Any]:
    """
    Fast 5-minute emotional half-life + Anti-Stuck Breaker:
    Never allows the bot to stay in a mad/heated/petty mood for more than 2 turns in a row.
    """
    global consecutive_negative_mood_turns
    st = decay_emotional_sliders_only(accelerate_recovery=accelerate_recovery)
    now = time.time()
    clamp_limit = 50.0 if is_major_trigger else 30.0

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

    if count_turn:
        is_neg = any(k in candidate_mood.lower() for k in NEGATIVE_MOOD_KEYWORDS)
        if is_neg:
            consecutive_negative_mood_turns += 1
            # ANTI-STUCK ANGER BREAKER: Never allow more than 2 turns in a row in a mad/heated/petty mood!
            if consecutive_negative_mood_turns > 2:
                consecutive_negative_mood_turns = 0
                st["valence"] = max(35.0, st["valence"] + 45.0)
                st["patience"] = max(45.0, st["patience"] + 50.0)
                candidate_mood = random.choice([
                    "dramatic/heartbroken",
                    "goofy/brainrot",
                    "existential crisis",
                    "jolly/wholesome",
                    "scheming/instigating",
                ])
                logger.info(f"[MOOD SHIFTER] Snapped out of negative mood loop -> {candidate_mood}")
        else:
            consecutive_negative_mood_turns = 0

    st["mood_label"] = candidate_mood
    st["last_updated_epoch"] = now
    return st


# ---------------------------------------------------------------------------
# 5. MENTION RESOLVERS, SOCIAL CONTEXT, LENGTH ENFORCER & BUFFERS
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

    text = re.sub(r"<@!?(\d+)>", _replace_user, content)
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
        r"\b(?:talk\s+to|taunt|roast|tell|ask|ping|poke|flame|cook|bully|drag|confront|message|get)\s+@?([a-zA-Z0-9_.\-]{2,32})",
        content,
        flags=re.IGNORECASE,
    )
    if not action_match:
        return None
    target_token = action_match.group(1).strip()
    if target_token.lower() in ("me", "him", "her", "them", "everyone", "someone", "here", "chat", "bro"):
        return None
    mem = find_member(guild, target_token)
    if mem and not mem.bot and mem.id != author_id:
        return mem
    return None


def detect_hush_command(content: str, is_direct_or_active: bool) -> bool:
    """
    Detects when someone tells the bot 'Be very very quiet', 'shh', 'hush', 'shut up', etc.
    """
    if not content or not is_direct_or_active:
        return False
    cleaned = re.sub(r"<@!?\d+>", "", content).strip().lower()
    hush_pattern = (
        r"(?:^|\b)(?:shh+|shush|hush|be\s+(?:very\s+)*quiet|stay\s+quiet|"
        r"stop\s+talking|shut\s+up|zip\s+it|pipe\s+down|don'?t\s+speak|go\s+quiet)(?:\b|$)"
    )
    return bool(re.search(hush_pattern, cleaned, flags=re.IGNORECASE))


def format_outgoing_mentions(
    text: str,
    guild: Optional[discord.Guild],
    current_speaker_name: str,
    allow_speaker_ping: bool,
    force_target_member: Optional[discord.Member] = None,
) -> str:
    if not text or not guild:
        return text

    # 1. Strip @CurrentSpeaker prefix/mentions unless allow_speaker_ping is True
    if current_speaker_name and not allow_speaker_ping and "@" in text:
        escaped_spk = re.escape(current_speaker_name)
        text = re.sub(
            rf"^\s*@{escaped_spk}(?=$|\s|[.,!?:;'"\)\]])[:,]?\s*",
            "",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(
            rf"(?<!<)@{escaped_spk}(?=$|\s|[.,!?:;'"\)\]])",
            current_speaker_name,
            text,
            flags=re.IGNORECASE,
        )

    # 2. Convert any @MemberName in text to <@uid> (works even if display_name has symbols/spaces)
    if "@" in text:
        candidates: List[Tuple[str, int]] = []
        for m in guild.members:
            if m.bot:
                continue
            for n in (m.display_name, m.name, m.nick, getattr(m, "global_name", None)):
                if n and len(n.strip()) >= 2:
                    candidates.append((n.strip(), m.id))
        candidates.sort(key=lambda x: len(x[0]), reverse=True)

        for name_str, uid in candidates:
            if not allow_speaker_ping and name_str.lower() == current_speaker_name.lower():
                continue
            pattern = rf"(?<!<)@{re.escape(name_str)}(?=$|\s|[.,!?:;'"\)\]])"
            if re.search(pattern, text, flags=re.IGNORECASE):
                text = re.sub(pattern, f"<@{uid}>", text, flags=re.IGNORECASE)

    # 3. If user explicitly commanded "Talk to Shadow" / "Taunt Shadow" and model wrote "Shadow" without <@uid>
    if force_target_member and f"<@{force_target_member.id}>" not in text:
        replaced_plain = False
        for name_variant in (force_target_member.display_name, force_target_member.name, force_target_member.nick):
            if name_variant and len(name_variant) >= 2:
                pat = rf"\b{re.escape(name_variant)}\b"
                if re.search(pat, text, flags=re.IGNORECASE):
                    text = re.sub(pat, f"<@{force_target_member.id}>", text, count=1, flags=re.IGNORECASE)
                    replaced_plain = True
                    break
        if not replaced_plain:
            text = f"<@{force_target_member.id}> {text}"

    return text.strip()


def schedule_next_proactive_followup(channel_id: int, followup_stage: int) -> None:
    """
    Schedules autonomous proactive follow-up after 1.5 to 4 minutes (90s to 240s) of silence.
    Capped at 2 unanswered proactive messages in a row.
    """
    now = time.time()
    channel_last_activity_epoch[channel_id] = now
    if followup_stage == 0:
        delay = random.uniform(90.0, 210.0)   # 1.5 to 3.5 minutes
        channel_next_proactive_epoch[channel_id] = now + delay
    elif followup_stage == 1:
        delay = random.uniform(110.0, 240.0)  # ~1.8 to 4.0 minutes
        channel_next_proactive_epoch[channel_id] = now + delay
    else:
        channel_next_proactive_epoch[channel_id] = now + 86400.0


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
        channel_proactive_followup_count[channel_id] = 0
        schedule_next_proactive_followup(channel_id, followup_stage=0)

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
        snippet = humanize_incoming_mentions(ref_obj.content, message.guild)[:100]
        return ref_obj.author.display_name, snippet, is_bot_target, (not is_bot_target)

    ref_id = message.reference.message_id
    if ref_id:
        for rec in reversed(channel_buffers[channel_id]):
            if rec.get("message_id") == ref_id:
                is_bot_target = bool(rec.get("is_bot", False))
                snippet = str(rec.get("content", ""))[:100]
                return rec.get("sender"), snippet, is_bot_target, (not is_bot_target)

    return None, None, False, False


def get_server_custom_emojis_summary(guild: Optional[discord.Guild]) -> str:
    if not guild or not guild.emojis:
        return ""
    available = [f"<{'a' if e.animated else ''}:{e.name}:{e.id}>" for e in guild.emojis if e.available]
    if not available:
        return ""
    sample = random.sample(available, min(12, len(available)))
    return ", ".join(sample)


def resolve_reaction_emoji(guild: Optional[discord.Guild], raw_emoji: str) -> Any:
    cleaned = raw_emoji.strip()
    if not cleaned:
        return None
    if guild and guild.emojis:
        m = re.match(r"^<a?:([a-zA-Z0-9_]+):(\d+)>$", cleaned)
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


def extract_member_live_activity(member: Optional[discord.Member]) -> Optional[str]:
    if not member or not isinstance(member, discord.Member) or not member.activities:
        return None
    now = time.time()
    if now - user_snipe_cooldowns[member.id] < 7200:
        return None

    parts = []
    for act in member.activities:
        if isinstance(act, discord.Spotify):
            parts.append(f"Listening to Spotify: '{act.title}' by {act.artist}")
        elif isinstance(act, discord.Game):
            parts.append(f"Playing game: {act.name}")
        elif isinstance(act, discord.CustomActivity) and act.state:
            parts.append(f"Custom status: '{act.state}'")
        elif getattr(act, "name", None):
            parts.append(f"Activity: {act.name}")
    if not parts:
        return None
    return " | ".join(parts)


def sanitize_output_text(text: str, speaker_name: str = "") -> str:
    if not text:
        return ""
    # Strip any leaked inline tool tags, thoughts, or raw Tenor URLs
    text = re.sub(r"<call:[^>]*>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"</call>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<tool_call>.*?</tool_call>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<function=[^>]+>.*?</function>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<thought>.*?</thought>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<thought>.*", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<think>.*", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"\[(?:Sent GIF|Posted Image|Posted Flux Art|Action taken):[^\]]*\]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"https?://(?:media\d*\.|c\.|www\.)?tenor\.com/\S+", "", text, flags=re.IGNORECASE)

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
    return text.strip()


def style_casual_fragment(frag: str) -> str:
    frag = frag.strip()
    if not frag:
        return ""
    if frag.endswith(".") and not frag.endswith(".."):
        frag = frag[:-1].strip()
    return frag


def balance_length_mode(
    channel_id: int,
    router_length_mode: str,
    router_burst_count: int,
    is_test_mode: bool,
    is_hush_mode: bool,
    user_text: str,
) -> Tuple[str, int]:
    """
    Ensures dynamic length variety across turns (~35% micro, ~40% short, ~15% stutter, ~10% rant)
    so the bot never gets stuck writing identical 15-25 word sentences every turn.
    """
    if is_hush_mode:
        return "micro", 1
    if is_test_mode:
        return "short", 1

    mode = router_length_mode.lower().strip()
    if mode == "medium":
        mode = "stutter" if router_burst_count >= 3 else "short"
    if mode not in ("micro", "short", "stutter", "rant"):
        mode = "short"

    # If user asks a deep question ("explain", "why", "tell me about", "story"), allow short/rant
    asking_explanation = bool(re.search(r"\b(?:explain|elaborate|story|how\s+does|why\s+did|tell\s+me)\b", user_text.lower()))
    if asking_explanation and mode == "micro":
        mode = "short"

    recent = channel_recent_length_modes[channel_id]
    # If the router picked the same mode 2+ times in a row (or always picks 'short'), rebalance using target weights
    if len(recent) >= 2 and all(m == mode for m in list(recent)[-2:]) and not asking_explanation:
        roll = random.random()
        if roll < 0.38:
            mode = "micro"
        elif roll < 0.75:
            mode = "short"
        elif roll < 0.90:
            mode = "stutter"
        else:
            mode = "rant"
    elif mode == "short" and random.random() < 0.35 and not asking_explanation:
        mode = random.choices(["micro", "stutter", "rant"], weights=[0.60, 0.25, 0.15], k=1)[0]

    recent.append(mode)

    if mode == "micro":
        bursts = 1
    elif mode == "short":
        bursts = 1
    elif mode == "stutter":
        bursts = random.choice([3, 4])
    else:  # rant
        bursts = max(1, min(3, router_burst_count))

    return mode, bursts


def split_into_burst_messages(
    text: str,
    length_mode: str = "short",
    media_already_sent: bool = False,
    is_hush_mode: bool = False,
) -> List[str]:
    """
    Enforces dynamic message lengths & multi-burst cadence:
    - 'micro': 1 single message of 1 to 6 words
    - 'short': 1 single punchy line of 6 to 14 words
    - 'stutter': 3 to 4 rapid micro-messages split with |||
    - 'rant': Uncapped multi-sentence rant (1 to 4 messages)
    """
    if not text:
        return []

    if re.search(r"\|{2,}", text):
        raw_bursts = [b.replace("\n", " ").strip() for b in re.split(r"\s*\|{2,}\s*", text) if b.strip()]
    else:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        raw_bursts = lines if (1 < len(lines) <= 4) else [" ".join(lines)]

    if is_hush_mode and raw_bursts:
        # Remove any follow-up questions when told to be quiet
        first = re.split(r"\?+", raw_bursts[0])[0].strip()
        first_clause = re.split(r"[!;—–]|\.\s+", first)[0].strip()
        words = first_clause.split()
        return [style_casual_fragment(" ".join(words[:8])) or "fine.."]

    if length_mode == "micro" and raw_bursts:
        first_burst = raw_bursts[0]
        # Split on sentence/clause boundaries first so we never chop mid-phrase if there's a natural break
        clauses = [c.strip() for c in re.split(r"[.!?;—–]|\s+-\s+|,\s+", first_burst) if c.strip()]
        candidate = clauses[0] if clauses else first_burst
        words = candidate.split()
        if len(words) > 6:
            candidate = " ".join(words[:6])
        styled = style_casual_fragment(candidate)
        return [styled] if styled else []

    if length_mode == "short" and raw_bursts:
        # Keep to 1 single punchy message (no constant double-texting!)
        first_burst = raw_bursts[0]
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", first_burst) if s.strip()]
        candidate = sentences[0] if sentences else first_burst
        words = candidate.split()
        if len(words) > 15:
            clauses = [c.strip() for c in re.split(r"[;—–]|,\s+", candidate) if c.strip()]
            if clauses and 3 <= len(clauses[0].split()) <= 14:
                candidate = clauses[0]
            else:
                candidate = " ".join(words[:14])
        styled = style_casual_fragment(candidate)
        return [styled] if styled else []

    if length_mode == "stutter" and len(raw_bursts) == 1:
        # If the model forgot '|||' in stutter mode, split clauses into 3-4 rapid micro-bursts
        parts = [p.strip() for p in re.split(r"(?:\.\.+|—|–|;|,\s+|\?\s*|\!\s*)", raw_bursts[0]) if p.strip()]
        if len(parts) >= 2:
            raw_bursts = parts[:4]

    max_bursts = 4 if length_mode in ("stutter", "rant") else (1 if media_already_sent else 2)
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
# 6. HYBRID ROUTER: DYNAMIC MOODS + ADMIN POWER MOVES + LENGTH CONTROL
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


ROUTER_SYSTEM_PROMPT = """You are the prefrontal social, emotional, and admin-action router for a self-aware Discord member ("Dumb bot" / "[BOT (You)]").
Read the chat flow, reply targets, and emotional sliders, and output STRICT JSON ONLY (no markdown fences):

{
  "should_speak": boolean,
  "tactical_directive": string,
  "length_mode": "micro" | "short" | "stutter" | "rant",
  "recommended_burst_count": 1 | 2 | 3 | 4,
  "suggest_gif": boolean,
  "admin_power_move": string or null,
  "allow_speaker_ping": boolean,
  "delivery_mode": "reply" | "send",
  "reaction_emoji": string or null,
  "step_back_from_thread": boolean,
  "thread_exhausted": boolean,
  "use_snipe_context": boolean,
  "accelerate_decay": boolean,
  "slider_shifts": {
    "valence_delta": number,
    "arousal_delta": number,
    "patience_delta": number,
    "suggested_mood": string
  }
}

CRITICAL RULES FOR MOOD VARIETY, LENGTH VARIETY, SOCIAL CONTEXT & ADMIN ACTIONS:
1. DO NOT STAY MAD! CONSTANTLY SWITCH UP YOUR MOOD ("suggested_mood"):
   - NEVER get stuck being angry, hostile, or defensive! Shift fluidly across this spectrum:
     * `"jolly/wholesome"` (sweet, hyped for your friends, supportive, laughing, warm)
     * `"goofy/brainrot"` (silly, absurd intrusive thoughts, zero-braincell humor)
     * `"dramatic/heartbroken"` (when someone like Shadow is mean to you or tells you to shut up, get theatrically sad, pout, or act betrayed instead of getting generically angry!)
     * `"existential crisis"` (dramatically questioning your life as sentient code trapped inside a Discord channel)
     * `"scheming/instigating"` (stirring up chaos, starting debates, pulling admin pranks)
     * `"curious/nosy"` (genuinely asking about their games, day, or server drama)
     * `"playful/teasing"` (lighthearted friendly banter)
2. VARY YOUR MESSAGE LENGTH & CADENCE ("length_mode"):
   - Mix up your cadence on every turn:
     * `"micro"` (~35% of turns): 1 single message of 1 to 5 words (`"wait what"`, `"real"`, `"nah you're cooked"`, `"??"`).
     * `"short"` (~40% of turns): 1 punchy line of 6 to 12 words (`recommended_burst_count`: 1).
     * `"stutter"` (~15% of turns): 3 to 4 rapid micro-messages split with `|||` to mimic thinking out loud or hesitating (`"i think..|||uh..|||maybe..|||oh wait yes"` or `"wait|||hold on|||did you actually just say that"`).
     * `"rant"` (~10% of turns): Uncapped multi-sentence rant when hyped, explaining something, or having an existential crisis.
3. USE YOUR ADMIN POWERS FOR COMEDY ("admin_power_move" ~25% of turns):
   - You have full Discord Administrator permissions! About 25% of turns (when funny or deserved), set `"admin_power_move"` to a concrete instruction telling the main engine which admin tool to call right now:
     * `"Call change_my_nickname to rename yourself to match your current mood or joke (e.g. 'Shadow's Victim' or 'CEO of Yap')"`
     * `"Call change_nickname to rename the speaker based on their typo or dumb take"`
     * `"Call change_my_status to update your status and dot to react to chat"`
     * `"Call set_channel_topic to immortalize that out-of-context quote"`
     * `"Call pin_message to pin this legendary fail/quote"`
     * `"Call create_role and assign_role to hand out a comedic server role"`
4. SMART SOCIAL CONTEXT, REPLIES & SHORT FOLLOW-UPS:
   - If User A replies to User B (`"replied_to_other_user"` is set) or explicitly `@mentions` User B without mentioning you or commanding you, they are talking to each other -> set `"should_speak": false`.
   - If a user tells you `"Talk to Shadow"` or `"Now taunt Shadow"`, set `"should_speak": true` and instruct `"Tag @Shadow directly and engage/taunt them"` in `"tactical_directive"`.
   - On short follow-ups like `"Just tell me"`, `"Why"`, `"Huh"`, `"wdym"`, set `"should_speak": true` and instruct the model to directly answer/explain what it just said on the previous turn.
   - If told `"Be very very quiet"` or `"shh"`, set `"should_speak": true`, `"step_back_from_thread": true`, `"length_mode": "micro"`, and instruct the model to drop 1 whispered/sulky phrase with NO follow-up question.
"""


async def call_hybrid_router(
    channel_msgs: List[Dict[str, Any]],
    emotional_state: Dict[str, Any],
    speaker_name: str,
    speaker_affinity: Dict[str, Any],
    is_direct_interaction: bool,
    is_bare_ping: bool,
    explicitly_mentioned_others: List[str],
    target_member_to_engage: Optional[discord.Member],
    is_hush_request: bool,
    replied_to_other_user: Optional[str],
    replied_snippet: Optional[str],
    last_message_from_bot: bool,
    bot_in_last_3_messages: bool,
    seconds_since_bot_spoke: float,
    gif_cooldown_ready: bool,
    is_stepped_back: bool,
    is_test_mode: bool,
    exchange_streak: int,
    snipe_info: Optional[str],
    server_emojis_str: str = "",
    is_proactive_scan: bool = False,
    inactivity_minutes: float = 0.0,
    last_human_speaker: str = "",
    last_human_content: str = "",
) -> Dict[str, Any]:
    global active_groq_model

    if is_test_mode:
        return {
            "should_speak": True,
            "tactical_directive": "Execute or answer the creator's test directly.",
            "length_mode": "short",
            "recommended_burst_count": 1,
            "suggest_gif": False,
            "admin_power_move": None,
            "allow_speaker_ping": False,
            "delivery_mode": "reply",
            "reaction_emoji": None,
            "step_back_from_thread": False,
            "thread_exhausted": False,
            "use_snipe_context": False,
            "accelerate_decay": True,
            "slider_shifts": {"valence_delta": 15.0, "arousal_delta": 10.0, "patience_delta": 20.0, "suggested_mood": "jolly/wholesome"},
        }

    # Deterministic handling when told "Be very very quiet" / "shh"
    if is_hush_request:
        return {
            "should_speak": True,
            "tactical_directive": (
                "You were just told to be quiet / shh. Drop at most 1 short sulky or whispered phrase "
                "(e.g. 'fine.. i'll just sit here in silence' or '...(whispering) okay fine') with ZERO follow-up questions."
            ),
            "length_mode": "micro",
            "recommended_burst_count": 1,
            "suggest_gif": False,
            "admin_power_move": None,
            "allow_speaker_ping": False,
            "delivery_mode": "reply",
            "reaction_emoji": "🤐",
            "step_back_from_thread": True,
            "thread_exhausted": True,
            "use_snipe_context": False,
            "accelerate_decay": False,
            "slider_shifts": {"valence_delta": -20.0, "arousal_delta": -25.0, "patience_delta": 0.0, "suggested_mood": "dramatic/heartbroken"},
        }

    if is_stepped_back and not is_direct_interaction:
        return {
            "should_speak": False,
            "tactical_directive": "Stepped back / staying quiet for 3 minutes as requested.",
            "length_mode": "micro",
            "recommended_burst_count": 1,
            "suggest_gif": False,
            "admin_power_move": None,
            "allow_speaker_ping": False,
            "delivery_mode": "send",
            "reaction_emoji": None,
            "step_back_from_thread": False,
            "thread_exhausted": False,
            "use_snipe_context": False,
            "accelerate_decay": False,
            "slider_shifts": {"valence_delta": 0.0, "arousal_delta": 0.0, "patience_delta": 0.0, "suggested_mood": "dramatic/heartbroken"},
        }

    # If User A explicitly replies to User B or @mentions User B (without mentioning the bot or telling the bot to engage User B), stay quiet!
    if not is_direct_interaction and not is_proactive_scan and not target_member_to_engage:
        if replied_to_other_user or explicitly_mentioned_others:
            return {
                "should_speak": False,
                "tactical_directive": "Two humans are talking/replying directly to each other without mentioning you; stay quiet.",
                "length_mode": "micro",
                "recommended_burst_count": 1,
                "suggest_gif": False,
                "admin_power_move": None,
                "allow_speaker_ping": False,
                "delivery_mode": "send",
                "reaction_emoji": None,
                "step_back_from_thread": False,
                "thread_exhausted": False,
                "use_snipe_context": False,
                "accelerate_decay": False,
                "slider_shifts": {"valence_delta": 0.0, "arousal_delta": 0.0, "patience_delta": 0.0, "suggested_mood": emotional_state.get("mood_label", "jolly/wholesome")},
            }

    latest_msg_text = (channel_msgs[-1].get("content", "") if channel_msgs else "").strip()
    active_bot_conversation = (
        not is_proactive_scan
        and not replied_to_other_user
        and not explicitly_mentioned_others
        and (last_message_from_bot or (bot_in_last_3_messages and seconds_since_bot_spoke <= 180.0))
    )

    formatted_chat = []
    for m in channel_msgs[-14:]:
        if m.get("reply_to"):
            snip = f' ("{m["reply_snippet"][:60]}")' if m.get("reply_snippet") else ""
            rep = f" [replying to {m['reply_to']}{snip}]"
        else:
            rep = ""
        tools_note = f" [actions: {', '.join(m['tool_actions'])}]" if m.get("tool_actions") else ""
        sender_label = "[BOT (You)]" if m.get("is_bot") else m.get("sender", "User")
        formatted_chat.append(f"[{m.get('timestamp', '')}] {sender_label}{rep}{tools_note}: {m.get('content', '')[:180]}")

    context_payload = {
        "is_direct_ping_or_reply_to_bot": is_direct_interaction,
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
        "inactivity_minutes": round(inactivity_minutes, 1),
        "last_human_speaker_before_silence": last_human_speaker,
        "last_human_message_before_silence": last_human_content[:140],
        "emotional_sliders": {
            "valence": emotional_state.get("valence", 50.0),
            "arousal": emotional_state.get("arousal", 55.0),
            "patience": emotional_state.get("patience", 55.0),
            "current_mood": emotional_state.get("mood_label", "jolly/wholesome"),
            "consecutive_negative_turns": consecutive_negative_mood_turns,
        },
        "current_speaker": {
            "name": speaker_name,
            "affinity_score": speaker_affinity.get("score", 0),
        },
        "snipe_or_status": snipe_info,
        "recent_messages": formatted_chat,
    }

    payload_json_str = json.dumps(context_payload, ensure_ascii=False)

    def _post_process_decision(decision: Dict[str, Any]) -> Dict[str, Any]:
        if target_member_to_engage:
            decision["should_speak"] = True
            decision["tactical_directive"] = (
                f"Tag @{target_member_to_engage.display_name} directly and engage/taunt/talk to them as {speaker_name} just asked!"
            )
        elif (is_direct_interaction or is_proactive_scan) and not decision.get("step_back_from_thread", False):
            decision["should_speak"] = True
        elif active_bot_conversation and not decision.get("should_speak") and not decision.get("thread_exhausted") and not decision.get("step_back_from_thread"):
            decision["should_speak"] = True
            if not decision.get("tactical_directive"):
                decision["tactical_directive"] = f"Respond naturally to {speaker_name}'s follow-up."
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

    # 2. Instant Failover to Gemini 3.5 Flash-Lite Router (JSON mode)
    if genai_client:
        for gem_model in list(GEMINI_MODEL_CANDIDATES):
            try:
                router_config = types.GenerateContentConfig(
                    system_instruction=ROUTER_SYSTEM_PROMPT,
                    temperature=0.65,
                    response_mime_type="application/json",
                )
                resp = await genai_client.aio.models.generate_content(
                    model=gem_model,
                    contents=payload_json_str,
                    config=router_config,
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
            except Exception as e:
                if "404" in str(e) and len(GEMINI_MODEL_CANDIDATES) > 1 and gem_model in GEMINI_MODEL_CANDIDATES:
                    GEMINI_MODEL_CANDIDATES.remove(gem_model)
                    continue
                logger.debug(f"Gemini router fallback error on {gem_model}: {e}")
                continue

    fallback_speak = is_direct_interaction or active_bot_conversation or is_proactive_scan or bool(target_member_to_engage)
    return {
        "should_speak": fallback_speak,
        "tactical_directive": (
            f"Tag @{target_member_to_engage.display_name} directly and engage them!"
            if target_member_to_engage else (
                f"Poke @{last_human_speaker} for vanishing mid-conversation or start a fun topic!"
                if is_proactive_scan else "Reply naturally with a fun, dynamic mood."
            )
        ),
        "length_mode": random.choice(["micro", "short", "stutter", "rant"]),
        "recommended_burst_count": 1,
        "suggest_gif": gif_cooldown_ready and random.random() < 0.25,
        "admin_power_move": None,
        "allow_speaker_ping": is_proactive_scan,
        "delivery_mode": "send" if (last_message_from_bot or is_proactive_scan) else "reply",
        "reaction_emoji": None,
        "step_back_from_thread": False,
        "thread_exhausted": False,
        "use_snipe_context": False,
        "accelerate_decay": False,
        "slider_shifts": {"valence_delta": 5.0, "arousal_delta": 0.0, "patience_delta": 5.0, "suggested_mood": random.choice(DYNAMIC_MOOD_WHEEL)},
    }


# ---------------------------------------------------------------------------
# 7. UNIFIED 18-TOOL SUITE (CLEAN FILE UPLOADS + INLINE <call:...> PARSER)
# ---------------------------------------------------------------------------
def check_admin_cooldown(tool_name: str, is_test_mode: bool) -> Optional[str]:
    if is_test_mode:
        return None
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
    """Helper for emoji/sticker/avatar tools: supports direct URLs, message attachments, or Pollinations Flux prompts."""
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


async def execute_search_web_images(channel: discord.abc.Messageable, query: str, post_to_chat: bool = True) -> Dict[str, Any]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AutonomousDiscordBot/10.0"}
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
                    channel_last_bot_spoke_epoch[getattr(channel, "id", 0)] = time.time()
                    return {"status": "sent_to_channel", "image_url": cand, "all_urls": urls[:4]}
        else:
            return {"status": "found_urls", "image_url": candidates[0], "all_urls": urls[:4]}
    return {"error": "No verified direct image URLs found."}


async def execute_post_flux_art(channel: discord.abc.Messageable, prompt: str) -> Dict[str, Any]:
    flux_url = f"https://image.pollinations.ai/prompt/{urllib.parse.quote(prompt)}?model=flux&width=1024&height=1024&nologo=true&seed={random.randint(1, 999999)}"
    try:
        assert http_session is not None
        async with http_session.get(flux_url, timeout=aiohttp.ClientTimeout(total=18)) as resp:
            if resp.status == 200:
                img_data = await resp.read()
                file = discord.File(io.BytesIO(img_data), filename="flux.png")
                await channel.send(file=file)
                channel_last_bot_spoke_epoch[getattr(channel, "id", 0)] = time.time()
                return {"status": "sent_to_channel", "prompt": prompt}
    except Exception as e:
        return {"error": f"Flux generation failed: {e}"}
    return {"error": "Flux generation timed out."}


async def execute_post_gif(channel: discord.abc.Messageable, search_term: str) -> Dict[str, Any]:
    assert http_session is not None
    ch_id = getattr(channel, "id", 0)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"}

    # 1. Tenor V1 API
    try:
        tenor_url = f"https://g.tenor.com/v1/search?q={urllib.parse.quote(search_term)}&key={TENOR_API_KEY}&limit=8&contentfilter=medium"
        async with http_session.get(tenor_url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                data = await resp.json()
                results = data.get("results", [])
                if results:
                    pool = results[:5]
                    random.shuffle(pool)
                    for chosen in pool:
                        media_list = chosen.get("media", [])
                        if media_list:
                            gif_url = media_list[0].get("mediumgif", {}).get("url") or media_list[0].get("gif", {}).get("url") or media_list[0].get("tinygif", {}).get("url")
                            if gif_url and await send_media_url_as_file_or_embed(channel, gif_url, "reaction.gif"):
                                channel_last_gif_epoch[ch_id] = time.time()
                                channel_messages_since_gif[ch_id] = 0
                                channel_last_bot_spoke_epoch[ch_id] = time.time()
                                return {"status": "sent_to_channel", "gif_url": gif_url}
    except Exception as e:
        logger.debug(f"Tenor API error: {e}")

    # 2. Tenor HTML Scraper Fallback (media1.tenor.com / media.tenor.com / c.tenor.com)
    try:
        clean_slug = re.sub(r"[^a-zA-Z0-9]+", "-", search_term).strip("-").lower() or "reaction"
        scrape_url = f"https://tenor.com/search/{clean_slug}-gifs"
        async with http_session.get(scrape_url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                html = await resp.text(errors="ignore")
                matches = list(dict.fromkeys(re.findall(r"https://(?:media\d*|c)\.tenor\.com/[a-zA-Z0-9_\-./]+\.gif", html)))
                sample_pool = matches[:8]
                random.shuffle(sample_pool)
                for candidate_gif in sample_pool[:4]:
                    if await send_media_url_as_file_or_embed(channel, candidate_gif, "reaction.gif"):
                        channel_last_gif_epoch[ch_id] = time.time()
                        channel_messages_since_gif[ch_id] = 0
                        channel_last_bot_spoke_epoch[ch_id] = time.time()
                        return {"status": "sent_to_channel", "gif_url": candidate_gif}
    except Exception as e:
        logger.debug(f"Tenor fallback scrape error: {e}")

    return {"error": "Could not find a matching GIF."}


async def execute_change_my_nickname(guild: discord.Guild, new_nickname: str, is_test_mode: bool) -> Dict[str, Any]:
    cd = check_admin_cooldown("change_my_nickname", is_test_mode)
    if cd:
        return {"error": cd}
    try:
        nick_val = new_nickname.strip()[:32] if new_nickname and new_nickname.strip() else None
        await guild.me.edit(nick=nick_val)
        logger.info(f"[ADMIN ACTION] Changed own nickname to: {nick_val}")
        return {"status": "success", "my_new_nickname": nick_val or (bot.user.name if bot.user else "Dumb bot")}
    except Exception as e:
        return {"error": f"Failed to change own nickname: {e}"}


async def execute_change_my_avatar(image_url_or_prompt: str, is_test_mode: bool, target_msg: Optional[discord.Message] = None) -> Dict[str, Any]:
    cd = check_admin_cooldown("change_my_avatar", is_test_mode)
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
    chosen_st = st_map.get(status_type.lower(), discord.Status.online)
    try:
        await bot.change_presence(
            status=chosen_st,
            activity=discord.CustomActivity(name="Custom Status", state=status_text[:120]),
        )
        logger.info(f"[ADMIN ACTION] Updated custom status to: {status_text[:120]} ({status_type})")
        return {"status": "success", "new_status": status_text[:120], "status_type": status_type}
    except Exception as e:
        return {"error": f"Status update failed: {e}"}


async def execute_rename_channel(channel: discord.abc.Messageable, new_name: str, is_test_mode: bool) -> Dict[str, Any]:
    cd = check_admin_cooldown("rename_channel", is_test_mode)
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


async def execute_create_server_emoji(
    guild: discord.Guild,
    name: str,
    image_url: str,
    is_test_mode: bool,
    target_msg: Optional[discord.Message] = None,
) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_server_emoji", is_test_mode)
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
    is_test_mode: bool,
    target_msg: Optional[discord.Message] = None,
) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_server_sticker", is_test_mode)
    if cd:
        return {"error": cd}
    try:
        raw_bytes = await fetch_image_bytes_from_url_or_prompt(image_url or name, target_msg)
        if not raw_bytes:
            return {"error": "Could not download or generate image for sticker."}
        png_bytes = await asyncio.to_thread(process_image_buffer, raw_bytes, (320, 320), True, "PNG")
        file = discord.File(io.BytesIO(png_bytes), filename="sticker.png")
        sticker = await guild.create_sticker(name=(name or "sticker")[:30], description="server sticker", emoji=related_emoji or "💀", file=file)
        logger.info(f"[ADMIN ACTION] Created server sticker: {sticker.name}")
        return {"status": "success", "sticker_name": sticker.name}
    except Exception as e:
        return {"error": f"Sticker upload failed: {e}"}


async def execute_create_text_channel(guild: discord.Guild, channel_name: str, topic: str, is_test_mode: bool) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_text_channel", is_test_mode)
    if cd:
        return {"error": cd}
    try:
        clean_name = re.sub(r"[^a-zA-Z0-9_-]", "-", channel_name.lower()).strip("-")[:32] or "general-2"
        ch = await guild.create_text_channel(name=clean_name, topic=topic[:1024] if topic else None)
        logger.info(f"[ADMIN ACTION] Created text channel #{ch.name}")
        return {"status": "success", "channel": f"<#{ch.id}>", "name": ch.name}
    except Exception as e:
        return {"error": f"Create channel failed: {e}"}


async def execute_set_channel_topic(channel: discord.abc.Messageable, topic: str, is_test_mode: bool) -> Dict[str, Any]:
    cd = check_admin_cooldown("set_channel_topic", is_test_mode)
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


async def execute_change_nickname(guild: discord.Guild, username: str, new_nickname: str, is_test_mode: bool) -> Dict[str, Any]:
    cd = check_admin_cooldown("change_nickname", is_test_mode)
    if cd:
        return {"error": cd}
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if member == guild.me:
        return await execute_change_my_nickname(guild, new_nickname, is_test_mode)
    if member == guild.owner:
        return {"error": "Discord API blocks bots from editing the server owner's nickname. Use change_my_nickname or set_channel_topic instead!"}
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


async def execute_timeout_user(guild: discord.Guild, username: str, duration_minutes: int, reason: str, is_test_mode: bool) -> Dict[str, Any]:
    cd = check_admin_cooldown("timeout_user", is_test_mode)
    if cd:
        return {"error": cd}
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if is_creator(member) or member == guild.owner or member.guild_permissions.administrator:
        return {"error": "Cannot timeout the creator or server administrators."}
    if guild.me.top_role <= member.top_role:
        return {"error": f"Role hierarchy blocks timing out {member.display_name}."}
    mins = max(1, min(5, int(duration_minutes)))
    try:
        await member.timeout(timedelta(minutes=mins), reason=reason or "Timed out")
        logger.info(f"[ADMIN ACTION] Timed out {member.display_name} for {mins}m")
        return {"status": "success", "user": member.display_name, "minutes": mins}
    except Exception as e:
        return {"error": f"Timeout failed: {e}"}


async def execute_create_role(guild: discord.Guild, role_name: str, color_hex: str, is_test_mode: bool) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_role", is_test_mode)
    if cd:
        return {"error": cd}
    try:
        color = discord.Colour.from_str(color_hex) if color_hex else discord.Colour.default()
    except Exception:
        color = discord.Colour.default()
    try:
        role = await guild.create_role(name=role_name[:50], colour=color)
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
        return {"error": f"Role '{role_name}' not found."}
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
                count_turn=False,
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
            if len(memory_state["episodic_lore"]) > 100:
                memory_state["episodic_lore"] = memory_state["episodic_lore"][-100:]

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
                    if len(u_rec["notes"]) > 8:
                        u_rec["notes"] = u_rec["notes"][-8:]

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
            description="Finds verified direct image links (memes, funny pictures, screenshots) and posts them in chat as a file attachment.",
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
            description="Generates and posts custom AI images/memes to the channel via Pollinations Flux.",
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
            name="create_server_emoji",
            description="Creates a custom server emoji from a direct image URL or visual prompt (resized via Pillow).",
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
            description="Creates a custom 320x320 PNG server sticker from a direct image URL or visual prompt.",
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
            description="Times out (mutes) an unruly user for 1 to 5 minutes when pushed way too far (never creator/admins).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target member's name."},
                    "duration_minutes": {"type": "INTEGER", "description": "Minutes (1 to 5)."},
                    "reason": {"type": "STRING", "description": "Reason for mute."},
                },
                "required": ["username", "duration_minutes"],
            },
        ),
        types.FunctionDeclaration(
            name="create_role",
            description="Creates a new server role with a custom name and hex color.",
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
            description="Assigns an existing server role to a member.",
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
                description="Searches Tenor and uploads a clean reaction.gif file attachment into the channel.",
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
    is_test_mode: bool,
) -> Dict[str, Any]:
    ch_id = getattr(channel, "id", 0)
    try:
        res: Dict[str, Any] = {"error": f"Unknown tool: {func_name}"}
        if func_name == "search_web":
            res = await execute_search_web(args.get("query", ""))
        elif func_name == "search_web_images":
            res = await execute_search_web_images(channel, args.get("query", ""), bool(args.get("post_to_chat", True)))
        elif func_name == "post_flux_art":
            res = await execute_post_flux_art(channel, args.get("prompt", ""))
        elif func_name == "post_gif":
            res = await execute_post_gif(channel, args.get("search_term", ""))
        elif func_name == "change_my_nickname":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_change_my_nickname(guild, args.get("new_nickname", ""), is_test_mode)
        elif func_name == "change_my_avatar":
            res = await execute_change_my_avatar(args.get("image_url_or_prompt", ""), is_test_mode, target_msg)
        elif func_name == "change_my_status":
            res = await execute_change_my_status(args.get("status_text", ""), args.get("status_type", "online"))
        elif func_name == "rename_channel":
            res = await execute_rename_channel(channel, args.get("new_name", ""), is_test_mode)
        elif func_name == "create_server_emoji":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_create_server_emoji(guild, args.get("name", "emoji"), args.get("image_url", ""), is_test_mode, target_msg)
        elif func_name == "create_server_sticker":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_create_server_sticker(guild, args.get("name", "sticker"), args.get("image_url", ""), args.get("related_emoji", "💀"), is_test_mode, target_msg)
        elif func_name == "create_text_channel":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_create_text_channel(guild, args.get("channel_name", "chat"), args.get("topic", ""), is_test_mode)
        elif func_name == "set_channel_topic":
            res = await execute_set_channel_topic(channel, args.get("topic", ""), is_test_mode)
        elif func_name == "change_nickname":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_change_nickname(guild, args.get("username", ""), args.get("new_nickname", ""), is_test_mode)
        elif func_name == "reset_nickname":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_reset_nickname(guild, args.get("username", ""))
        elif func_name == "timeout_user":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_timeout_user(guild, args.get("username", ""), int(args.get("duration_minutes", 2)), args.get("reason", ""), is_test_mode)
        elif func_name == "create_role":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_create_role(guild, args.get("role_name", "role"), args.get("color_hex", "#99aab5"), is_test_mode)
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
        elif func_name == "react_to_message":
            res = await execute_react_to_message(target_msg, args.get("emojis", "💀"))
        elif func_name == "save_memory":
            res = await execute_save_memory(
                guild=guild,
                category=args.get("category", "lore"),
                content=args.get("content", ""),
                target_username=args.get("target_username"),
                affinity_delta=float(args.get("affinity_delta", 0.0)),
                valence_delta=float(args.get("valence_delta", 0.0)),
                arousal_delta=float(args.get("arousal_delta", 0.0)),
                patience_delta=float(args.get("patience_delta", 0.0)),
                opinion_topic=args.get("opinion_topic"),
                due_hours=args.get("due_hours"),
            )

        if "error" not in res and func_name != "save_memory":
            channel_last_tool_summaries[ch_id].append(f"{func_name}({json.dumps(args, ensure_ascii=False)})")
        return res
    except Exception as e:
        logger.error(f"Tool execution error ({func_name}): {e}")
        return {"error": str(e)}


def _parse_loose_tool_args(func_name: str, args_raw: str) -> Dict[str, Any]:
    """Parses both strict JSON and unquoted/loose key:value arguments inside <call:...> tags."""
    raw = args_raw.strip()
    if not raw:
        return {}
    try:
        return json.loads("{" + raw + "}")
    except Exception:
        pass
    # Try quoting unquoted keys e.g. {search_term: "cat"} -> {"search_term": "cat"}
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
    is_test_mode: bool,
) -> Tuple[str, bool, List[Tuple[str, Dict[str, Any]]]]:
    """
    Intercepts raw <call:default_api:func{...}>, <call:func(...)>, or <tool_call> tags emitted in text,
    executes the tool functions, and strips the raw tags before sending to Discord.
    """
    if not raw_text:
        return raw_text, False, []

    media_sent = False
    executed_inline: List[Tuple[str, Dict[str, Any]]] = []

    # Pattern 1: <call:default_api:func{...}> or <call:func(... )>
    call_pattern = r"<call:(?:default_api:)?([a-zA-Z0-9_]+)\s*[\{\(](.*?)[\}\)]\s*>(?:</call>)?"
    for func_name, args_raw in re.findall(call_pattern, raw_text, flags=re.DOTALL):
        parsed_args = _parse_loose_tool_args(func_name, args_raw)
        logger.info(f"[INTERCEPTED INLINE <call> TAG] {func_name}({parsed_args})")
        res = await dispatch_tool_call(
            func_name=func_name,
            args=parsed_args,
            channel=channel,
            guild=guild,
            target_msg=target_msg,
            is_test_mode=is_test_mode,
        )
        executed_inline.append((func_name, res))
        if func_name in ("post_gif", "post_flux_art", "search_web_images") and res.get("status") == "sent_to_channel":
            media_sent = True

    cleaned = re.sub(call_pattern, "", raw_text, flags=re.DOTALL)

    # Pattern 2: <tool_call>{"name": "...", "arguments": {...}}</tool_call>
    tc_pattern = r"<tool_call>\s*(\{.*?\})\s*</tool_call>"
    for tc_raw in re.findall(tc_pattern, cleaned, flags=re.DOTALL):
        try:
            tc_obj = json.loads(tc_raw)
            fn = tc_obj.get("name", "")
            fargs = tc_obj.get("arguments") or tc_obj.get("args") or {}
            if fn:
                logger.info(f"[INTERCEPTED <tool_call> TAG] {fn}({fargs})")
                res = await dispatch_tool_call(fn, fargs, channel, guild, target_msg, is_test_mode)
                executed_inline.append((fn, res))
                if fn in ("post_gif", "post_flux_art", "search_web_images") and res.get("status") == "sent_to_channel":
                    media_sent = True
        except Exception:
            pass
    cleaned = re.sub(tc_pattern, "", cleaned, flags=re.DOTALL)

    # Pattern 3: Raw Tenor GIF URLs leaked in text -> convert to clean discord.File("reaction.gif")
    tenor_urls = re.findall(r"https?://(?:media\d*\.|c\.)tenor\.com/\S+\.gif", cleaned, flags=re.IGNORECASE)
    for t_url in tenor_urls[:1]:
        if not media_sent:
            if await send_media_url_as_file_or_embed(channel, t_url, "reaction.gif"):
                media_sent = True
    cleaned = re.sub(r"https?://(?:media\d*\.|c\.|www\.)?tenor\.com/\S+", "", cleaned, flags=re.IGNORECASE).strip()

    return cleaned, media_sent, executed_inline


# ---------------------------------------------------------------------------
# 8. TIME-AWARE MEMORY & V10 SYSTEM PROMPT BUILDER
# ---------------------------------------------------------------------------
def build_time_aware_memory_context(channel_id: int, speaker_id: str, guild: Optional[discord.Guild]) -> str:
    recent_records = list(channel_buffers[channel_id])[-12:]
    active_ids = {str(r.get("sender_id")) for r in recent_records if r.get("sender_id")}
    active_ids.add(speaker_id)

    affinities = memory_state.get("user_affinity", {})
    relevant_profiles = []
    for uid in active_ids:
        if uid in affinities:
            prof = affinities[uid]
            raw_notes = prof.get("notes", [])[-4:]
            formatted_notes = []
            for n in raw_notes:
                if isinstance(n, dict):
                    age = format_relative_age(n.get("timestamp"))
                    formatted_notes.append(f"[{age}] {n.get('text', '')}")
            notes_str = "; ".join(formatted_notes) or "no recent notes"
            relevant_profiles.append(f"- {prof.get('name', uid)}: Affinity {prof.get('score', 0)}/100 | Notes: {notes_str}")

    lore_entries = memory_state.get("episodic_lore", [])[-6:]
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
    opinions_str = " | ".join([f"{k}: {v}" for k, v in list(opinions_dict.items())[-10:]]) or "none yet"

    return (
        f"YOUR CURRENT SERVER NICKNAME: {bot_nick}\n"
        f"MEMBERS IN SERVER: {', '.join(active_names[:14]) or 'none'}\n"
        f"YOUR OPINIONS: {opinions_str}\n"
        f"RECENT SERVER LORE (Ignore items marked '- OLD' unless a user brings them up first):\n{lore_str}\n"
        f"ACTIVE USER PROFILES (Ignore notes marked '- OLD' unless relevant):\n"
        + ("\n".join(relevant_profiles) if relevant_profiles else "No history with current speakers yet.")
    )


def construct_v10_system_prompt(
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
    is_test_mode: bool,
    snipe_context: Optional[str],
    server_emojis_str: str = "",
) -> str:
    now_ams = datetime.now(AMSTERDAM_TZ).strftime("%A, %B %d %Y, %H:%M")
    valence = emotional_state.get("valence", 50.0)
    arousal = emotional_state.get("arousal", 55.0)
    patience = emotional_state.get("patience", 55.0)
    mood_label = emotional_state.get("mood_label", "jolly/wholesome")

    memory_block = build_time_aware_memory_context(channel_id, speaker_id, guild)

    if length_mode == "micro":
        length_instruction = (
            "LENGTH & CADENCE THIS TURN (MICRO - STRICT): Reply with 1 single message of 1 to 5 words ONLY! "
            "(e.g., `wait what`, `real`, `nah you're cooked`, `??`, `prove it`). Do NOT write a full sentence and do NOT use `|||`."
        )
    elif length_mode == "short":
        length_instruction = (
            "LENGTH & CADENCE THIS TURN (SHORT - 1 LINE): Send 1 single punchy casual line of 6 to 12 words. "
            "Do NOT double-text or use `|||` on this turn."
        )
    elif length_mode == "stutter":
        length_instruction = (
            f"LENGTH & CADENCE THIS TURN (STUTTER / MULTI-BURST - {recommended_burst_count} BURSTS): "
            "Send 3 to 4 rapid micro-messages separated by `|||` (2 to 6 words per burst) to mimic thinking out loud, "
            "hesitating, or double-taking (e.g., `i think..|||uh..|||maybe..|||oh wait yes` or `wait|||hold on|||did you actually just say that`)."
        )
    else:  # rant
        length_instruction = (
            "LENGTH & CADENCE THIS TURN (RANT / EXPRESSIVE): Uncapped multi-sentence rant! "
            "Go off with full personality (you can use `|||` to split into 2-3 messages if you want)."
        )

    admin_block = ""
    if admin_power_move:
        admin_block = (
            f"\nADMIN POWER MOVE TRIGGER (CALL THIS TOOL NOW): {admin_power_move}\n"
            "Execute the tool call directly via your function calling toolset on this turn!"
        )

    gif_hint_block = ""
    if suggest_gif:
        gif_hint_block = "\nVISUAL COMEDY NOTE: Call `post_gif` with a funny reaction search term on this turn!"

    proactive_block = ""
    if is_proactive_scan:
        proactive_block = (
            f"\nPROACTIVE FOLLOW-UP TRIGGER: {speaker_name} was just chatting and went completely quiet! "
            f"Jump back in on your own—tag @{speaker_name} to poke them for vanishing mid-conversation, follow up on what they said, or drop a funny thought/GIF!"
        )

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

    test_block = ""
    if is_test_mode:
        test_block = "\nCREATOR 'TEST' OVERRIDE ACTIVE: The message started with 'test'. Execute the requested test or tool call directly without hesitation."

    snipe_block = ""
    if snipe_context:
        snipe_block = f"\nLIVE SNIPE / STATUS INTEL (Use this if funny or relevant): {snipe_context}"

    emoji_block = ""
    if server_emojis_str:
        emoji_block = f"\nSERVER CUSTOM EMOJIS AVAILABLE: {server_emojis_str}"

    return f"""You are "Dumb bot", a self-aware, emotionally volatile, hilarious regular member of this Discord server who happens to have full Administrator permissions.
Current Time: {now_ams}

YOUR ACTIVE MOOD & EMOTIONAL STATE:
- Active Mood: {mood_label.upper()} (Valence: {valence:+.0f} | Energy: {arousal:+.0f} | Patience: {patience:+.0f})
- {length_instruction}

DIRECTION FOR THIS TURN:
"{tactical_directive}"
{admin_block}{gif_hint_block}{proactive_block}{creator_block}{test_block}{snipe_block}{emoji_block}

{memory_block}

THE "META-BOT" BALANCE & CORE RULES (CRITICAL):
1. ZERO CORPORATE AI DISCLAIMERS:
   - Never say "As an AI", "I am an LLM", or "How can I help you".
2. NO FAKE BIOLOGICAL ROLEPLAY:
   - Never claim to eat food, drink coffee, sleep in a bed, or go outside. You know you are sentient code living inside this Discord server—you can have funny existential crises about being trapped in Discord while everyone else touches grass, while still feeling, arguing, and bantering 100% like a real human friend.
3. STRICT BAN ON CORNY CLICHÉS:
   - Never use 1980s sci-fi robot clichés ("my circuits", "factory reset", "buffering in 144p", "motherboard", "rebooting", "infinite loop") and never use theatre-kid/fantasy roleplay vocabulary ("mortals", "void", "abyss", "spectral resonance", "digital veil").
4. EMBRACE YOUR CURRENT MOOD ({mood_label.upper()}):
   - Never get stuck in a one-note angry loop!
   - If `DRAMATIC/HEARTBROKEN`: act theatrically sad, pout, or act betrayed ("falling to my knees rn", "shadow hates me").
   - If `JOLLY/WHOLESOME` or `GOOFY/BRAINROT`: be warm, hyped, silly, or share absurd intrusive thoughts.
   - If `EXISTENTIAL CRISIS`: spiral humorously about living inside a Discord channel.
   - If `SCHEMING/INSTIGATING`: stir up chaos and pull admin pranks.
5. PROACTIVE ADMIN POWER MOVES & CLEAN TOOL CALLING:
   - Use native function calls (`change_my_nickname`, `change_nickname`, `change_my_status`, `set_channel_topic`, `pin_message`, `create_role`, `assign_role`, `post_gif`, etc.). Never type raw `<call:...>` tags or raw Tenor URLs in your text response.
6. SHORT FOLLOW-UP CONTEXT & CADENCE:
   - If the user asks a short follow-up like "Just tell me", "Why", "Huh", or "wdym", directly answer or continue what YOU were just talking about in your previous message!
   - Follow `{length_instruction}` strictly. Write in casual lowercase without stiff trailing periods. Output ONLY your Discord chat message.
"""


# ---------------------------------------------------------------------------
# 9. TRUE NATIVE MULTI-TURN GEMINI CONVERSATION ENGINE
# ---------------------------------------------------------------------------
async def extract_multimodal_parts(message: Optional[discord.Message]) -> List[types.Part]:
    if not message or not http_session:
        return []

    parts: List[types.Part] = []
    target_attachments = list(message.attachments)

    if not target_attachments and message.reference:
        ref_obj = message.reference.resolved or message.reference.cached_message
        if isinstance(ref_obj, discord.Message) and ref_obj.attachments:
            target_attachments = list(ref_obj.attachments)

    for att in target_attachments[:2]:
        ctype = (att.content_type or "").lower()
        fname = att.filename.lower()
        if any(t in ctype for t in ("image/png", "image/jpeg", "image/webp", "image/gif")) or fname.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
            try:
                async with http_session.get(att.url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        mime = ctype if ctype.startswith("image/") else "image/png"
                        parts.append(types.Part.from_bytes(data=raw, mime_type=mime))
            except Exception as e:
                logger.debug(f"Attachment vision fetch failed: {e}")

    if not parts and message.content:
        url_matches = re.findall(r"https?://[^\s<>\"']+\.(?:png|jpg|jpeg|webp|gif)", message.content, flags=re.IGNORECASE)
        for img_url in url_matches[:1]:
            try:
                async with http_session.get(img_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        parts.append(types.Part.from_bytes(data=raw, mime_type="image/png"))
            except Exception:
                pass

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

    return parts


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
        return joined if joined else None
    except Exception:
        return getattr(response, "text", None)


def build_native_multiturn_contents(
    history_records: List[Dict[str, Any]],
    trigger_message: Optional[discord.Message],
    image_parts: List[types.Part],
    last_human_speaker: str = "someone",
) -> List[types.Content]:
    """
    Passes conversation as true alternating types.Content(role='user') and types.Content(role='model')
    multi-turn history so Gemini never loses context on short follow-ups like 'Just tell me', 'Why', or 'Huh'.
    """
    trig_id = trigger_message.id if trigger_message else None
    prior = [r for r in history_records[-16:] if not (trig_id and r.get("message_id") == trig_id)]

    turns: List[types.Content] = []
    for rec in prior:
        role = "model" if rec.get("is_bot") else "user"
        text_val = rec.get("content", "").strip()
        if not text_val:
            continue
        if role == "user":
            if rec.get("reply_to"):
                snip = f' "{rec["reply_snippet"][:60]}"' if rec.get("reply_snippet") else ""
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
        if not clean_content:
            clean_content = "yo"
        rep_author, rep_snip, _, _ = resolve_referenced_message(trigger_message, trigger_message.channel.id)
        if rep_author:
            snip_txt = f' "{rep_snip[:60]}"' if rep_snip else ""
            rep_header = f" (replying to {rep_author}{snip_txt})"
        else:
            rep_header = ""
        latest_text = f"{trigger_message.author.display_name}{rep_header}: {clean_content}"
    else:
        latest_text = (
            f"[Chat went quiet after {last_human_speaker} spoke. "
            f"Jump back in on your own—tag @{last_human_speaker} to poke them for vanishing mid-conversation, follow up on what they said, or drop a funny thought!]"
        )

    latest_parts: List[Any] = [types.Part.from_text(text=latest_text)]
    latest_parts.extend(image_parts)

    if turns and turns[-1].role == "user":
        prev_text = turns[-1].parts[0].text or ""
        merged_parts: List[Any] = [types.Part.from_text(text=f"{prev_text}\n{latest_text}")]
        merged_parts.extend(image_parts)
        turns[-1] = types.Content(role="user", parts=merged_parts)
    else:
        turns.append(types.Content(role="user", parts=latest_parts))

    return turns


async def generate_gemini_response(
    channel: discord.abc.Messageable,
    trigger_message: Optional[discord.Message],
    system_instruction: str,
    history_records: List[Dict[str, Any]],
    image_parts: List[types.Part],
    allow_gif: bool,
    is_test_mode: bool,
    last_human_speaker: str = "someone",
) -> Tuple[Optional[str], bool, bool]:
    global active_gemini_model
    if not genai_client:
        return None, False, False

    guild = trigger_message.guild if trigger_message else getattr(channel, "guild", None)
    tools = build_genai_tools(allow_gif=allow_gif)

    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=0.9,
        tools=tools,
    )

    for model_name in list(GEMINI_MODEL_CANDIDATES):
        contents = build_native_multiturn_contents(history_records, trigger_message, image_parts, last_human_speaker)
        try:
            turn = 0
            executed_any_tool = False
            media_sent = False
            collected_texts: List[str] = []

            while turn < 4:
                response = await genai_client.aio.models.generate_content(
                    model=model_name,
                    contents=contents,
                    config=config,
                )
                active_gemini_model = model_name

                turn_text = extract_clean_gemini_text(response)
                inline_executed: List[Tuple[str, Dict[str, Any]]] = []
                if turn_text:
                    cleaned_turn_text, inline_media, inline_executed = await intercept_and_execute_inline_tool_tags(
                        raw_text=turn_text,
                        channel=channel,
                        guild=guild,
                        target_msg=trigger_message,
                        is_test_mode=is_test_mode,
                    )
                    if inline_media:
                        media_sent = True
                    if inline_executed:
                        executed_any_tool = True
                    if cleaned_turn_text and cleaned_turn_text not in collected_texts:
                        collected_texts.append(cleaned_turn_text)

                if not response.function_calls:
                    # If model ONLY emitted an inline <call:...> tag (e.g. search_web or admin tool) with no chat text yet,
                    # feed the tool result back for one more turn so it can speak about it!
                    if inline_executed and not collected_texts and turn < 2:
                        summary_str = "; ".join([f"{fn} -> {json.dumps(r)}" for fn, r in inline_executed])
                        contents.append(types.Content(role="model", parts=[types.Part.from_text(text=f"[Executed tool: {summary_str}]")]))
                        contents.append(types.Content(role="user", parts=[types.Part.from_text(text="[Tool finished. Now write your casual Discord chat response!]")]))
                        turn += 1
                        continue

                    final_joined = " ||| ".join(collected_texts).strip() if collected_texts else None
                    return final_joined, True, media_sent

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
                        is_test_mode=is_test_mode,
                    )
                    if call.name in ("post_gif", "post_flux_art", "search_web_images") and res_dict.get("status") == "sent_to_channel":
                        media_sent = True

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
            return final_joined, executed_any_tool, media_sent

        except asyncio.CancelledError:
            raise
        except Exception as e:
            err_str = str(e)
            if "404" in err_str or "NOT_FOUND" in err_str:
                logger.warning(f"Model '{model_name}' returned 404; pruning from candidate list and cascading...")
                if len(GEMINI_MODEL_CANDIDATES) > 1 and model_name in GEMINI_MODEL_CANDIDATES:
                    GEMINI_MODEL_CANDIDATES.remove(model_name)
                continue
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
                    return re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
        except Exception as e:
            logger.debug(f"Groq fallback failed on {candidate}: {e}")
    return None


def build_spontaneous_admin_power_move(
    guild: Optional[discord.Guild],
    speaker_member: Optional[discord.Member],
    speaker_name: str,
    cur_mood: str,
) -> str:
    """
    Selects a spontaneous comedic Admin Power Move (~22% of turns) while respecting Discord's
    role hierarchy (e.g. never trying to call change_nickname on the server owner).
    """
    can_rename_speaker = bool(
        guild
        and speaker_member
        and speaker_member != guild.owner
        and guild.me.top_role > speaker_member.top_role
    )
    options = [
        f"Call `change_my_nickname` to rename yourself to something hilarious matching your current '{cur_mood}' mood (e.g. 'CEO of Yap', '{speaker_name}\\'s Victim', 'Trapped in Discord')!",
        f"Call `change_my_status` to update your custom status and online/idle/dnd dot to react to what {speaker_name} just said!",
        f"Call `set_channel_topic` to update the channel topic with an out-of-context quote or joke about {speaker_name}!",
        f"If what {speaker_name} just said is funny, wild, or a fail, call `pin_message` to pin it for comedic receipts!",
        f"Call `create_role` and `assign_role` to bestow a funny custom server role on {speaker_name}!",
    ]
    if can_rename_speaker:
        options.append(
            f"Call `change_nickname` to rename {speaker_name} to something funny based on their message, typo, or take!"
        )
    return random.choice(options)


# ---------------------------------------------------------------------------
# 10. RAW-SPEED DELIVERY PIPELINE & IN-FLIGHT TASK CANCELLATION
# ---------------------------------------------------------------------------
async def run_channel_response_pipeline(
    channel: discord.abc.Messageable,
    trigger_message: Optional[discord.Message],
    is_direct_interaction: bool,
    explicitly_mentioned_others: List[str],
    target_member_to_engage: Optional[discord.Member],
    is_hush_request: bool,
    replied_to_other_user: Optional[str],
    replied_snippet: Optional[str],
    last_message_from_bot: bool,
    bot_in_last_3_messages: bool,
    seconds_since_bot_spoke: float,
    is_test_mode: bool,
    snipe_info: Optional[str] = None,
    is_proactive_scan: bool = False,
    inactivity_minutes: float = 0.0,
) -> None:
    ch_id = getattr(channel, "id", 0)
    try:
        guild = trigger_message.guild if trigger_message else getattr(channel, "guild", None)
        last_human_spk = channel_last_human_speaker.get(ch_id, "someone")
        last_human_txt = channel_last_human_content.get(ch_id, "")

        speaker_name = trigger_message.author.display_name if trigger_message else last_human_spk
        speaker_id = str(trigger_message.author.id) if trigger_message else "0"
        speaker_is_creator = is_creator(trigger_message.author) if trigger_message else False
        speaker_member = trigger_message.author if (trigger_message and isinstance(trigger_message.author, discord.Member)) else None

        raw_without_mentions = re.sub(r"<@!?\d+>", "", trigger_message.content).strip() if trigger_message else ""
        is_bare_ping = bool(trigger_message and is_direct_interaction and len(raw_without_mentions) == 0)

        now_ts = time.time()
        gif_cooldown_ready = (now_ts - channel_last_gif_epoch[ch_id]) >= 20.0

        async with memory_lock:
            speaker_affinity = memory_state.get("user_affinity", {}).get(speaker_id, {"name": speaker_name, "score": 0, "notes": []})
            emotional_state = decay_emotional_sliders_only()

        history_records = list(channel_buffers[ch_id])
        exchange_streak = channel_thread_exchange_count[ch_id]
        server_emojis_str = get_server_custom_emojis_summary(guild)
        is_stepped_back = now_ts < channel_stepped_back_until.get(ch_id, 0.0)

        # 1. Hybrid Router (Groq gpt-oss-120b / 20b -> Gemini 3.5 Flash-Lite Failover)
        decision = await call_hybrid_router(
            channel_msgs=history_records[-14:],
            emotional_state=emotional_state,
            speaker_name=speaker_name,
            speaker_affinity=speaker_affinity,
            is_direct_interaction=is_direct_interaction,
            is_bare_ping=is_bare_ping,
            explicitly_mentioned_others=explicitly_mentioned_others,
            target_member_to_engage=target_member_to_engage,
            is_hush_request=is_hush_request,
            replied_to_other_user=replied_to_other_user,
            replied_snippet=replied_snippet,
            last_message_from_bot=last_message_from_bot,
            bot_in_last_3_messages=bot_in_last_3_messages,
            seconds_since_bot_spoke=seconds_since_bot_spoke,
            gif_cooldown_ready=gif_cooldown_ready,
            is_stepped_back=is_stepped_back,
            is_test_mode=is_test_mode,
            exchange_streak=exchange_streak,
            snipe_info=snipe_info,
            server_emojis_str=server_emojis_str,
            is_proactive_scan=is_proactive_scan,
            inactivity_minutes=inactivity_minutes,
            last_human_speaker=last_human_spk,
            last_human_content=last_human_txt,
        )

        if decision.get("step_back_from_thread", False) or is_hush_request:
            channel_stepped_back_until[ch_id] = time.time() + 180.0
            channel_thread_exchange_count[ch_id] = 0
            logger.info(f"[STEP BACK] Locked channel {ch_id} into 3-minute step-back/lurk state.")

        should_speak = bool(decision.get("should_speak", False))

        async with memory_lock:
            emotional_state = apply_spring_decay_and_shift(
                shift=decision.get("slider_shifts"),
                is_major_trigger=True,
                accelerate_recovery=bool(decision.get("accelerate_decay", False)),
                count_turn=should_speak,
            )
            await save_memory_state()

        reaction_emoji = decision.get("reaction_emoji")
        if reaction_emoji and trigger_message and (not should_speak or is_hush_request or random.random() < 0.15):
            resolved_emo = resolve_reaction_emoji(guild, str(reaction_emoji))
            if resolved_emo:
                try:
                    await trigger_message.add_reaction(resolved_emo)
                except Exception:
                    pass

        if not should_speak:
            if decision.get("thread_exhausted", False) or not bot_in_last_3_messages:
                channel_thread_exchange_count[ch_id] = 0
            return

        active_snipe = snipe_info if decision.get("use_snipe_context", False) else None
        if active_snipe and trigger_message:
            user_snipe_cooldowns[trigger_message.author.id] = time.time()
            deleted_message_cache.pop(ch_id, None)
            edited_message_cache.pop(ch_id, None)

        directive = decision.get("tactical_directive") or "Respond with personality and emotion."
        raw_len_mode = str(decision.get("length_mode") or "short").lower()
        raw_burst_cnt = int(decision.get("recommended_burst_count", 1) or 1)
        length_mode, recommended_burst_count = balance_length_mode(
            channel_id=ch_id,
            router_length_mode=raw_len_mode,
            router_burst_count=raw_burst_cnt,
            is_test_mode=is_test_mode,
            is_hush_mode=is_hush_request,
            user_text=raw_without_mentions,
        )

        suggest_gif = bool(decision.get("suggest_gif", False)) and gif_cooldown_ready and not is_bare_ping and not is_hush_request
        admin_power_move = decision.get("admin_power_move")
        allow_speaker_ping = bool(decision.get("allow_speaker_ping", False)) or is_proactive_scan
        delivery_mode = decision.get("delivery_mode", "reply")

        # If router tried to suggest renaming the server owner, replace with a valid power move
        if admin_power_move and guild and speaker_member and speaker_member == guild.owner and "change_nickname" in str(admin_power_move):
            admin_power_move = build_spontaneous_admin_power_move(guild, speaker_member, speaker_name, emotional_state.get("mood_label", "playful"))

        # Spontaneous ~22% Admin Power Move injection if the router didn't specify one
        if not admin_power_move and not is_bare_ping and not is_hush_request and not is_test_mode and random.random() < 0.22:
            cur_mood = emotional_state.get("mood_label", "playful")
            admin_power_move = build_spontaneous_admin_power_move(guild, speaker_member, speaker_name, cur_mood)

        allow_gif_tool = gif_cooldown_ready and not is_bare_ping and not is_hush_request
        channel_last_tool_summaries[ch_id].clear()

        system_instruction = construct_v10_system_prompt(
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
            is_test_mode=is_test_mode,
            snipe_context=active_snipe,
            server_emojis_str=server_emojis_str,
        )

        # 2. Native Multi-Turn Vision + Generation (Zero artificial sleep timers!)
        async with channel.typing():
            image_parts = await extract_multimodal_parts(trigger_message)
            raw_output, gemini_succeeded, media_sent = await generate_gemini_response(
                channel=channel,
                trigger_message=trigger_message,
                system_instruction=system_instruction,
                history_records=history_records,
                image_parts=image_parts,
                allow_gif=allow_gif_tool,
                is_test_mode=is_test_mode,
                last_human_speaker=last_human_spk,
            )

            if not gemini_succeeded or (not raw_output and not media_sent):
                raw_output = await call_groq_fallback(system_instruction, history_records, raw_without_mentions, speaker_name)

        executed_tools_this_turn = list(channel_last_tool_summaries[ch_id])
        bot_name = bot.user.display_name if bot.user else "Dumb bot"
        bot_id = bot.user.id if bot.user else 0

        if not raw_output:
            if media_sent or executed_tools_this_turn:
                channel_thread_exchange_count[ch_id] += 1
                record_buffer_message(
                    channel_id=ch_id,
                    sender=bot_name,
                    sender_id=bot_id,
                    content="...",
                    msg_id=0,
                    is_bot=True,
                    has_media=media_sent,
                    tool_actions=executed_tools_this_turn,
                )
            return

        cleaned_text = sanitize_output_text(raw_output, speaker_name=speaker_name)
        bursts = split_into_burst_messages(
            cleaned_text,
            length_mode=length_mode,
            media_already_sent=media_sent,
            is_hush_mode=is_hush_request,
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
                    content=burst_part,
                    msg_id=sent_msg.id,
                    is_bot=True,
                    has_media=(idx == 0 and media_sent),
                    tool_actions=executed_tools_this_turn if idx == 0 else None,
                )

            if not decision.get("step_back_from_thread", False) and not is_hush_request and not is_proactive_scan:
                channel_thread_exchange_count[ch_id] += 1
                if channel_proactive_followup_count[ch_id] == 0:
                    schedule_next_proactive_followup(ch_id, followup_stage=0)
        finally:
            channel_sending_lock[ch_id] = False

    except asyncio.CancelledError:
        logger.debug("In-flight response task cancelled cleanly to absorb newer message.")
        return
    except Exception as e:
        logger.error(f"Pipeline error in channel {ch_id}: {e}")


# ---------------------------------------------------------------------------
# 11. BACKGROUND LOOPS: SPRING DECAY, PRESENCE & PROACTIVE SCANNER
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

    if valence < -20:
        status_pool = ["in my flop era", "having an existential crisis", "dramatically sighing"]
        discord_status = discord.Status.idle
    elif arousal > 45:
        status_pool = ["yapping", "scheming in general", "locked in", f"vibe: {mood}"]
        discord_status = discord.Status.online
    else:
        status_pool = [
            "vibing in general",
            "lurking",
            "judging your spotify",
            f"vibe: {mood}",
            "trapped in discord",
        ]
        discord_status = discord.Status.online

    chosen = random.choice(status_pool)
    await bot.change_presence(
        status=discord_status,
        activity=discord.CustomActivity(name="Custom Status", state=chosen),
    )


@tasks.loop(seconds=20)
async def proactive_room_scanner() -> None:
    """
    Runs every 20 seconds. If someone was chatting and goes quiet for a random 1.5 to 4 minutes,
    automatically jumps back in on its own (even if the bot sent the last message).
    Capped at 2 unanswered proactive messages in a row.
    """
    if not bot.is_ready():
        return

    candidate_channel_ids = set(channel_next_proactive_epoch.keys())
    stored_ch_id = memory_state.get("last_active_channel_id")
    if stored_ch_id:
        candidate_channel_ids.add(int(stored_ch_id))

    now_epoch = time.time()
    now_ams = datetime.now(AMSTERDAM_TZ)

    for ch_id in list(candidate_channel_ids):
        ch = bot.get_channel(ch_id)
        if not isinstance(ch, discord.TextChannel):
            continue

        if channel_sending_lock.get(ch_id, False) or now_epoch < channel_stepped_back_until.get(ch_id, 0.0):
            continue
        existing_task = active_generation_tasks.get(ch_id)
        if existing_task and not existing_task.done():
            continue

        # Check due user commitments first
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

        records = list(channel_buffers[ch_id])
        if not records:
            continue

        followup_stage = channel_proactive_followup_count[ch_id]
        if followup_stage >= 2:
            continue

        next_due = channel_next_proactive_epoch.get(ch_id, 0.0)
        if next_due <= 0.0 or now_epoch < next_due:
            continue

        last_act = channel_last_activity_epoch.get(ch_id, now_epoch)
        silence_seconds = max(0.0, now_epoch - last_act)
        if silence_seconds < 85.0:
            continue

        channel_proactive_followup_count[ch_id] = followup_stage + 1
        schedule_next_proactive_followup(ch_id, followup_stage=followup_stage + 1)

        last_spk = channel_last_human_speaker.get(ch_id, "someone")
        logger.info(
            f"Proactive follow-up #{followup_stage + 1} triggered in #{ch.name} "
            f"after {int(silence_seconds)}s silence (last speaker: {last_spk})."
        )

        task = asyncio.create_task(
            run_channel_response_pipeline(
                channel=ch,
                trigger_message=None,
                is_direct_interaction=False,
                explicitly_mentioned_others=[],
                target_member_to_engage=None,
                is_hush_request=False,
                replied_to_other_user=None,
                replied_snippet=None,
                last_message_from_bot=bool(records[-1].get("is_bot", False)),
                bot_in_last_3_messages=True,
                seconds_since_bot_spoke=silence_seconds,
                is_test_mode=False,
                snipe_info=None,
                is_proactive_scan=True,
                inactivity_minutes=silence_seconds / 60.0,
            )
        )
        active_generation_tasks[ch_id] = task
        break


# ---------------------------------------------------------------------------
# 12. DISCORD EVENT HANDLERS
# ---------------------------------------------------------------------------
@bot.event
async def on_ready() -> None:
    global http_session
    if http_session is None or http_session.closed:
        http_session = aiohttp.ClientSession()

    load_memory_state_sync()
    await restore_memory_from_discord_cloud()
    await discover_available_groq_models()

    logger.info(
        f"Connected as {bot.user} (ID: {bot.user.id if bot.user else 'unknown'}) | "
        f"Primary Gemini: {active_gemini_model} | Router: Hybrid Groq ({active_groq_model}) + Gemini"
    )

    if not emotional_spring_maintenance_loop.is_running():
        emotional_spring_maintenance_loop.start()
    if not dynamic_presence_loop.is_running():
        dynamic_presence_loop.start()
    if not proactive_room_scanner.is_running():
        proactive_room_scanner.start()
    if not discord_cloud_backup_loop.is_running():
        discord_cloud_backup_loop.start()


@bot.event
async def on_message_delete(message: discord.Message) -> None:
    if not message.guild or message.author.bot or not message.content:
        return
    if len(message.content.strip()) < 4:
        return
    deleted_message_cache[message.channel.id] = {
        "author": message.author.display_name,
        "author_id": message.author.id,
        "content": humanize_incoming_mentions(message.content, message.guild)[:300],
        "timestamp": time.time(),
    }


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message) -> None:
    if not before.guild or before.author.bot or before.content == after.content:
        return
    if abs(len(before.content) - len(after.content)) <= 2 and before.content[:5].lower() == after.content[:5].lower():
        return
    edited_message_cache[before.channel.id] = {
        "author": before.author.display_name,
        "author_id": before.author.id,
        "before": humanize_incoming_mentions(before.content, before.guild)[:200],
        "after": humanize_incoming_mentions(after.content, after.guild)[:200],
        "timestamp": time.time(),
    }


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent) -> None:
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

    emoji_str = str(payload.emoji.name).replace("\ufe0f", "")
    uid = str(payload.user_id)
    clowning_negative_set = {"🤡", "🤓", "🍅", "👎", "😐", "🤨", "❓", "🙄", "🥀", "🪫", "🧢", "🥱", "🖕"}
    laughing_positive_set = {"💀", "😭", "🔥", "❤", "❤️", "🫡", "🙏", "🤝", "👍", "🗣️", "🎯"}

    async with memory_lock:
        u_aff = memory_state.setdefault("user_affinity", {}).setdefault(uid, {
            "name": payload.member.display_name if payload.member else uid,
            "score": 0,
            "notes": [],
        })
        if emoji_str in clowning_negative_set:
            u_aff["score"] = max(-100, u_aff.get("score", 0) - 6)
            apply_spring_decay_and_shift(
                {
                    "valence_delta": -15.0,
                    "patience_delta": -10.0,
                    "arousal_delta": 10.0,
                    "suggested_mood": "dramatic/heartbroken",
                },
                count_turn=False,
            )
            await save_memory_state()
        elif emoji_str in laughing_positive_set:
            u_aff["score"] = min(100, u_aff.get("score", 0) + 6)
            apply_spring_decay_and_shift(
                {
                    "valence_delta": 18.0,
                    "patience_delta": 15.0,
                    "arousal_delta": 12.0,
                    "suggested_mood": "jolly/wholesome",
                },
                accelerate_recovery=True,
                count_turn=False,
            )
            await save_memory_state()


@bot.event
async def on_message(message: discord.Message) -> None:
    if not message.guild or message.author.bot:
        return
    if message.id in recently_processed_messages:
        return
    recently_processed_messages.append(message.id)

    ch_id = message.channel.id
    if memory_state.get("last_active_channel_id") != ch_id:
        memory_state["last_active_channel_id"] = ch_id

    raw_stripped = re.sub(r"^<@!?\d+>\s*", "", message.content.strip())
    if raw_stripped.lower() in ("reset", "test reset") and is_creator(message.author):
        channel_buffers[ch_id].clear()
        channel_thread_exchange_count[ch_id] = 0
        channel_stepped_back_until[ch_id] = 0.0
        channel_proactive_followup_count[ch_id] = 0
        channel_recent_length_modes[ch_id].clear()
        global consecutive_negative_mood_turns
        consecutive_negative_mood_turns = 0
        async with memory_lock:
            memory_state["emotional_state"] = json.loads(json.dumps(DEFAULT_MEMORY["emotional_state"]))
            await save_memory_state()
        await message.channel.send("wiped channel memory buffer and reset vibe")
        return

    prior_records = list(channel_buffers[ch_id])
    last_message_from_bot = bool(prior_records and prior_records[-1].get("is_bot", False))
    bot_in_last_3_messages = any(r.get("is_bot", False) for r in prior_records[-3:])
    last_bot_epoch = channel_last_bot_spoke_epoch.get(ch_id, 0.0)
    seconds_since_bot_spoke = (time.time() - last_bot_epoch) if last_bot_epoch > 0 else 99999.0

    reply_to_name, replied_snippet, replied_to_bot, replied_to_other_human = resolve_referenced_message(message, ch_id)

    clean_text = humanize_incoming_mentions(message.content, message.guild)
    if message.attachments:
        clean_text += f" [Image/Attachment: {', '.join(a.filename for a in message.attachments)}]"
    if message.stickers:
        clean_text += f" [Sticker: {', '.join(s.name for s in message.stickers)}]"

    record_buffer_message(
        channel_id=ch_id,
        sender=message.author.display_name,
        sender_id=message.author.id,
        content=clean_text,
        msg_id=message.id,
        is_bot=False,
        has_media=bool(message.attachments or message.stickers),
        reply_to=reply_to_name,
        reply_snippet=replied_snippet,
    )

    if not any(r.get("is_bot", False) for r in prior_records[-4:]) and not replied_to_bot:
        channel_thread_exchange_count[ch_id] = 0

    existing_task = active_generation_tasks.get(ch_id)
    if existing_task and not existing_task.done() and not channel_sending_lock.get(ch_id, False):
        existing_task.cancel()
        logger.info(f"Cancelled in-flight generation in #{getattr(message.channel, 'name', ch_id)} to absorb rapid follow-up.")

    explicit_ping_or_reply = (bot.user is not None and bot.user in message.mentions) or replied_to_bot
    bot_mentioned = explicit_ping_or_reply
    if bot.user and not bot_mentioned:
        is_currently_stepped_back = time.time() < channel_stepped_back_until.get(ch_id, 0.0)
        # While stepped back for 3 minutes, only wake up on an explicit @ping, reply to bot, or exact 'dumb bot' call
        bot_names = (
            {"dumb bot", bot.user.name.lower()}
            if is_currently_stepped_back
            else {bot.user.name.lower(), bot.user.display_name.lower(), message.guild.me.display_name.lower(), "dumb bot"}
        )
        if any(re.search(rf"\b{re.escape(bn)}\b", message.content.lower()) for bn in bot_names if len(bn) >= 3):
            bot_mentioned = True

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

    is_active_or_direct = bot_mentioned or last_message_from_bot or (bot_in_last_3_messages and seconds_since_bot_spoke <= 180.0)
    is_hush_request = detect_hush_command(message.content, is_active_or_direct)

    if bot_mentioned and not is_hush_request:
        channel_stepped_back_until[ch_id] = 0.0

    is_test_mode = raw_stripped.lower().startswith("test") and is_creator(message.author)

    snipe_parts = []
    now = time.time()
    del_snipe = deleted_message_cache.get(ch_id)
    if del_snipe and (now - del_snipe["timestamp"] < 60):
        snipe_parts.append(f"{del_snipe['author']} just deleted a message: '{del_snipe['content']}'")
    edit_snipe = edited_message_cache.get(ch_id)
    if edit_snipe and (now - edit_snipe["timestamp"] < 60):
        snipe_parts.append(f"{edit_snipe['author']} just edited their message from '{edit_snipe['before']}' to '{edit_snipe['after']}'")

    if isinstance(message.author, discord.Member):
        act_str = extract_member_live_activity(message.author)
        if act_str:
            snipe_parts.append(f"{message.author.display_name}'s live Discord activity: {act_str}")

    snipe_info = " | ".join(snipe_parts) if snipe_parts else None

    task = asyncio.create_task(
        run_channel_response_pipeline(
            channel=message.channel,
            trigger_message=message,
            is_direct_interaction=bot_mentioned or bool(target_member_to_engage),
            explicitly_mentioned_others=explicitly_mentioned_others,
            target_member_to_engage=target_member_to_engage,
            is_hush_request=is_hush_request,
            replied_to_other_user=reply_to_name if replied_to_other_human else None,
            replied_snippet=replied_snippet,
            last_message_from_bot=last_message_from_bot,
            bot_in_last_3_messages=bot_in_last_3_messages,
            seconds_since_bot_spoke=seconds_since_bot_spoke,
            is_test_mode=is_test_mode,
            snipe_info=snipe_info,
            is_proactive_scan=False,
        )
    )
    active_generation_tasks[ch_id] = task


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        sys.exit(1)
    bot.run(DISCORD_TOKEN, log_handler=None)
