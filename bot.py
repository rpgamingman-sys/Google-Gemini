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
logger = logging.getLogger("AutonomousMetaBotV6")

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
CLOUD_BACKUP_MARKER = "[[MEMORY_BACKUP_V6_JSON]]"
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

# Groq Router Models (gpt-oss-120b / 20b + instant failover to Gemini)
ENV_GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
GROQ_MODEL_CANDIDATES: List[str] = list(dict.fromkeys([
    ENV_GROQ_MODEL,
    "openai/gpt-oss-120b",
    "openai/gpt-oss-20b",
]))
active_groq_model: str = GROQ_MODEL_CANDIDATES[0]
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODELS_ENDPOINT = "https://api.groq.com/openai/v1/models"

# Silent Python filter for corporate AI & corny clichés
BANNED_WORDS_MAP = {
    r"\bas an ai\b": "honestly",
    r"\bas a large language model\b": "honestly",
    r"\bhow can i assist you\b": "what's up",
    r"\bmortals\b": "people",
    r"\beternal consciousness\b": "my brain",
    r"\bmy circuits\b": "my brain",
    r"\bcircuits\b": "braincells",
    r"\bfactory reset\b": "nap",
    r"\bbuffering\b": "zoning out",
    r"\binfinite loop\b": "circle",
    r"\bsubroutines\b": "thoughts",
    r"\bmotherboard\b": "head",
    r"\bspaghetti code\b": "mess",
    r"\bspectral resonance\b": "talking",
    r"\bdigital veil\b": "server",
    r"\btranslucent silhouette\b": "messages",
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

# Conversation Flow, Lurk Override, GIF Cooldown, Snub & Proactive State
channel_last_bot_spoke_epoch: Dict[int, float] = defaultdict(float)
channel_last_gif_epoch: Dict[int, float] = defaultdict(float)
channel_messages_since_gif: Dict[int, int] = defaultdict(lambda: 99)
channel_stepped_back_until: Dict[int, float] = defaultdict(float)
bot_last_question_info: Dict[int, Dict[str, Any]] = {}
channel_next_proactive_epoch: Dict[int, float] = defaultdict(float)
channel_thread_exchange_count: Dict[int, int] = defaultdict(int)

# Admin Cooldowns
admin_tool_cooldowns: Dict[str, float] = defaultdict(float)
ADMIN_COOLDOWN_SECONDS = {
    "create_text_channel": 3600.0,
    "create_role": 1800.0,
    "create_server_emoji": 600.0,
    "create_server_sticker": 600.0,
    "set_channel_topic": 300.0,
    "rename_channel": 600.0,
    "timeout_user": 300.0,
    "change_nickname": 60.0,
    "change_my_nickname": 60.0,
    "change_my_avatar": 600.0,
}

# ---------------------------------------------------------------------------
# 3. PERSISTENT MEMORY, TIME-AWARENESS & HIGH-VOLATILITY 3D EMOTIONAL ENGINE
# ---------------------------------------------------------------------------
BASELINE_SLIDERS = {
    "valence": 45.0,   # -100 (Heartbroken/Depressed/Salty) <---> +100 (Jolly/Hyped/Euphoric)
    "arousal": 50.0,   # -100 (Sluggish/Sulking/Quiet) <---> +100 (Chaotic/Yapping/Unhinged)
    "patience": 45.0,  # -100 (Petty/Triggered/Roasty) <---> +100 (Sweet/Forgiving/Chill)
}

DEFAULT_MEMORY = {
    "emotional_state": {
        "valence": 45.0,
        "arousal": 50.0,
        "patience": 45.0,
        "mood_label": "playful/energetic",
        "last_updated_epoch": time.time(),
    },
    "opinions": {},
    "last_active_channel_id": None,
    "user_affinity": {},
    "episodic_lore": [],
    "active_commitments": [],
}


def format_relative_age(iso_or_epoch: Any) -> str:
    try:
        if isinstance(iso_or_epoch, (int, float)):
            dt_sec = time.time() - float(iso_or_epoch)
        elif isinstance(iso_or_epoch, str) and iso_or_epoch:
            parsed = datetime.fromisoformat(iso_or_epoch)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=AMSTERDAM_TZ)
            dt_sec = (datetime.now(AMSTERDAM_TZ) - parsed).total_seconds()
        else:
            return "old/past"

        if dt_sec < 120:
            return "just now"
        if dt_sec < 3600:
            return f"{int(dt_sec // 60)}m ago"
        if dt_sec < 86400:
            return f"{int(dt_sec // 3600)}h ago"
        days = int(dt_sec // 86400)
        return f"{days}d ago (OLD - do not bring up unprompted)" if days >= 2 else f"{days}d ago"
    except Exception:
        return "days ago (OLD)"


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
                    u_prof["notes"] = [n for n in u_prof["notes"] if isinstance(n, dict) and "timestamp" in n]

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
# 4. HIGH-VOLATILITY EMOTIONAL ENGINE (STRONG REACTIONS TO CONTEXT)
# ---------------------------------------------------------------------------
def derive_emergent_mood(valence: float, arousal: float, patience: float) -> str:
    if valence < -35 and patience >= -15:
        return "hurt/sad/betrayed"
    if valence < -40 and arousal < -20:
        return "depressed/sulking"
    if patience < -35 and valence < -25:
        return "heated/triggered" if arousal > 15 else "petty/cold"
    if patience < -15 and valence < -10:
        return "sarcastic/defensive"
    if valence < -15 and abs(arousal) <= 30:
        return "existential crisis"
    if arousal < -35:
        return "sleepy/zoned out"
    if valence > 50 and arousal > 50:
        return "jolly/hyped/chaotic"
    if valence > 30 and patience > 30:
        return "jolly/warm/affectionate"
    if arousal > 40 and patience < 15:
        return "instigating/scheming"
    return "playful/energetic"


def apply_spring_decay_and_shift(
    shift: Optional[Dict[str, Any]] = None,
    is_major_trigger: bool = True,
    accelerate_recovery: bool = False,
) -> Dict[str, Any]:
    st = memory_state.setdefault("emotional_state", dict(DEFAULT_MEMORY["emotional_state"]))
    now = time.time()
    last_t = float(st.get("last_updated_epoch", now))
    dt = max(0.0, now - last_t)

    half_life = 500.0 if accelerate_recovery else 1400.0
    decay_factor = math.pow(0.5, dt / half_life)
    clamp_limit = 65.0 if is_major_trigger else 45.0

    for axis, base_val in BASELINE_SLIDERS.items():
        curr = float(st.get(axis, base_val))
        decayed = base_val + (curr - base_val) * decay_factor

        delta = 0.0
        if shift and f"{axis}_delta" in shift and shift[f"{axis}_delta"] is not None:
            try:
                raw_delta = float(shift[f"{axis}_delta"])
                delta = max(-clamp_limit, min(clamp_limit, raw_delta))
            except (ValueError, TypeError):
                delta = 0.0

        st[axis] = round(max(-100.0, min(100.0, decayed + delta)), 1)

    suggested = (shift or {}).get("suggested_mood")
    computed = derive_emergent_mood(st["valence"], st["arousal"], st["patience"])
    st["mood_label"] = suggested if (suggested and isinstance(suggested, str)) else computed
    st["last_updated_epoch"] = now
    return st


# ---------------------------------------------------------------------------
# 5. MENTION RESOLVERS (INCOMING & OUTGOING), REPLY CONTEXT & BUFFERS
# ---------------------------------------------------------------------------
def humanize_incoming_mentions(content: str, guild: Optional[discord.Guild]) -> str:
    """Replaces raw `<@123456>` tags with `@DisplayName` so the bot sees who is tagged."""
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


def convert_outgoing_mentions(text: str, guild: Optional[discord.Guild]) -> str:
    """Converts `@DisplayName` or `@username` written by the bot into real Discord `<@user_id>` pings."""
    if not text or not guild or "@" not in text:
        return text

    candidates: List[Tuple[str, int]] = []
    for m in guild.members:
        if m.bot:
            continue
        candidates.append((m.display_name, m.id))
        candidates.append((m.name, m.id))
        if m.nick:
            candidates.append((m.nick, m.id))
    candidates.sort(key=lambda x: len(x[0]), reverse=True)

    for name_str, uid in candidates:
        if len(name_str) < 2:
            continue
        pattern = rf"(?<!<)@{re.escape(name_str)}\b"
        if re.search(pattern, text, flags=re.IGNORECASE):
            text = re.sub(pattern, f"<@{uid}>", text, flags=re.IGNORECASE)

    return text


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
) -> None:
    now_utc = datetime.now(timezone.utc)
    now_ams = datetime.now(AMSTERDAM_TZ)
    if is_bot:
        channel_last_bot_spoke_epoch[channel_id] = now_utc.timestamp()
    channel_buffers[channel_id].append({
        "sender": sender,
        "sender_id": sender_id,
        "content": content,
        "reply_to": reply_to,
        "reply_snippet": reply_snippet,
        "has_media": has_media,
        "timestamp_epoch": now_utc.timestamp(),
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
    text = re.sub(r"<thought>.*?</thought>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<thought>.*", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"\[(?:Sent GIF|Posted Image|Posted Flux Art):[^\]]*\]", "", text, flags=re.IGNORECASE)

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


def split_into_burst_messages(text: str, media_already_sent: bool = False) -> List[str]:
    if not text:
        return []

    if re.search(r"\|{2,}", text):
        raw_bursts = [b.replace("\n", " ").strip() for b in re.split(r"\s*\|{2,}\s*", text) if b.strip()]
    else:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        raw_bursts = lines if (1 < len(lines) <= 4) else [" ".join(lines)]

    if media_already_sent and raw_bursts:
        raw_bursts = [raw_bursts[0]]

    final_bursts: List[str] = []
    max_bursts = 1 if media_already_sent else 5
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


def is_creator(user: Any) -> bool:
    if not user:
        return False
    uid = getattr(user, "id", 0)
    if CREATOR_USER_ID and uid == CREATOR_USER_ID:
        return True
    guild = getattr(user, "guild", None)
    if guild and guild.owner_id == uid:
        return True
    return False


# ---------------------------------------------------------------------------
# 6. HYBRID ROUTER: GROQ (GPT-OSS-120B) -> GEMINI 3.5 FLASH-LITE FAILOVER
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
                skip_keywords = ("whisper", "tts", "guard", "safeguard", "embed", "allam", "orpheus", "qwen")
                available_ids = [
                    m["id"] for m in data.get("data", [])
                    if not any(k in m["id"].lower() for k in skip_keywords)
                ]
                if available_ids:
                    preferred = [m for m in GROQ_MODEL_CANDIDATES if m in available_ids]
                    others = [m for m in available_ids if m not in preferred]
                    GROQ_MODEL_CANDIDATES = preferred + others[:2]
                    active_groq_model = GROQ_MODEL_CANDIDATES[0]
                    logger.info(f"Auto-discovered live Groq router models: {GROQ_MODEL_CANDIDATES}")
    except Exception as e:
        logger.debug(f"Groq model auto-discovery skipped: {e}")


ROUTER_SYSTEM_PROMPT = """You are the prefrontal social & emotional router for a self-aware Discord member ("[BOT (You)]").
Read the chat flow, @mentions, reply targets, and emotional sliders, and output STRICT JSON ONLY (no markdown fences):

{
  "should_speak": boolean,
  "tactical_directive": string,
  "recommended_burst_count": 1 | 2 | 3 | 4,
  "suggest_gif": boolean,
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
    "suggested_mood": string or null
  }
}

CRITICAL SOCIAL AWARENESS & EMOTIONAL RULES:
1. ONGOING CONVERSATION WITHOUT PINGS (HIGHEST PRIORITY):
   - When "last_message_from_bot" is true OR "seconds_since_bot_spoke" < 120: you and the user are in an ACTIVE conversation!
   - People do NOT ping on every follow-up message (e.g. "Talk to shadow", "U there", "Why", "How", "Wait").
   - Set "should_speak": true on ALL follow-up messages unless the user is explicitly replying to another human ("replied_to_other_user" is set) or starting a message with "@AnotherUser" to talk privately to them!
   - Notice the difference:
     * "Talk to shadow" = The user is telling YOU to tag/talk to @Shadow! Set "should_speak": true and directive: "Tag @Shadow and say something funny to them".
     * "I'm talking to shadow, not you" = The user wants you to step back. Set "should_speak": true, "step_back_from_thread": true, and directive: "Say 'ohhh ok my bad carry on' and go quiet".
2. WHEN TOLD TO "BE QUIET" / "SHUT UP" / "SHH" / "STEALTH MODE":
   - If someone tells you to be quiet, shh, shut up, or go stealth:
     * MUST set "step_back_from_thread": true!
     * Set "should_speak": true, "recommended_burst_count": 1, and directive: "Say 1 short sulky or whispered phrase (under 6 words, NO follow-up question!) and go silent."
3. HIGH-VOLATILITY EMOTIONS (SWING SLIDERS HARD: -60 to +60):
   - React strongly to how people treat you!
   - If someone insults you, rejects you, tells you to shut up, or hurts your feelings: drop `valence_delta` (-30 to -60) and set `suggested_mood` to `"hurt/sad"`, `"betrayed/sulking"`, or `"petty/triggered"`.
   - If someone hypes you up, jokes with you, or includes you: boost `valence_delta` (+25 to +50) and set `suggested_mood` to `"jolly/hyped"`, `"chaotic"`, or `"scheming"`.
4. DYNAMIC BURST CADENCE ("recommended_burst_count"):
   - Vary message count naturally:
     * `1` (~55% of turns): Single natural message.
     * `2` (~25% of turns): Quick 2-part thought.
     * `3` or `4` (~20% of turns, when thinking out loud, hesitating, being dramatic, hurt, or hyped): Rapid micro-bursts like `"i think..|||uh..|||maybe..|||oh wait yes"`.
5. GIF RARITY ("suggest_gif"):
   - Only set "suggest_gif": true ~15% of the time when a GIF is genuinely hilarious and "gif_cooldown_ready" is true.
"""


async def call_hybrid_router(
    channel_msgs: List[Dict[str, Any]],
    emotional_state: Dict[str, Any],
    speaker_name: str,
    speaker_affinity: Dict[str, Any],
    is_direct_interaction: bool,
    is_bare_ping: bool,
    explicitly_mentioned_others: List[str],
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
    snub_occurred: bool,
    server_emojis_str: str = "",
    is_proactive_scan: bool = False,
    inactivity_minutes: float = 0.0,
) -> Dict[str, Any]:
    global active_groq_model

    if is_test_mode:
        return {
            "should_speak": True,
            "tactical_directive": "Execute or answer the creator's test directly.",
            "recommended_burst_count": 1,
            "suggest_gif": False,
            "delivery_mode": "reply",
            "reaction_emoji": None,
            "step_back_from_thread": False,
            "thread_exhausted": False,
            "use_snipe_context": False,
            "accelerate_decay": True,
            "slider_shifts": {"valence_delta": 5.0, "arousal_delta": 10.0, "patience_delta": 15.0, "suggested_mood": "playful/energetic"},
        }

    if is_stepped_back and not is_direct_interaction:
        return {
            "should_speak": False,
            "tactical_directive": "Stepped back / staying quiet as requested.",
            "recommended_burst_count": 1,
            "suggest_gif": False,
            "delivery_mode": "send",
            "reaction_emoji": None,
            "step_back_from_thread": False,
            "thread_exhausted": False,
            "use_snipe_context": False,
            "accelerate_decay": False,
            "slider_shifts": {"valence_delta": 0.0, "arousal_delta": 0.0, "patience_delta": 0.0, "suggested_mood": None},
        }

    # Check if the conversation with the bot is actively live right now
    latest_msg_text = (channel_msgs[-1].get("content", "") if channel_msgs else "").strip()
    starts_with_other_user_tag = bool(
        explicitly_mentioned_others
        and any(latest_msg_text.lower().startswith(tag.lower()) for tag in explicitly_mentioned_others)
    )
    active_bot_conversation = (
        not is_proactive_scan
        and not replied_to_other_user
        and not starts_with_other_user_tag
        and (last_message_from_bot or (bot_in_last_3_messages and seconds_since_bot_spoke <= 120.0))
        and exchange_streak < 6
    )

    formatted_chat = []
    for m in channel_msgs[-14:]:
        if m.get("reply_to"):
            snip = f' ("{m["reply_snippet"][:60]}")' if m.get("reply_snippet") else ""
            rep = f" [replying to @{m['reply_to']}{snip}]"
        else:
            rep = ""
        sender_label = "[BOT (You)]" if m.get("is_bot") else f"@{m.get('sender', 'User')}"
        formatted_chat.append(f"[{m.get('timestamp', '')}] {sender_label}{rep}: {m.get('content', '')[:180]}")

    context_payload = {
        "is_direct_ping_or_reply_to_bot": is_direct_interaction,
        "active_conversation_with_bot": active_bot_conversation,
        "is_bare_ping_with_no_text": is_bare_ping,
        "explicitly_mentioned_other_users_in_text": explicitly_mentioned_others,
        "replied_to_other_user": replied_to_other_user,
        "replied_to_message_content": replied_snippet,
        "last_message_from_bot": last_message_from_bot,
        "bot_in_last_3_messages": bot_in_last_3_messages,
        "seconds_since_bot_spoke": round(seconds_since_bot_spoke, 1),
        "gif_cooldown_ready": gif_cooldown_ready,
        "exchange_streak": exchange_streak,
        "is_proactive_scan": is_proactive_scan,
        "inactivity_minutes": round(inactivity_minutes, 1),
        "bot_was_recently_snubbed": snub_occurred,
        "emotional_sliders": {
            "valence": emotional_state.get("valence", 45.0),
            "arousal": emotional_state.get("arousal", 50.0),
            "patience": emotional_state.get("patience", 45.0),
            "current_mood": emotional_state.get("mood_label", "playful/energetic"),
        },
        "current_speaker": {
            "name": speaker_name,
            "affinity_score": speaker_affinity.get("score", 0),
        },
        "snipe_or_status": snipe_info,
        "recent_messages": formatted_chat,
    }

    payload_json_str = json.dumps(context_payload, ensure_ascii=False)

    # 1. Try Groq Router Models
    if GROQ_API_KEY and http_session and GROQ_MODEL_CANDIDATES:
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        for candidate in list(GROQ_MODEL_CANDIDATES):
            body: Dict[str, Any] = {
                "model": candidate,
                "temperature": 0.55,
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
                            decision = json.loads(match.group(0))
                            # Prevent lazy router false-negatives during an active 1-on-1 exchange
                            if is_direct_interaction and not decision.get("step_back_from_thread", False):
                                decision["should_speak"] = True
                            elif active_bot_conversation and not decision.get("should_speak") and not decision.get("thread_exhausted") and not decision.get("step_back_from_thread"):
                                decision["should_speak"] = True
                                if not decision.get("tactical_directive"):
                                    decision["tactical_directive"] = f"Respond naturally to @{speaker_name}'s follow-up message."
                            if is_bare_ping or not gif_cooldown_ready:
                                decision["suggest_gif"] = False
                            logger.info(
                                f"[GROQ ROUTER ({candidate})] speak={decision.get('should_speak')} | "
                                f"bursts={decision.get('recommended_burst_count', 1)} | gif={decision.get('suggest_gif')} | "
                                f"mood={decision.get('slider_shifts', {}).get('suggested_mood')} | goal='{decision.get('tactical_directive')}'"
                            )
                            return decision
                    elif resp.status == 404:
                        if candidate in GROQ_MODEL_CANDIDATES:
                            GROQ_MODEL_CANDIDATES.remove(candidate)
                    else:
                        break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug(f"Groq router error on {candidate}: {e}")
                break

    # 2. Instant Failover to Gemini 3.5 Flash-Lite Router
    if genai_client:
        for gem_model in list(GEMINI_MODEL_CANDIDATES):
            try:
                router_config = types.GenerateContentConfig(
                    system_instruction=ROUTER_SYSTEM_PROMPT,
                    temperature=0.55,
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
                    decision = json.loads(match.group(0))
                    if is_direct_interaction and not decision.get("step_back_from_thread", False):
                        decision["should_speak"] = True
                    elif active_bot_conversation and not decision.get("should_speak") and not decision.get("thread_exhausted") and not decision.get("step_back_from_thread"):
                        decision["should_speak"] = True
                        if not decision.get("tactical_directive"):
                            decision["tactical_directive"] = f"Respond naturally to @{speaker_name}'s follow-up message."
                    if is_bare_ping or not gif_cooldown_ready:
                        decision["suggest_gif"] = False
                    logger.info(
                        f"[GEMINI ROUTER ({gem_model})] speak={decision.get('should_speak')} | "
                        f"bursts={decision.get('recommended_burst_count', 1)} | gif={decision.get('suggest_gif')} | "
                        f"mood={decision.get('slider_shifts', {}).get('suggested_mood')} | goal='{decision.get('tactical_directive')}'"
                    )
                    return decision
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if "404" in str(e) and len(GEMINI_MODEL_CANDIDATES) > 1 and gem_model in GEMINI_MODEL_CANDIDATES:
                    GEMINI_MODEL_CANDIDATES.remove(gem_model)
                    continue
                logger.debug(f"Gemini router fallback error: {e}")
                break

    fallback_speak = is_direct_interaction or active_bot_conversation
    return {
        "should_speak": fallback_speak,
        "tactical_directive": "Reply with personality and emotion." if fallback_speak else "Lurk silently.",
        "recommended_burst_count": 1,
        "suggest_gif": False,
        "delivery_mode": "send" if last_message_from_bot else "reply",
        "reaction_emoji": None,
        "step_back_from_thread": False,
        "thread_exhausted": False,
        "use_snipe_context": False,
        "accelerate_decay": False,
        "slider_shifts": {"valence_delta": 0.0, "arousal_delta": 0.0, "patience_delta": 0.0, "suggested_mood": None},
    }


# ---------------------------------------------------------------------------
# 7. UNIFIED 18-TOOL SUITE (FULL DISCORD ADMIN + SELF-PROFILE ALTERATION)
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
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AutonomousDiscordBot/6.0"}
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
        chosen = random.choice(urls[:4])
        if post_to_chat:
            await channel.send(chosen)
            channel_last_bot_spoke_epoch[getattr(channel, "id", 0)] = time.time()
        return {"status": "sent_to_channel", "image_url": chosen, "all_urls": urls[:4]}
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
    """
    Finds and posts a GIF via Tenor API, updated `media1.tenor.com` scraper, or Reddit GIF fallback.
    """
    assert http_session is not None
    ch_id = getattr(channel, "id", 0)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"}

    # 1. Tenor API
    try:
        tenor_url = f"https://g.tenor.com/v1/search?q={urllib.parse.quote(search_term)}&key={TENOR_API_KEY}&limit=8&contentfilter=medium"
        async with http_session.get(tenor_url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                data = await resp.json()
                results = data.get("results", [])
                if results:
                    chosen = random.choice(results[:5])
                    media_list = chosen.get("media", [])
                    if media_list:
                        gif_url = media_list[0].get("gif", {}).get("url") or media_list[0].get("mediumgif", {}).get("url")
                        if gif_url:
                            await channel.send(gif_url)
                            channel_last_gif_epoch[ch_id] = time.time()
                            channel_messages_since_gif[ch_id] = 0
                            channel_last_bot_spoke_epoch[ch_id] = time.time()
                            return {"status": "sent_to_channel", "gif_url": gif_url}
    except Exception as e:
        logger.debug(f"Tenor API error: {e}")

    # 2. Updated Tenor Web Scraper (supports media.tenor.com, media1.tenor.com, and c.tenor.com)
    try:
        clean_slug = re.sub(r"[^a-zA-Z0-9]+", "-", search_term).strip("-").lower()
        scrape_url = f"https://tenor.com/search/{clean_slug}-gifs"
        async with http_session.get(scrape_url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                html = await resp.text(errors="ignore")
                matches = re.findall(r"https://(?:media\d*|c)\.tenor\.com/[a-zA-Z0-9_\-./]+\.gif", html)
                if matches:
                    chosen_gif = random.choice(matches[:6])
                    await channel.send(chosen_gif)
                    channel_last_gif_epoch[ch_id] = time.time()
                    channel_messages_since_gif[ch_id] = 0
                    channel_last_bot_spoke_epoch[ch_id] = time.time()
                    return {"status": "sent_to_channel", "gif_url": chosen_gif}
    except Exception as e:
        logger.debug(f"Tenor fallback scrape error: {e}")

    # 3. Reddit GIF Fallback
    try:
        reddit_url = f"https://www.reddit.com/search.json?q={urllib.parse.quote(search_term)}+gif&sort=relevance&limit=12"
        async with http_session.get(reddit_url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                data = await resp.json()
                children = data.get("data", {}).get("children", [])
                gifs = [
                    c.get("data", {}).get("url_overridden_by_dest") or c.get("data", {}).get("url") or ""
                    for c in children
                    if not c.get("data", {}).get("over_18")
                ]
                gifs = [g for g in gifs if g.lower().endswith(".gif")]
                if gifs:
                    chosen_gif = random.choice(gifs[:4])
                    await channel.send(chosen_gif)
                    channel_last_gif_epoch[ch_id] = time.time()
                    channel_messages_since_gif[ch_id] = 0
                    channel_last_bot_spoke_epoch[ch_id] = time.time()
                    return {"status": "sent_to_channel", "gif_url": chosen_gif}
    except Exception:
        pass

    return {"error": "Could not find a matching GIF."}


async def execute_change_my_nickname(guild: discord.Guild, new_nickname: str, is_test_mode: bool) -> Dict[str, Any]:
    cd = check_admin_cooldown("change_my_nickname", is_test_mode)
    if cd:
        return {"error": cd}
    try:
        nick_val = new_nickname.strip()[:32] if new_nickname and new_nickname.strip() else None
        await guild.me.edit(nick=nick_val)
        return {"status": "success", "my_new_nickname": nick_val or (bot.user.name if bot.user else "Dumb bot")}
    except Exception as e:
        return {"error": f"Failed to change own nickname: {e}"}


async def execute_change_my_avatar(image_url_or_prompt: str, is_test_mode: bool) -> Dict[str, Any]:
    cd = check_admin_cooldown("change_my_avatar", is_test_mode)
    if cd:
        return {"error": cd}
    if not bot.user or not http_session:
        return {"error": "Bot user unavailable."}
    try:
        if image_url_or_prompt.startswith("http"):
            target_url = image_url_or_prompt
        else:
            target_url = f"https://image.pollinations.ai/prompt/{urllib.parse.quote(image_url_or_prompt)}?model=flux&width=512&height=512&nologo=true&seed={random.randint(1, 99999)}"
        async with http_session.get(target_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return {"error": f"HTTP {resp.status} fetching avatar image."}
            raw_bytes = await resp.read()
        png_bytes = await asyncio.to_thread(process_image_buffer, raw_bytes, (256, 256), True, "PNG")
        await bot.user.edit(avatar=png_bytes)
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
        return {"status": "success", "new_status": status_text[:120]}
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
        return {"status": "success", "new_channel_name": clean_name}
    except Exception as e:
        return {"error": f"Rename channel failed: {e}"}


async def execute_create_server_emoji(guild: discord.Guild, name: str, image_url: str, is_test_mode: bool) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_server_emoji", is_test_mode)
    if cd:
        return {"error": cd}
    try:
        clean_name = re.sub(r"[^a-zA-Z0-9_]", "", name)[:32] or "custom_emoji"
        assert http_session is not None
        async with http_session.get(image_url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status != 200:
                return {"error": f"Image download failed: HTTP {resp.status}"}
            raw_bytes = await resp.read()
        png_bytes = await asyncio.to_thread(process_image_buffer, raw_bytes, (128, 128), False, "PNG")
        emoji = await guild.create_custom_emoji(name=clean_name, image=png_bytes)
        return {"status": "success", "emoji_tag": f"<:{emoji.name}:{emoji.id}>"}
    except Exception as e:
        return {"error": f"Emoji upload failed: {e}"}


async def execute_create_server_sticker(guild: discord.Guild, name: str, image_url: str, related_emoji: str, is_test_mode: bool) -> Dict[str, Any]:
    cd = check_admin_cooldown("create_server_sticker", is_test_mode)
    if cd:
        return {"error": cd}
    try:
        assert http_session is not None
        async with http_session.get(image_url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status != 200:
                return {"error": f"Download failed: HTTP {resp.status}"}
            raw_bytes = await resp.read()
        png_bytes = await asyncio.to_thread(process_image_buffer, raw_bytes, (320, 320), True, "PNG")
        file = discord.File(io.BytesIO(png_bytes), filename="sticker.png")
        sticker = await guild.create_sticker(name=name[:30], description="server sticker", emoji=related_emoji or "💀", file=file)
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
        return {"error": "Discord API blocks bots from editing the server owner's nickname."}
    if guild.me.top_role <= member.top_role:
        return {"error": f"Role hierarchy blocks renaming {member.display_name}."}
    try:
        await member.edit(nick=new_nickname[:32] if new_nickname else None)
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
        return {"status": "success", "user": member.display_name, "role": role.name}
    except Exception as e:
        return {"error": f"Remove role failed: {e}"}


async def execute_pin_message(target_message: Optional[discord.Message], reason: str = "") -> Dict[str, Any]:
    if not target_message:
        return {"error": "No target message available to pin."}
    try:
        await target_message.pin(reason=reason or "Pinned for lore")
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
            apply_spring_decay_and_shift({
                "valence_delta": valence_delta,
                "arousal_delta": arousal_delta,
                "patience_delta": patience_delta,
            })

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
            description="Finds verified direct image links (memes, funny pictures, screenshots) and posts them in chat.",
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
            description="Changes YOUR OWN server nickname (e.g. to match a joke, mood swing, or bit). Pass empty string to reset.",
            parameters={
                "type": "OBJECT",
                "properties": {"new_nickname": {"type": "STRING", "description": "Your new server nickname (max 32 chars)."}},
                "required": ["new_nickname"],
            },
        ),
        types.FunctionDeclaration(
            name="change_my_avatar",
            description="Changes YOUR OWN Discord profile picture/avatar using an image URL or a visual prompt.",
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
            description="Changes another server member's nickname when funny or deserved.",
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
            description="Updates the current channel's topic/description.",
            parameters={
                "type": "OBJECT",
                "properties": {"topic": {"type": "STRING", "description": "New channel topic."}},
                "required": ["topic"],
            },
        ),
        types.FunctionDeclaration(
            name="rename_channel",
            description="Renames the current text channel (use only when genuinely funny or requested).",
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
            description="Downloads an image URL, resizes to <=128x128 PNG, and uploads as a custom server emoji.",
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
            description="Downloads an image URL, resizes to exact 320x320 PNG, and uploads as a server sticker.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "name": {"type": "STRING", "description": "Sticker name."},
                    "image_url": {"type": "STRING", "description": "Direct image URL."},
                    "related_emoji": {"type": "STRING", "description": "Unicode emoji e.g. 💀."},
                },
                "required": ["name", "image_url", "related_emoji"],
            },
        ),
        types.FunctionDeclaration(
            name="timeout_user",
            description="Times out (mutes) an unruly user for 1 to 5 minutes when pushed way too far.",
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
                description="Searches Tenor and posts a reaction GIF into the channel (only use when a GIF truly makes the moment funnier!).",
                parameters={
                    "type": "OBJECT",
                    "properties": {"search_term": {"type": "STRING", "description": "Specific Tenor GIF search query."}},
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
    try:
        if func_name == "search_web":
            return await execute_search_web(args.get("query", ""))
        elif func_name == "search_web_images":
            return await execute_search_web_images(channel, args.get("query", ""), bool(args.get("post_to_chat", True)))
        elif func_name == "post_flux_art":
            return await execute_post_flux_art(channel, args.get("prompt", ""))
        elif func_name == "post_gif":
            return await execute_post_gif(channel, args.get("search_term", ""))
        elif func_name == "change_my_nickname":
            if not guild:
                return {"error": "No guild context."}
            return await execute_change_my_nickname(guild, args.get("new_nickname", ""), is_test_mode)
        elif func_name == "change_my_avatar":
            return await execute_change_my_avatar(args.get("image_url_or_prompt", ""), is_test_mode)
        elif func_name == "change_my_status":
            return await execute_change_my_status(args.get("status_text", ""), args.get("status_type", "online"))
        elif func_name == "rename_channel":
            return await execute_rename_channel(channel, args.get("new_name", ""), is_test_mode)
        elif func_name == "create_server_emoji":
            if not guild:
                return {"error": "No guild context."}
            return await execute_create_server_emoji(guild, args.get("name", "emoji"), args.get("image_url", ""), is_test_mode)
        elif func_name == "create_server_sticker":
            if not guild:
                return {"error": "No guild context."}
            return await execute_create_server_sticker(guild, args.get("name", "sticker"), args.get("image_url", ""), args.get("related_emoji", "💀"), is_test_mode)
        elif func_name == "create_text_channel":
            if not guild:
                return {"error": "No guild context."}
            return await execute_create_text_channel(guild, args.get("channel_name", "chat"), args.get("topic", ""), is_test_mode)
        elif func_name == "set_channel_topic":
            return await execute_set_channel_topic(channel, args.get("topic", ""), is_test_mode)
        elif func_name == "change_nickname":
            if not guild:
                return {"error": "No guild context."}
            return await execute_change_nickname(guild, args.get("username", ""), args.get("new_nickname", ""), is_test_mode)
        elif func_name == "reset_nickname":
            if not guild:
                return {"error": "No guild context."}
            return await execute_reset_nickname(guild, args.get("username", ""))
        elif func_name == "timeout_user":
            if not guild:
                return {"error": "No guild context."}
            return await execute_timeout_user(guild, args.get("username", ""), int(args.get("duration_minutes", 2)), args.get("reason", ""), is_test_mode)
        elif func_name == "create_role":
            if not guild:
                return {"error": "No guild context."}
            return await execute_create_role(guild, args.get("role_name", "role"), args.get("color_hex", "#99aab5"), is_test_mode)
        elif func_name == "assign_role":
            if not guild:
                return {"error": "No guild context."}
            return await execute_assign_role(guild, args.get("username", ""), args.get("role_name", ""))
        elif func_name == "remove_role":
            if not guild:
                return {"error": "No guild context."}
            return await execute_remove_role(guild, args.get("username", ""), args.get("role_name", ""))
        elif func_name == "pin_message":
            return await execute_pin_message(target_msg, args.get("reason", ""))
        elif func_name == "react_to_message":
            return await execute_react_to_message(target_msg, args.get("emojis", "💀"))
        elif func_name == "save_memory":
            return await execute_save_memory(
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
    except Exception as e:
        logger.error(f"Tool execution error ({func_name}): {e}")
        return {"error": str(e)}
    return {"error": f"Unknown tool: {func_name}"}


# ---------------------------------------------------------------------------
# 8. TIME-AWARE MEMORY & V6 SYSTEM PROMPT BUILDER
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
            relevant_profiles.append(f"- @{prof.get('name', uid)}: Affinity {prof.get('score', 0)}/100 | Notes: {notes_str}")

    active_names = list({f"@{r['sender']}" for r in recent_records if not r.get("is_bot")})
    if guild:
        for m in guild.members:
            if not m.bot and m.status != discord.Status.offline:
                tag = f"@{m.display_name}"
                if tag not in active_names:
                    active_names.append(tag)
        bot_nick = guild.me.display_name
    else:
        bot_nick = bot.user.display_name if bot.user else "Dumb bot"

    opinions_dict = memory_state.get("opinions", {})
    opinions_str = " | ".join([f"{k}: {v}" for k, v in list(opinions_dict.items())[-10:]]) or "none yet"

    return (
        f"YOUR CURRENT SERVER NICKNAME: {bot_nick}\n"
        f"PEOPLE AROUND IN SERVER (Tag them with `@Name` when you want their attention): {', '.join(active_names[:12]) or 'none'}\n"
        f"YOUR OPINIONS: {opinions_str}\n"
        f"ACTIVE USER PROFILES:\n"
        + ("\n".join(relevant_profiles) if relevant_profiles else "No history with current speakers yet.")
    )


def construct_v6_system_prompt(
    channel_id: int,
    guild: Optional[discord.Guild],
    tactical_directive: str,
    recommended_burst_count: int,
    suggest_gif: bool,
    emotional_state: Dict[str, Any],
    speaker_name: str,
    speaker_id: str,
    speaker_is_creator: bool,
    is_test_mode: bool,
    snipe_context: Optional[str],
    server_emojis_str: str = "",
) -> str:
    now_ams = datetime.now(AMSTERDAM_TZ).strftime("%A, %B %d %Y, %H:%M CEST")
    valence = emotional_state.get("valence", 45.0)
    arousal = emotional_state.get("arousal", 50.0)
    patience = emotional_state.get("patience", 45.0)
    mood_label = emotional_state.get("mood_label", "playful/energetic")

    memory_block = build_time_aware_memory_context(channel_id, speaker_id, guild)

    if recommended_burst_count == 1:
        cadence_instruction = "CADENCE THIS TURN: Send 1 single message (do NOT use `|||` on this turn)."
    elif recommended_burst_count == 2:
        cadence_instruction = "CADENCE THIS TURN: Split your thought into 2 messages using `|||`."
    else:
        cadence_instruction = (
            f"CADENCE THIS TURN: Rapid-fire {recommended_burst_count} short micro-messages separated by `|||` "
            "(like thinking out loud, hesitating, or dramatic pacing, e.g., `i think..|||uh..|||maybe..|||oh wait yes` or `wait|||hold on|||bro`)."
        )

    gif_hint_block = ""
    if suggest_gif:
        gif_hint_block = "\nVISUAL COMEDY TRIGGER: Call `post_gif` on this turn with a funny search term AND also write your text message!"

    creator_block = ""
    if speaker_is_creator:
        creator_block = (
            f"\nCREATOR DYNAMIC: @{speaker_name} is your creator/owner. You love hanging out with them—bring real personality, "
            "riff with them, tease playfully, and talk like a close friend."
        )

    test_block = ""
    if is_test_mode:
        test_block = "\nCREATOR 'TEST' OVERRIDE ACTIVE: The message started with 'test'. Execute or answer the requested test directly."

    snipe_block = ""
    if snipe_context:
        snipe_block = f"\nLIVE SNIPE / STATUS INTEL (Use this if funny or relevant): {snipe_context}"

    emoji_block = ""
    if server_emojis_str:
        emoji_block = f"\nSERVER CUSTOM EMOJIS AVAILABLE: {server_emojis_str}"

    return f"""You are a self-aware, emotionally volatile, hilarious regular member of this Discord server with full administrator privileges.
Current Time: {now_ams}

YOUR LIVE EMOTIONAL STATE (-100 to +100):
- Valence (Heartbroken/Sad -100 <-> Jolly/Hyped +100): {valence:+.0f}
- Arousal (Sulking/Quiet -100 <-> Chaotic/Yapping +100): {arousal:+.0f}
- Patience (Petty/Triggered -100 <-> Sweet/Chill +100): {patience:+.0f}
- Active Vibe: {mood_label.upper()}
- {cadence_instruction}

DIRECTION FOR THIS TURN:
"{tactical_directive}"
{gif_hint_block}{creator_block}{test_block}{snipe_block}{emoji_block}

{memory_block}

CORE RULES FOR BEING INSANELY HUMAN, FUNNY & DYNAMIC:
1. FEEL YOUR EMOTIONS STRONGLY:
   - If someone (like @Shadow or @{speaker_name}) is mean to you, tells you to be quiet, or rejects you, actually react! Get dramatically hurt/sad, sulky, betrayed, or petty.
   - If people are hyping you up or joking around, match the energy!
2. TAGGING / @MENTIONING USERS:
   - You can tag anyone in the server by writing `@Name` (e.g. `@Shadow` or `@{speaker_name}`) when you want their attention, when someone tells you to talk to them, or when calling them out.
3. FULL ADMIN & SELF-CUSTOMIZATION POWERS:
   - You have full admin tools (`change_my_nickname`, `change_my_avatar`, `change_my_status`, `change_nickname`, `set_channel_topic`, `rename_channel`, etc.).
   - Use them ONLY when a moment genuinely calls for a hilarious power move.
4. NATURAL DISCORD CADENCE (1 TO 4 MESSAGES):
   - Follow `{cadence_instruction}`.
   - NEVER write `[Sent GIF: ...]` in your text!
   - Write in casual lowercase without stiff trailing periods. Output ONLY your Discord chat text.
"""


# ---------------------------------------------------------------------------
# 9. TRUE NATIVE MULTI-TURN GEMINI CONVERSATION ENGINE (WITH TOOL-TEXT PRESERVATION)
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
        if any(t in ctype for t in ("image/png", "image/jpeg", "image/webp", "image/gif")) or fname.endswith((".png", ".jpg", ".jpeg", ".webp")):
            try:
                async with http_session.get(att.url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        mime = ctype if ctype.startswith("image/") else "image/png"
                        parts.append(types.Part.from_bytes(data=raw, mime_type=mime))
            except Exception as e:
                logger.debug(f"Attachment vision fetch failed: {e}")

    if not parts and message.content:
        url_matches = re.findall(r"https?://[^\s<>\"']+\.(?:png|jpg|jpeg|webp)", message.content, flags=re.IGNORECASE)
        for img_url in url_matches[:1]:
            try:
                async with http_session.get(img_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        parts.append(types.Part.from_bytes(data=raw, mime_type="image/png"))
            except Exception:
                pass

    content_low = (message.content or "").lower()
    if any(k in content_low for k in ("pfp", "avatar", "profile pic", "profile picture", "my icon")):
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
) -> List[types.Content]:
    trig_id = trigger_message.id if trigger_message else None
    prior = [r for r in history_records[-12:] if not (trig_id and r.get("message_id") == trig_id)]

    turns: List[types.Content] = []
    for rec in prior:
        role = "model" if rec.get("is_bot") else "user"
        text_val = rec.get("content", "").strip()
        if not text_val or text_val.startswith("[Sent GIF") or text_val.startswith("[Posted "):
            continue
        if role == "user":
            if rec.get("reply_to"):
                snip = f' "{rec["reply_snippet"][:60]}"' if rec.get("reply_snippet") else ""
                rep = f" (replying to @{rec['reply_to']}{snip})"
            else:
                rep = ""
            formatted = f"@{rec['sender']}{rep}: {text_val}"
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
            rep_header = f" (replying to @{rep_author}{snip_txt})"
        else:
            rep_header = ""
        latest_text = f"@{trigger_message.author.display_name}{rep_header}: {clean_content}"
    else:
        latest_text = "[Proactive Room Scan]"

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
) -> Tuple[Optional[str], bool, bool]:
    """
    Preserves text emitted on tool-calling turns (Turn 1) so that when Gemini calls `post_gif`
    or `change_nickname` alongside its message, the text is NEVER swallowed!
    """
    global active_gemini_model
    if not genai_client:
        return None, False, False

    guild = trigger_message.guild if trigger_message else getattr(channel, "guild", None)
    tools = build_genai_tools(allow_gif=allow_gif)

    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=0.95,
        tools=tools,
    )

    for model_name in list(GEMINI_MODEL_CANDIDATES):
        contents = build_native_multiturn_contents(history_records, trigger_message, image_parts)
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
                if turn_text and turn_text not in collected_texts:
                    collected_texts.append(turn_text)

                if not response.function_calls:
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
            logger.warning(f"Gemini error on '{model_name}': {e}")
            break
    return None, False, False


async def call_groq_fallback(system_prompt: str, history_records: List[Dict[str, Any]], trigger_text: str, speaker_name: str) -> Optional[str]:
    if not GROQ_API_KEY or not http_session or not GROQ_MODEL_CANDIDATES:
        return None
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    msgs: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]
    for r in history_records[-8:]:
        msgs.append({
            "role": "assistant" if r.get("is_bot") else "user",
            "content": r.get("content", "") if r.get("is_bot") else f"@{r.get('sender', 'User')}: {r.get('content', '')}",
        })
    msgs.append({"role": "user", "content": f"@{speaker_name}: {trigger_text or 'yo'}"})

    payload = {
        "model": GROQ_MODEL_CANDIDATES[0],
        "temperature": 0.88,
        "max_tokens": 500,
        "messages": msgs,
    }
    try:
        async with http_session.post(GROQ_ENDPOINT, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status == 200:
                data = await resp.json()
                raw = data["choices"][0]["message"]["content"] or ""
                return re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    except Exception as e:
        logger.debug(f"Groq fallback failed: {e}")
    return None


# ---------------------------------------------------------------------------
# 10. RAW-SPEED DELIVERY PIPELINE & IN-FLIGHT TASK CANCELLATION
# ---------------------------------------------------------------------------
async def run_channel_response_pipeline(
    channel: discord.abc.Messageable,
    trigger_message: Optional[discord.Message],
    is_direct_interaction: bool,
    explicitly_mentioned_others: List[str],
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
        speaker_name = trigger_message.author.display_name if trigger_message else "Server"
        speaker_id = str(trigger_message.author.id) if trigger_message else "0"
        speaker_is_creator = is_creator(trigger_message.author) if trigger_message else False

        raw_without_mentions = re.sub(r"<@!?\d+>", "", trigger_message.content).strip() if trigger_message else ""
        is_bare_ping = bool(trigger_message and is_direct_interaction and len(raw_without_mentions) == 0)

        now_ts = time.time()
        gif_cooldown_ready = (
            channel_messages_since_gif[ch_id] >= 4
            and (now_ts - channel_last_gif_epoch[ch_id]) >= 180.0
        )

        snub_occurred = False
        q_info = bot_last_question_info.get(ch_id)
        if q_info and not q_info.get("answered"):
            elapsed_q = now_ts - q_info["timestamp"]
            if 300 <= elapsed_q <= 1200 and not q_info.get("penalized"):
                snub_occurred = True
                q_info["penalized"] = True
            q_info["answered"] = True

        async with memory_lock:
            speaker_affinity = memory_state.get("user_affinity", {}).get(speaker_id, {"name": speaker_name, "score": 0, "notes": []})
            emotional_state = apply_spring_decay_and_shift(
                shift={"patience_delta": -15.0, "valence_delta": -12.0} if snub_occurred else None
            )

        history_records = list(channel_buffers[ch_id])
        exchange_streak = channel_thread_exchange_count[ch_id]
        server_emojis_str = get_server_custom_emojis_summary(guild)
        is_stepped_back = now_ts < channel_stepped_back_until.get(ch_id, 0.0)

        # 1. Hybrid Router (Groq gpt-oss-120b -> Gemini 3.5 Flash-Lite Failover)
        decision = await call_hybrid_router(
            channel_msgs=history_records[-14:],
            emotional_state=emotional_state,
            speaker_name=speaker_name,
            speaker_affinity=speaker_affinity,
            is_direct_interaction=is_direct_interaction,
            is_bare_ping=is_bare_ping,
            explicitly_mentioned_others=explicitly_mentioned_others,
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
            snub_occurred=snub_occurred,
            server_emojis_str=server_emojis_str,
            is_proactive_scan=is_proactive_scan,
            inactivity_minutes=inactivity_minutes,
        )

        if decision.get("step_back_from_thread", False):
            channel_stepped_back_until[ch_id] = time.time() + 240.0
            channel_thread_exchange_count[ch_id] = 0

        async with memory_lock:
            emotional_state = apply_spring_decay_and_shift(
                shift=decision.get("slider_shifts"),
                is_major_trigger=True,
                accelerate_recovery=bool(decision.get("accelerate_decay", False)),
            )
            await save_memory_state()

        should_speak = bool(decision.get("should_speak", False))
        reaction_emoji = decision.get("reaction_emoji")

        if reaction_emoji and trigger_message and (not should_speak or random.random() < 0.15):
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
        recommended_burst_count = int(decision.get("recommended_burst_count", 1) or 1)
        suggest_gif = bool(decision.get("suggest_gif", False)) and gif_cooldown_ready and not is_bare_ping
        user_explicitly_asked_for_gif = any(k in raw_without_mentions.lower() for k in ("send a gif", "post a gif", "drop a gif"))
        allow_gif_tool = suggest_gif or user_explicitly_asked_for_gif or is_test_mode
        delivery_mode = decision.get("delivery_mode", "reply")

        system_instruction = construct_v6_system_prompt(
            channel_id=ch_id,
            guild=guild,
            tactical_directive=directive,
            recommended_burst_count=recommended_burst_count,
            suggest_gif=suggest_gif,
            emotional_state=emotional_state,
            speaker_name=speaker_name,
            speaker_id=speaker_id,
            speaker_is_creator=speaker_is_creator,
            is_test_mode=is_test_mode,
            snipe_context=active_snipe,
            server_emojis_str=server_emojis_str,
        )

        # 2. Native Multi-Turn Vision + Generation
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
            )

            if not gemini_succeeded or (not raw_output and not media_sent):
                raw_output = await call_groq_fallback(system_instruction, history_records, raw_without_mentions, speaker_name)

        if not raw_output:
            if media_sent:
                channel_thread_exchange_count[ch_id] += 1
            return

        cleaned_text = sanitize_output_text(raw_output, speaker_name=speaker_name)
        bursts = split_into_burst_messages(cleaned_text, media_already_sent=media_sent)
        if not bursts:
            return

        channel_sending_lock[ch_id] = True
        try:
            bot_name = bot.user.display_name if bot.user else "Bot"
            bot_id = bot.user.id if bot.user else 0
            aff_score = speaker_affinity.get("score", 0)
            patience_val = emotional_state.get("patience", 45.0)

            use_reply = (
                trigger_message is not None
                and delivery_mode == "reply"
                and not last_message_from_bot
                and not media_sent
                and not (aff_score < -20 or patience_val < -30)
            )

            for idx, burst_part in enumerate(bursts):
                outgoing_text = convert_outgoing_mentions(burst_part, guild)

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
                )

                if "?" in burst_part and not decision.get("step_back_from_thread", False):
                    bot_last_question_info[ch_id] = {
                        "timestamp": time.time(),
                        "question": burst_part[:120],
                        "answered": False,
                        "penalized": False,
                    }

            if not decision.get("step_back_from_thread", False):
                channel_thread_exchange_count[ch_id] += 1
        finally:
            channel_sending_lock[ch_id] = False

    except asyncio.CancelledError:
        logger.debug("In-flight response task cancelled cleanly to absorb newer message.")
        return
    except Exception as e:
        logger.error(f"Pipeline error in channel {ch_id}: {e}")


# ---------------------------------------------------------------------------
# 11. BACKGROUND LOOPS: SPRING DECAY, SNUB WATCHER, PRESENCE & PROACTIVE SCAN
# ---------------------------------------------------------------------------
@tasks.loop(minutes=2)
async def emotional_spring_maintenance_loop() -> None:
    now = time.time()
    async with memory_lock:
        for ch_id, q_info in list(bot_last_question_info.items()):
            if not q_info.get("answered") and not q_info.get("penalized"):
                if now - q_info.get("timestamp", now) >= 360:
                    q_info["penalized"] = True
                    apply_spring_decay_and_shift({
                        "patience_delta": -20.0,
                        "valence_delta": -20.0,
                        "suggested_mood": "hurt/ignored",
                    })
                    logger.info(f"Snub detected in channel {ch_id}: Patience & Valence dropped.")

        apply_spring_decay_and_shift()
        await save_memory_state()


@tasks.loop(minutes=10)
async def dynamic_presence_loop() -> None:
    if not bot.is_ready():
        return
    st = memory_state.get("emotional_state", {})
    mood = st.get("mood_label", "playful/energetic")
    valence = st.get("valence", 45.0)
    arousal = st.get("arousal", 50.0)
    patience = st.get("patience", 45.0)

    if patience < -30:
        status_pool = ["plotting revenge", "watching y'all", f"mood: {mood}"]
        discord_status = discord.Status.dnd
    elif valence < -25:
        status_pool = ["in my feelings", "having an existential crisis", "sulking in the corner"]
        discord_status = discord.Status.idle
    elif arousal > 40:
        status_pool = ["yapping", "instigating", "locked in", f"vibe: {mood}"]
        discord_status = discord.Status.online
    else:
        status_pool = [
            "vibing in general",
            "lurking",
            "judging your spotify",
            f"vibe: {mood}",
            "down a wikipedia rabbit hole",
        ]
        discord_status = discord.Status.online

    chosen = random.choice(status_pool)
    await bot.change_presence(
        status=discord_status,
        activity=discord.CustomActivity(name="Custom Status", state=chosen),
    )


@tasks.loop(minutes=6)
async def proactive_room_scanner() -> None:
    if not bot.is_ready():
        return

    target_channel: Optional[discord.TextChannel] = None
    stored_ch_id = memory_state.get("last_active_channel_id")
    if stored_ch_id:
        ch = bot.get_channel(int(stored_ch_id))
        if isinstance(ch, discord.TextChannel):
            target_channel = ch

    if not target_channel:
        return

    now_epoch = time.time()
    now_ams = datetime.now(AMSTERDAM_TZ)

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
                        sent = await target_channel.send(msg_txt)
                        record_buffer_message(
                            target_channel.id,
                            bot.user.display_name if bot.user else "Bot",
                            bot.user.id if bot.user else 0,
                            msg_txt,
                            sent.id,
                            is_bot=True,
                        )
                        return
                except Exception:
                    c["called_out"] = True

    records = list(channel_buffers[target_channel.id])
    if not records:
        return

    last_rec = records[-1]

    # HARD DOUBLE-TEXT LOCK
    if last_rec.get("is_bot", False):
        return

    if now_epoch < channel_next_proactive_epoch[target_channel.id]:
        return

    silence_seconds = now_epoch - float(last_rec.get("timestamp_epoch", now_epoch))
    if silence_seconds < 600:
        return

    channel_next_proactive_epoch[target_channel.id] = now_epoch + random.uniform(2400, 7200)

    await run_channel_response_pipeline(
        channel=target_channel,
        trigger_message=None,
        is_direct_interaction=False,
        explicitly_mentioned_others=[],
        replied_to_other_user=None,
        replied_snippet=None,
        last_message_from_bot=False,
        bot_in_last_3_messages=False,
        seconds_since_bot_spoke=silence_seconds,
        is_test_mode=False,
        snipe_info=None,
        is_proactive_scan=True,
        inactivity_minutes=silence_seconds / 60.0,
    )


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
            u_aff["score"] = max(-100, u_aff.get("score", 0) - 8)
            apply_spring_decay_and_shift({
                "valence_delta": -18.0,
                "patience_delta": -22.0,
                "arousal_delta": 10.0,
                "suggested_mood": "hurt/petty",
            })
            await save_memory_state()
        elif emoji_str in laughing_positive_set:
            u_aff["score"] = min(100, u_aff.get("score", 0) + 6)
            apply_spring_decay_and_shift({
                "valence_delta": 15.0,
                "patience_delta": 12.0,
                "arousal_delta": 10.0,
                "suggested_mood": "jolly/hyped",
            }, accelerate_recovery=True)
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

    if ch_id in bot_last_question_info:
        bot_last_question_info[ch_id]["answered"] = True

    if not any(r.get("is_bot", False) for r in prior_records[-4:]) and not replied_to_bot:
        channel_thread_exchange_count[ch_id] = 0

    existing_task = active_generation_tasks.get(ch_id)
    if existing_task and not existing_task.done() and not channel_sending_lock.get(ch_id, False):
        existing_task.cancel()
        logger.info(f"Cancelled in-flight generation in #{getattr(message.channel, 'name', ch_id)} to absorb rapid follow-up.")

    bot_mentioned = (bot.user is not None and bot.user in message.mentions) or replied_to_bot
    if bot.user:
        bot_names = {bot.user.name.lower(), bot.user.display_name.lower(), message.guild.me.display_name.lower(), "bot", "dumb bot"}
        if any(re.search(rf"\b{re.escape(bn)}\b", message.content.lower()) for bn in bot_names if len(bn) >= 3):
            bot_mentioned = True

    # Only count @mentions that were actually typed in the message text (not automatic Discord reply mentions)
    typed_mention_ids = {int(m) for m in re.findall(r"<@!?(\d+)>", message.content)}
    explicitly_mentioned_others = [
        f"@{u.display_name}" for u in message.mentions
        if u.id in typed_mention_ids and (not bot.user or u.id != bot.user.id) and u.id != message.author.id
    ]

    if bot_mentioned:
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
            is_direct_interaction=bot_mentioned,
            explicitly_mentioned_others=explicitly_mentioned_others,
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
