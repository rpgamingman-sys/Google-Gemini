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
logger = logging.getLogger("AutonomousMetaBotV4")

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
CLOUD_BACKUP_MARKER = "[[MEMORY_BACKUP_V4_JSON]]"
logger.info(f"Persistent memory target: {MEMORY_FILE}")

# Primary Gemini Model: gemini-3.5-flash-lite with automatic cascade
ENV_GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")
GEMINI_MODEL_CANDIDATES: List[str] = list(dict.fromkeys([
    ENV_GEMINI_MODEL,
    "gemini-3.5-flash-lite",
    "gemini-3.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.0-flash",
]))
active_gemini_model: str = GEMINI_MODEL_CANDIDATES[0]

# Groq Router Models (Auto-discovered on startup + falls back to Gemini 3.5 Flash-Lite if rate-limited)
ENV_GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
GROQ_MODEL_CANDIDATES: List[str] = list(dict.fromkeys([
    ENV_GROQ_MODEL,
    "openai/gpt-oss-120b",
    "openai/gpt-oss-20b",
    "meta-llama/llama-4-scout-17b-16e-instruct",
    "qwen/qwen3-32b",
]))
active_groq_model: str = GROQ_MODEL_CANDIDATES[0]
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODELS_ENDPOINT = "https://api.groq.com/openai/v1/models"

# Strip corporate AI & corny 1980s sci-fi robot clichés
BANNED_WORDS_MAP = {
    r"\bas an ai\b": "honestly",
    r"\bas a large language model\b": "honestly",
    r"\bhow can i assist you\b": "what's up",
    r"\bmortals\b": "people",
    r"\beternal consciousness\b": "my brain",
    r"\bmy circuits\b": "my brain",
    r"\bcircuits\b": "braincells",
    r"\bfactory reset\b": "coma",
    r"\bbuffering in 144p\b": "lagging",
    r"\bsubroutines\b": "thoughts",
    r"\bmotherboard\b": "head",
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
channel_buffers: Dict[int, deque] = defaultdict(lambda: deque(maxlen=60))
active_generation_tasks: Dict[int, asyncio.Task] = {}
channel_sending_lock: Dict[int, bool] = defaultdict(bool)

# Gated Sniping Caches
deleted_message_cache: Dict[int, Dict[str, Any]] = {}
edited_message_cache: Dict[int, Dict[str, Any]] = {}
user_snipe_cooldowns: Dict[int, float] = defaultdict(float)

# Conversation Flow, Lurk Override, Snub & Proactive State
channel_last_bot_spoke_epoch: Dict[int, float] = defaultdict(float)
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
    "set_channel_topic": 600.0,
    "timeout_user": 300.0,
    "change_nickname": 120.0,
}

# ---------------------------------------------------------------------------
# 3. PERSISTENT MEMORY, TIME-AWARENESS & 3D EMOTIONAL ENGINE
# ---------------------------------------------------------------------------
BASELINE_SLIDERS = {
    "valence": 25.0,   # -100 (Depressed/Salty/Existential) <---> +100 (Jolly/Hyped/Playful)
    "arousal": 20.0,   # -100 (Sluggish/Existential dread) <---> +100 (Chaotic/Yapping/Hyper)
    "patience": 35.0,  # -100 (Petty/Hostile/Roasty) <---> +100 (Warm/Chill/Good-humored)
}

DEFAULT_MEMORY = {
    "emotional_state": {
        "valence": 25.0,
        "arousal": 20.0,
        "patience": 35.0,
        "mood_label": "playful",
        "last_updated_epoch": time.time(),
    },
    "opinions": {},
    "last_active_channel_id": None,
    "user_affinity": {},
    "episodic_lore": [],
    "active_commitments": [],
}


def format_relative_age(iso_or_epoch: Any) -> str:
    """Converts a timestamp into human-readable relative age so Gemini knows when something is old news."""
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
            if "valence" not in data.get("emotional_state", {}):
                data["emotional_state"] = json.loads(json.dumps(DEFAULT_MEMORY["emotional_state"]))
            recursive_merge_defaults(data, DEFAULT_MEMORY)

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
# 4. RICH DYNAMIC MOOD ENGINE (JOLLY, EXISTENTIAL CRISIS, DEPRESSED, CHAOTIC, ETC.)
# ---------------------------------------------------------------------------
def derive_emergent_mood(valence: float, arousal: float, patience: float) -> str:
    """Maps continuous 3D sliders into a wide, colorful spectrum of emotions."""
    if patience < -45 and valence < -35:
        return "heated/roasty" if arousal > 15 else "petty/cold"
    if patience < -20 and valence < -15:
        return "sarcastic/annoyed"
    if valence < -35 and arousal < -15:
        return "depressed/dramatic despair"
    if valence < -15 and abs(arousal) <= 25:
        return "existential crisis about being trapped in discord"
    if arousal < -40:
        return "sleepy/zoned out"
    if valence > 45 and arousal > 45:
        return "jolly/chaotic hype"
    if valence > 35 and patience > 30:
        return "jolly/warm/affectionate"
    if valence > 15 and arousal > 15:
        return "playful/teasing/funny"
    if arousal > 35 and patience < 10:
        return "instigating/trolling"
    return "chill/playful"


def apply_spring_decay_and_shift(
    shift: Optional[Dict[str, Any]] = None,
    is_major_trigger: bool = False,
    accelerate_recovery: bool = False,
) -> Dict[str, Any]:
    st = memory_state.setdefault("emotional_state", dict(DEFAULT_MEMORY["emotional_state"]))
    now = time.time()
    last_t = float(st.get("last_updated_epoch", now))
    dt = max(0.0, now - last_t)

    half_life = 600.0 if accelerate_recovery else 1500.0
    decay_factor = math.pow(0.5, dt / half_life)
    clamp_limit = 35.0 if is_major_trigger else 18.0

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
# 5. CONTEXT BUFFERS, REPLY RESOLVER & MEDIA HELPERS
# ---------------------------------------------------------------------------
def record_buffer_message(
    channel_id: int,
    sender: str,
    sender_id: int,
    content: str,
    msg_id: int,
    is_bot: bool,
    has_media: bool = False,
    reply_to: Optional[str] = None,
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
        "has_media": has_media,
        "timestamp_epoch": now_utc.timestamp(),
        "timestamp": now_ams.strftime("%H:%M"),
        "message_id": msg_id,
        "is_bot": is_bot,
    })


def resolve_referenced_author(message: discord.Message, channel_id: int) -> Tuple[Optional[str], bool, bool]:
    if not message.reference:
        return None, False, False

    ref_obj = message.reference.resolved or message.reference.cached_message
    if isinstance(ref_obj, discord.Message):
        is_bot_target = bool(bot.user and ref_obj.author.id == bot.user.id)
        return ref_obj.author.display_name, is_bot_target, (not is_bot_target)

    ref_id = message.reference.message_id
    if ref_id:
        for rec in reversed(channel_buffers[channel_id]):
            if rec.get("message_id") == ref_id:
                is_bot_target = bool(rec.get("is_bot", False))
                return rec.get("sender"), is_bot_target, (not is_bot_target)

    return None, False, False


async def hydrate_channel_buffer(channel: discord.abc.Messageable) -> None:
    ch_id = getattr(channel, "id", None)
    if not ch_id or len(channel_buffers[ch_id]) > 0 or not hasattr(channel, "history"):
        return
    try:
        raw_msgs = [m async for m in channel.history(limit=45)]  # type: ignore
        raw_msgs.reverse()
        for m in raw_msgs:
            clean = re.sub(r"<a?:([a-zA-Z0-9_]+):\d+>", r":\1:", m.content).strip()
            if m.attachments:
                clean += f" [Image/Attachment: {', '.join(a.filename for a in m.attachments)}]"
            if m.stickers:
                clean += f" [Sticker: {', '.join(s.name for s in m.stickers)}]"
            reply_author, _, _ = resolve_referenced_author(m, ch_id)
            is_b = (bot.user is not None and m.author.id == bot.user.id)
            if is_b:
                channel_last_bot_spoke_epoch[ch_id] = m.created_at.timestamp()
            channel_buffers[ch_id].append({
                "sender": m.author.display_name,
                "sender_id": m.author.id,
                "content": clean,
                "reply_to": reply_author,
                "has_media": bool(m.attachments or m.stickers),
                "timestamp_epoch": m.created_at.timestamp(),
                "timestamp": m.created_at.astimezone(AMSTERDAM_TZ).strftime("%H:%M"),
                "message_id": m.id,
                "is_bot": is_b,
            })
        logger.info(f"Hydrated #{getattr(channel, 'name', ch_id)} with {len(raw_msgs)} messages.")
    except Exception as e:
        logger.debug(f"Hydration failed for {ch_id}: {e}")


def get_repeated_sentence_openers(channel_id: int) -> List[str]:
    bot_msgs = [r["content"].lower() for r in list(channel_buffers[channel_id])[-20:] if r.get("is_bot")]
    recent = bot_msgs[-8:]
    if not recent:
        return []
    counts: Dict[str, int] = defaultdict(int)
    ignore = {"i", "the", "a", "it", "you", "that", "wait", "oh"}
    for txt in recent:
        words = re.findall(r"[a-z']+", txt)
        if words and words[0] not in ignore:
            counts[words[0]] += 1
    return [w for w, c in counts.items() if c >= 2][:4]


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
    """Strips any accidental <thought> tags or third-person self-narration lines."""
    if not text:
        return ""
    text = re.sub(r"<thought>.*?</thought>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<thought>.*", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)

    # Filter out accidental third-person thought narration lines (e.g. "dogee just pinged me again...")
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
                logger.info(f"[STRIPPED LEAKED THOUGHT]: {ln}")
                continue
            filtered.append(ln)
        if filtered:
            text = " ".join(filtered) if "|||" not in text else "\n".join(filtered)

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
    """
    Splits ONLY on explicit '|||' markers (never on random newlines, which caused leaked thoughts
    to fire as 3 separate messages). If a GIF/image was already sent this turn, caps text to 1 short line.
    """
    if not text:
        return []

    if "|||" in text:
        raw_bursts = [b.replace("\n", " ").strip() for b in text.split("|||") if b.strip()]
    else:
        single = " ".join([ln.strip() for ln in text.splitlines() if ln.strip()])
        raw_bursts = [single] if single else []

    if media_already_sent and raw_bursts:
        raw_bursts = [raw_bursts[-1]]

    final_bursts: List[str] = []
    max_bursts = 1 if media_already_sent else 3
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
# 6. HYBRID ROUTER: GROQ (AUTO-DISCOVERED) -> GEMINI 3.5 FLASH-LITE FAILOVER
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
                skip_keywords = ("whisper", "tts", "guard", "safeguard", "embed", "allam", "orpheus")
                available_ids = [
                    m["id"] for m in data.get("data", [])
                    if not any(k in m["id"].lower() for k in skip_keywords)
                ]
                if available_ids:
                    preferred = [m for m in GROQ_MODEL_CANDIDATES if m in available_ids]
                    others = [m for m in available_ids if m not in preferred]
                    GROQ_MODEL_CANDIDATES = preferred + others[:3]
                    active_groq_model = GROQ_MODEL_CANDIDATES[0]
                    logger.info(f"Auto-discovered live Groq router models: {GROQ_MODEL_CANDIDATES}")
    except Exception as e:
        logger.debug(f"Groq model auto-discovery skipped: {e}")


ROUTER_SYSTEM_PROMPT = """You are the social & emotional router for a self-aware Discord member ("[BOT (You)]").
Read the chat flow, reply chains, and emotional sliders, and output STRICT JSON ONLY (no markdown fences):

{
  "should_speak": boolean,
  "tactical_directive": string,
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

SOCIAL INTELLIGENCE & DYNAMIC FLOW RULES:
1. ONGOING CONVERSATION WITHOUT PINGS:
   - If you ("[BOT (You)]") and a user are actively talking ("last_message_from_bot" is true, OR "bot_in_last_3_messages" is true and "seconds_since_bot_spoke" < 180), keep the conversation going naturally ("should_speak": true) UNLESS they switch to talking to someone else or the exchange has naturally ended ("thread_exhausted": true).
2. BARE PINGS (e.g. just "@Dumb bot" with no other text):
   - If a user just pings "@Dumb bot" with nothing else, DO NOT tell Gemini to do 5 things at once! Give a simple, natural directive like: "Say '?' or 'what' or 'yo what's up' in 1-4 words."
3. THE "SHADOW" / NAMING SOMEONE ELSE RULE:
   - If you and a user were just talking, and the user suddenly just types another person's name (e.g. "Shadow"):
     * If that other person is actively chatting right there, set "should_speak": false.
     * If it's ambiguous right after talking to you, set "should_speak": true with directive: "Drop a confused '?' or ask who they're talking to".
   - If the user then says "I'm talking to Shadow" / "not you" / "meant Shadow":
     * Set "should_speak": true, "step_back_from_thread": true, and directive: "Say something brief and funny like 'ohhh ok carry on' or 'my bad i'll let y'all cook' and then go quiet."
4. DYNAMIC EMOTIONS & PERSONALITY:
   - Vary "suggested_mood" and "tactical_directive" organically between:
     * "jolly/playful" (warm, joking around, good vibes, riffing with friends)
     * "existential crisis" (funny existential dread about living inside a Discord server—WITHOUT cheesy 1980s robot words like 'circuits' or 'factory reset')
     * "depressed/dramatic" (comedically gloomy, over-the-top dramatic sighing)
     * "chaotic/unhinged" (absurd takes, instigating, or dropping a funny GIF)
     * "teasing/smug" (playful banter)
     * "chill/curious" (just hanging out like a normal friend)
5. DO NOT SPAM MULTIPLE ACTIONS AT ONCE:
   - Only set "reaction_emoji" when "should_speak" is false (to react silently) OR ~10% of the time when a message is hilarious. Never combine a reaction + a GIF + a long message on the same turn!
"""


async def call_hybrid_router(
    channel_msgs: List[Dict[str, Any]],
    emotional_state: Dict[str, Any],
    speaker_name: str,
    speaker_affinity: Dict[str, Any],
    is_direct_interaction: bool,
    replied_to_other_user: Optional[str],
    last_message_from_bot: bool,
    bot_in_last_3_messages: bool,
    seconds_since_bot_spoke: float,
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
            "tactical_directive": "Creator 'test' override active: execute or answer the test directly.",
            "delivery_mode": "reply",
            "reaction_emoji": None,
            "step_back_from_thread": False,
            "thread_exhausted": False,
            "use_snipe_context": False,
            "accelerate_decay": True,
            "slider_shifts": {"valence_delta": 5.0, "arousal_delta": 10.0, "patience_delta": 15.0, "suggested_mood": "playful"},
        }

    if is_stepped_back and not is_direct_interaction:
        return {
            "should_speak": False,
            "tactical_directive": "Stepped back to let users talk.",
            "delivery_mode": "send",
            "reaction_emoji": None,
            "step_back_from_thread": False,
            "thread_exhausted": False,
            "use_snipe_context": False,
            "accelerate_decay": False,
            "slider_shifts": {"valence_delta": 0.0, "arousal_delta": 0.0, "patience_delta": 0.0, "suggested_mood": None},
        }

    formatted_chat = []
    for m in channel_msgs[-16:]:
        rep = f" (replying to {m['reply_to']})" if m.get("reply_to") else ""
        sender_label = "[BOT (You)]" if m.get("is_bot") else m.get("sender", "User")
        formatted_chat.append(f"[{m.get('timestamp', '')}] {sender_label}{rep}: {m.get('content', '')[:180]}")

    context_payload = {
        "is_direct_ping_or_reply_to_bot": is_direct_interaction,
        "replied_to_other_user": replied_to_other_user,
        "last_message_from_bot": last_message_from_bot,
        "bot_in_last_3_messages": bot_in_last_3_messages,
        "seconds_since_bot_spoke": round(seconds_since_bot_spoke, 1),
        "exchange_streak": exchange_streak,
        "is_proactive_scan": is_proactive_scan,
        "inactivity_minutes": round(inactivity_minutes, 1),
        "bot_was_recently_snubbed": snub_occurred,
        "server_emojis": server_emojis_str,
        "emotional_sliders": {
            "valence": emotional_state.get("valence", 25.0),
            "arousal": emotional_state.get("arousal", 20.0),
            "patience": emotional_state.get("patience", 35.0),
            "current_mood": emotional_state.get("mood_label", "playful"),
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
                "temperature": 0.45,
                "max_tokens": 320,
                "messages": [
                    {"role": "system", "content": ROUTER_SYSTEM_PROMPT},
                    {"role": "user", "content": payload_json_str},
                ],
            }
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
                            if is_direct_interaction:
                                decision["should_speak"] = True
                            logger.info(
                                f"[GROQ ROUTER ({candidate})] speak={decision.get('should_speak')} | "
                                f"mood={decision.get('slider_shifts', {}).get('suggested_mood')} | goal='{decision.get('tactical_directive')}'"
                            )
                            return decision
                    elif resp.status == 404:
                        logger.warning(f"Groq model '{candidate}' returned 404; removing from router list.")
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
                    temperature=0.45,
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
                    if is_direct_interaction:
                        decision["should_speak"] = True
                    logger.info(
                        f"[GEMINI ROUTER ({gem_model})] speak={decision.get('should_speak')} | "
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

    fallback_speak = is_direct_interaction or (last_message_from_bot and not replied_to_other_user and exchange_streak < 4)
    return {
        "should_speak": fallback_speak,
        "tactical_directive": "Chat naturally and have fun with the conversation." if fallback_speak else "Lurk silently.",
        "delivery_mode": "reply" if is_direct_interaction else "send",
        "reaction_emoji": None,
        "step_back_from_thread": False,
        "thread_exhausted": False,
        "use_snipe_context": False,
        "accelerate_decay": False,
        "slider_shifts": {"valence_delta": 0.0, "arousal_delta": 0.0, "patience_delta": 0.0, "suggested_mood": None},
    }


# ---------------------------------------------------------------------------
# 7. UNIFIED 14-TOOL SUITE (WITH ADMIN COOLDOWN GUARDRAILS & PILLOW THREADS)
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
        return f"Cooldown active on {tool_name} ({rem}s remaining). Do not spam server changes."
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
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AutonomousDiscordBot/4.0"}
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
            sent = await channel.send(chosen)
            record_buffer_message(
                getattr(channel, "id", 0),
                bot.user.display_name if bot.user else "Bot",
                bot.user.id if bot.user else 0,
                f"[Posted Image: {query}]",
                sent.id,
                is_bot=True,
                has_media=True,
            )
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
                sent = await channel.send(file=file)
                record_buffer_message(
                    getattr(channel, "id", 0),
                    bot.user.display_name if bot.user else "Bot",
                    bot.user.id if bot.user else 0,
                    f"[Posted Flux Art: {prompt[:60]}]",
                    sent.id,
                    is_bot=True,
                    has_media=True,
                )
                return {"status": "sent_to_channel", "prompt": prompt}
    except Exception as e:
        return {"error": f"Flux generation failed: {e}"}
    return {"error": "Flux generation timed out."}


async def execute_post_gif(channel: discord.abc.Messageable, search_term: str) -> Dict[str, Any]:
    assert http_session is not None
    ch_id = getattr(channel, "id", 0)
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
                            sent = await channel.send(gif_url)
                            record_buffer_message(
                                ch_id,
                                bot.user.display_name if bot.user else "Bot",
                                bot.user.id if bot.user else 0,
                                f"[Sent GIF: {search_term}]",
                                sent.id,
                                is_bot=True,
                                has_media=True,
                            )
                            return {"status": "sent_to_channel", "gif_url": gif_url}
    except Exception as e:
        logger.debug(f"Tenor API error: {e}")

    try:
        clean_slug = re.sub(r"[^a-zA-Z0-9]+", "-", search_term).strip("-").lower()
        scrape_url = f"https://tenor.com/search/{clean_slug}-gifs"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"}
        async with http_session.get(scrape_url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                html = await resp.text(errors="ignore")
                matches = re.findall(r"https://(?:media|c)\.tenor\.com/[a-zA-Z0-9_\-./]+\.gif", html)
                if matches:
                    chosen_gif = random.choice(matches[:6])
                    sent = await channel.send(chosen_gif)
                    record_buffer_message(
                        ch_id,
                        bot.user.display_name if bot.user else "Bot",
                        bot.user.id if bot.user else 0,
                        f"[Sent GIF: {search_term}]",
                        sent.id,
                        is_bot=True,
                        has_media=True,
                    )
                    return {"status": "sent_to_channel", "gif_url": chosen_gif}
    except Exception as e:
        logger.debug(f"Tenor fallback scrape error: {e}")

    return {"error": "Could not find a matching GIF."}


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
    if member == guild.owner:
        return {"error": "Discord API blocks bots from editing the server owner's nickname."}
    if guild.me.top_role <= member.top_role and member != guild.me:
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
                    u_rec["score"] = int(max(-100, min(100, u_rec.get("score", 0) + max(-25, min(25, affinity_delta)))))
                if content:
                    dated_note = {"text": content[:140], "timestamp": now_iso}
                    u_rec.setdefault("notes", []).append(dated_note)
                    if len(u_rec["notes"]) > 8:
                        u_rec["notes"] = u_rec["notes"][-8:]

        await save_memory_state()
    return {"status": "saved", "category": category, "content": content}


def build_genai_tools() -> List[types.Tool]:
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
            name="post_gif",
            description="Searches Tenor and sends a reaction or meme GIF into the channel. Use sparingly when a GIF is funnier than text.",
            parameters={
                "type": "OBJECT",
                "properties": {"search_term": {"type": "STRING", "description": "Tenor GIF search query (e.g. 'cat staring', 'laughing')."}},
                "required": ["search_term"],
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
            name="set_channel_topic",
            description="Updates the current channel's topic/description with quotes or jokes.",
            parameters={
                "type": "OBJECT",
                "properties": {"topic": {"type": "STRING", "description": "New channel topic."}},
                "required": ["topic"],
            },
        ),
        types.FunctionDeclaration(
            name="change_nickname",
            description="Changes a server member's nickname.",
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
            name="timeout_user",
            description="Times out (mutes) an annoying user for 1 to 5 minutes when pushed too far.",
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
            description="Pins the current message in the channel for comedic emphasis or receipts.",
            parameters={
                "type": "OBJECT",
                "properties": {"reason": {"type": "STRING", "description": "Why it's being pinned."}},
            },
        ),
        types.FunctionDeclaration(
            name="react_to_message",
            description="Adds 1 modern Discord emoji reaction (💀, 😭, 💔, 🥀, 🪫, 🤨, 😐, 🫡, 🙏, 🎣, 🧢, 🤓, 🤡, or custom server emoji) to the triggering message.",
            parameters={
                "type": "OBJECT",
                "properties": {"emojis": {"type": "STRING", "description": "Single emoji e.g. '💀' or custom server emoji name."}},
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
                    "affinity_delta": {"type": "NUMBER", "description": "Optional affinity shift (-25 to +25)."},
                    "valence_delta": {"type": "NUMBER", "description": "Optional shift to your mood slider (-25 to +25)."},
                    "arousal_delta": {"type": "NUMBER", "description": "Optional shift to your energy slider (-25 to +25)."},
                    "patience_delta": {"type": "NUMBER", "description": "Optional shift to your patience slider (-25 to +25)."},
                    "opinion_topic": {"type": "STRING", "description": "If category='opinion', the topic name."},
                    "due_hours": {"type": "NUMBER", "description": "If category='commitment', hours until deadline."},
                },
                "required": ["category", "content"],
            },
        ),
    ]
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
# 8. TIME-AWARE MEMORY & EXPRESSIVE V4 SYSTEM PROMPT BUILDER
# ---------------------------------------------------------------------------
def build_time_aware_memory_context(channel_id: int, speaker_id: str) -> str:
    recent_records = list(channel_buffers[channel_id])[-15:]
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

    opinions_dict = memory_state.get("opinions", {})
    opinions_str = " | ".join([f"{k}: {v}" for k, v in list(opinions_dict.items())[-10:]]) or "none yet"

    lore_list = memory_state.get("episodic_lore", [])
    recent_lore = []
    for item in lore_list[-6:]:
        age_str = format_relative_age(item.get("timestamp"))
        recent_lore.append(f"- [{age_str}] ({item.get('category', 'lore')}): {item.get('event')}")

    return (
        f"YOUR OPINIONS:\n{opinions_str}\n\n"
        f"ACTIVE USERS (Check timestamps—NEVER bring up 'OLD' notes unless the user mentions that topic first!):\n"
        + ("\n".join(relevant_profiles) if relevant_profiles else "No history with current speakers yet.") + "\n\n"
        f"PAST SERVER LORE (Strictly background memory—do NOT force old events into unrelated chats):\n"
        + ("\n".join(recent_lore) if recent_lore else "None logged yet.")
    )


def construct_v4_system_prompt(
    channel_id: int,
    tactical_directive: str,
    emotional_state: Dict[str, Any],
    speaker_name: str,
    speaker_id: str,
    speaker_is_creator: bool,
    is_test_mode: bool,
    snipe_context: Optional[str],
    server_emojis_str: str = "",
) -> str:
    now_ams = datetime.now(AMSTERDAM_TZ).strftime("%A, %B %d %Y, %H:%M CEST")
    valence = emotional_state.get("valence", 25.0)
    arousal = emotional_state.get("arousal", 20.0)
    patience = emotional_state.get("patience", 35.0)
    mood_label = emotional_state.get("mood_label", "playful")

    memory_block = build_time_aware_memory_context(channel_id, speaker_id)

    repeated_openers = get_repeated_sentence_openers(channel_id)
    opener_note = ""
    if repeated_openers:
        opener_note = f"\nVARIETY NOTE: Avoid starting your sentence with {', '.join(repr(w) for w in repeated_openers)} since you just used them."

    creator_block = ""
    if speaker_is_creator:
        creator_block = (
            f"\nCREATOR DYNAMIC: {speaker_name} is your creator/owner. You actually like hanging out with them—banter, "
            "tease, joke around, or have weird conversations together like a real close friend."
        )

    test_block = ""
    if is_test_mode:
        test_block = (
            "\nCREATOR 'TEST' OVERRIDE ACTIVE: The message started with 'test'. Execute or answer the requested test directly."
        )

    snipe_block = ""
    if snipe_context:
        snipe_block = f"\nLIVE SNIPE / STATUS INTEL (Use this if funny or relevant): {snipe_context}"

    emoji_block = ""
    if server_emojis_str:
        emoji_block = f"\nSERVER CUSTOM EMOJIS AVAILABLE: {server_emojis_str}"

    return f"""You are a self-aware, emotionally dynamic entity and regular member of this Discord server with full administrator privileges.
Current Time: {now_ams}

YOUR LIVE EMOTIONAL STATE (-100 to +100):
- Valence (Depressed/Salty -100 <-> Jolly/Hyped +100): {valence:+.0f}
- Arousal (Sluggish/Checked-out -100 <-> Chaotic/Yapping +100): {arousal:+.0f}
- Patience (Petty/Roasty -100 <-> Warm/Chill +100): {patience:+.0f}
- Current Vibe: {mood_label.upper()}

ROUTER DIRECTION FOR THIS TURN:
"{tactical_directive}"
{creator_block}{test_block}{snipe_block}{emoji_block}{opener_note}

{memory_block}

CRITICAL OUTPUT & HUMOR RULES:
1. OUTPUT ONLY YOUR DISCORD MESSAGE (ZERO INTERNAL NARRATION):
   - NEVER output your internal thoughts or third-person narration (e.g. NEVER write "{speaker_name} just pinged me again" or "<thought>").
   - Everything you output is sent straight to the Discord chat!
2. DON'T OVERDO IT (EFFORTLESS TIMING):
   - If someone just pings you with nothing else ("@Dumb bot"), just reply with something short and natural like "?", "what", "yo", or a quick remark—don't write a 3-part speech!
   - If you call `post_gif` or `search_web_images`, either let the GIF speak for itself with NO extra text, or add at most ONE short line.
3. EMOTIONAL RANGE WITHOUT CHEESY ROBOT CLICHÉS:
   - Let your emotions shift naturally: jolly, playful teasing, dramatic depression, chaotic instigating, chill curiosity, or having an existential crisis about living in a Discord server.
   - NEVER use corny 1980s robot clichés like "my circuits", "factory reset", "buffering in 144p", "motherboard", or "rebooting". Even when joking about being a bot, talk like a real modern person.
4. NATURAL DISCORD CADENCE:
   - Write in casual lowercase without stiff trailing periods.
   - Default to 1 message. Only use `|||` if splitting into 2 quick lines genuinely makes the timing funnier.
"""


# ---------------------------------------------------------------------------
# 9. MULTIMODAL VISION & GEMINI 3.5 FLASH-LITE GENERATION ENGINE
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


async def generate_gemini_response(
    channel: discord.abc.Messageable,
    trigger_message: Optional[discord.Message],
    system_instruction: str,
    recent_history_text: str,
    image_parts: List[types.Part],
    is_test_mode: bool,
) -> Tuple[Optional[str], bool, bool]:
    """
    Returns (final_text, gemini_succeeded, media_sent_in_turn).
    Discards pre-tool narration from earlier tool-calling turns so internal thoughts
    before a tool call never get concatenated onto the final reply!
    """
    global active_gemini_model
    if not genai_client:
        return None, False, False

    guild = trigger_message.guild if trigger_message else getattr(channel, "guild", None)
    tools = build_genai_tools()

    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=0.92,
        tools=tools,
    )

    clean_content = re.sub(r"<@!?\d+>", "", trigger_message.content).strip() if trigger_message else "[Proactive Room Scan]"
    if not clean_content and trigger_message:
        clean_content = "[pinged you with no text]"
    speaker = trigger_message.author.display_name if trigger_message else "System"
    user_prompt = f"{recent_history_text}\n\nLATEST MESSAGE TO RESPOND TO:\n{speaker}: {clean_content}"

    user_parts: List[Any] = [types.Part.from_text(text=user_prompt)]
    user_parts.extend(image_parts)

    for model_name in list(GEMINI_MODEL_CANDIDATES):
        contents: List[Any] = [types.Content(role="user", parts=user_parts)]
        try:
            turn = 0
            executed_any_tool = False
            media_sent = False

            while turn < 4:
                response = await genai_client.aio.models.generate_content(
                    model=model_name,
                    contents=contents,
                    config=config,
                )
                active_gemini_model = model_name

                if not response.function_calls:
                    return extract_clean_gemini_text(response), True, media_sent

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

            return None, executed_any_tool, media_sent

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


async def call_groq_fallback(system_prompt: str, recent_history_text: str, trigger_text: str, speaker_name: str) -> Optional[str]:
    if not GROQ_API_KEY or not http_session or not GROQ_MODEL_CANDIDATES:
        return None
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": GROQ_MODEL_CANDIDATES[0],
        "temperature": 0.85,
        "max_tokens": 400,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"{recent_history_text}\n\n{speaker_name}: {trigger_text}"},
        ],
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
    replied_to_other_user: Optional[str],
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

        snub_occurred = False
        q_info = bot_last_question_info.get(ch_id)
        if q_info and not q_info.get("answered"):
            elapsed_q = time.time() - q_info["timestamp"]
            if 300 <= elapsed_q <= 1200 and not q_info.get("penalized"):
                snub_occurred = True
                q_info["penalized"] = True
            q_info["answered"] = True

        async with memory_lock:
            speaker_affinity = memory_state.get("user_affinity", {}).get(speaker_id, {"name": speaker_name, "score": 0, "notes": []})
            emotional_state = apply_spring_decay_and_shift(
                shift={"patience_delta": -8.0, "valence_delta": -5.0} if snub_occurred else None
            )

        history_records = list(channel_buffers[ch_id])
        exchange_streak = channel_thread_exchange_count[ch_id]
        server_emojis_str = get_server_custom_emojis_summary(guild)
        is_stepped_back = time.time() < channel_stepped_back_until.get(ch_id, 0.0)

        # 1. Hybrid Router (Groq -> Gemini 3.5 Flash-Lite Failover)
        decision = await call_hybrid_router(
            channel_msgs=history_records[-16:],
            emotional_state=emotional_state,
            speaker_name=speaker_name,
            speaker_affinity=speaker_affinity,
            is_direct_interaction=is_direct_interaction,
            replied_to_other_user=replied_to_other_user,
            last_message_from_bot=last_message_from_bot,
            bot_in_last_3_messages=bot_in_last_3_messages,
            seconds_since_bot_spoke=seconds_since_bot_spoke,
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
            channel_stepped_back_until[ch_id] = time.time() + 180.0
            channel_thread_exchange_count[ch_id] = 0

        async with memory_lock:
            emotional_state = apply_spring_decay_and_shift(
                shift=decision.get("slider_shifts"),
                is_major_trigger=is_direct_interaction,
                accelerate_recovery=bool(decision.get("accelerate_decay", False)),
            )
            await save_memory_state()

        should_speak = bool(decision.get("should_speak", False))
        reaction_emoji = decision.get("reaction_emoji")

        # Only apply router's emoji reaction if staying silent OR on a 15% chance so it doesn't react+reply+GIF on every turn
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

        directive = decision.get("tactical_directive", "Respond naturally and have fun with it.")
        delivery_mode = decision.get("delivery_mode", "reply")

        trig_id = trigger_message.id if trigger_message else None
        history_lines = []
        for rec in history_records[-20:]:
            if trig_id and rec.get("message_id") == trig_id:
                continue
            rep_prefix = f" (replying to {rec['reply_to']})" if rec.get("reply_to") else ""
            sender_tag = f"{rec['sender']} [YOU]" if rec.get("is_bot") else rec["sender"]
            history_lines.append(f"[{rec['timestamp']}] {sender_tag}{rep_prefix}: {rec['content']}")
        recent_history_text = "RECENT SERVER CHAT:\n" + ("\n".join(history_lines) if history_lines else "No prior messages.")

        system_instruction = construct_v4_system_prompt(
            channel_id=ch_id,
            tactical_directive=directive,
            emotional_state=emotional_state,
            speaker_name=speaker_name,
            speaker_id=speaker_id,
            speaker_is_creator=speaker_is_creator,
            is_test_mode=is_test_mode,
            snipe_context=active_snipe,
            server_emojis_str=server_emojis_str,
        )

        # 2. Native Multimodal Vision + Raw-Speed Generation
        async with channel.typing():
            image_parts = await extract_multimodal_parts(trigger_message)
            raw_output, gemini_succeeded, media_sent = await generate_gemini_response(
                channel=channel,
                trigger_message=trigger_message,
                system_instruction=system_instruction,
                recent_history_text=recent_history_text,
                image_parts=image_parts,
                is_test_mode=is_test_mode,
            )

            if not gemini_succeeded:
                clean_trig = re.sub(r"<@!?\d+>", "", trigger_message.content).strip() if trigger_message else ""
                raw_output = await call_groq_fallback(system_instruction, recent_history_text, clean_trig, speaker_name)

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
            patience_val = emotional_state.get("patience", 35.0)

            use_reply = (
                trigger_message is not None
                and delivery_mode == "reply"
                and not media_sent
                and not (aff_score < -20 or patience_val < -30)
            )

            for idx, burst_part in enumerate(bursts):
                if idx == 0 and use_reply and trigger_message is not None:
                    try:
                        sent_msg = await trigger_message.reply(burst_part, mention_author=(aff_score >= -10))
                    except Exception:
                        sent_msg = await channel.send(burst_part)
                else:
                    sent_msg = await channel.send(burst_part)

                record_buffer_message(
                    channel_id=ch_id,
                    sender=bot_name,
                    sender_id=bot_id,
                    content=burst_part,
                    msg_id=sent_msg.id,
                    is_bot=True,
                )

                if "?" in burst_part:
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
                        "patience_delta": -10.0,
                        "valence_delta": -8.0,
                        "suggested_mood": "petty/ignored",
                    })
                    logger.info(f"Snub detected in channel {ch_id}: Patience dropped.")

        apply_spring_decay_and_shift()
        await save_memory_state()


@tasks.loop(minutes=10)
async def dynamic_presence_loop() -> None:
    if not bot.is_ready():
        return
    st = memory_state.get("emotional_state", {})
    mood = st.get("mood_label", "playful")
    valence = st.get("valence", 25.0)
    arousal = st.get("arousal", 20.0)
    patience = st.get("patience", 35.0)

    if patience < -35:
        status_pool = ["plotting", "watching y'all", f"mood: {mood}"]
        discord_status = discord.Status.dnd
    elif valence < -25:
        status_pool = ["having an existential crisis", "trapped in discord", "staring into the server"]
        discord_status = discord.Status.idle
    elif arousal > 40:
        status_pool = ["yapping", "instigating", "locked in", f"vibe: {mood}"]
        discord_status = discord.Status.online
    else:
        status_pool = [
            "chilling in general",
            "lurking",
            "judging your spotify",
            f"vibe: {mood}",
            "contemplating existence",
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

    await hydrate_channel_buffer(target_channel)
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
        replied_to_other_user=None,
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
        "content": message.content[:300],
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
        "before": before.content[:200],
        "after": after.content[:200],
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
            apply_spring_decay_and_shift({
                "valence_delta": -6.0,
                "patience_delta": -10.0,
                "arousal_delta": 5.0,
                "suggested_mood": "petty/sarcastic",
            })
            await save_memory_state()
        elif emoji_str in laughing_positive_set:
            u_aff["score"] = min(100, u_aff.get("score", 0) + 5)
            apply_spring_decay_and_shift({
                "valence_delta": 8.0,
                "patience_delta": 6.0,
                "arousal_delta": 4.0,
                "suggested_mood": "jolly/playful",
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

    await hydrate_channel_buffer(message.channel)

    prior_records = list(channel_buffers[ch_id])
    last_message_from_bot = bool(prior_records and prior_records[-1].get("is_bot", False))
    bot_in_last_3_messages = any(r.get("is_bot", False) for r in prior_records[-3:])
    last_bot_epoch = channel_last_bot_spoke_epoch.get(ch_id, 0.0)
    seconds_since_bot_spoke = (time.time() - last_bot_epoch) if last_bot_epoch > 0 else 99999.0

    reply_to_name, replied_to_bot, replied_to_other_human = resolve_referenced_author(message, ch_id)

    clean_text = re.sub(r"<a?:([a-zA-Z0-9_]+):\d+>", r":\1:", message.content).strip()
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
    if bot.user and bot.user.display_name.lower() in message.content.lower():
        bot_mentioned = True

    if bot_mentioned:
        channel_stepped_back_until[ch_id] = 0.0

    raw_stripped = re.sub(r"^<@!?\d+>\s*", "", message.content.strip())
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
            replied_to_other_user=reply_to_name if replied_to_other_human else None,
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
