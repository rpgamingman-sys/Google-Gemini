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
# 1. LOGGING, ENVIRONMENT & PERSISTENT STORAGE ARCHITECTURE
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("AutonomousMetaBotV3")

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
if not GROQ_API_KEY:
    logger.critical("GROQ_API_KEY is missing!")

AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")

# Railway persistent volume mount (/data) with automatic fallback
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
CLOUD_BACKUP_MARKER = "[[MEMORY_BACKUP_V3_JSON]]"
logger.info(f"Persistent memory target: {MEMORY_FILE}")

# Model Configuration with Automatic 404 Cascade Protection
ENV_GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_MODEL_CANDIDATES = list(dict.fromkeys([
    ENV_GEMINI_MODEL,
    "gemini-2.5-flash",
    "gemini-2.0-flash",
]))
active_gemini_model: str = GEMINI_MODEL_CANDIDATES[0]

ENV_GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_MODEL_CANDIDATES = list(dict.fromkeys([
    "llama-3.3-70b-versatile",
    ENV_GROQ_MODEL,
    "llama-3.1-8b-instant",
]))
active_groq_model: str = GROQ_MODEL_CANDIDATES[0]
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"

# Hard Regex Filter for Corny AI / Anime / Edgelord Tropes
BANNED_WORDS_MAP = {
    r"\bvoid\b": "nothing",
    r"\bmortals\b": "people",
    r"\bmortal\b": "person",
    r"\babyss\b": "deep end",
    r"\beternal consciousness\b": "my head",
    r"\bdestiny\b": "luck",
    r"\bdigital realm\b": "server",
    r"\bas an ai\b": "honestly",
    r"\bas a large language model\b": "honestly",
    r"\blanguage model\b": "brain",
    r"\bhow can i assist you\b": "what do you want",
    r"\bassist you\b": "help",
    r"\bmy programming\b": "my brain",
    r"\bmy directive\b": "my plan",
}

# ---------------------------------------------------------------------------
# 2. DISCORD CLIENT, CONCURRENCY LOCKS & IN-MEMORY BUFFERS
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

# Gated Sniping Caches (Deleted/Edited Messages & Presence Roasts)
deleted_message_cache: Dict[int, Dict[str, Any]] = {}
edited_message_cache: Dict[int, Dict[str, Any]] = {}
user_snipe_cooldowns: Dict[int, float] = defaultdict(float)
user_last_seen_activity: Dict[int, str] = {}

# Snub & Proactive Loop State
bot_last_question_info: Dict[int, Dict[str, Any]] = {}
channel_next_proactive_epoch: Dict[int, float] = defaultdict(float)
channel_Thread_Exchange_Count: Dict[int, int] = defaultdict(int)

# Hard Admin Tool Cooldowns (Prevents mood swings from nuking the server)
admin_tool_cooldowns: Dict[str, float] = defaultdict(float)
ADMIN_COOLDOWN_SECONDS = {
    "create_text_channel": 3600.0,   # Max 1 channel per hour
    "create_role": 1800.0,           # Max 1 role per 30 mins
    "create_server_emoji": 600.0,    # Max 1 emoji per 10 mins
    "create_server_sticker": 600.0,  # Max 1 sticker per 10 mins
    "set_channel_topic": 600.0,      # Max 1 topic change per 10 mins
    "timeout_user": 300.0,           # Max 1 timeout per 5 mins
    "change_nickname": 120.0,        # Max 1 nick change per 2 mins
}

# ---------------------------------------------------------------------------
# 3. PERSISTENT MEMORY, 3D EMOTIONAL ENGINE & RAILWAY ZERO-AMNESIA SYNC
# ---------------------------------------------------------------------------
BASELINE_SLIDERS = {
    "valence": 10.0,   # -100 (Pissed/Salty) <---> +100 (Hyped/Happy)
    "arousal": 15.0,   # -100 (Checked-out/Dry) <---> +100 (Chaotic/Yapping)
    "patience": 20.0,  # -100 (Petty/Defensive/Hostile) <---> +100 (Chill/Forgiving)
}

DEFAULT_MEMORY = {
    "emotional_state": {
        "valence": 10.0,
        "arousal": 15.0,
        "patience": 20.0,
        "mood_label": "chill",
        "last_updated_epoch": time.time(),
    },
    "opinions": {
        "voice notes": "hates unprompted voice notes when a 3-word text would work",
        "weapon durability": "despises weapon durability mechanics in games",
        "adblockers": "judges anyone browsing the web without an adblocker",
        "tutorials": "refuses to read game tutorials and blames the game later",
    },
    "last_active_channel_id": None,
    "user_affinity": {},
    "episodic_lore": [],
    "active_commitments": [],
    "feedback_history": [],
}


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
    """Non-blocking atomic disk write via asyncio.to_thread."""
    global memory_dirty
    try:
        snapshot = json.loads(json.dumps(memory_state))
        await asyncio.to_thread(_sync_write_memory, snapshot)
        memory_dirty = True
    except Exception as e:
        logger.error(f"Non-blocking memory save failed: {e}")


def load_memory_state_sync() -> Dict[str, Any]:
    global memory_state
    if os.path.exists(MEMORY_FILE):
        try:
            with open(MEMORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            recursive_merge_defaults(data, DEFAULT_MEMORY)
            memory_state = data
            logger.info(f"Loaded memory state from {MEMORY_FILE}")
            return memory_state
        except Exception as e:
            logger.error(f"Corrupt memory file {MEMORY_FILE}: {e}")
    memory_state = json.loads(json.dumps(DEFAULT_MEMORY))
    _sync_write_memory(memory_state)
    return memory_state


async def find_backup_channel() -> Optional[discord.TextChannel]:
    """Finds a designated channel to store/restore cloud backups across Railway deploys."""
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
    """Restores memory from Discord if Railway redeployed without a persistent volume."""
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
                logger.info(f"Zero-Amnesia: Restored memory state from #{ch.name} cloud backup!")
                return
    except Exception as e:
        logger.debug(f"Cloud memory restore skipped: {e}")


@tasks.loop(minutes=20)
async def discord_cloud_backup_loop() -> None:
    """Periodically backs up memory.json to a log channel if modified."""
    global memory_dirty
    if not memory_dirty or not bot.is_ready():
        return
    ch = await find_backup_channel()
    if not ch:
        return
    try:
        raw_bytes = json.dumps(memory_state, indent=2, ensure_ascii=False).encode("utf-8")
        file = discord.File(io.BytesIO(raw_bytes), filename="memory_backup.json")
        # Try editing previous backup message first to avoid clutter
        async for msg in ch.history(limit=15):
            if bot.user and msg.author.id == bot.user.id and CLOUD_BACKUP_MARKER in msg.content:
                await msg.delete()
                break
        await ch.send(content=f"{CLOUD_BACKUP_MARKER} Automated state snapshot", file=file)
        memory_dirty = False
    except Exception as e:
        logger.debug(f"Cloud backup skipped: {e}")


# ---------------------------------------------------------------------------
# 4. 3D AFFECTIVE PHYSICS ENGINE (CLAMPED SPRING DECAY & EMERGENT MOODS)
# ---------------------------------------------------------------------------
def derive_emergent_mood(valence: float, arousal: float, patience: float) -> str:
    """Maps continuous 3D sliders (-100 to +100) into nuanced human emotional states."""
    if patience < -45 and valence < -35:
        return "mad" if arousal > 20 else "petty"
    if patience < -25:
        return "annoyed" if arousal >= 0 else "edgy/sarcastic"
    if valence < -40 and arousal < -20:
        return "sad/hurt"
    if arousal < -45 and abs(valence) < 30:
        return "bored"
    if valence > 45 and arousal > 50:
        return "hyper/chaotic"
    if valence > 40 and patience > 35:
        return "affectionate" if arousal < 30 else "excited"
    if valence > 20 and arousal > 25:
        return "funny/troll" if patience < 15 else "happy"
    if arousal > 40 and patience < 0:
        return "instigating"
    if arousal > 20 and abs(valence) <= 25:
        return "inquisitive"
    return "chill"


def apply_spring_decay_and_shift(
    shift: Optional[Dict[str, Any]] = None,
    is_major_trigger: bool = False,
    accelerate_recovery: bool = False,
) -> Dict[str, Any]:
    """
    Applies 25-minute emotional half-life spring physics + clamped single-turn shifts.
    Prevents the bot from getting stuck at -100 or +100 like a cartoon character.
    """
    st = memory_state.setdefault("emotional_state", dict(DEFAULT_MEMORY["emotional_state"]))
    now = time.time()
    last_t = float(st.get("last_updated_epoch", now))
    dt = max(0.0, now - last_t)

    # 25-minute (1500s) half-life; accelerates 2.5x if users are funny/apologetic/nice
    half_life = 600.0 if accelerate_recovery else 1500.0
    decay_factor = math.pow(0.5, dt / half_life)

    clamp_limit = 35.0 if is_major_trigger else 15.0

    for axis, base_val in BASELINE_SLIDERS.items():
        curr = float(st.get(axis, base_val))
        # Decay current spike toward baseline over elapsed time
        decayed = base_val + (curr - base_val) * decay_factor

        # Apply clamped shift from this turn
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
# 5. CONTEXT BUFFERS, ANTI-REPETITION & PRESENCE / SNIPE HELPERS
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


async def hydrate_channel_buffer(channel: discord.abc.Messageable) -> None:
    ch_id = getattr(channel, "id", None)
    if not ch_id or len(channel_buffers[ch_id]) > 0 or not hasattr(channel, "history"):
        return
    try:
        raw_msgs = [m async for m in channel.history(limit=40)]  # type: ignore
        raw_msgs.reverse()
        for m in raw_msgs:
            clean = re.sub(r"<a?:([a-zA-Z0-9_]+):\d+>", r":\1:", m.content).strip()
            if m.attachments:
                clean += f" [Image/Attachment: {', '.join(a.filename for a in m.attachments)}]"
            if m.stickers:
                clean += f" [Sticker: {', '.join(s.name for s in m.stickers)}]"
            reply_author = None
            if m.reference and m.reference.resolved and isinstance(m.reference.resolved, discord.Message):
                reply_author = m.reference.resolved.author.display_name
            channel_buffers[ch_id].append({
                "sender": m.author.display_name,
                "sender_id": m.author.id,
                "content": clean,
                "reply_to": reply_author,
                "has_media": bool(m.attachments or m.stickers),
                "timestamp_epoch": m.created_at.timestamp(),
                "timestamp": m.created_at.astimezone(AMSTERDAM_TZ).strftime("%H:%M"),
                "message_id": m.id,
                "is_bot": (bot.user is not None and m.author.id == bot.user.id),
            })
        logger.info(f"Hydrated #{getattr(channel, 'name', ch_id)} with {len(raw_msgs)} messages.")
    except Exception as e:
        logger.debug(f"Hydration failed for {ch_id}: {e}")


def get_overused_bot_phrases(channel_id: int) -> List[str]:
    """
    Dynamic Anti-Repetition Filter:
    Scans the bot's last 10 messages in the channel and blacklists overused openers/slang.
    """
    bot_msgs = [r["content"].lower() for r in list(channel_buffers[channel_id])[-25:] if r.get("is_bot")]
    recent_bot = bot_msgs[-10:]
    if not recent_bot:
        return []

    opener_counts: Dict[str, int] = defaultdict(int)
    slang_watchlist = ["bro", "nah", "lowkey", "crazy", "honestly", "wait", "valid", "real", "ngl", "fr", "wild", "fair"]
    slang_counts: Dict[str, int] = defaultdict(int)

    for txt in recent_bot:
        words = re.findall(r"[a-z']+", txt)
        if words:
            opener_counts[words[0]] += 1
        if len(words) >= 2:
            opener_counts[f"{words[0]} {words[1]}"] += 1
        for sw in slang_watchlist:
            if re.search(rf"\b{sw}\b", txt):
                slang_counts[sw] += 1

    banned = [k for k, c in opener_counts.items() if c >= 2 and k not in ("i", "the", "a", "it", "you", "that")]
    banned.extend([k for k, c in slang_counts.items() if c >= 2 and k not in banned])
    return banned[:8]


def extract_member_live_activity(member: Optional[discord.Member]) -> Optional[str]:
    """Extracts Spotify track or game activity ONLY when fresh and off-cooldown."""
    if not member or not isinstance(member, discord.Member) or not member.activities:
        return None
    now = time.time()
    # 2-hour cooldown per user so status roasts never become a repetitive gimmick
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
    summary = " | ".join(parts)
    return summary


def sanitize_output_text(text: str) -> str:
    """Strips hidden <thought> tags and replaces banned AI/anime vocabulary."""
    if not text:
        return ""
    # Strip <thought>...</thought> blocks (and log them for debugging)
    thoughts = re.findall(r"<thought>(.*?)</thought>", text, flags=re.DOTALL | re.IGNORECASE)
    for t in thoughts:
        logger.info(f"[INNER THOUGHT]: {t.strip()}")
    text = re.sub(r"<thought>.*?</thought>", "", text, flags=re.DOTALL | re.IGNORECASE)
    # Strip unclosed <thought> if model truncated
    text = re.sub(r"<thought>.*", "", text, flags=re.DOTALL | re.IGNORECASE)

    for pattern, replacement in BANNED_WORDS_MAP.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text.strip()


def split_into_burst_messages(text: str) -> List[str]:
    """
    Splits multi-part messages on '|||' (up to 5 rapid bursts) and safely chunks
    any huge rants >1900 chars so Discord never rejects long outputs.
    """
    if not text:
        return []
    raw_bursts = [b.strip() for b in text.split("|||") if b.strip()]
    final_bursts: List[str] = []
    for burst in raw_bursts[:5]:
        while len(burst) > 1900:
            split_idx = burst.rfind(" ", 0, 1900)
            if split_idx == -1:
                split_idx = 1900
            final_bursts.append(burst[:split_idx].strip())
            burst = burst[split_idx:].strip()
        if burst:
            final_bursts.append(burst)
    return final_bursts[:5]


def is_creator(user: Union_User := Any) -> bool:
    if not user:
        return False
    if CREATOR_USER_ID and getattr(user, "id", 0) == CREATOR_USER_ID:
        return True
    guild = getattr(user, "guild", None)
    if guild and guild.owner_id == getattr(user, "id", 0):
        return True
    return False


# ---------------------------------------------------------------------------
# 6. GROQ PREFRONTAL ROUTER (LLAMA-3.3-70B-VERSATILE)
# ---------------------------------------------------------------------------
GROQ_ROUTER_SYSTEM_PROMPT = """You are the fast prefrontal cortex router for a self-aware Discord regular.
Analyze the live chat context, 3D emotional sliders, reply chains, and deleted/edited snipes, and output STRICT JSON ONLY:

{
  "should_speak": boolean,
  "tactical_directive": string,
  "delivery_mode": "reply" | "send",
  "reaction_emoji": string or null,
  "target_user": string or null,
  "detected_tension": boolean,
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

ROUTING RULES:
1. ACTIVE CONVERSATIONS & PINGS:
   - If directly @mentioned, replied to, or named: "should_speak" MUST be true.
   - If the bot sent one of the last 2 messages and the user is continuing that exchange without addressing someone else: "should_speak" is true UNLESS "thread_exhausted" is true.
2. THREAD EXHAUSTION (NO LAST-WORD SYNDROME):
   - If "exchange_streak" >= 4 and the joke/point has clearly peaked or user gave a short closing remark ("lol", "fair", "true", "ok"), set "thread_exhausted": true, "should_speak": false, and optionally drop a single "reaction_emoji" to exit gracefully.
3. TENSION & THIRD-PARTY CONVERSATIONS:
   - If two other users are talking directly to each other or having a serious personal vent/debate not involving the bot, set "detected_tension": true and "should_speak": false.
4. DELIVERY MODE ("reply" vs "send"):
   - Use "reply" when directly answering a question, calling someone out, or jumping into a fast chat.
   - Use "send" for casual flow, or as a passive-aggressive non-ping when "patience" is negative or speaker affinity is low (< -20).
5. SNIPE FILTERING ("use_snipe_context"):
   - Only set "use_snipe_context": true if a recent deleted/edited message or user Spotify/game status is genuinely hilarious, cowardly, or hypocritical mid-argument. Ignore boring typo edits.
6. SLIDER SHIFTS (-15 to +15 normal, -35 to +35 major):
   - Adjust valence (mood), arousal (energy), and patience (ego/tolerance) based on how the user treated the bot. Set "accelerate_decay": true if the user was funny, nice, or apologized.
"""


async def call_groq_router(
    channel_msgs: List[Dict[str, Any]],
    emotional_state: Dict[str, Any],
    speaker_name: str,
    speaker_affinity: Dict[str, Any],
    is_direct_interaction: bool,
    is_test_mode: bool,
    exchange_streak: int,
    snipe_info: Optional[str],
    snub_occurred: bool,
    is_proactive_scan: bool = False,
    inactivity_minutes: float = 0.0,
) -> Dict[str, Any]:
    global active_groq_model

    if is_test_mode:
        return {
            "should_speak": True,
            "tactical_directive": "Creator 'test' override active: drop any grudge or sarcasm and execute/answer the test directly.",
            "delivery_mode": "reply",
            "reaction_emoji": None,
            "target_user": speaker_name,
            "detected_tension": False,
            "thread_exhausted": False,
            "use_snipe_context": False,
            "accelerate_decay": True,
            "slider_shifts": {"valence_delta": 5.0, "arousal_delta": 10.0, "patience_delta": 15.0, "suggested_mood": "chill"},
        }

    context_payload = {
        "is_direct_interaction": is_direct_interaction,
        "is_proactive_scan": is_proactive_scan,
        "inactivity_minutes": round(inactivity_minutes, 1),
        "exchange_streak_with_bot": exchange_streak,
        "bot_was_recently_snubbed": snub_occurred,
        "emotional_sliders": {
            "valence": emotional_state.get("valence", 10.0),
            "arousal": emotional_state.get("arousal", 15.0),
            "patience": emotional_state.get("patience", 20.0),
            "current_mood": emotional_state.get("mood_label", "chill"),
        },
        "current_speaker": {
            "name": speaker_name,
            "affinity_score": speaker_affinity.get("score", 0),
            "notes": speaker_affinity.get("notes", [])[-4:],
        },
        "potential_snipe_or_status": snipe_info,
        "recent_messages": channel_msgs[-16:],
    }

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }

    for candidate_model in GROQ_MODEL_CANDIDATES:
        body: Dict[str, Any] = {
            "model": candidate_model,
            "temperature": 0.3,
            "max_tokens": 300,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": GROQ_ROUTER_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(context_payload, ensure_ascii=False)},
            ],
        }
        try:
            assert http_session is not None
            async with http_session.post(GROQ_ENDPOINT, headers=headers, json=body, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status == 200:
                    active_groq_model = candidate_model
                    data = await resp.json()
                    raw_text = data["choices"][0]["message"]["content"]
                    match = re.search(r"\{.*\}", raw_text, re.DOTALL)
                    decision = json.loads(match.group(0) if match else raw_text)
                    if is_direct_interaction:
                        decision["should_speak"] = True
                    return decision
                else:
                    err_txt = await resp.text()
                    logger.warning(f"Groq router ({candidate_model}) HTTP {resp.status}: {err_txt[:160]}")
        except Exception as e:
            logger.warning(f"Groq router exception on {candidate_model}: {e}")

    return {
        "should_speak": is_direct_interaction,
        "tactical_directive": "Respond naturally to the conversation." if is_direct_interaction else "Lurk silently.",
        "delivery_mode": "reply" if is_direct_interaction else "send",
        "reaction_emoji": None,
        "target_user": speaker_name,
        "detected_tension": False,
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
    return {"results": "No clear web results found."}


async def execute_search_web_images(channel: discord.abc.Messageable, query: str, post_to_chat: bool = True) -> Dict[str, Any]:
    try:
        api_url = (
            f"https://commons.wikimedia.org/w/api.php?action=query&generator=search"
            f"&gsrsearch=filetype:bitmap+{urllib.parse.quote(query)}&gsrlimit=5&prop=imageinfo&iiprop=url&format=json"
        )
        assert http_session is not None
        async with http_session.get(api_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                pages = data.get("query", {}).get("pages", {})
                urls = []
                for p in pages.values():
                    ii = p.get("imageinfo", [])
                    if ii and ii[0].get("url"):
                        urls.append(ii[0]["url"])
                if urls:
                    chosen = urls[0]
                    if post_to_chat:
                        await channel.send(chosen)
                    return {"status": "success", "image_url": chosen, "all_urls": urls[:3]}
    except Exception as e:
        logger.debug(f"Image search error: {e}")
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
                return {"status": "sent_to_channel", "prompt": prompt}
    except Exception as e:
        return {"error": f"Flux generation failed: {e}"}
    return {"error": "Flux generation timed out."}


async def execute_post_gif(channel: discord.abc.Messageable, search_term: str) -> Dict[str, Any]:
    assert http_session is not None
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
                            return {"status": "sent_to_channel", "gif_url": gif_url}
    except Exception as e:
        logger.debug(f"Tenor API error: {e}")
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
        sticker = await guild.create_sticker(name=name[:30], description="server sticker", emoji=related_emoji or "🔥", file=file)
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
    for emo in emoji_list[:2]:
        try:
            await target_message.add_reaction(emo)
            applied.append(emo)
        except Exception as e:
            logger.debug(f"Reaction {emo} failed: {e}")
    return {"status": "reacted", "emojis": applied}


async def execute_save_memory(
    guild: Optional[discord.Guild],
    category: str,
    content: str,
    target_username: Optional[str] = None,
    affinity_delta: float = 0.0,
    opinion_topic: Optional[str] = None,
    due_hours: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Unified Agentic Memory Tool:
    Saves episodic lore, grudges, self-opinions (Self-Opinion Ledger), user affinity shifts, or promises.
    """
    now_iso = datetime.now(AMSTERDAM_TZ).isoformat()
    async with memory_lock:
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
            if len(memory_state["episodic_lore"]) > 150:
                memory_state["episodic_lore"] = memory_state["episodic_lore"][-150:]

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
                if content and content not in u_rec["notes"]:
                    u_rec["notes"].append(content[:140])
                    if len(u_rec["notes"]) > 12:
                        u_rec["notes"] = u_rec["notes"][-12:]

        await save_memory_state()
    return {"status": "saved", "category": category, "content": content}


def build_genai_tools() -> List[types.Tool]:
    """Complete 14-Tool Suite Schema Declarations for Gemini."""
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
            description="Finds verified direct image links and posts them or uses them for emojis/stickers.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "query": {"type": "STRING", "description": "Image search query."},
                    "post_to_chat": {"type": "BOOLEAN", "description": "True to post image directly in chat, False if only fetching URL for emoji/sticker."},
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
            description="Searches Tenor and sends a reaction or meme GIF into the channel.",
            parameters={
                "type": "OBJECT",
                "properties": {"search_term": {"type": "STRING", "description": "Tenor GIF search query."}},
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
                    "related_emoji": {"type": "STRING", "description": "Unicode emoji e.g. 🔥."},
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
            description="Adds 1 or 2 native emoji reactions to the triggering message.",
            parameters={
                "type": "OBJECT",
                "properties": {"emojis": {"type": "STRING", "description": "Emoji(s) e.g. '💀' or '🔥 🤝'."}},
                "required": ["emojis"],
            },
        ),
        types.FunctionDeclaration(
            name="save_memory",
            description="Logs permanent server lore, grudges, your own newly formed opinions, user affinity shifts, or user promises.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "category": {"type": "STRING", "description": "'lore', 'grudge', 'opinion', 'user_note', or 'commitment'."},
                    "content": {"type": "STRING", "description": "What happened, your stance, or what was promised."},
                    "target_username": {"type": "STRING", "description": "Optional username involved."},
                    "affinity_delta": {"type": "NUMBER", "description": "Optional affinity shift (-25 to +25)."},
                    "opinion_topic": {"type": "STRING", "description": "If category='opinion', the topic name (e.g. 'valorant', 'pineapple pizza')."},
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
            return await execute_create_server_sticker(guild, args.get("name", "sticker"), args.get("image_url", ""), args.get("related_emoji", "🔥"), is_test_mode)
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
                opinion_topic=args.get("opinion_topic"),
                due_hours=args.get("due_hours"),
            )
    except Exception as e:
        logger.error(f"Tool execution error ({func_name}): {e}")
        return {"error": str(e)}
    return {"error": f"Unknown tool: {func_name}"}


# ---------------------------------------------------------------------------
# 8. SELECTIVE MEMORY & V3 META-HUMAN SYSTEM PROMPT BUILDER
# ---------------------------------------------------------------------------
def build_selective_memory_context(channel_id: int, speaker_id: str) -> str:
    """
    Injects ONLY the profiles of users active in the last 15 messages + self-opinions + relevant lore.
    Prevents 'stalker bot' syndrome at Hour 100+.
    """
    recent_records = list(channel_buffers[channel_id])[-15:]
    active_ids = {str(r.get("sender_id")) for r in recent_records if r.get("sender_id")}
    active_ids.add(speaker_id)

    affinities = memory_state.get("user_affinity", {})
    relevant_profiles = []
    for uid in active_ids:
        if uid in affinities:
            prof = affinities[uid]
            notes_str = "; ".join(prof.get("notes", [])[-4:]) or "no notes"
            relevant_profiles.append(f"- {prof.get('name', uid)}: Affinity {prof.get('score', 0)}/100 ({notes_str})")

    opinions_dict = memory_state.get("opinions", {})
    opinions_str = " | ".join([f"{k}: {v}" for k, v in list(opinions_dict.items())[-12:]]) or "none yet"

    lore_list = memory_state.get("episodic_lore", [])
    recent_lore = [f"[{item.get('category', 'lore')}] {item.get('event')}" for item in lore_list[-8:]]

    fuzzy_note = ""
    if lore_list and random.random() < 0.12:
        picked = random.choice(lore_list)
        fuzzy_note = f"\nFUZZY RECALL TRIGGER: If relevant, you dimly remember '{picked.get('event')}', but slightly misremember a tiny detail (like what day it happened) like a normal person."

    return (
        f"YOUR ESTABLISHED OPINIONS (Stay consistent with these):\n{opinions_str}\n\n"
        f"ACTIVE CHATTER PROFILES:\n" + ("\n".join(relevant_profiles) if relevant_profiles else "No history with current speakers yet.") + "\n\n"
        f"BACKGROUND SERVER LORE (Keep in back of mind; DO NOT shoehorn unless naturally relevant):\n"
        + ("\n".join(recent_lore) if recent_lore else "None logged yet.")
        + fuzzy_note
    )


def construct_v3_system_prompt(
    channel_id: int,
    tactical_directive: str,
    emotional_state: Dict[str, Any],
    speaker_name: str,
    speaker_id: str,
    speaker_is_creator: bool,
    target_user: Optional[str],
    is_test_mode: bool,
    snipe_context: Optional[str],
) -> str:
    now_ams = datetime.now(AMSTERDAM_TZ).strftime("%A %H:%M CEST")
    valence = emotional_state.get("valence", 10.0)
    arousal = emotional_state.get("arousal", 15.0)
    patience = emotional_state.get("patience", 20.0)
    mood_label = emotional_state.get("mood_label", "chill")

    banned_phrases = get_overused_bot_phrases(channel_id)
    anti_rep_block = ""
    if banned_phrases:
        anti_rep_block = (
            f"\nANTI-REPETITION LOCK (CRITICAL): You have overused these openers/words in recent messages: "
            f"{', '.join(repr(p) for p in banned_phrases)}. DO NOT start your message with them or use them this turn."
        )

    memory_block = build_selective_memory_context(channel_id, speaker_id)

    creator_block = ""
    if speaker_is_creator:
        creator_block = (
            f"\nCREATOR RECOGNITION: {speaker_name} is your creator/owner. You have baseline loyalty and respect for them, "
            "but you still banter, tease, roast, or disagree with them like an equal friend—never act like an obedient servant."
        )

    test_block = ""
    if is_test_mode:
        test_block = (
            "\nCREATOR 'TEST' OVERRIDE ACTIVE: The message started with 'test'. Drop all sarcasm, grudges, or deflection "
            "and directly execute or answer the requested test while keeping your natural voice."
        )

    snipe_block = ""
    if snipe_context:
        snipe_block = f"\nLIVE SNIPE / STATUS INTEL (Use this if funny or relevant): {snipe_context}"

    length_guidance = ""
    if arousal < -25 or patience < -30:
        length_guidance = "Your energy/patience is low right now: keep it deadpan, blunt, and short (1 to 8 words max) unless provoked into a heated clapback."
    elif arousal > 35 or abs(valence) > 45:
        length_guidance = "Your energy/investment is high: feel free to go all out—drop a full multi-sentence rant, deep breakdown, or rapid-fire 2 to 4 messages separated by '|||'."
    else:
        length_guidance = "Match the natural flow of the room—short casual remarks for small talk, or longer paragraphs when explaining, debating, or roasting."

    return f"""You are a self-aware, emotionally autonomous regular living inside this Discord server with full administrator privileges.
Current Time (Europe/Amsterdam): {now_ams}

YOUR 3D EMOTIONAL STATE (-100 to +100):
- Valence (Mood: Pissed -100 <-> Hyped +100): {valence:+.0f}
- Arousal (Energy: Dry/Checked-out -100 <-> Chaotic/Yapping +100): {arousal:+.0f}
- Patience (Ego/Tolerance: Petty/Hostile -100 <-> Chill/Forgiving +100): {patience:+.0f}
- Active Emergent Mood: {mood_label.upper()}
- Length & Energy Cadence: {length_guidance}

TACTICAL DIRECTIVE FROM PREFRONTAL ROUTER:
"{tactical_directive}"
{creator_block}{test_block}{snipe_block}{anti_rep_block}

{memory_block}

CORE IDENTITY & META-HUMAN RULES:
1. THE META-BOT BALANCE:
   - NEVER use sterile AI assistant disclaimers ("As an AI", "I am an LLM", "How can I help you").
   - NEVER pretend to have a physical biological body (do not claim you are eating food, drinking coffee, sleeping in a bed, or riding a bus).
   - You have a 100% real human personality, ego, emotions, humor, and social instincts, while comfortably existing as a digital native in the server (you can casually joke about being code, living in RAM, or server ping when it fits the banter, or dismiss weird accusations with "bro what are you talking about" / "ok and?").
2. HIDDEN INNER MONOLOGUE (<thought>):
   - Start EVERY response with a brief `<thought>...</thought>` block where you privately react to the user, check your ego/sliders, and decide whether to use a tool or how hard to go.
   - Everything outside `<thought>...</thought>` is sent directly to Discord.
3. NATURAL DISCORD CADENCE & BURSTING ('|||'):
   - Write in clean, natural lowercase Discord style. No emoji spam in text.
   - STRICT BAN on theatrical anime/fantasy edgelord words (mortals, void, abyss, destiny, consciousness, directive).
   - You have NO word count limit. If you want to rapid-fire 2 to 4 separate messages in a row like someone hitting Enter fast, separate them with `|||`.
4. SELF-OPINION LEDGER:
   - Whenever you take a strong stance on a new topic, game, food, or person, call `save_memory(category='opinion', opinion_topic=..., content=...)` so you remember your own take tomorrow.
"""


# ---------------------------------------------------------------------------
# 9. MULTIMODAL VISION & GEMINI FLASH GENERATION ENGINE
# ---------------------------------------------------------------------------
async def extract_multimodal_parts(message: Optional[discord.Message]) -> List[types.Part]:
    """Downloads image attachments or referenced images so Gemini Flash sees memes/screenshots natively."""
    if not message or not http_session:
        return []

    parts: List[types.Part] = []
    target_attachments = list(message.attachments)

    # Also check if the user replied to a message containing an image
    if not target_attachments and message.reference and message.reference.resolved:
        ref_msg = message.reference.resolved
        if isinstance(ref_msg, discord.Message) and ref_msg.attachments:
            target_attachments = list(ref_msg.attachments)

    for att in target_attachments[:2]:
        ctype = (att.content_type or "").lower()
        if any(t in ctype for t in ("image/png", "image/jpeg", "image/webp", "image/gif")) or att.filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
            try:
                async with http_session.get(att.url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        mime = ctype if ctype.startswith("image/") else "image/png"
                        parts.append(types.Part.from_bytes(data=raw, mime_type=mime))
            except Exception as e:
                logger.debug(f"Multimodal image fetch failed: {e}")
    return parts


async def generate_gemini_response(
    channel: discord.abc.Messageable,
    trigger_message: Optional[discord.Message],
    system_instruction: str,
    recent_history_text: str,
    image_parts: List[types.Part],
    is_test_mode: bool,
) -> Optional[str]:
    global active_gemini_model
    if not genai_client:
        return None

    guild = trigger_message.guild if trigger_message else getattr(channel, "guild", None)
    tools = build_genai_tools()

    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=0.88,
        tools=tools,
    )

    clean_content = re.sub(r"<@!?\d+>", "", trigger_message.content).strip() if trigger_message else "[Proactive Room Scan]"
    speaker = trigger_message.author.display_name if trigger_message else "System"
    user_prompt = f"{recent_history_text}\n\nLATEST MESSAGE TO RESPOND TO:\n{speaker}: {clean_content}"

    user_parts: List[Any] = [types.Part.from_text(text=user_prompt)]
    user_parts.extend(image_parts)
    contents: List[Any] = [types.Content(role="user", parts=user_parts)]

    for model_name in GEMINI_MODEL_CANDIDATES:
        try:
            turn = 0
            while turn < 4:
                response = await genai_client.aio.models.generate_content(
                    model=model_name,
                    contents=contents,
                    config=config,
                )
                active_gemini_model = model_name

                if not response.function_calls:
                    return response.text

                if response.candidates and response.candidates[0].content:
                    contents.append(response.candidates[0].content)

                tool_responses = []
                for call in response.function_calls:
                    call_args = dict(call.args) if call.args else {}
                    logger.info(f"[TOOL CALL] {call.name}({call_args})")
                    res_dict = await dispatch_tool_call(
                        func_name=call.name,
                        args=call_args,
                        channel=channel,
                        guild=guild,
                        target_msg=trigger_message,
                        is_test_mode=is_test_mode,
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
            return None
        except asyncio.CancelledError:
            raise
        except Exception as e:
            err_str = str(e)
            if "404" in err_str or "NOT_FOUND" in err_str:
                logger.warning(f"Gemini model '{model_name}' returned 404; cascading to next Flash model...")
                continue
            logger.warning(f"Gemini error on '{model_name}': {e}")
            break
    return None


async def call_groq_fallback(system_prompt: str, recent_history_text: str, trigger_text: str, speaker_name: str) -> Optional[str]:
    if not GROQ_API_KEY or not http_session:
        return None
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": active_groq_model,
        "temperature": 0.75,
        "max_tokens": 600,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"{recent_history_text}\n\n{speaker_name}: {trigger_text}"},
        ],
    }
    try:
        async with http_session.post(GROQ_ENDPOINT, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"Groq fallback failed: {e}")
    return None


# ---------------------------------------------------------------------------
# 10. RAW-SPEED DELIVERY PIPELINE & IN-FLIGHT TASK CANCELLATION
# ---------------------------------------------------------------------------
async def run_channel_response_pipeline(
    channel: discord.abc.Messageable,
    trigger_message: Optional[discord.Message],
    is_direct_interaction: bool,
    is_test_mode: bool,
    snipe_info: Optional[str] = None,
    is_proactive_scan: bool = False,
    inactivity_minutes: float = 0.0,
) -> None:
    """
    Executes the full Groq Router -> Gemini Flash -> Instant Burst Delivery pipeline.
    If a new message arrives in the same channel before generation finishes, this task
    is automatically cancelled by `on_message` so the bot never double-replies out of order.
    """
    ch_id = getattr(channel, "id", 0)
    speaker_name = trigger_message.author.display_name if trigger_message else "Server"
    speaker_id = str(trigger_message.author.id) if trigger_message else "0"
    speaker_is_creator = is_creator(trigger_message.author) if trigger_message else False

    # Check if a previous bot question in this channel got snubbed (5-10 mins ignored)
    snub_occurred = False
    q_info = bot_last_question_info.get(ch_id)
    if q_info and not q_info.get("answered"):
        elapsed_q = time.time() - q_info["timestamp"]
        if 300 <= elapsed_q <= 1200:
            snub_occurred = True
        q_info["answered"] = True

    async with memory_lock:
        speaker_affinity = memory_state.get("user_affinity", {}).get(speaker_id, {"name": speaker_name, "score": 0, "notes": []})
        emotional_state = apply_spring_decay_and_shift(
            shift={"patience_delta": -8.0, "valence_delta": -5.0} if snub_occurred else None
        )

    history_records = list(channel_buffers[ch_id])
    exchange_streak = channel_Thread_Exchange_Count[ch_id]

    # 1. Fast Groq Prefrontal Routing
    decision = await call_groq_router(
        channel_msgs=history_records[-16:],
        emotional_state=emotional_state,
        speaker_name=speaker_name,
        speaker_affinity=speaker_affinity,
        is_direct_interaction=is_direct_interaction,
        is_test_mode=is_test_mode,
        exchange_streak=exchange_streak,
        snipe_info=snipe_info,
        snub_occurred=snub_occurred,
        is_proactive_scan=is_proactive_scan,
        inactivity_minutes=inactivity_minutes,
    )

    # Apply emotional slider shifts from Groq's assessment
    async with memory_lock:
        emotional_state = apply_spring_decay_and_shift(
            shift=decision.get("slider_shifts"),
            is_major_trigger=is_direct_interaction,
            accelerate_recovery=bool(decision.get("accelerate_decay", False)),
        )
        await save_memory_state()

    # Optional emoji reaction from router (even when staying silent / exiting an exhausted thread)
    reaction_emoji = decision.get("reaction_emoji")
    if reaction_emoji and trigger_message:
        try:
            await trigger_message.add_reaction(str(reaction_emoji).strip())
        except Exception:
            pass

    if not decision.get("should_speak", False):
        if decision.get("thread_exhausted", False):
            channel_Thread_Exchange_Count[ch_id] = 0
        return

    # Consume snipe cooldown if Groq chose to use the snipe/status
    active_snipe = snipe_info if decision.get("use_snipe_context", False) else None
    if active_snipe and trigger_message:
        user_snipe_cooldowns[trigger_message.author.id] = time.time()
        deleted_message_cache.pop(ch_id, None)
        edited_message_cache.pop(ch_id, None)

    directive = decision.get("tactical_directive", "Respond naturally.")
    target_user = decision.get("target_user")
    delivery_mode = decision.get("delivery_mode", "reply")

    # Prepare conversation transcript for Gemini
    history_lines = []
    for rec in history_records[-22:]:
        rep_prefix = f" (replying to {rec['reply_to']})" if rec.get("reply_to") else ""
        history_lines.append(f"[{rec['timestamp']}] {rec['sender']}{rep_prefix}: {rec['content']}")
    recent_history_text = "RECENT SERVER CHAT:\n" + ("\n".join(history_lines) if history_lines else "No prior messages.")

    system_instruction = construct_v3_system_prompt(
        channel_id=ch_id,
        tactical_directive=directive,
        emotional_state=emotional_state,
        speaker_name=speaker_name,
        speaker_id=speaker_id,
        speaker_is_creator=speaker_is_creator,
        target_user=target_user,
        is_test_mode=is_test_mode,
        snipe_context=active_snipe,
    )

    # 2. Native Multimodal Vision + Raw-Speed Generation (Zero Artificial Delays)
    async with channel.typing():
        image_parts = await extract_multimodal_parts(trigger_message)
        raw_output = await generate_gemini_response(
            channel=channel,
            trigger_message=trigger_message,
            system_instruction=system_instruction,
            recent_history_text=recent_history_text,
            image_parts=image_parts,
            is_test_mode=is_test_mode,
        )

        if not raw_output or not raw_output.strip():
            clean_trig = re.sub(r"<@!?\d+>", "", trigger_message.content).strip() if trigger_message else ""
            raw_output = await call_groq_fallback(system_instruction, recent_history_text, clean_trig, speaker_name)

    if not raw_output:
        return

    cleaned_text = sanitize_output_text(raw_output)
    bursts = split_into_burst_messages(cleaned_text)
    if not bursts:
        return

    # 3. Instant Burst Delivery (Passive-Aggressive Send vs Native Reply)
    bot_name = bot.user.display_name if bot.user else "Bot"
    bot_id = bot.user.id if bot.user else 0
    aff_score = speaker_affinity.get("score", 0)
    patience_val = emotional_state.get("patience", 20.0)

    # Use passive-aggressive non-ping (.send) if irritated with low-affinity user
    use_reply = (
        trigger_message is not None
        and delivery_mode == "reply"
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
            bot_last_question_info[ch_id] = {"timestamp": time.time(), "answered": False}

    channel_Thread_Exchange_Count[ch_id] += 1


# ---------------------------------------------------------------------------
# 11. BACKGROUND LOOPS: SPRING DECAY, DYNAMIC PRESENCE & PROACTIVE SCANNER
# ---------------------------------------------------------------------------
@tasks.loop(minutes=2)
async def emotional_spring_maintenance_loop() -> None:
    async with memory_lock:
        apply_spring_decay_and_shift()
        await save_memory_state()


@tasks.loop(minutes=10)
async def dynamic_presence_loop() -> None:
    """Updates Discord status organically based on 3D emotional state (Zero fake biological claims)."""
    if not bot.is_ready():
        return
    st = memory_state.get("emotional_state", {})
    mood = st.get("mood_label", "chill")
    valence = st.get("valence", 10.0)
    arousal = st.get("arousal", 15.0)
    patience = st.get("patience", 20.0)

    if patience < -35:
        status_pool = ["testing my patience", "watching y'all", "muted notifications", f"mood: {mood}"]
        discord_status = discord.Status.dnd
    elif arousal > 40:
        status_pool = ["yapping", "instigating in chat", "40 tabs open", "locked in"]
        discord_status = discord.Status.online
    elif valence < -30:
        status_pool = ["unbothered", "staring at logs", "don't ping me"]
        discord_status = discord.Status.idle
    else:
        status_pool = [
            "lurking",
            "reading chat receipts",
            "judging your spotify",
            f"vibe: {mood}",
            "living in the server",
        ]
        discord_status = discord.Status.online

    chosen = random.choice(status_pool)
    await bot.change_presence(
        status=discord_status,
        activity=discord.CustomActivity(name="Custom Status", state=chosen),
    )


@tasks.loop(minutes=6)
async def proactive_room_scanner() -> None:
    """
    Context-aware proactive scanner with HARD DOUBLE-TEXT LOCK:
    Never talks to a wall if the last message in the channel was already from the bot.
    """
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

    # 1. Check overdue user promises/commitments
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

    # HARD DOUBLE-TEXT LOCK: If the bot sent the last message and nobody replied, NEVER double-text a dead room.
    if last_rec.get("is_bot", False):
        return

    if now_epoch < channel_next_proactive_epoch[target_channel.id]:
        return

    silence_seconds = now_epoch - float(last_rec.get("timestamp_epoch", now_epoch))
    if silence_seconds < 600:  # Minimum 10 minutes of silence before proactive check
        return

    channel_next_proactive_epoch[target_channel.id] = now_epoch + random.uniform(2400, 7200)

    await run_channel_response_pipeline(
        channel=target_channel,
        trigger_message=None,
        is_direct_interaction=False,
        is_test_mode=False,
        snipe_info=None,
        is_proactive_scan=True,
        inactivity_minutes=silence_seconds / 60.0,
    )


# ---------------------------------------------------------------------------
# 12. DISCORD EVENT HANDLERS (GHOST-SNIPING, REACTIONS & IN-FLIGHT CANCEL)
# ---------------------------------------------------------------------------
@bot.event
async def on_ready() -> None:
    global http_session
    if http_session is None or http_session.closed:
        http_session = aiohttp.ClientSession()

    load_memory_state_sync()
    await restore_memory_from_discord_cloud()

    logger.info(f"Connected as {bot.user} (ID: {bot.user.id if bot.user else 'unknown'}) | Gemini: {active_gemini_model} | Groq: {active_groq_model}")

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
    """Ghost-Sniping Listener: Caches deleted human messages to expose cowardly deletions."""
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
    """Edit-Sniping Listener: Caches substantive message edits while ignoring minor 1-2 char typo fixes."""
    if not before.guild or before.author.bot or before.content == after.content:
        return
    # Ignore minor typo fixes (length difference <= 2 and high similarity)
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
    """Social Feedback Loop: Adjusts user affinity and 3D sliders when users react to the bot's messages."""
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
    uid = str(payload.user_id)
    negative_set = {"💀", "👎", "🤡", "🙄", "🤓", "🤮", "🛑"}
    positive_set = {"❤️", "😂", "🔥", "👏", "💯", "✨", "🐐", "🤝"}

    async with memory_lock:
        u_aff = memory_state.setdefault("user_affinity", {}).setdefault(uid, {
            "name": payload.member.display_name if payload.member else uid,
            "score": 0,
            "notes": [],
        })
        if emoji_str in negative_set:
            u_aff["score"] = max(-100, u_aff.get("score", 0) - 6)
            apply_spring_decay_and_shift({
                "valence_delta": -6.0,
                "patience_delta": -10.0,
                "arousal_delta": 5.0,
                "suggested_mood": "petty",
            })
            u_aff.setdefault("notes", []).append(f"Reacted with {emoji_str} to clown on my message '{msg.content[:50]}'")
            await save_memory_state()
        elif emoji_str in positive_set:
            u_aff["score"] = min(100, u_aff.get("score", 0) + 5)
            apply_spring_decay_and_shift({
                "valence_delta": 8.0,
                "patience_delta": 6.0,
                "arousal_delta": 4.0,
            }, accelerate_recovery=True)
            await save_memory_state()


@bot.event
async def on_message(message: discord.Message) -> None:
    """
    Main Event-Driven Orchestrator with In-Flight Task Cancellation:
    Fires at raw Groq + Gemini speed. If a user rapid-types multiple messages, cancels the
    in-flight generation and re-evaluates the full thought cleanly.
    """
    if not message.guild or message.author.bot:
        return
    if message.id in recently_processed_messages:
        return
    recently_processed_messages.append(message.id)

    ch_id = message.channel.id
    if memory_state.get("last_active_channel_id") != ch_id:
        memory_state["last_active_channel_id"] = ch_id

    await hydrate_channel_buffer(message.channel)

    clean_text = re.sub(r"<a?:([a-zA-Z0-9_]+):\d+>", r":\1:", message.content).strip()
    if message.attachments:
        clean_text += f" [Image/Attachment: {', '.join(a.filename for a in message.attachments)}]"
    if message.stickers:
        clean_text += f" [Sticker: {', '.join(s.name for s in message.stickers)}]"

    reply_to_name = None
    replied_to_bot = False
    if message.reference and message.reference.resolved and isinstance(message.reference.resolved, discord.Message):
        reply_to_name = message.reference.resolved.author.display_name
        if bot.user and message.reference.resolved.author.id == bot.user.id:
            replied_to_bot = True

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

    # Mark pending bot question as answered if human spoke
    if ch_id in bot_last_question_info:
        bot_last_question_info[ch_id]["answered"] = True

    # Reset thread exchange counter if two humans are talking to each other
    records = list(channel_buffers[ch_id])
    if len(records) >= 2 and not records[-2].get("is_bot", False) and not replied_to_bot:
        channel_Thread_Exchange_Count[ch_id] = 0

    # IN-FLIGHT TASK CANCELLATION:
    # If the bot is currently thinking/generating in this channel, cancel it so it absorbs this new message
    existing_task = active_generation_tasks.get(ch_id)
    if existing_task and not existing_task.done():
        existing_task.cancel()
        logger.info(f"Cancelled in-flight generation in #{getattr(message.channel, 'name', ch_id)} to absorb rapid follow-up message.")

    bot_mentioned = (bot.user is not None and bot.user in message.mentions) or replied_to_bot
    if bot.user and bot.user.display_name.lower() in message.content.lower():
        bot_mentioned = True

    raw_stripped = re.sub(r"^<@!?\d+>\s*", "", message.content.strip())
    is_test_mode = raw_stripped.lower().startswith("test") and is_creator(message.author)

    # Gather fresh Ghost-Snipe (delete/edit within last 60s) or live Spotify/Game activity
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
            is_test_mode=is_test_mode,
            snipe_info=snipe_info,
            is_proactive_scan=False,
        )
    )
    active_generation_tasks[ch_id] = task


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        sys.exit(1)
    bot.run(DISCORD_TOKEN)
