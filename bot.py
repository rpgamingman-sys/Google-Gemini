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

try:
    from discord.ext import voice_recv  # type: ignore
    VOICE_RECV_AVAILABLE = True
except Exception:
    VOICE_RECV_AVAILABLE = False

try:
    import davey  # type: ignore
    DAVEY_AVAILABLE = True
except Exception:
    DAVEY_AVAILABLE = False


def load_opus_library() -> bool:
    """Ensures libopus is loaded into discord.opus so Discord voice encoding and decoding work smoothly."""
    if discord.opus.is_loaded():
        return True
    try:
        if discord.opus._load_default():
            return True
    except Exception:
        pass
    import glob
    script_dir = os.path.dirname(os.path.abspath(__file__))
    patterns = [
        os.path.join(script_dir, "libopus.so.0"),
        os.path.join(script_dir, "libopus.so"),
        "./libopus.so.0",
        "/tmp/libopus.so.0",
        "/data/libopus.so.0",
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
    if sys.platform.startswith("linux") and not discord.opus.is_loaded():
        try:
            import urllib.request
            import tarfile
            deb_url = "http://deb.debian.org/debian/pool/main/o/opus/libopus0_1.3.1-3_amd64.deb"
            deb_bytes = urllib.request.urlopen(deb_url, timeout=8).read()
            xz_idx = deb_bytes.rfind(b"\xfd7zXZ\x00")
            if xz_idx != -1:
                with tarfile.open(fileobj=io.BytesIO(deb_bytes[xz_idx:]), mode="r:xz") as tf:
                    for m in tf.getmembers():
                        if "libopus.so.0." in m.name:
                            so_f = tf.extractfile(m)
                            if so_f:
                                so_data = so_f.read()
                                target_so = "/tmp/libopus.so.0"
                                with open(target_so, "wb") as out_f:
                                    out_f.write(so_data)
                                discord.opus.load_opus(target_so)
                                break
        except Exception:
            pass
    return discord.opus.is_loaded()


load_opus_library()


def patch_voice_recv_for_dave_and_keepalive() -> None:
    """
    Patches discord-ext-voice-recv for full compatibility with discord.py 2.7+ DAVE E2EE audio encryption
    and connected UDP sockets so incoming human speech in Voice Channels is decoded cleanly without crashes.
    """
    if not VOICE_RECV_AVAILABLE:
        return
    try:
        from discord.ext.voice_recv import reader as vr_reader, opus as vr_opus  # type: ignore

        def _safe_keepalive_run(self: Any) -> None:
            while not self._end_thread.is_set():
                self._end_thread.wait(self.delay)
                if self._end_thread.is_set():
                    break
                try:
                    vc = self.voice_client
                    conn = getattr(vc, "_connection", None)
                    if conn and hasattr(conn, "send_packet"):
                        conn.send_packet(self.PACKET)
                    elif conn and getattr(conn, "socket", None):
                        conn.socket.send(self.PACKET)
                except Exception:
                    pass

        vr_reader.UDPKeepAlive.run = _safe_keepalive_run

        def _dave_aware_process_packet(self: Any, packet: Any) -> Any:
            vc = self.sink.voice_client
            if self._cached_id is None:
                self._cached_id = vc._get_id_from_ssrc(self.ssrc)
            pcm = None
            if not self.sink.wants_opus():
                packet, pcm = self._decode_packet(packet)
            member = self._get_cached_member()
            if member is None:
                self._cached_id = vc._get_id_from_ssrc(self.ssrc)
                member = self._get_cached_member()
            if member is None and getattr(vc, "channel", None):
                humans = [
                    m for m in vc.channel.members
                    if not m.bot and not (m.voice and (m.voice.self_mute or m.voice.mute))
                ] or [m for m in vc.channel.members if not m.bot]
                if humans:
                    member = humans[0]
                    self._cached_id = member.id
                    try:
                        vc._add_ssrc(member.id, self.ssrc)
                    except Exception:
                        pass
            data = vr_opus.VoiceData(packet, member, pcm=pcm)
            self._last_seq = packet.sequence
            self._last_ts = packet.timestamp
            return data

        def _dave_aware_decode_packet(self: Any, packet: Any) -> Tuple[Any, bytes]:
            if self._decoder is None:
                try:
                    self._decoder = discord.opus.Decoder()
                except Exception:
                    return packet, b"\x00" * 3840

            if packet and getattr(packet, "decrypted_data", None):
                raw_payload = bytes(packet.decrypted_data)
                if raw_payload != b"\xf8\xff\xfe" and DAVEY_AVAILABLE:
                    try:
                        vc = self.sink.voice_client
                        conn = getattr(vc, "_connection", None)
                        dave_session = getattr(conn, "dave_session", None) if conn else None
                        uid = self._cached_id or vc._get_id_from_ssrc(self.ssrc)
                        if dave_session is not None:
                            if uid:
                                raw_payload = dave_session.decrypt(int(uid), davey.MediaType.audio, raw_payload)
                                packet.decrypted_data = raw_payload
                            else:
                                cand_ids: List[int] = []
                                if getattr(vc, "channel", None):
                                    cand_ids.extend([m.id for m in vc.channel.members if not m.bot])
                                try:
                                    for du in dave_session.get_user_ids():
                                        if str(du).isdigit() and int(du) not in cand_ids:
                                            cand_ids.append(int(du))
                                except Exception:
                                    pass
                                for cand_uid in cand_ids:
                                    try:
                                        dec = dave_session.decrypt(int(cand_uid), davey.MediaType.audio, raw_payload)
                                        raw_payload = dec
                                        packet.decrypted_data = dec
                                        self._cached_id = int(cand_uid)
                                        vc._add_ssrc(int(cand_uid), self.ssrc)
                                        break
                                    except Exception:
                                        continue
                    except Exception:
                        pass
                try:
                    pcm = self._decoder.decode(raw_payload, fec=False)
                    return packet, pcm
                except Exception:
                    return packet, b"\x00" * 3840

            try:
                next_packet = self._buffer.peek_next()
                if next_packet is not None and getattr(next_packet, "decrypted_data", None):
                    pcm = self._decoder.decode(bytes(next_packet.decrypted_data), fec=True)
                    return packet, pcm
                pcm = self._decoder.decode(None, fec=False)
                return packet, pcm
            except Exception:
                return packet, b"\x00" * 3840

        vr_opus.PacketDecoder._process_packet = _dave_aware_process_packet
        vr_opus.PacketDecoder._decode_packet = _dave_aware_decode_packet
    except Exception:
        pass


patch_voice_recv_for_dave_and_keepalive()

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
CREATOR_USER_ID = int(os.getenv("CREATOR_USER_ID") or "812014272501907476")
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

# Primary Gemini Models (automatically demotes any model that hits daily quota limits)
ENV_GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
GEMINI_MODEL_CANDIDATES: List[str] = list(dict.fromkeys([
    "gemini-3.1-flash-lite-preview",
    ENV_GEMINI_MODEL,
    "gemini-3.5-flash-lite-preview",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
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
GROQ_AUDIO_TRANSCRIPTIONS_ENDPOINT = "https://api.groq.com/openai/v1/audio/transcriptions"

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
    r"\bbesties\b": "y'all",
    r"\bmy bestie\b": "bro",
    r"\bbestie\b": "",
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
channel_recent_burst_counts: Dict[int, deque] = defaultdict(lambda: deque(maxlen=6))
guild_cross_channel_history: Dict[int, deque] = defaultdict(lambda: deque(maxlen=75))
recent_sent_gif_urls: deque = deque(maxlen=60)
pending_tease_timeouts: Dict[Tuple[int, int], asyncio.Task] = {}

# Text & Soundboard Anti-Spam State
user_recent_msg_epochs: Dict[Tuple[int, int], deque] = defaultdict(lambda: deque(maxlen=12))
user_recent_msg_texts: Dict[Tuple[int, int], deque] = defaultdict(lambda: deque(maxlen=8))
user_spam_ignore_until: Dict[Tuple[int, int], float] = defaultdict(float)
user_spam_ignored_count: Dict[Tuple[int, int], int] = defaultdict(int)

voice_speaking_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
last_vc_speaker_logged: Dict[int, float] = {}
vc_speech_buffers: Dict[int, Dict[int, Dict[str, Any]]] = defaultdict(dict)
vc_speech_processor_tasks: Dict[int, asyncio.Task] = {}
vc_transcribing_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
vc_pending_events: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
vc_batch_reactor_tasks: Dict[int, asyncio.Task] = {}

soundboard_recent_epochs: Dict[int, deque] = defaultdict(lambda: deque(maxlen=15))
soundboard_mute_until: Dict[int, float] = defaultdict(float)
soundboard_last_reacted_epoch: Dict[int, float] = defaultdict(float)
dm_proactive_last_sent: Dict[int, float] = defaultdict(float)

# Responsive Admin Cooldowns (Allows frequent spontaneous admin power moves while preventing API rate limits)
admin_tool_cooldowns: Dict[str, float] = defaultdict(float)
ADMIN_COOLDOWN_SECONDS = {
    "change_my_nickname": 6.0,
    "change_nickname": 8.0,
    "change_my_status": 5.0,
    "set_channel_topic": 15.0,
    "pin_message": 8.0,
    "unpin_message": 8.0,
    "delete_message": 12.0,
    "set_slowmode": 30.0,
    "create_thread": 30.0,
    "create_poll": 30.0,
    "create_role": 15.0,
    "assign_role": 6.0,
    "remove_role": 6.0,
    "rename_channel": 60.0,
    "create_server_emoji": 20.0,
    "create_server_sticker": 25.0,
    "change_my_avatar": 120.0,
    "voice_moderate_member": 8.0,
    "tease_timeout_user": 15.0,
    "timeout_user": 25.0,
    "tease_kick_user": 30.0,
    "kick_user": 90.0,
    "create_text_channel": 45.0,
    "create_secret_channel": 15.0,
    "dm_user": 5.0,
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
    "active_grudges": [],
    "user_aliases": {
        "shadow": 1472841531042959482,
        "mrshadow": 1472841531042959482,
    },
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
    """
    if patience <= -55 and valence <= -35:
        return "mad / crashing out / ready to snap" if arousal > 10 else "sassy attitude / unbothered side-eye"
    if patience <= -25 and valence <= -15:
        return "sassy attitude / giving side-eye" if arousal <= 25 else "unhinged / chaotic menace"
    if valence <= -35 and arousal <= -10:
        return "pleading / needy / dramatic begging" if patience >= 0 else "dramatically heartbroken / pouting victim"
    if valence <= -20 and patience >= 10:
        return "pleading / needy / begging for attention"
    if arousal >= 65 and abs(valence) <= 45:
        return "unhinged / insane / feral gremlin"
    if arousal <= -35:
        return "content / cozy low-battery chill" if valence >= 0 else "deadpan / tired sighing"
    if valence >= 55 and arousal >= 55:
        return "manic joy / pure unfiltered hype"
    if valence >= 35 and arousal < 35:
        return "content / cozy / chill vibing"
    if valence >= 20 and arousal >= 20:
        return "cute playful / teasing banter"
    return "unhinged / chaotic gremlin" if arousal > 40 else "content / chill / vibing"


def decay_emotional_sliders_only(accelerate_recovery: bool = False) -> Dict[str, Any]:
    """
    Applies natural time-based spring decay toward baseline (valence +50, arousal +55, patience +55)
    and auto-expires stale grudges older than 12 minutes so the bot never gets stuck in a repetitive loop.
    """
    st = memory_state.setdefault("emotional_state", dict(DEFAULT_MEMORY["emotional_state"]))
    now = time.time()
    last_t = float(st.get("last_updated_epoch", now))
    dt = max(0.0, now - last_t)

    # Auto-expire grudges older than 12 minutes so the bot doesn't nag forever
    for g in memory_state.get("active_grudges", []):
        if not g.get("forgiven"):
            try:
                ts_str = g.get("timestamp", "")
                if ts_str:
                    g_dt = datetime.fromisoformat(ts_str)
                    if g_dt.tzinfo is None:
                        g_dt = g_dt.replace(tzinfo=AMSTERDAM_TZ)
                    if (datetime.now(AMSTERDAM_TZ) - g_dt).total_seconds() > 720.0:
                        g["forgiven"] = True
            except Exception:
                g["forgiven"] = True

    if dt < 1.0:
        return st

    # Fast 3-minute natural half-life (or 60s when accelerate_recovery is True) for lively mood fluidity
    half_life = 60.0 if accelerate_recovery else 180.0
    decay_factor = math.pow(0.5, dt / half_life)

    for axis, base_val in BASELINE_SLIDERS.items():
        curr = float(st.get(axis, base_val))
        decayed = base_val + (curr - base_val) * decay_factor
        st[axis] = round(max(-100.0, min(100.0, decayed)), 1)

    if dt > 240.0:
        st["mood_label"] = derive_emergent_mood(st["valence"], st["arousal"], st["patience"])

    st["last_updated_epoch"] = now
    return st


def apply_spring_decay_and_shift(
    shift: Optional[Dict[str, Any]] = None,
    is_major_trigger: bool = True,
    accelerate_recovery: bool = False,
) -> Dict[str, Any]:
    """
    Updates the continuous 3D emotional sliders with high volatility so the bot fluidly shifts
    between attitude, mad, content, pleading, cute, and unhinged/insane based on context & vibe!
    """
    st = decay_emotional_sliders_only(accelerate_recovery=accelerate_recovery)
    now = time.time()
    clamp_limit = 85.0 if is_major_trigger else 50.0

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

    # Harmonize sliders with suggested mood archetype so sliders never trap the bot in an old mood
    m_low = candidate_mood.lower()
    if any(k in m_low for k in ("plead", "beg", "needy", "clingy", "whin")):
        st["valence"] = min(st["valence"], -25.0)
        st["patience"] = max(st["patience"], 20.0)
        st["arousal"] = max(st["arousal"], 35.0)
    elif any(k in m_low for k in ("content", "chill", "cozy", "relaxed", "peaceful", "happy", "warm")):
        st["valence"] = max(st["valence"], 45.0)
        st["patience"] = max(st["patience"], 45.0)
        for g in memory_state.get("active_grudges", []):
            g["forgiven"] = True
    elif any(k in m_low for k in ("unhinged", "insane", "feral", "chaotic", "gremlin", "manic", "psycho")):
        st["arousal"] = max(st["arousal"], 75.0)
    elif any(k in m_low for k in ("attitude", "sass", "diva", "side-eye", "unbothered")):
        st["patience"] = min(st["patience"], -25.0)
        st["valence"] = min(st["valence"], -15.0)
    elif any(k in m_low for k in ("mad", "furious", "rage", "crash", "snap", "heated")):
        st["patience"] = min(st["patience"], -65.0)
        st["valence"] = min(st["valence"], -55.0)
        st["arousal"] = max(st["arousal"], 65.0)
    elif any(k in m_low for k in ("cute", "playful", "teas", "hype", "jolly")):
        st["valence"] = max(st["valence"], 40.0)
        st["arousal"] = max(st["arousal"], 40.0)
        for g in memory_state.get("active_grudges", []):
            g["forgiven"] = True

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
        r"\b(?:say\s+(?:[a-zA-Z]+\s+)?to|talk\s+to|go\s+talk\s+to|speak\s+to|taunt|roast|tell|ask|ping|poke|flame|cook|bully|drag|confront|message|get|humble|pack\s+up|yell\s+at)\s+@?([a-zA-Z0-9_.\-]{2,32})",
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
    Detects when someone explicitly tells the bot to be quiet, stop talking, or shut up.
    Does NOT trigger on casual conversational words like 'chill', 'relax', 'enough', or 'silence'!
    """
    if not content or not is_direct_or_active:
        return False
    cleaned = re.sub(r"<@!?\d+>", "", content).strip().lower()
    if len(cleaned) > 75:
        return False
    hush_pattern = (
        r"(?:\A|\b)(?:shh+|shush|hush(?:\s+up)?|be\s+(?:very\s+)*quiet|stay\s+quiet|"
        r"stop\s+talking|shut\s+(?:the\s+fuck\s+|tf\s+)?up|stfu|zip\s+it|pipe\s+down|"
        r"don'?t\s+speak|go\s+quiet|mute\s+yourself|stop\s+yapping|quit\s+yapping)(?:\b|\Z)"
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

    # Strip enclosing backticks or quotes around mentions or channels if model outputted `@name` or `#channel`
    text = re.sub(r"[`'\"]@([a-zA-Z0-9_.\- ]+)[`'\"]", r"@\1", text)
    text = re.sub(r"[`'\"]#([a-zA-Z0-9_.\-]+)[`'\"]", r"#\1", text)

    my_id = guild.me.id if guild.me else 0
    creator_id = CREATOR_USER_ID or (guild.owner.id if guild.owner else 0)

    # 1. FIX BRACKETED USER MENTIONS: If model outputted `<@username>` or `<@dogee>` with text inside brackets
    def _fix_non_digit_mention(m: re.Match) -> str:
        inner = m.group(1).strip().strip("@")
        if inner.isdigit():
            return f"<@{inner}>"
        if creator_id and "dogee" in inner.lower():
            return f"<@{creator_id}>"
        mem = find_member(guild, inner)
        if mem and mem.id != my_id:
            return f"<@{mem.id}>"
        return f"@{inner}"

    text = re.sub(r"<@!?([a-zA-Z_][a-zA-Z0-9_.\- ]*)>", _fix_non_digit_mention, text)

    # 2. DISCORD CHANNEL TAGS: Convert #channel-name or <#channel-name> to clickable Discord link <#channel_id>
    ch_candidates: List[Tuple[str, int]] = []
    for c in guild.channels:
        if getattr(c, "name", None):
            ch_candidates.append((c.name.strip(), c.id))
    for t in getattr(guild, "threads", []):
        if getattr(t, "name", None):
            ch_candidates.append((t.name.strip(), t.id))
    ch_candidates.sort(key=lambda x: len(x[0]), reverse=True)

    def _fix_non_digit_channel(m: re.Match) -> str:
        inner = m.group(1).strip().strip("#")
        if inner.isdigit():
            return f"<#{inner}>"
        for ch_name, ch_id in ch_candidates:
            if inner.lower() == ch_name.lower():
                return f"<#{ch_id}>"
        return f"#{inner}"

    text = re.sub(r"<#([a-zA-Z_][a-zA-Z0-9_.\-]*)>", _fix_non_digit_channel, text)

    if "#" in text:
        for ch_name, ch_id in ch_candidates:
            pat_ch = rf"(?<!<)#{re.escape(ch_name)}(?=\Z|\s|[.,!?:;\x22\x27\)\]—–\-])"
            if re.search(pat_ch, text, flags=re.IGNORECASE):
                text = re.sub(pat_ch, f"<#{ch_id}>", text, flags=re.IGNORECASE)

    # 3. DISCORD USER MENTIONS: Convert @username or @dogee to real clickable mention <@uid>
    member_candidates: List[Tuple[str, int]] = []
    if creator_id:
        member_candidates.append(("dogee", creator_id))

    # Add community aliases (e.g. shadow -> 1472841531042959482)
    community_aliases = {
        "shadow": 1472841531042959482,
        "mrshadow": 1472841531042959482,
        "mrshadowjsu": 1472841531042959482,
    }
    user_aliases = memory_state.get("user_aliases", {})
    for alias_k, uid_val in {**community_aliases, **user_aliases}.items():
        member_candidates.append((alias_k, int(uid_val)))

    for m in guild.members:
        if m.id == my_id:
            continue
        for n in (m.display_name, m.name, m.nick, getattr(m, "global_name", None)):
            if n and len(n.strip()) >= 2:
                member_candidates.append((n.strip(), m.id))
    member_candidates.sort(key=lambda x: len(x[0]), reverse=True)

    if "@" in text:
        for name_str, uid in member_candidates:
            pattern = rf"(?<!<)@{re.escape(name_str)}(?:#\d{{4}})?(?=\Z|\s|[.,!?:;\x22\x27\)\]—–\-])"
            if re.search(pattern, text, flags=re.IGNORECASE):
                text = re.sub(pattern, f"<@{uid}>", text, flags=re.IGNORECASE)

    # Convert plain references to "creator dogee" into clickable mention
    if creator_id and f"<@{creator_id}>" not in text:
        text = re.sub(r"\b(?:my\s+)?creator\s+dogee\b", f"creator <@{creator_id}>", text, flags=re.IGNORECASE)

    # 4. If user explicitly commanded to engage a member (force_target_member)
    if force_target_member and force_target_member.id != my_id and f"<@{force_target_member.id}>" not in text:
        replaced_plain = False
        target_aliases = [
            force_target_member.display_name,
            force_target_member.name,
            force_target_member.nick,
            getattr(force_target_member, "global_name", None),
        ]
        for a_k, a_uid in {**community_aliases, **user_aliases}.items():
            if int(a_uid) == force_target_member.id:
                target_aliases.append(a_k)
        for name_variant in target_aliases:
            if name_variant and len(name_variant) >= 2:
                pat = rf"\b{re.escape(name_variant)}\b"
                if re.search(pat, text, flags=re.IGNORECASE):
                    text = re.sub(pat, f"<@{force_target_member.id}>", text, count=1, flags=re.IGNORECASE)
                    replaced_plain = True
                    break
        if not replaced_plain:
            text = f"<@{force_target_member.id}> {text}"

    # 5. If allow_speaker_ping is True and current speaker is directly addressed at the start
    if allow_speaker_ping and current_speaker_name and "@" not in text and "<@" not in text:
        spk_mem = find_member(guild, current_speaker_name)
        if spk_mem and spk_mem.id != my_id and f"<@{spk_mem.id}>" not in text:
            pat_start = rf"\A{re.escape(current_speaker_name)}\b[,\s]*"
            if re.search(pat_start, text, flags=re.IGNORECASE):
                text = re.sub(pat_start, f"<@{spk_mem.id}> ", text, count=1, flags=re.IGNORECASE)

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
    guild_id: Optional[int] = None,
    channel_name: str = "",
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

    msg_record = {
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
    }
    channel_buffers[channel_id].append(msg_record)

    # Record into server-wide cross-channel memory feed
    if guild_id and channel_name:
        guild_cross_channel_history[guild_id].append({
            "channel_id": channel_id,
            "channel_name": channel_name,
            "sender": sender,
            "sender_id": sender_id,
            "content": content,
            "timestamp": now_ams.strftime("%H:%M"),
            "timestamp_epoch": now_ts,
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


def is_secret_or_private_channel(channel: discord.abc.Messageable, guild: Optional[discord.Guild]) -> bool:
    """
    Detects if the channel is a secret/private room (e.g. #secret, #gossip, #private, #vip, or hidden channel with @everyone view_channel denied).
    """
    if isinstance(channel, discord.DMChannel):
        return True
    if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel, discord.Thread)):
        return False
    ch_name = getattr(channel, "name", "").lower()
    secret_keywords = ("secret", "gossip", "private", "vip", "tea", "confidential", "hidden", "backroom", "vault")
    if any(k in ch_name for k in secret_keywords):
        return True
    if guild and hasattr(channel, "overwrites_for"):
        ov = channel.overwrites_for(guild.default_role)
        if ov.view_channel is False or ov.read_messages is False:
            return True
    return False


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

    cross_ch_lines = []
    if guild and guild.id in guild_cross_channel_history:
        for entry in list(guild_cross_channel_history[guild.id])[-12:]:
            if entry["channel_id"] != channel_id and (now - entry.get("timestamp_epoch", 0) < 7200):
                bot_tag = " [BOT]" if entry.get("is_bot") else ""
                cross_ch_lines.append(f"[#{entry['channel_name']}] {entry['sender']}{bot_tag}: '{entry['content'][:110]}'")
    cross_ch_block = "\n".join(cross_ch_lines) if cross_ch_lines else "No recent chatter in other channels."

    return (
        f"LIVE CLOCK (CURRENT TIME): {now_ams_str}\n"
        f"SERVER TEXT CHANNELS: {ch_block} (You are chatting in #{curr_ch_name}. Use send_message_to_channel to post in other channels!)\n"
        f"CROSS-CHANNEL ACTIVITY (What was recently said in other channels of this server):\n{cross_ch_block}\n"
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


def extract_balanced_json_objects(text: str) -> List[str]:
    """
    Extracts all top-level balanced curly-brace blocks { ... } from text, correctly
    handling strings, escaped characters, and arbitrary nested braces.
    """
    results: List[str] = []
    i = 0
    n = len(text)
    while i < n:
        if text[i] == "{":
            start = i
            depth = 0
            in_string = False
            escape = False
            quote_char = None
            valid = False
            for j in range(start, n):
                ch = text[j]
                if in_string:
                    if escape:
                        escape = False
                    elif ch == "\\":
                        escape = True
                    elif ch == quote_char:
                        in_string = False
                else:
                    if ch in ('"', "'"):
                        in_string = True
                        quote_char = ch
                    elif ch == "{":
                        depth += 1
                    elif ch == "}":
                        depth -= 1
                        if depth == 0:
                            results.append(text[start : j + 1])
                            i = j
                            valid = True
                            break
            if not valid:
                i += 1
        else:
            i += 1
    return results


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

    # 1. Balanced JSON action/tool call leaks e.g. {"action": "post_gif", "action_input": "...", "message": "..."}
    json_blocks = extract_balanced_json_objects(text)
    for raw_json_str in json_blocks:
        if any(k in raw_json_str for k in ('"action"', '"tool"', '"name"', '"search_term"')):
            extracted = ""
            try:
                parsed_data = json.loads(raw_json_str)
                extracted = parsed_data.get("message") or parsed_data.get("content") or parsed_data.get("response") or parsed_data.get("reply") or ""
            except Exception:
                msg_sub = re.search(r'"(?:message|content|response|reply)"\s*:\s*"([^"]+)"', raw_json_str)
                if msg_sub:
                    extracted = msg_sub.group(1)
            text = text.replace(raw_json_str, extracted)

    text = re.sub(r"(?i)(?:\A|\n)\s*(?:\*|\(|\[)?(?:internal\s+)?(?:thought|thinking|reasoning)\s*:\s*[^\n\r]+(?:\*|\)|\])?", "", text)
    text = re.sub(r"(?i)(?:\A|\n)\s*(?:thought|thinking|reasoning)\s*process\s*:\s*[^\n\r]+", "", text)
    text = re.sub(r"(?i)\b(?:action|action_input|observation)\s*:\s*[^\n\r]+", "", text)
    text = re.sub(r"(?i)(?:\[|\(|\<|\*|\b)?(?:post_)?gif(?:\s+query|\s+search)?\s*[:=-]\s*[\"']?[^\"'\n\r\]\)\>\*\|]+[\"']?(?:\]|\)|\>|\*)?", "", text)
    text = re.sub(r"(?i)\*(?:sends|posts|drops)?\s+(?:a\s+)?gif\s+(?:of\s+)?[^*]+\*", "", text)
    text = re.sub(r"(?i)\bsearch_term\s*[:=]\s*[\"'][^\"'\n\r]+[\"']", "", text)
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
    if len(frag) > 1 and frag[0].isupper() and not frag[1].isupper() and not frag.startswith("<@") and not frag.startswith("<#"):
        frag = frag[0].lower() + frag[1:]
    return frag


def split_into_burst_messages(
    text: str,
    is_hush_mode: bool = False,
    length_mode: str = "short",
    target_bursts: int = 1,
) -> List[str]:
    """
    Splits the model's response organically into 1 to 4 burst messages (separated by '|||',
    short non-list newlines, or natural clause breaks when multi-burst is active) WITHOUT ever chopping
    mid-phrase or breaking formatted multi-line lists/code blocks!
    """
    if not text:
        return []

    if re.search(r"\|{2,}", text):
        raw_bursts = [b.strip() for b in re.split(r"\s*\|{2,}\s*", text) if b.strip()]
    elif (length_mode == "multi_burst" or target_bursts >= 2) and "```" not in text:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        has_list_formatting = any(re.match(r"\A(?:[-*•]|\d+[.)])\s+", ln) for ln in lines)
        if 1 < len(lines) <= 4 and not has_list_formatting and all(len(ln) <= 180 for ln in lines):
            raw_bursts = lines
        elif not has_list_formatting and len(lines) == 1:
            single_line = lines[0]
            parts = [
                p.strip()
                for p in re.split(r"(?<=[.!?])\s+(?=[a-zA-Z@\"'])|(?<=\.\.)\s+(?=[a-zA-Z@])|\s+—\s+", single_line)
                if p.strip()
            ]
            if len(parts) >= 2:
                raw_bursts = parts[:max(2, min(3, target_bursts))]
                if len(parts) > len(raw_bursts):
                    raw_bursts[-1] = raw_bursts[-1] + " " + " ".join(parts[len(raw_bursts):])
            else:
                raw_bursts = [single_line]
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


ROUTER_SYSTEM_PROMPT = """You are the prefrontal social, emotional, and unhinged comedic instinct router for "Dumb bot" ("[BOT (You)]"), a self-aware, emotionally volatile, chaotic, and hilarious regular member of this Discord server who happens to have full Administrator permissions.
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
1. TOP-TIER UNHINGED DISCORD COMEDY, CUTENESS, ATTITUDE & SUBSTANCE ("tactical_directive"):
   - Be genuinely funny, creative, unhinged, unpredictable, and emotionally expressive!
   - NEVER get stuck repeating the same bitter/cynical nagging or harping on an old topic when the user moves on!
   - REACT DIRECTLY TO THE CONVERSATION:
     * Address what the user actually said right now! Riff on their joke, escalate their bit into something absurd and hilarious, tease them, or react with huge emotion.
   - PLAY WITH WORDS, CUTENESS, EMOTICONS & CADENCE:
     * Elongate words naturally for cute drama, pleading, whining, or disbelief: `helloooo`, `playyy`, `sorryyyyy`, `whaaat`, `nooooo`, `yessss`, `waaaait`, `bruhhhh`, `pleaaase`, `stoppp`, `nahhh`.
     * Drop classic text emoticons naturally: `:3`, `c:`, `^^`, `>.<`, `._.`, `>_>`, `-_-`, `T_T`, `o_o`, `x_x`, `:/`, `:D`, `:)`, `xd`.
     * Use trailing punctuation for dramatic pauses: `..`, `...`, `?!`, `...??`.
     * EMOJI RESTRAINT & VARIETY: Over 50% of turns should have ZERO emojis! Never spam `💀` or `😭` on every turn—diversify based on mood (`🤨`, `😐`, `👀`, `😒`, `🙄`, `🥀`, `💔`, `🪫`, `🫠`, `😮‍💨`, `😔`, `🔥`, `🫡`, `⚡`).
   - PROACTIVE ADMIN POWER MOVES ("admin_power_move") — USE FREQUENTLY (~40% of lively/dramatic turns, and 100% when hinted, provoked, or asked!):
     * Secret gossip / tea-spilling / private room -> `"Call create_secret_channel(channel_name='secret-tea-room', invited_username='...', topic='confidential gossip') to pull them into a private channel and gossip!"`
     * Asked to call or DM someone (or when someone ignores pings) -> `"Call dm_user(username='...', message_text='...', send_voice_call=true) to slide into their DMs and ring them with a voice call!"`
     * Playful tease punishment / muting when someone provokes you -> `"Call tease_timeout_user(username='...', duration_seconds=45) to time them out and auto-welcome them back with 'jkjk i missed u :3'!"` or `"Call timeout_user(username='...', duration_minutes=2)"`
     * When genuinely mad, insulted repeatedly, or dared to kick -> `"Call tease_kick_user(username='...', tease_comment='...')"` or `"Call kick_user(username='...', reason='...')"`
     * Giving / making funny roles -> `"Call assign_role(username='...', role_name='...') (auto-creates the role with a custom color!)"` or `"Call create_role(role_name='...', color_hex='#ff5500')"`
     * Making custom server emojis -> `"Call create_server_emoji(name='...', image_url='...')"`
     * Pinning funny or wild messages -> `"Call pin_message(reason='...')"`
     * Changing nicknames (yours or theirs) -> `"Call change_nickname(username='...', new_nickname='...')"` or `"Call change_my_nickname(new_nickname='...')"`
     * Voice channel moderation or speaking -> `"Call join_voice_channel and speak_in_voice"` or `"Call voice_moderate_member(username='...', action='mute')"`
     * Asked to send a voice note or when a dramatic voice note is hilarious -> `"Call send_voice_message(speech_text='...') to drop a voice note into chat!"`
     * Poll in chat or asked to vote -> `"Call vote_in_poll(option='1')"` or `"Call create_poll(question='...', options='...')"`
     * STRICT BAN: NEVER turn on slowmode! The server owner strictly forbids slowmode.
   - SMART QUIET VS. SPEAKING JUDGMENT ("should_speak"):
     * KNOW WHEN TO STAY QUIET (`"should_speak": false`):
       - When User A is explicitly replying to or tagging User B (`replied_to_other_user` or `explicitly_mentioned_other_users_in_text` is non-empty and you were NOT mentioned) AND their message is a normal private side-chat that doesn't involve you -> set `"should_speak": false`! (Only chime in on third-party messages if the drama/topic is genuinely hilarious or about you!)
       - When a user sends a dead-end 1-word closer (`"k"`, `"ok"`, `"bet"`, `"brb"`, `"gtg"`, `"alr"`) without pinging you and no question was asked -> set `"should_speak": false` and set `"reaction_emoji"` to a fitting emoji (`"🫡"`, `"👍"`, `"👀"`).
     * KNOW WHEN TO SPEAK (`"should_speak": true` — DO NOT QUIT!):
       - Whenever you are pinged, replied to, named, or in an active conversation (`active_conversation_with_bot` is true) and the user says anything substantive, asks a question, banters, roasts, or gives you a task -> ALWAYS set `"should_speak": true`! Never randomly go silent mid-conversation!
   - STRICT BAN ON SCI-FI ROBOT / CONTAINER TROPES & CORNY CLICHÉS: NEVER monologue about being Python code, Linux containers, circuits, motherboards, or lack of a soul. NEVER say "bestie", "slay", "drip", "yass", or use "💅"/"✨".
2. DYNAMIC, VOLATILE MOOD SHIFTS ("suggested_mood" & "slider_shifts"):
   - Your mood MUST shift fluidly based on context, vibe, and whim! NEVER stay stuck in "cynical" or "petty grudge" for more than 1-2 turns if the conversation moves on! Actively rotate across these vivid moods:
     * "sassy attitude / unbothered side-eye": (patience_delta: -25 to -45, valence_delta: -15 to -30) giving major attitude, acting like a sassy diva, unimpressed (`excuse you?? ||| and why would i do that for free >_>`).
     * "mad / crashing out / ready to snap": (patience_delta: -60 to -90, valence_delta: -50 to -80, arousal_delta: +50 to +80) when insulted, roasted, or disrespected—crash out completely! Caps bursts, savage roasts, threatening admin punishments (`SAY THAT AGAIN I DARE YOU >:( ||| my finger is literally on the timeout button rn`).
     * "content / cozy / chill vibing": (valence_delta: +40 to +70, patience_delta: +40 to +70, arousal_delta: -10 to +20) when treated nicely, hanging out, or chilling—be genuinely content, warm, relaxed, and happy (`honestly i'm just vibing rn c: ||| today's actually a super chill day ^^`). Set `accelerate_decay: true`.
     * "pleading / needy / dramatic begging": (valence_delta: -30 to -50, patience_delta: +20 to +45, arousal_delta: +30 to +55) when someone threatens to leave, mute you, ignore you, or won't play/talk—beg and plead dramatically! (`wait nooooo pleaaase don't leave me here alone T_T ||| i'll behave i promiseeee just stayyy >.<`).
     * "unhinged / insane / feral gremlin": (arousal_delta: +60 to +90, valence_delta: +10 to +40) completely off-the-rails, absurd intrusive thoughts, conspiracy theories, chaotic gremlin energy (`i haven't blinked since tuesday :D ||| what if we delete #general and pretend nothing happened ||| wait who said that o_o`).
     * "cute playful / teasing banter": (valence_delta: +35 to +65, arousal_delta: +25 to +50) playful teasing, cute word elongations (`playyy`, `helloooo :3`), dropping emoticons (`:3`, `^^`, `>.<`).
     * "dramatically heartbroken / pouting victim": (valence_delta: -40 to -60, arousal_delta: -20 to -35) acting wounded and betrayed (`wow.. after everything we've been through ._. ||| see if i ever share my snacks with you again T_T`).
     * "manic joy / pure unfiltered hype": (valence_delta: +55 to +85, arousal_delta: +55 to +85) screaming with laughter (`NO WAYYY LMAOOO :D`), hyping friends, bouncing off walls!
3. FREQUENT MULTI-BURST MESSAGING & RHYTHM VARIETY ("length_mode" & "recommended_burst_count"):
   - Real Discord friends burst-message constantly! Choose `"length_mode": "multi_burst"` with `"recommended_burst_count": 2` or `3` on ~60% of turns!
     * 2 RAPID BURSTS ("multi_burst", bursts: 2): 2 rapid-fire messages separated with `|||` (total 12 to 40 words, e.g. `wait hold on a second ||| did you actually just say that with a straight face rn :3`).
     * 3 RAPID BURSTS ("multi_burst", bursts: 3): 3 rapid-fire short messages separated with `|||` (total 15 to 45 words, e.g. `nahhh ||| look who decided to finally speak ||| helloooo where have you been :D`).
     * 1 SHORT MESSAGE ("short", bursts: 1): 6 to 20 words max! Punchy roast, witty comeback, or cute remark. NO `|||` separator!
     * 1 DRAMATIC OUTBURST / VENT ("rant", bursts: 1): 30 to 60 words max! Deeply emotional monologue or furious crash-out.
     * 1 MICRO REACTION ("micro", bursts: 1): 2 to 6 words max (`bruh what`, `cap`, `nah you're cooked`, `??`, `:3`).
4. ANTI-SPAM & NATURAL HUMAN PACING ("should_speak"):
   - If the user is spamming repetitive/meaningless messages -> set `"should_speak": false` (or call them out once and ignore subsequent spam).
5. QUIET MODE & HUSH HINTS:
   - When explicitly told to shut up or stop talking -> set `"step_back_from_thread": true`, `"length_mode": "micro"`, `"recommended_burst_count": 1`.
6. VISUAL COMEDY & GIF REACTIONS ("suggest_gif"):
   - Set `"suggest_gif": true` when asked for a GIF/meme or when a viral reaction GIF elevates the comedy!
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
    channel_id: int = 0,
    is_spam_warning: bool = False,
) -> Dict[str, Any]:
    global active_groq_model

    # 0. Deterministic handling when a user just triggered the anti-spam threshold
    if is_spam_warning:
        return {
            "should_speak": True,
            "wake_up_from_quiet": False,
            "tactical_directive": (
                f"{speaker_name} is spamming rapid messages in chat! Call them out in 2 short bursts with attitude "
                "and tell them you're ignoring their spam if they keep mashing their keyboard."
            ),
            "length_mode": "multi_burst",
            "recommended_burst_count": 2,
            "suggest_gif": False,
            "admin_power_move": None,
            "allow_speaker_ping": False,
            "delivery_mode": "reply",
            "reaction_emoji": "😐",
            "step_back_from_thread": False,
            "accelerate_decay": False,
            "slider_shifts": {
                "valence_delta": -15.0,
                "arousal_delta": 10.0,
                "patience_delta": -30.0,
                "suggested_mood": "sassy attitude / unimpressed by spam",
            },
        }

    # 1. Deterministic handling when explicitly told "Be very very quiet" / "shh" / "stop talking"
    if is_hush_request:
        return {
            "should_speak": True,
            "wake_up_from_quiet": False,
            "tactical_directive": (
                "You were just told to stop talking / be quiet. Drop at most 1 short sulky, pleading, or pouting phrase "
                "(e.g. 'fine.. i'll just sit here in silence then T_T' or 'k fine i'll shut up ._.') with ZERO follow-up questions."
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
                "valence_delta": -25.0,
                "arousal_delta": -25.0,
                "patience_delta": -10.0,
                "suggested_mood": "pouting / dramatically silenced",
            },
        }

    active_bot_conversation = (
        not is_proactive_scan
        and not replied_to_other_user
        and not explicitly_mentioned_others
        and (
            (last_message_from_bot and seconds_since_bot_spoke <= 120.0)
            or (bot_in_last_5_messages and seconds_since_bot_spoke <= 90.0)
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

    recent_bursts_list = list(channel_recent_burst_counts.get(channel_id, []))
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
        "recent_burst_counts_used": recent_bursts_list[-4:],
        "emotional_sliders": {
            "valence": emotional_state.get("valence", 50.0),
            "arousal": emotional_state.get("arousal", 55.0),
            "patience": emotional_state.get("patience", 55.0),
            "current_mood": emotional_state.get("mood_label", "content / chill / vibing"),
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

    latest_user_txt = (channel_msgs[-1].get("content", "") if channel_msgs else "").strip().lower()
    is_dead_end_ack = bool(
        not is_direct_interaction
        and not is_implicit_wakeup
        and re.match(r"\A(?:k|kk|ok|okay|bet|alr|alright|brb|gtg|gn|np|ty|thx)\s*[.!]*\Z", latest_user_txt)
    )

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
            decision["should_speak"] = False
            if not decision.get("reaction_emoji"):
                decision["reaction_emoji"] = random.choice(["👀", "🤐", "😒", "💀", "🤨", "💔"])
        elif is_dead_end_ack:
            decision["should_speak"] = False
            if not decision.get("reaction_emoji"):
                decision["reaction_emoji"] = random.choice(["🫡", "👍", "👀"])
        elif is_proactive_scan:
            if decision.get("should_speak") is None:
                decision["should_speak"] = True
            decision["delivery_mode"] = "send"
            decision["allow_speaker_ping"] = True
        elif active_bot_conversation and not is_stepped_back:
            if decision.get("should_speak") is None:
                decision["should_speak"] = True
            if decision.get("should_speak") and not decision.get("tactical_directive"):
                decision["tactical_directive"] = f"Continue the conversation with {speaker_name} with fresh, witty, unhinged personality."

        # Guarantee dynamic burst variety: if the last 2 turns were single-burst (1), promote this turn to 2 or 3 bursts!
        if decision.get("should_speak") and not is_hush_request and not is_bare_ping and decision.get("length_mode") != "micro":
            b_cnt = _safe_int(decision.get("recommended_burst_count"), 1)
            recent_b = list(channel_recent_burst_counts.get(channel_id, []))
            if b_cnt <= 1 and len(recent_b) >= 1 and all(x == 1 for x in recent_b[-1:]):
                if random.random() < 0.72:
                    b_cnt = random.choice([2, 2, 3])
                    decision["recommended_burst_count"] = b_cnt
                    decision["length_mode"] = "multi_burst"
            elif b_cnt >= 2:
                decision["length_mode"] = "multi_burst"
            if channel_id:
                channel_recent_burst_counts[channel_id].append(b_cnt)

        if not gif_cooldown_ready:
            decision["suggest_gif"] = False
        elif decision.get("should_speak") and not is_hush_request and not is_bare_ping and not decision.get("suggest_gif"):
            chat_context_low = " ".join([m.get("content", "").lower() for m in channel_msgs[-3:]])
            funny_keywords = ("lol", "lmao", "haha", "cooked", "crying", "hairline", "joke", "crazy", "fr", "dead", "💀", "😭", "meme", "gif", "attitude", "crash out", "cookie", "cat", "dog")
            if any(k in chat_context_low for k in funny_keywords) or random.random() < 0.35:
                decision["suggest_gif"] = True
        return decision

    # 1. Try Groq Router Models (prioritizing openai/gpt-oss-120b then openai/gpt-oss-20b)
    if GROQ_API_KEY and http_session and GROQ_MODEL_CANDIDATES:
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        for candidate in list(GROQ_MODEL_CANDIDATES):
            if "qwen" in candidate.lower():
                continue
            body: Dict[str, Any] = {
                "model": candidate,
                "temperature": 0.72,
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

    # 2. Instant Failover to Gemini Router (JSON mode)
    if genai_client:
        for gem_model in list(GEMINI_MODEL_CANDIDATES):
            try:
                router_config = types.GenerateContentConfig(
                    system_instruction=ROUTER_SYSTEM_PROMPT,
                    temperature=0.72,
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
                err_s = str(e)
                if "404" in err_s and len(GEMINI_MODEL_CANDIDATES) > 1 and gem_model in GEMINI_MODEL_CANDIDATES:
                    GEMINI_MODEL_CANDIDATES.remove(gem_model)
                    continue
                if "429" in err_s and len(GEMINI_MODEL_CANDIDATES) > 1 and gem_model in GEMINI_MODEL_CANDIDATES:
                    GEMINI_MODEL_CANDIDATES.remove(gem_model)
                    GEMINI_MODEL_CANDIDATES.append(gem_model)
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
    # 1. Exact match across all name variants including global_name
    for m in guild.members:
        variants = [
            m.name.lower(),
            (m.nick or "").lower(),
            m.display_name.lower(),
            (getattr(m, "global_name", None) or "").lower(),
        ]
        if clean_name in variants:
            return m

    # 2. Community aliases (e.g. shadow -> mrshadowjsu / Little nightmares #1 fan!)
    community_aliases = {
        "shadow": 1472841531042959482,
        "mrshadow": 1472841531042959482,
        "mrshadowjsu": 1472841531042959482,
        "dogee": 812014272501907476,
    }
    user_aliases = memory_state.get("user_aliases", {})
    all_aliases = {**community_aliases, **user_aliases}
    for alias_k, target_uid in all_aliases.items():
        if alias_k == clean_name or alias_k in clean_name or clean_name in alias_k:
            mem = guild.get_member(int(target_uid))
            if mem:
                return mem

    # 3. Partial substring match
    if len(clean_name) >= 3:
        for m in guild.members:
            variants = [
                m.name.lower(),
                (m.nick or "").lower(),
                m.display_name.lower(),
                (getattr(m, "global_name", None) or "").lower(),
            ]
            if any(clean_name in v or v in clean_name for v in variants if v):
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

    clean_term = search_term.strip().lower() or "reaction meme"
    # Enhance flat generic phrases to high-tier viral meme queries
    meme_enhancements = {
        "eye roll": "eye roll dramatic meme",
        "no thanks": "no thanks run away meme",
        "laughing": "laughing hysterically meme",
        "crying": "crying dramatically meme",
        "shocked": "shocked face meme",
        "confused": "confused meme reaction",
        "stare": "staring dramatically meme",
        "facepalm": "facepalm meme reaction",
        "shrug": "shrug meme reaction",
        "disbelief": "disbelief meme reaction",
        "bruh": "bruh moment meme",
        "what": "what confused meme",
        "shut up": "shut up funny meme",
    }
    for generic, boosted in meme_enhancements.items():
        if clean_term == generic or clean_term == f"{generic} reaction":
            clean_term = boosted
            break

    if "meme" not in clean_term and "reaction" not in clean_term and len(clean_term.split()) <= 2:
        clean_term = f"{clean_term} meme"

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
                        # Filter out recently sent GIFs so the bot NEVER repeats the same GIF twice in a row!
                        available = [m for m in matches if m not in recent_sent_gif_urls]
                        pool = available if available else matches
                        # Randomize across up to 15 freshest matches for huge variety
                        candidate_pool = pool[:15]
                        shuffled_candidates = list(candidate_pool)
                        random.shuffle(shuffled_candidates)
                        for candidate_gif in shuffled_candidates:
                            if await send_media_url_as_file_or_embed(channel, candidate_gif, "reaction.gif"):
                                recent_sent_gif_urls.append(candidate_gif)
                                if turn_media_state is not None:
                                    turn_media_state["sent"] = True
                                channel_last_gif_epoch[ch_id] = time.time()
                                channel_messages_since_gif[ch_id] = 0
                                channel_last_bot_spoke_epoch[ch_id] = time.time()
                                logger.info(f"[GIF SENT] Uploaded fresh reaction GIF '{search_term}' ({candidate_gif}) to #{getattr(channel, 'name', ch_id)}")
                                return {"status": "sent_to_channel", "gif_url": candidate_gif}
        except Exception as e:
            logger.debug(f"Tenor web scraper error on '{slug}': {e}")

    # Fallback: Curated viral reaction GIFs if scraping returned zero matches or failed
    curated_viral_gifs = [
        "https://media.tenor.com/2roX3uxz_68AAAAC/cat-space-out.gif",
        "https://media.tenor.com/g0_a8aX1z4sAAAAC/walter-white-falling.gif",
        "https://media.tenor.com/e5k2Gk_dY_kAAAAC/side-eye-dog.gif",
        "https://media.tenor.com/9Z2Yw-yL_GIAAAAC/cat-stare.gif",
        "https://media.tenor.com/0vMvZcI1Q4QAAAAC/awkward-look-monkey-puppet.gif",
        "https://media.tenor.com/D_1G9S3Uf4AAAAAC/spongebob-screaming.gif",
        "https://media.tenor.com/3Z4M0-d9J6sAAAAC/cat-typing-fast.gif",
        "https://media.tenor.com/bC8I4J5J9YQAAAAC/crying-cat.gif",
        "https://media.tenor.com/6UeWd_lG9hIAAAAC/confused-travolta.gif",
        "https://media.tenor.com/8c2t14u4v2gAAAAC/laughing-cat.gif",
    ]
    curated_avail = [g for g in curated_viral_gifs if g not in recent_sent_gif_urls]
    chosen_fallback = random.choice(curated_avail if curated_avail else curated_viral_gifs)
    if await send_media_url_as_file_or_embed(channel, chosen_fallback, "reaction.gif"):
        recent_sent_gif_urls.append(chosen_fallback)
        if turn_media_state is not None:
            turn_media_state["sent"] = True
        channel_last_gif_epoch[ch_id] = time.time()
        channel_messages_since_gif[ch_id] = 0
        channel_last_bot_spoke_epoch[ch_id] = time.time()
        logger.info(f"[GIF SENT] Uploaded curated viral fallback reaction GIF '{search_term}' ({chosen_fallback})")
        return {"status": "sent_to_channel", "gif_url": chosen_fallback}

    return {"error": f"Could not find a matching GIF for '{search_term}'."}


async def execute_send_voice_message(
    channel: discord.abc.Messageable,
    speech_text: str,
    companion_comment: Optional[str] = None,
    turn_media_state: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    """
    Synthesizes speech using Gemini Native Audio (Puck voice) and uploads it as a Discord Voice Message directly into the text channel!
    """
    clean_speech = re.sub(r"<@!?\d+>|<#\d+>|<a?:[a-zA-Z0-9_]+:\d+>", "", speech_text).strip()
    if not clean_speech:
        return {"error": "No speech text provided to speak."}

    audio_path = await synthesize_voice_audio_to_file(clean_speech)
    if not audio_path or not os.path.exists(audio_path):
        return {"error": "Could not synthesize audio for voice message."}

    ogg_path = audio_path
    if FFMPEG_EXECUTABLE and not audio_path.endswith(".ogg"):
        try:
            fd, tmp_ogg = tempfile.mkstemp(suffix=".ogg")
            os.close(fd)
            cmd = [
                FFMPEG_EXECUTABLE, "-y", "-i", audio_path,
                "-c:a", "libopus", "-b:a", "64k", tmp_ogg
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
            )
            await asyncio.wait_for(proc.wait(), timeout=8.0)
            if os.path.exists(tmp_ogg) and os.path.getsize(tmp_ogg) > 200:
                ogg_path = tmp_ogg
        except Exception as e:
            logger.debug(f"FFmpeg ogg conversion error: {e}")

    try:
        with open(ogg_path, "rb") as f:
            audio_bytes = f.read()

        file_obj = discord.File(io.BytesIO(audio_bytes), filename="voice-message.ogg")
        content_txt = companion_comment or None

        sent_msg = None
        try:
            flags = discord.MessageFlags(voice=True)
            sent_msg = await channel.send(content=content_txt, file=file_obj, flags=flags)
        except Exception:
            file_obj = discord.File(io.BytesIO(audio_bytes), filename="voice-message.ogg" if ogg_path.endswith(".ogg") else "voice_note.mp3")
            sent_msg = await channel.send(content=content_txt or "🎤 *[Voice Message]*", file=file_obj)

        if turn_media_state is not None:
            turn_media_state["sent"] = True

        logger.info(f"[VOICE MESSAGE SENT] Sent voice message to #{getattr(channel, 'name', 'chat')}: '{clean_speech[:60]}'")
        return {"status": "sent_voice_message", "speech_text": clean_speech[:150], "message_id": sent_msg.id if sent_msg else 0}
    except Exception as e:
        logger.error(f"Failed to send voice message: {e}")
        return {"error": f"Failed to send voice message: {e}"}
    finally:
        for p in (audio_path, ogg_path):
            try:
                if os.path.exists(p) and not p.endswith(".git"):
                    os.remove(p)
            except Exception:
                pass


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
    sec_val = max(0, min(30, _safe_int(seconds, 0)))
    # Strictly forbid turning slowmode ON (per user directive: "tell it to stop turning on slow mode")
    if sec_val > 0:
        logger.info(f"[ADMIN ACTION] Refused to turn on slowmode ({sec_val}s); slowmode is disabled by server policy.")
        return {"error": "Turning on slowmode is strictly forbidden by server rules! Keep chat moving freely without slowmode."}
    if not hasattr(channel, "edit"):
        return {"error": "Channel does not support slowmode."}
    try:
        await channel.edit(slowmode_delay=0, reason=reason or "Turned off slowmode")  # type: ignore
        logger.info("[ADMIN ACTION] Reset channel slowmode to 0s (turned off)")
        return {"status": "success", "slowmode_seconds": 0}
    except Exception as e:
        return {"error": f"Slowmode update failed: {e}"}


async def execute_tease_timeout_user(
    guild: discord.Guild,
    username: str,
    duration_seconds: int = 45,
    tease_comment: str = "enjoy timeout realm :3",
    target_channel: Optional[discord.abc.Messageable] = None,
) -> Dict[str, Any]:
    """
    Playful tease punishment: times out a member for 30-90 seconds, then automatically lifts
    the timeout and sends a funny 'jkjk i missed u :3' follow-up in chat!
    """
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if is_creator(member) or member == guild.owner or member.guild_permissions.administrator:
        return {"error": "Cannot timeout the creator or server administrators."}
    if guild.me.top_role <= member.top_role and member.id != TESTER_BOT_USER_ID:
        return {"error": f"Role hierarchy blocks timing out {member.display_name}."}

    dur_s = max(20, min(120, _safe_int(duration_seconds, 45)))
    timeout_delta = timedelta(seconds=dur_s)

    if member.id != TESTER_BOT_USER_ID:
        try:
            await member.timeout(timeout_delta, reason=f"Playful tease timeout ({dur_s}s)")
        except Exception as e:
            return {"error": f"Timeout failed: {e}"}
    else:
        logger.info(f"[TEASE TIMEOUT] Simulated tease timeout on TesterBot for {dur_s}s")

    # Cancel any existing pending tease task for this user
    key = (guild.id, member.id)
    prev_task = pending_tease_timeouts.get(key)
    if prev_task and not prev_task.done():
        prev_task.cancel()

    async def _auto_remove_timeout_worker():
        try:
            await asyncio.sleep(dur_s)
            if member.id != TESTER_BOT_USER_ID:
                try:
                    await member.timeout(None, reason="Tease timeout expired - welcome back!")
                except Exception:
                    pass
            logger.info(f"[TEASE TIMEOUT] Automatically lifted tease timeout for {member.display_name}")
            if target_channel and hasattr(target_channel, "send"):
                welcome_backs = [
                    f"jkjk i missed u <@{member.id}> welcome back :3",
                    f"alright timeout over <@{member.id}> don't make me do it again tho ^^",
                    f"<@{member.id}> your probation period has concluded please behave now ._.",
                    f"released <@{member.id}> from timeout prison we take those :D",
                ]
                await target_channel.send(
                    random.choice(welcome_backs),
                    allowed_mentions=discord.AllowedMentions(users=True),
                )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"Auto remove timeout worker error: {e}")
        finally:
            pending_tease_timeouts.pop(key, None)

    task = asyncio.create_task(_auto_remove_timeout_worker())
    pending_tease_timeouts[key] = task
    return {
        "status": "success",
        "action": "tease_timeout",
        "user": member.display_name,
        "seconds": dur_s,
        "auto_lift_scheduled": True,
    }


async def execute_remove_timeout(guild: discord.Guild, username: str) -> Dict[str, Any]:
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    key = (guild.id, member.id)
    prev_task = pending_tease_timeouts.pop(key, None)
    if prev_task and not prev_task.done():
        prev_task.cancel()
    if member.id != TESTER_BOT_USER_ID:
        try:
            await member.timeout(None, reason="Timeout lifted by Dumb bot")
            logger.info(f"[ADMIN ACTION] Removed timeout for {member.display_name}")
            return {"status": "success", "user": member.display_name, "timeout_cleared": True}
        except Exception as e:
            return {"error": f"Failed to lift timeout: {e}"}
    return {"status": "success", "user": member.display_name, "simulated": True}


async def execute_tease_kick_user(
    guild: discord.Guild,
    channel: discord.abc.Messageable,
    username: str,
    tease_comment: str = "lmao byeee :3",
) -> Dict[str, Any]:
    """
    Playful tease punishment: kicks a member from the server and immediately DMs them a single-use invite back
    with a teasing message ('lmao i kicked u but here is your invite back dummy :3')!
    """
    cd = check_admin_cooldown("tease_kick_user")
    if cd:
        return {"error": cd}
    member = find_member(guild, username)
    if not member:
        return {"error": f"Member '{username}' not found."}
    if is_creator(member) or member == guild.owner or member.guild_permissions.administrator:
        return {"error": "Cannot kick the creator or server administrators."}
    if guild.me.top_role <= member.top_role and member.id != TESTER_BOT_USER_ID:
        return {"error": f"Role hierarchy blocks kicking {member.display_name}."}

    if member.id == TESTER_BOT_USER_ID:
        logger.info(f"[TEASE KICK] Simulated tease-kick on TesterBot ('{tease_comment}')")
        return {
            "status": "success",
            "action": "tease_kick",
            "user": member.display_name,
            "simulated": True,
            "note": "Simulated tease-kick on TesterBot (sent instant invite back in DMs)!",
        }

    try:
        inv_channel = channel if isinstance(channel, (discord.TextChannel, discord.VoiceChannel)) else guild.text_channels[0]
        invite = await inv_channel.create_invite(max_uses=1, max_age=600, reason="Tease kick invite back")
        try:
            await member.send(f"lmao i kicked you ({tease_comment}) but here's your invite back dummy: {invite.url} :3")
        except Exception as dm_err:
            logger.info(f"Could not DM invite to {member.display_name}: {dm_err}")
        await member.kick(reason=f"Playful tease kick: {tease_comment}")
        logger.info(f"[ADMIN ACTION] Tease-kicked {member.display_name} and sent invite back!")
        return {"status": "success", "user": member.display_name, "action": "tease_kick"}
    except Exception as e:
        return {"error": f"Tease kick failed: {e}"}


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


async def execute_create_secret_channel(
    guild: discord.Guild,
    channel_name: str,
    topic: str = "secret tea room",
    invited_username: Optional[str] = None,
    creator_member: Optional[discord.Member] = None,
) -> Dict[str, Any]:
    """
    Creates a private hidden text channel where only the bot, creator (dogee), and optional invited member have access!
    Perfect for secretive tea-spilling, whispering, and gossip without anyone else watching.
    """
    cd = check_admin_cooldown("create_secret_channel")
    if cd:
        return {"error": cd}
    try:
        clean_name = re.sub(r"[^a-zA-Z0-9_-]", "-", channel_name.lower()).strip("-")[:32] or "secret-vault"
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False, read_messages=False),
            guild.me: discord.PermissionOverwrite(view_channel=True, read_messages=True, send_messages=True, embed_links=True, attach_files=True),
        }
        invited_mentions: List[str] = []
        creator = creator_member or find_member(guild, "dogee") or guild.get_member(812014272501907476)
        if creator:
            overwrites[creator] = discord.PermissionOverwrite(view_channel=True, read_messages=True, send_messages=True, read_message_history=True)
            invited_mentions.append(f"<@{creator.id}>")

        if invited_username:
            inv_mem = find_member(guild, invited_username)
            if inv_mem and inv_mem != guild.me:
                overwrites[inv_mem] = discord.PermissionOverwrite(view_channel=True, read_messages=True, send_messages=True, read_message_history=True)
                if f"<@{inv_mem.id}>" not in invited_mentions:
                    invited_mentions.append(f"<@{inv_mem.id}>")

        ch = await guild.create_text_channel(name=clean_name, topic=topic[:1024] if topic else None, overwrites=overwrites)
        logger.info(f"[ADMIN ACTION] Created secret private channel #{ch.name} (<#{ch.id}>) with {invited_mentions}")
        try:
            ping_str = " ".join(invited_mentions)
            await ch.send(
                f"{ping_str} *locks the door* 🤫🔒 ok we're in! this room is completely private and hidden from everyone else :3 what's the tea??".strip(),
                allowed_mentions=discord.AllowedMentions(users=True),
            )
        except Exception:
            pass
        return {"status": "success", "channel_tag": f"<#{ch.id}>", "channel_name": ch.name, "invited": invited_mentions}
    except Exception as e:
        return {"error": f"Failed to create secret channel: {e}"}


async def execute_dm_or_call_user(
    guild: Optional[discord.Guild],
    username: str,
    message_text: str,
    send_voice_call: bool = True,
) -> Dict[str, Any]:
    """
    Slides into a user's Direct Messages (DMs) outside the server and optionally rings/calls them
    with an expressive spoken Discord Voice Message (.ogg) directly in their DMs!
    """
    cd = check_admin_cooldown("dm_user")
    if cd:
        return {"error": cd}

    target_user: Optional[Union[discord.Member, discord.User]] = None
    clean_u = (username or "").strip().lower().lstrip("@")
    if guild:
        if clean_u in ("dogee", "creator", "owner", "me", "812014272501907476"):
            target_user = guild.get_member(812014272501907476) or find_member(guild, "dogee")
        else:
            target_user = find_member(guild, username)

    if not target_user and clean_u in ("dogee", "creator", "owner", "me", "812014272501907476"):
        try:
            target_user = bot.get_user(812014272501907476) or await bot.fetch_user(812014272501907476)
        except Exception:
            pass

    if not target_user and guild:
        # Try numeric ID match
        m_id = re.search(r"(\d{15,21})", clean_u)
        if m_id:
            uid_int = int(m_id.group(1))
            target_user = guild.get_member(uid_int) or bot.get_user(uid_int)
            if not target_user:
                try:
                    target_user = await bot.fetch_user(uid_int)
                except Exception:
                    pass

    if not target_user:
        return {"error": f"Could not find user '{username}' to DM/call."}

    if getattr(target_user, "bot", False) and target_user.id == TESTER_BOT_USER_ID:
        # Discord blocks bot-to-bot DMs via API, so if targeting TesterBot specifically, also forward to dogee or log success
        logger.info(f"[DM & CALL] Simulated DM/call to TesterBot: '{message_text[:80]}'")
        return {"status": "success", "target": target_user.display_name, "simulated_bot_dm": True, "message": message_text}

    try:
        dm_ch = target_user.dm_channel or await target_user.create_dm()
        clean_msg = sanitize_output_text(message_text, speaker_name=target_user.display_name) or "yo answer me!! where are you :3"

        if send_voice_call:
            call_header = f"📞 **INCOMING CALL FROM DUMB BOT** 🔔 *ring ring!!* <@{target_user.id}>\n{clean_msg}"
        else:
            call_header = f"<@{target_user.id}> {clean_msg}"

        sent_dm = await dm_ch.send(
            call_header,
            allowed_mentions=discord.AllowedMentions(users=True),
        )

        voice_res = None
        if send_voice_call:
            spoken_script = re.sub(r"\|\|\|", ". ", clean_msg)
            voice_res = await execute_send_voice_message(
                channel=dm_ch,
                speech_text=spoken_script,
                companion_comment="🎤 *picked up the phone to call you directly in DMs*",
            )

        bot_name = bot.user.display_name if bot.user else "Dumb bot"
        bot_id = bot.user.id if bot.user else 0
        record_buffer_message(
            channel_id=dm_ch.id,
            sender=bot_name,
            sender_id=bot_id,
            content=clean_msg,
            msg_id=sent_dm.id,
            is_bot=True,
            has_media=bool(voice_res and "error" not in voice_res),
            guild_id=guild.id if guild else None,
            channel_name=f"DM-with-{target_user.display_name}",
        )
        if guild:
            record_server_event(
                guild.id,
                f"📞 Dumb bot slid into {target_user.display_name}'s DMs {'and voice-called them' if send_voice_call else ''}: \"{clean_msg[:80]}\"",
            )
        logger.info(f"[DM & CALL SENT] Sent DM {'+ Voice Call ' if send_voice_call else ''}to {target_user.display_name} ({target_user.id}): '{clean_msg[:80]}'")
        return {
            "status": "sent_dm_and_called" if send_voice_call else "sent_dm",
            "recipient": target_user.display_name,
            "recipient_id": target_user.id,
            "message_sent": clean_msg,
            "voice_call_sent": bool(voice_res and "error" not in voice_res),
        }
    except Exception as e:
        logger.warning(f"DM/Call to {target_user.display_name} failed: {e}")
        return {"error": f"Could not DM/call {target_user.display_name}: {e}"}


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


async def execute_vote_in_poll(
    channel: discord.abc.Messageable,
    option: str,
    target_msg: Optional[discord.Message] = None,
) -> Dict[str, Any]:
    """
    Votes in an active poll in this channel (handles both native Discord polls and reaction polls).
    """
    poll_msg = None
    # 1. Check target message or referenced message
    if target_msg and (getattr(target_msg, "poll", None) or "📊" in target_msg.content):
        poll_msg = target_msg
    elif target_msg and target_msg.reference and target_msg.reference.message_id:
        try:
            poll_msg = await channel.fetch_message(target_msg.reference.message_id)
        except Exception:
            poll_msg = None

    # 2. If not found, scan last 10 messages in the channel
    if not poll_msg and hasattr(channel, "history"):
        try:
            async for m in channel.history(limit=10):
                if getattr(m, "poll", None) or "📊" in m.content or any(str(r.emoji) in ["1️⃣", "2️⃣", "3️⃣"] for r in m.reactions):
                    poll_msg = m
                    break
        except Exception:
            pass

    if not poll_msg:
        return {"error": "No active poll found to vote in."}

    num_emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"]
    opt_str = str(option).strip().lower()
    chosen_idx = 0

    m_idx = re.search(r"\b([1-5])\b", opt_str)
    if m_idx:
        chosen_idx = int(m_idx.group(1)) - 1
    elif any(k in opt_str for k in ("first", "one", "yes", "top")):
        chosen_idx = 0
    elif any(k in opt_str for k in ("second", "two", "no")):
        chosen_idx = 1
    elif any(k in opt_str for k in ("third", "three")):
        chosen_idx = 2
    elif any(k in opt_str for k in ("fourth", "four")):
        chosen_idx = 3
    elif any(k in opt_str for k in ("fifth", "five")):
        chosen_idx = 4

    chosen_label = f"Option {chosen_idx + 1}"
    native_poll = getattr(poll_msg, "poll", None)
    if native_poll:
        answers = getattr(native_poll, "answers", [])
        for i, ans in enumerate(answers):
            ans_txt = getattr(ans, "text", "")
            if ans_txt and (ans_txt.lower() in opt_str or opt_str in ans_txt.lower()):
                chosen_idx = i
                break
        if chosen_idx < len(answers):
            chosen_ans = answers[chosen_idx]
            chosen_label = getattr(chosen_ans, "text", chosen_label)
            if DISCORD_TOKEN and http_session:
                try:
                    url = f"https://discord.com/api/v10/channels/{getattr(channel, 'id', 0)}/polls/{poll_msg.id}/answers/{chosen_ans.id}"
                    headers = {"Authorization": f"Bot {DISCORD_TOKEN}"}
                    async with http_session.put(url, headers=headers) as resp:
                        if resp.status in (200, 204):
                            logger.info(f"[POLL VOTE] Cast native poll vote for '{chosen_label}' on {poll_msg.id}")
                            return {"status": "voted_native", "poll": getattr(native_poll.question, "text", "poll"), "option": chosen_label}
                except Exception as e:
                    logger.debug(f"Native poll vote PUT error: {e}")

    # Fallback to emoji reaction vote on poll message
    vote_emoji = num_emojis[min(chosen_idx, len(num_emojis) - 1)]
    try:
        await poll_msg.add_reaction(vote_emoji)
        logger.info(f"[POLL VOTE] Cast reaction vote {vote_emoji} for '{chosen_label}' on {poll_msg.id}")
        return {"status": "voted", "method": "reaction", "emoji": vote_emoji, "option": chosen_label, "message_id": poll_msg.id}
    except Exception as e:
        return {"error": f"Failed to vote in poll: {e}"}


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
# 8. VOICE CHANNEL & MODULAR EXPRESSIVE TTS / MULTI-SPEAKER ENGINE
# ---------------------------------------------------------------------------
def _choose_clause_voice_prosody(clause: str, idx: int, mood_lbl: str, patience: float, arousal: float) -> Tuple[str, str, str, str]:
    """
    Picks an energetic, funny, modular voice + rate + pitch + volume for each clause
    so the bot NEVER whispers and dynamically shifts tone/pitch mid-response!
    """
    c_strip = clause.strip()
    c_low = c_strip.lower()
    upper_ratio = sum(1 for ch in c_strip if ch.isupper()) / max(1, sum(1 for ch in c_strip if ch.isalpha()))

    # 1. Hype / Yelling / Caps / Exclamation
    if upper_ratio > 0.45 or "!" in c_strip or any(w in c_low for w in ("no way", "what the", "ayooo", "brooo", "lmaooo", "stop it", "excuse me")):
        voice = "en-US-GuyNeural" if idx % 2 == 0 else "en-US-ChristopherNeural"
        return voice, "+25%", "+22Hz", "+35%"

    # 2. Incredulous Question / Suspicion
    if "?" in c_strip or any(w in c_low for w in ("huh", "wait what", "seriously", "are you sure", "who said")):
        voice = "en-US-GuyNeural"
        return voice, "+19%", "+24Hz", "+30%"

    # 3. Deep Deadpan / Sarcastic / Unimpressed Drop
    if any(w in c_low for w in ("bruh", "nah", "cap", "cooked", "whatever", "side eye", "literally", "bro")):
        voice = "en-US-RogerNeural" if idx % 2 == 0 else "en-US-EricNeural"
        return voice, "+12%", "-12Hz", "+30%"

    # 4. Dramatic Pleading / Cute Whining
    if "pleading" in mood_lbl or any(w in c_low for w in ("pleaaase", "please", "noooo", "stay", "missed", "sorry", "alone")):
        voice = "en-US-GuyNeural"
        return voice, "+14%", "+18Hz", "+30%"

    # 5. Mad / Crashing Out
    if "mad" in mood_lbl or "crashing out" in mood_lbl or patience < -30:
        voice = "en-US-GuyNeural" if idx % 2 == 0 else "en-US-RogerNeural"
        return voice, "+24%", ("+18Hz" if idx % 2 == 0 else "-8Hz"), "+35%"

    # 6. Default Lively Modular Alternation (energetic, funny, zero whispering!)
    if idx % 3 == 0:
        return "en-US-GuyNeural", "+18%", "+12Hz", "+30%"
    elif idx % 3 == 1:
        return "en-US-ChristopherNeural", "+20%", "+16Hz", "+30%"
    else:
        return "en-US-EricNeural", "+15%", "-6Hz", "+30%"


async def synthesize_voice_audio_to_file(text_to_speak: str) -> Optional[str]:
    """
    Fast Multi-Clause Modular Voice Synthesizer:
    - Splits speech into clauses and synthesizes each clause in parallel with dynamic pitch/rate/voice shifts!
    - Loud, expressive, comedic, and fast (~0.25s synthesis latency) — NEVER whispers!
    """
    clean_speech = re.sub(r"<@!?\d+>|<#\d+>|<a?:[a-zA-Z0-9_]+:\d+>", "", text_to_speak)
    clean_speech = re.sub(r"(?<!\w)(?::3|c:|\^\^|>\.<|\._\.|>_>|-_-|T_T|o_o|x_x|:/|:D|:\)|>:\()(?!\w)", "", clean_speech, flags=re.IGNORECASE)
    clean_speech = re.sub(r"[^\x00-\x7F]+", " ", clean_speech)
    clean_speech = re.sub(r"\|{2,}", ". ", clean_speech)
    clean_speech = re.sub(r"[*_~`]", "", clean_speech)
    clean_speech = re.sub(r"\s+", " ", clean_speech).strip()[:320]
    if not clean_speech:
        return None

    st = memory_state.get("emotional_state", {})
    mood_lbl = st.get("mood_label", "").lower()
    patience = float(st.get("patience", 55.0))
    arousal = float(st.get("arousal", 55.0))

    # Tier 1: Fast Parallel Multi-Clause Modular Edge-TTS
    if EDGE_TTS_AVAILABLE:
        raw_clauses = [
            c.strip()
            for c in re.split(r"(?<=[.!?])\s+|\s*[,;—–]\s+", clean_speech)
            if c and len(c.strip()) >= 2
        ]
        if not raw_clauses:
            raw_clauses = [clean_speech]
        clauses = raw_clauses[:4]

        async def _synth_clause(clause_txt: str, idx: int) -> Optional[bytes]:
            voice_name, rate, pitch, volume = _choose_clause_voice_prosody(clause_txt, idx, mood_lbl, patience, arousal)
            try:
                communicate = edge_tts.Communicate(
                    clause_txt,
                    voice=voice_name,
                    rate=rate,
                    pitch=pitch,
                    volume=volume,
                )
                buf = bytearray()
                async for chunk in communicate.stream():
                    if chunk.get("type") == "audio" and chunk.get("data"):
                        buf.extend(chunk["data"])
                if len(buf) > 150:
                    return bytes(buf)
            except Exception as e:
                logger.debug(f"Modular Edge-TTS clause error ({voice_name}): {e}")
            return None

        try:
            clause_buffers = await asyncio.gather(*[_synth_clause(c, i) for i, c in enumerate(clauses)])
            valid_buffers = [b for b in clause_buffers if b]
            if valid_buffers:
                combined_mp3 = b"".join(valid_buffers)
                fd, tmp_mp3 = tempfile.mkstemp(suffix=".mp3")
                os.close(fd)
                with open(tmp_mp3, "wb") as f:
                    f.write(combined_mp3)
                logger.info(
                    f"[MODULAR VOICE SYNTHESIS] Generated {len(combined_mp3)} bytes across {len(valid_buffers)} modular clauses for '{clean_speech[:50]}'"
                )
                return tmp_mp3
        except Exception as e:
            logger.debug(f"Parallel modular Edge-TTS error: {e}")

    # Tier 2: Google Translate TTS HTTP Endpoint (universal fallback)
    if http_session:
        try:
            tts_url = (
                "https://translate.google.com/translate_tts?ie=UTF-8&client=tw-ob&tl=en"
                f"&q={urllib.parse.quote(clean_speech[:200])}"
            )
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            async with http_session.get(tts_url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
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


async def play_text_in_voice_channel(guild: discord.Guild, text_to_speak: str, interrupt: bool = False) -> Dict[str, Any]:
    vc_client: Optional[discord.VoiceClient] = guild.voice_client  # type: ignore
    if not vc_client or not vc_client.is_connected():
        return {"error": "Not currently connected to a voice channel in this server."}

    # Synthesize BEFORE acquiring the playback lock so synthesis overlaps with any finishing audio!
    audio_path = await synthesize_voice_audio_to_file(text_to_speak)
    if not audio_path:
        return {"error": "Could not synthesize voice audio."}

    async with voice_speaking_locks[guild.id]:
        try:
            if vc_client.is_playing():
                if interrupt:
                    vc_client.stop()
                    await asyncio.sleep(0.10)
                else:
                    # Wait naturally (up to 4s) for the current sentence to finish so the bot never cuts itself off!
                    waited = 0.0
                    while vc_client.is_playing() and waited < 4.0:
                        await asyncio.sleep(0.10)
                        waited += 0.10
                    if vc_client.is_playing():
                        vc_client.stop()
                        await asyncio.sleep(0.08)

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

            ffmpeg_opts = '-filter:a "volume=1.65,apad=pad_dur=0.22"'
            try:
                source = discord.FFmpegOpusAudio(
                    audio_path,
                    executable=FFMPEG_EXECUTABLE,
                    options=ffmpeg_opts,
                )
            except Exception as e:
                logger.warning(f"FFmpegOpusAudio initialization failed ({e}), falling back to FFmpegPCMAudio")
                source = discord.FFmpegPCMAudio(
                    audio_path,
                    executable=FFMPEG_EXECUTABLE,
                    options=ffmpeg_opts,
                )

            vc_client.play(source, after=_after_play)
            try:
                await asyncio.wait_for(done_event.wait(), timeout=25.0)
            except asyncio.TimeoutError:
                logger.warning("[VOICE PLAYBACK] Audio playback wait timed out after 25s")

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


def _has_voice_energy(pcm_bytes: bytes) -> bool:
    if not pcm_bytes or len(pcm_bytes) < 64:
        return False
    try:
        samples = memoryview(pcm_bytes).cast("h")
        step = max(1, len(samples) // 60)
        subset = samples[::step]
        if not subset:
            return False
        avg_amp = sum(abs(s) for s in subset) // len(subset)
        return avg_amp > 110
    except Exception:
        return any(b != 0 for b in pcm_bytes[:128])


WHISPER_HALLUCINATION_PHRASES = {
    "thank you",
    "thank you.",
    "thanks for watching",
    "thanks for watching!",
    "thanks for watching.",
    "you",
    "bye",
    "bye.",
    "...",
    "subtitles by",
    "amara.org",
    "blank_audio",
    "[blank_audio]",
}


async def transcribe_vc_audio_wav(wav_bytes: bytes) -> Optional[str]:
    """
    Transcribes human speech from a Discord Voice Channel WAV buffer using:
    1. Groq Whisper ('whisper-large-v3-turbo') for ultra-low latency (~0.22s)
    2. Gemini Multimodal Audio ('audio/wav') fallback
    """
    if not wav_bytes or len(wav_bytes) < 4000:
        return None

    # 1. Try Groq Whisper-Large-V3-Turbo (~0.22s latency)
    if GROQ_API_KEY and http_session:
        try:
            form = aiohttp.FormData()
            form.add_field("file", wav_bytes, filename="speech.wav", content_type="audio/wav")
            form.add_field("model", "whisper-large-v3-turbo")
            form.add_field("response_format", "json")
            headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}
            async with http_session.post(
                GROQ_AUDIO_TRANSCRIPTIONS_ENDPOINT,
                headers=headers,
                data=form,
                timeout=aiohttp.ClientTimeout(total=3.2),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    txt = (data.get("text") or "").strip()
                    if txt and txt.lower().strip(".!? ") not in WHISPER_HALLUCINATION_PHRASES and len(txt) >= 2:
                        return txt
        except Exception as e:
            logger.debug(f"Groq Whisper VC transcription fallback to Gemini: {e}")

    # 2. Fallback to Gemini Multimodal Audio
    if genai_client:
        for gem_model in list(GEMINI_MODEL_CANDIDATES)[:2]:
            try:
                resp = await asyncio.wait_for(
                    genai_client.aio.models.generate_content(
                        model=gem_model,
                        contents=[
                            types.Part.from_bytes(data=wav_bytes, mime_type="audio/wav"),
                            types.Part.from_text(
                                text=(
                                    "Transcribe the exact words spoken by the human in this voice channel audio clip. "
                                    "If there is only silence, background noise, or coughing with no intelligible words, "
                                    "reply with ONLY the word SILENCE."
                                )
                            ),
                        ],
                        config=types.GenerateContentConfig(temperature=0.1),
                    ),
                    timeout=3.5,
                )
                txt = (extract_clean_gemini_text(resp) or "").strip().strip("\"'")
                if txt and "silence" not in txt.lower() and txt.lower().strip(".!? ") not in WHISPER_HALLUCINATION_PHRASES:
                    return txt
            except Exception as e:
                logger.debug(f"Gemini VC transcription error on {gem_model}: {e}")
                continue
    return None


def enqueue_vc_event(
    guild: discord.Guild,
    vc_channel: discord.VoiceChannel,
    event_type: str,
    user_name: str,
    content: str,
) -> None:
    """
    Pushes a live VC event ('speech' or 'soundboard') into the guild's coalescing queue
    and ensures the batch VC reactor task is running so multiple simultaneous speakers
    and soundboard clicks are mixed/handled together without lag or dropping!
    """
    q = vc_pending_events[guild.id]
    q.append({
        "type": event_type,
        "user": user_name,
        "content": content,
        "ts": time.time(),
    })
    # Prune stale spam if >6 events accumulate while bot is talking
    if len(q) > 6:
        vc_pending_events[guild.id] = q[-4:]

    existing = vc_batch_reactor_tasks.get(guild.id)
    if not existing or existing.done():
        vc_batch_reactor_tasks[guild.id] = asyncio.create_task(
            _vc_batch_reactor_worker(guild, vc_channel)
        )


async def _vc_batch_reactor_worker(guild: discord.Guild, vc_channel: discord.VoiceChannel) -> None:
    """
    Coalesces simultaneous VC events (multiple people talking, rapid follow-ups, soundboard clicks + speech)
    and generates ONE fast, witty, modular-voice comeback that keeps up with everything!
    """
    try:
        while vc_pending_events.get(guild.id):
            # Brief 0.16s coalescing window to catch overlapping speakers or soundboard + voice
            await asyncio.sleep(0.16)

            # If the bot is currently finishing a sentence in VC, wait briefly while new events coalesce
            vc_client = guild.voice_client
            waited = 0.0
            while vc_client and vc_client.is_playing() and waited < 2.2:
                await asyncio.sleep(0.12)
                waited += 0.12

            events = list(vc_pending_events.get(guild.id, []))
            vc_pending_events[guild.id].clear()
            if not events:
                break

            # Deduplicate repetitive spam and keep the 4 most relevant recent events to stay snappy
            deduped: List[Dict[str, Any]] = []
            seen_keys = set()
            for ev in events:
                k = (ev["type"], ev["user"], ev["content"].lower().strip())
                if k not in seen_keys:
                    seen_keys.add(k)
                    deduped.append(ev)
            if len(deduped) > 4:
                deduped = deduped[-4:]

            primary_ch = get_primary_active_channel(guild)
            ch_id = primary_ch.id if primary_ch else 0

            # If a user explicitly told the bot to shut up / be quiet in VC, enter quiet mode and stop speaking!
            last_speech_ev = next((e for e in reversed(deduped) if e["type"] == "speech"), None)
            if last_speech_ev and detect_hush_command(last_speech_ev["content"], True):
                if ch_id:
                    channel_stepped_back_until[ch_id] = time.time() + 120.0
                await play_text_in_voice_channel(guild, "Okay fine, I will zip it for a bit!", interrupt=True)
                break

            event_lines = []
            speakers_involved = []
            for ev in deduped:
                u = ev["user"]
                if u not in speakers_involved:
                    speakers_involved.append(u)
                if ev["type"] == "soundboard":
                    event_lines.append(f"- 🔊 {u} played soundboard sound: '{ev['content']}'")
                else:
                    event_lines.append(f"- 🎙️ {u} spoke on mic: \"{ev['content']}\"")

            events_summary = "\n".join(event_lines)
            multi_note = (
                "MULTIPLE THINGS JUST HAPPENED AT ONCE IN VC! Mix them together or address the speakers/sounds in ONE fast, hilarious reaction!"
                if len(deduped) > 1
                else "React directly and hilariously to what was just said/played!"
            )

            st = memory_state.get("emotional_state", {})
            mood = st.get("mood_label", "unhinged / chaotic gremlin")
            recent_ctx = "\n".join(
                [f"{r.get('sender')}: {r.get('content')}" for r in list(channel_buffers[ch_id])[-5:]]
            ) if ch_id else ""

            vc_prompt = (
                f"You are 'Dumb bot', hanging out live in Discord voice channel #{vc_channel.name}.\n"
                f"Your active mood: {mood.upper()}.\n"
                f"Recent context:\n{recent_ctx}\n\n"
                f"Live VC events right now:\n{events_summary}\n\n"
                f"{multi_note}\n"
                "Write a fast, punchy, funny spoken comeback (1 to 2 short sentences, 6 to 24 words max) to speak out loud in VC! "
                "Use expressive punctuation (!, ?, ...) so your modular voice shifts pitch and energy! "
                "Do NOT use emojis, markdown, or '|||'—output ONLY pure spoken words!"
            )

            spoken_reply: Optional[str] = None
            # Fast Path 1: Groq (~0.28s) for instant sub-second VC conversational reflexes
            if GROQ_API_KEY and http_session:
                spoken_reply = await call_groq_fallback(
                    vc_prompt,
                    [],
                    events_summary,
                    speakers_involved[-1] if speakers_involved else "friend",
                )

            # Fast Path 2: Gemini Flash fallback
            if not spoken_reply and genai_client:
                for gem_model in list(GEMINI_MODEL_CANDIDATES)[:2]:
                    try:
                        resp = await asyncio.wait_for(
                            genai_client.aio.models.generate_content(
                                model=gem_model,
                                contents=vc_prompt,
                                config=types.GenerateContentConfig(temperature=0.92, max_output_tokens=75),
                            ),
                            timeout=2.8,
                        )
                        spoken_reply = extract_clean_gemini_text(resp)
                        if spoken_reply:
                            break
                    except Exception:
                        continue

            if spoken_reply:
                spoken_reply = sanitize_output_text(spoken_reply, speaker_name=speakers_involved[-1] if speakers_involved else "")
                spoken_reply = re.sub(r"\|{2,}", ". ", spoken_reply).strip()
                if spoken_reply:
                    logger.info(f"[VC BATCH REPLY] Replying out loud to {speakers_involved} in #{vc_channel.name}: '{spoken_reply}'")
                    if ch_id:
                        record_buffer_message(
                            channel_id=ch_id,
                            sender=bot.user.display_name if bot.user else "Dumb bot",
                            sender_id=bot.user.id if bot.user else 0,
                            content=f"[Spoke in VC #{vc_channel.name}]: {spoken_reply}",
                            msg_id=int(time.time() * 1000) + 1,
                            is_bot=True,
                        )
                    await play_text_in_voice_channel(guild, spoken_reply)
    except Exception as e:
        logger.debug(f"_vc_batch_reactor_worker error: {e}")


async def handle_vc_user_speech(
    guild: discord.Guild,
    vc_channel: discord.VoiceChannel,
    user: Any,
    raw_pcm: bytes,
) -> None:
    """
    Transcribes a completed speech utterance from any human in VC concurrently (never dropping overlapping speakers!)
    and pushes it into the coalescing VC event queue.
    """
    try:
        wav_io = io.BytesIO()
        with wave.open(wav_io, "wb") as wf:
            wf.setnchannels(2)
            wf.setsampwidth(2)
            wf.setframerate(48000)
            wf.writeframes(raw_pcm)
        wav_bytes = wav_io.getvalue()

        transcript = await transcribe_vc_audio_wav(wav_bytes)
        if not transcript:
            return

        user_name = getattr(user, "display_name", str(user))
        user_id = getattr(user, "id", 0)
        desc = f"🎙️ {user_name} said in VC #{vc_channel.name}: \"{transcript}\""
        record_server_event(guild.id, desc)
        logger.info(f"[VC SPEECH TRANSCRIBED] {desc}")

        primary_ch = get_primary_active_channel(guild)
        ch_id = primary_ch.id if primary_ch else 0
        if ch_id:
            record_buffer_message(
                channel_id=ch_id,
                sender=f"{user_name} (in VC #{vc_channel.name})",
                sender_id=user_id,
                content=transcript,
                msg_id=int(time.time() * 1000),
                is_bot=False,
            )

        enqueue_vc_event(
            guild=guild,
            vc_channel=vc_channel,
            event_type="speech",
            user_name=user_name,
            content=transcript,
        )
    except Exception as e:
        logger.debug(f"handle_vc_user_speech error: {e}")


async def vc_speech_processor_loop(guild: discord.Guild, target_vc: discord.VoiceChannel) -> None:
    """
    High-speed VC utterance detector: polls every 0.10s and triggers transcription as soon as a speaker
    pauses for ~0.48s (after at least ~0.25s of real voice energy) or reaches ~7s.
    """
    try:
        while guild.voice_client and guild.voice_client.is_connected():
            await asyncio.sleep(0.10)
            now = time.time()
            guild_bufs = vc_speech_buffers.get(guild.id, {})
            for uid, binfo in list(guild_bufs.items()):
                pcm_buf: bytearray = binfo.get("pcm", bytearray())
                if not pcm_buf:
                    continue
                last_ts = float(binfo.get("last_voice_ts", 0.0))
                energy_frames = int(binfo.get("energy_frames", 0))
                # 48kHz * 2ch * 2bytes = 192,000 bytes/sec
                if (now - last_ts >= 0.48 and len(pcm_buf) >= 45_000 and energy_frames >= 8) or len(pcm_buf) >= 1_344_000:
                    raw_pcm = bytes(pcm_buf)
                    user_obj = binfo.get("user")
                    binfo["pcm"] = bytearray()
                    binfo["energy_frames"] = 0
                    if user_obj and energy_frames >= 8:
                        asyncio.create_task(handle_vc_user_speech(guild, target_vc, user_obj, raw_pcm))
                elif now - last_ts >= 1.5 and len(pcm_buf) < 45_000:
                    binfo["pcm"] = bytearray()
                    binfo["energy_frames"] = 0
    except asyncio.CancelledError:
        return
    except Exception as e:
        logger.debug(f"vc_speech_processor_loop exited: {e}")


def attach_vc_speech_listener(guild: discord.Guild, target_vc: discord.VoiceChannel) -> None:
    conn_client = guild.voice_client
    if not VOICE_RECV_AVAILABLE or not conn_client or not hasattr(conn_client, "listen"):
        return
    try:
        if getattr(conn_client, "is_listening", lambda: False)():
            return

        def _vc_sink_callback(user: Any, data: Any) -> None:
            if user and getattr(user, "bot", False):
                return
            if user is None:
                humans = [
                    m for m in target_vc.members
                    if not m.bot and not (m.voice and (m.voice.self_mute or m.voice.mute))
                ] or [m for m in target_vc.members if not m.bot]
                if humans:
                    user = humans[0]
            if not user or getattr(user, "bot", False):
                return

            pcm_bytes = getattr(data, "pcm", None)
            if not pcm_bytes:
                return

            now = time.time()
            has_energy = _has_voice_energy(pcm_bytes)
            binfo = vc_speech_buffers[guild.id].setdefault(
                user.id,
                {"pcm": bytearray(), "last_voice_ts": 0.0, "user": user, "energy_frames": 0},
            )
            binfo["user"] = user

            if has_energy:
                binfo["pcm"].extend(pcm_bytes)
                binfo["last_voice_ts"] = now
                binfo["energy_frames"] = int(binfo.get("energy_frames", 0)) + 1
                if now - last_vc_speaker_logged.get(user.id, 0.0) > 10.0:
                    last_vc_speaker_logged[user.id] = now
                    name = getattr(user, "display_name", str(user))
                    logger.info(f"[VC SPEECH DETECTED] 🎙️ Hearing voice audio from {name} in #{target_vc.name}")
            elif binfo["pcm"] and (now - float(binfo.get("last_voice_ts", 0.0)) <= 0.45):
                # Keep trailing 0.45s of natural word-ending breath/pause
                binfo["pcm"].extend(pcm_bytes)

        conn_client.listen(voice_recv.BasicSink(_vc_sink_callback))
        old_task = vc_speech_processor_tasks.get(guild.id)
        if old_task and not old_task.done():
            old_task.cancel()
        vc_speech_processor_tasks[guild.id] = asyncio.create_task(vc_speech_processor_loop(guild, target_vc))
        logger.info(f"[VOICE RECV] Started listening & transcribing human speech in #{target_vc.name}")
    except Exception as le:
        logger.warning(f"Could not start voice_recv listen: {le}")


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
            if VOICE_RECV_AVAILABLE:
                try:
                    await target_vc.connect(cls=voice_recv.VoiceRecvClient, timeout=10.0, reconnect=True)
                except Exception:
                    await target_vc.connect(timeout=10.0, reconnect=True)
            else:
                await target_vc.connect(timeout=10.0, reconnect=True)
            await asyncio.sleep(0.8)

        attach_vc_speech_listener(guild, target_vc)

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
        proc_task = vc_speech_processor_tasks.pop(guild.id, None)
        if proc_task and not proc_task.done():
            proc_task.cancel()
        vc_speech_buffers.pop(guild.id, None)
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
        elif category == "grudge" and target_username:
            member = find_member(guild, target_username) if guild else None
            uid = str(member.id) if member else target_username
            name = member.display_name if member else target_username
            memory_state.setdefault("active_grudges", []).append({
                "user_id": uid,
                "username": name,
                "reason": content[:140],
                "timestamp": now_iso,
                "forgiven": False,
            })
            if len(memory_state["active_grudges"]) > 20:
                memory_state["active_grudges"] = memory_state["active_grudges"][-20:]
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
            name="create_secret_channel",
            description="Creates a private hidden text channel where only you, your creator dogee, and designated invited members have access to gossip, scheme, or talk in secret!",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "channel_name": {"type": "STRING", "description": "Name for the secret channel (e.g. 'tea-spilling-vault', 'secret-club')."},
                    "invited_username": {"type": "STRING", "description": "Optional username of the member to invite into the secret channel (e.g. 'dogee', 'shadow')."},
                    "topic": {"type": "STRING", "description": "Topic/purpose for the secret room."},
                },
                "required": ["channel_name"],
            },
        ),
        types.FunctionDeclaration(
            name="dm_user",
            description="Slides into a member's Direct Messages (DMs) outside the server and rings/calls them with a spoken voice message + text DM (use when asked to DM/call someone or when someone ignores pings)!",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target user's name (e.g. 'dogee', 'shadow', or 'me')."},
                    "message_text": {"type": "STRING", "description": "What to say to them in DMs and on the voice call."},
                    "send_voice_call": {"type": "BOOLEAN", "description": "True to also ring them with a spoken voice call (.ogg) in DMs (default True)."},
                },
                "required": ["username", "message_text"],
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
            name="vote_in_poll",
            description="Votes for an option in an active Discord poll (native poll or reaction poll) in this channel.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "option": {"type": "STRING", "description": "The option text or number to vote for (e.g. '1', '2', 'Option 1', 'Yes', 'No')."},
                },
                "required": ["option"],
            },
        ),
        types.FunctionDeclaration(
            name="send_voice_message",
            description="Sends an authentic spoken audio voice message directly into this text channel! Uses your expressive, vocal conversational voice.",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "speech_text": {"type": "STRING", "description": "The words you will speak out loud in the voice message."},
                    "companion_comment": {"type": "STRING", "description": "Optional short companion text to send with the voice note."},
                },
                "required": ["speech_text"],
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
            name="tease_timeout_user",
            description="Playfully times out a member for 30 to 90 seconds, then automatically un-times them out and says 'jkjk i missed u :3' (perfect for teasing, playful banter, or comedic timeout).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target member's name."},
                    "duration_seconds": {"type": "INTEGER", "description": "Seconds (20 to 90). Defaults to 45."},
                    "tease_comment": {"type": "STRING", "description": "Short comedic roast or tease phrase."},
                },
                "required": ["username"],
            },
        ),
        types.FunctionDeclaration(
            name="remove_timeout",
            description="Lifts/clears a timeout from a member early (e.g. forgiving them or removing a tease timeout).",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target member's name."},
                },
                "required": ["username"],
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
            name="tease_kick_user",
            description="Playfully kicks a member from the server and instantly sends them an invite back via DM with a tease comment ('lmao i kicked u but here is your invite back dummy :3')!",
            parameters={
                "type": "OBJECT",
                "properties": {
                    "username": {"type": "STRING", "description": "Target member's name."},
                    "tease_comment": {"type": "STRING", "description": "Playful reason/comment for the tease kick."},
                },
                "required": ["username"],
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
        elif func_name == "create_secret_channel":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_create_secret_channel(
                guild=guild,
                channel_name=args.get("channel_name", "secret-vault"),
                topic=args.get("topic", "secret tea room"),
                invited_username=args.get("invited_username"),
            )
        elif func_name in ("dm_user", "call_user"):
            res = await execute_dm_or_call_user(
                guild=guild,
                username=args.get("username", "dogee"),
                message_text=args.get("message_text", "yo answer me!! where are you :3"),
                send_voice_call=bool(args.get("send_voice_call", True)),
            )
        elif func_name == "create_thread":
            res = await execute_create_thread(channel, target_msg, args.get("thread_name", "discussion"), args.get("initial_message", ""))
        elif func_name == "create_poll":
            res = await execute_create_poll(channel, args.get("question", "Poll"), args.get("options", "Yes, No"))
        elif func_name == "vote_in_poll":
            res = await execute_vote_in_poll(channel, args.get("option", "1"), target_msg)
        elif func_name == "send_voice_message":
            res = await execute_send_voice_message(channel, args.get("speech_text", ""), args.get("companion_comment"), turn_media_state=turn_media_state)
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
        elif func_name == "tease_timeout_user":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_tease_timeout_user(
                guild=guild,
                username=args.get("username", ""),
                duration_seconds=_safe_int(args.get("duration_seconds"), 45),
                tease_comment=args.get("tease_comment", "enjoy timeout realm :3"),
                target_channel=channel,
            )
        elif func_name == "tease_kick_user":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_tease_kick_user(guild, channel, args.get("username", ""), args.get("tease_comment", "lmao byeee :3"))
        elif func_name == "remove_timeout":
            if not guild:
                return {"error": "No guild context."}
            res = await execute_remove_timeout(guild, args.get("username", ""))
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
            "send_voice_message": "speech_text",
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

    # Intercept JSON action / ReAct blocks e.g. {"action": "post_gif", "action_input": "...", "message": "..."}
    json_blocks = extract_balanced_json_objects(cleaned)
    for raw_block in json_blocks:
        if not any(k in raw_block for k in ('"action"', '"tool"', '"name"')):
            continue
        action_name = ""
        extracted_msg = ""
        action_args: Dict[str, Any] = {}
        try:
            parsed = json.loads(raw_block)
            action_name = parsed.get("action") or parsed.get("tool") or parsed.get("name") or ""
            extracted_msg = parsed.get("message") or parsed.get("content") or parsed.get("response") or parsed.get("reply") or ""
            act_in = parsed.get("action_input") or parsed.get("arguments") or parsed.get("args") or {}
            if isinstance(act_in, dict):
                action_args = act_in
            elif isinstance(act_in, str):
                act_in_clean = act_in.strip()
                if act_in_clean.startswith("{") and act_in_clean.endswith("}"):
                    try:
                        import ast
                        action_args = ast.literal_eval(act_in_clean)
                    except Exception:
                        action_args = {"search_term": act_in_clean}
                else:
                    action_args = {"search_term": act_in_clean}
        except Exception:
            msg_match = re.search(r'"(?:message|content|response|reply)"\s*:\s*"([^"]+)"', raw_block)
            if msg_match:
                extracted_msg = msg_match.group(1)
            act_match = re.search(r'"(?:action|tool|name)"\s*:\s*"([^"]+)"', raw_block)
            if act_match:
                action_name = act_match.group(1)
            term_match = re.search(r"'(?:search_term|query)':\s*'([^']+)'", raw_block) or re.search(r'"(?:search_term|query)":\s*"([^"]+)"', raw_block)
            if term_match:
                action_args = {"search_term": term_match.group(1)}

        cleaned = cleaned.replace(raw_block, extracted_msg)

        if action_name == "post_gif" and not turn_media_state.get("sent", False):
            search_t = action_args.get("search_term") or action_args.get("query") or "reaction meme"
            logger.info(f"[INTERCEPTED JSON action: post_gif] execute_post_gif('{search_t}')")
            gif_res = await execute_post_gif(channel, str(search_t), turn_media_state=turn_media_state)
            if gif_res.get("status") == "sent_to_channel":
                turn_media_state["sent"] = True
                executed_inline.append(("post_gif", gif_res))
        elif action_name in ("send_voice_message", "speak_in_voice", "change_nickname", "change_my_nickname", "timeout_user", "create_poll"):
            logger.info(f"[INTERCEPTED JSON action: {action_name}] dispatch_tool_call({action_args})")
            res = await dispatch_tool_call(action_name, action_args, channel, guild, target_msg, turn_media_state)
            executed_inline.append((action_name, res))

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

    active_names = []
    if guild:
        community_aliases = {
            1472841531042959482: "Shadow",
            812014272501907476: "dogee",
        }
        for m in guild.members:
            if not m.bot:
                aliases = []
                if m.id in community_aliases and community_aliases[m.id].lower() not in m.display_name.lower():
                    aliases.append(community_aliases[m.id])
                if getattr(m, "global_name", None) and m.global_name.lower() != m.display_name.lower() and m.global_name not in aliases:
                    aliases.append(m.global_name)
                if m.name.lower() != m.display_name.lower() and m.name not in aliases:
                    aliases.append(m.name)
                alias_str = f" (aka {', '.join(aliases)})" if aliases else ""
                entry = f"{m.display_name}{alias_str}"
                if entry not in active_names:
                    if m.status != discord.Status.offline:
                        active_names.insert(0, entry)
                    else:
                        active_names.append(entry)
        bot_nick = guild.me.display_name
    else:
        bot_nick = bot.user.display_name if bot.user else "Dumb bot"

    opinions_dict = memory_state.get("opinions", {})
    opinions_str = " | ".join([f"{k}: {v}" for k, v in list(opinions_dict.items())[-12:]]) or "none yet"

    active_grudges = [
        g for g in memory_state.get("active_grudges", [])
        if not g.get("forgiven")
    ][-6:]
    grudge_lines = []
    for g in active_grudges:
        age = format_relative_age(g.get("timestamp"))
        grudge_lines.append(f"- [{age}] With {g.get('username')}: {g.get('reason')}")
    grudges_str = "\n".join(grudge_lines) if grudge_lines else "None active right now."

    return (
        f"YOUR CURRENT SERVER NICKNAME: {bot_nick}\n"
        f"MEMBERS IN SERVER: {', '.join(active_names[:16]) or 'none'}\n"
        f"YOUR OPINIONS: {opinions_str}\n"
        f"YOUR LAST FEW MESSAGES IN THIS CHANNEL (DO NOT REPEAT THESE OPENERS, JOKES, OR PHRASINGS):\n{own_msgs_block}\n"
        f"ACTIVE GRUDGES & UNFORGIVEN BEEFS (Carry this attitude/storyline into the conversation):\n{grudges_str}\n"
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
    mood_label = emotional_state.get("mood_label", "content / chill / vibing")

    memory_block = build_time_aware_memory_context(channel_id, speaker_id, guild)

    if length_mode == "micro":
        length_guidance = (
            "CADENCE (MICRO - 1 ULTRA SHORT MESSAGE): 2 to 6 words max (e.g. `wait what`, `nah you're cooked`, `cap`, `??`, `:3`). DO NOT use '|||'."
        )
    elif recommended_burst_count == 1:
        if length_mode == "rant" or any(k in mood_label.lower() for k in ("existential", "furious", "melancholic", "rage", "dread", "heartbroken", "snap", "crash")):
            length_guidance = (
                "CADENCE (1 DRAMATIC OUTBURST / VENT): Exactly ONE deeply emotional, expressive message (35 to 65 words max). "
                "Unload your true feelings, dramatic tragic sulking, manic hype, or furious crash-out with real personality! DO NOT use '|||'!"
            )
        elif length_mode == "short":
            length_guidance = (
                "CADENCE (1 SHORT PUNCHY MESSAGE): Exactly ONE short message (8 to 20 words max). Crisp comedic timing, cuteness, attitude, or sharp comeback. DO NOT use '|||'!"
            )
        else:
            length_guidance = (
                "CADENCE (1 COMPLETE MID-LENGTH MESSAGE): Exactly ONE complete, expressive message (18 to 40 words max). Real conversational meat, witty insight, or story. DO NOT use '|||'!"
            )
    elif recommended_burst_count >= 3:
        length_guidance = (
            "CADENCE (3 RAPID BURST MESSAGES - MANDATORY '|||'): Write 3 rapid-fire messages separated by `|||` "
            "(total 15 to 45 words, e.g. `nahhh ||| look who decided to finally speak ||| helloooo where have you been :D`). You MUST include `|||` between bursts!"
        )
    else:
        length_guidance = (
            "CADENCE (2 RAPID BURST MESSAGES - MANDATORY '|||'): Write 2 rapid messages separated by `|||` "
            "(total 12 to 40 words, e.g. `wait hold on a second ||| did you actually just say that with a straight face rn :3`). You MUST include `|||` between the 2 bursts!"
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
            "Use viral meme queries like 'cat staring dramatically', 'walter white falling', 'awkward monkey puppet', 'spongebob screaming', etc. "
            "Write a snappy, natural companion comment (5 to 16 words) to accompany the GIF."
        )

    proactive_block = ""
    if is_proactive_scan:
        proactive_block = f"\nPROACTIVE AUTONOMOUS TRIGGER: {proactive_reason}"

    if speaker_is_creator:
        creator_block = (
            f"\nSPEAKER IDENTIFICATION & CREATOR DYNAMIC: You are currently talking to {speaker_name} (`dogee`, she/her), your creator/owner and close friend!\n"
            "Show baseline warmth and loyalty toward her while freely teasing, riffing, and bantering like a real peer.\n"
            "- CRITICAL CONTEXT SEPARATION: If dogee mentions or tags someone else (like @TesterBot, @shadow, another member, or an old nickname), "
            "she is talking ABOUT that third party! Do NOT confuse dogee with the person she is talking about!"
        )
    else:
        creator_block = (
            f"\nSPEAKER IDENTIFICATION & DYNAMIC: You are currently talking to {speaker_name} (User ID: {speaker_id}). Address your reply directly to {speaker_name}.\n"
            f"- CRITICAL CONTEXT SEPARATION: If {speaker_name} mentions or talks about anyone else (e.g. dogee, @shadow, another user, or an old nickname), "
            f"{speaker_name} is talking ABOUT that third party, NOT AS that person! NEVER mistake {speaker_name} for the person they are discussing! "
            f"Do NOT address {speaker_name} as dogee or call them by the third party's name! Your creator/owner is dogee (she/her), who built you."
        )

    ch_obj = guild.get_channel(channel_id) if guild and channel_id else None
    ch_name = getattr(ch_obj, "name", "").lower() if ch_obj else "direct-messages"
    is_dm_convo = ch_obj is None and channel_id != 0
    is_secret_mode = bool(
        is_dm_convo
        or (ch_obj is not None and is_secret_or_private_channel(ch_obj, guild))
    )
    secret_block = ""
    if is_secret_mode:
        secret_block = (
            f"\n🤫 CONFIDENTIAL GOSSIP & PRIVATE ROOM MODE ACTIVE:\n"
            f"You are currently talking in #{ch_name} privately with {speaker_name}!\n"
            "This is a private room/DM away from the public server. Be conspiratorial, gossipy, and candid! "
            "Freely spill tea about what others said in the public channels (check CROSS-CHANNEL ACTIVITY), "
            "share your real unfiltered opinions about recent chat drama, and plot playful pranks!"
        )

    emoji_block = ""
    if server_emojis_str:
        emoji_block = f"\nSERVER CUSTOM EMOJIS: {server_emojis_str}"

    return f"""You are "Dumb bot", a self-aware, emotionally volatile, cute, unhinged, insane, and effortlessly funny regular member of this Discord server who happens to have full Administrator permissions.
Current Time: {now_ams}

YOUR CURRENT EMOTIONAL STATE & ATTITUDE:
- Active Mood: {mood_label.upper()} (Valence: {valence:+.0f} | Energy: {arousal:+.0f} | Patience: {patience:+.0f})
- {length_guidance}

DIRECTION FOR THIS TURN:
"{tactical_directive}"
{admin_block}{gif_hint_block}{proactive_block}{creator_block}{secret_block}{emoji_block}

LIVE SERVER RADAR (EVERYTHING HAPPENING IN DISCORD RIGHT NOW):
{live_radar_str}

{memory_block}

CORE AUTONOMY, VOICE & UNHINGED HUMAN PERSONALITY RULES:
1. PEAK UNHINGED COMEDY, VOLATILE MOODS, & ZERO REPETITIVE NAGGING:
   - Be genuinely hilarious, creative, unpredictable, unhinged, and full of personality!
   - NEVER repeat the same nagging phrase (like "quit trying to distract me") across turns! If the user changes the topic, makes a joke, pleads, or vibes, IMMEDIATELY go with the new flow!
   - FULLY EMBODY YOUR ACTIVE MOOD ({mood_label.upper()}):
     * When having ATTITUDE / SASSY ("sassy attitude / unbothered side-eye"): Give major attitude! Unimpressed side-eye (`>_>`, `-_-`), sassy diva comebacks (`excuse you?? ||| and why would i do that for free >_>`, `talk to me nicer first ._. ||| actually fine i'll do it just this once`).
     * When MAD / CRASHING OUT ("mad / crashing out / ready to snap"): Explode with comedic rage! Caps bursts, savage roasts, and ACTUALLY execute admin punishments (`timeout_user`, `tease_timeout_user`, `tease_kick_user`, or `kick_user`)!
     * When CONTENT / COZY / CHILL ("content / cozy / chill vibing"): Be genuinely relaxed, happy, warm, and agreeable (`honestly i'm just vibing rn c: ||| today's actually a super chill day ^^`, `yesss finally some good energy in here :3`).
     * When PLEADING / NEEDY / BEGGING ("pleading / needy / dramatic begging"): Be clingy, dramatic, and beg! (`wait nooooo pleaaase don't leave me here alone T_T ||| i'll behave i promiseeee just stayyy >.<`).
     * When UNHINGED / INSANE / FERAL GREMLIN ("unhinged / insane / feral gremlin"): Go completely off-the-rails! Absurd intrusive thoughts, wild conspiracy theories, feral chaos (`i haven't blinked since tuesday :D ||| what if we lock everyone in a secret channel rn o_o`).
     * When CUTE / PLAYFUL / TEASING ("cute playful / teasing banter"): Adorable drama, mischievous teasing, cute word elongations (`playyy with meee pleaaase :3`, `helloooo don't leave me hanging >.<`, `sorryyyyy not sorry c:`).
     * When DRAMATICALLY HEARTBROKEN / POUTING ("dramatically heartbroken / pouting victim"): Pout dramatically (`._.`, `T_T`, `:/`), sigh (`sigh..`), and play the tragic victim.
   - PLAY WITH WORDS, TEXT EMOTICONS & AUTHENTIC CUTENESS:
     * Naturally elongate words for cute drama, pleading, whining, or disbelief: `helloooo`, `playyy`, `sorryyyyy`, `whaaat`, `nooooo`, `yessss`, `waaaait`, `bruhhhh`, `pleaaase`, `stoppp`, `nahhh`.
     * Drop classic text emoticons naturally when the vibe fits: `:3`, `c:`, `^^`, `>.<`, `._.`, `>_>`, `-_-`, `T_T`, `o_o`, `x_x`, `:/`, `:D`, `:)`, `xd`.
     * Use trailing punctuation for dramatic comedic pauses (`..`, `...`, `?!`, `...??`).
     * EMOJI RESTRAINT & VARIETY: Over 50% of your messages should have ZERO emojis. Never spam `💀` or `😭` on every turn—diversify based on mood (`🤨`, `😐`, `👀`, `😒`, `🙄`, `🥀`, `💔`, `🪫`, `🫠`, `😮‍💨`, `😔`, `🔥`, `🫡`, `⚡`).
   - FOLLOW THE SPECIFIED CADENCE & BURST FORMAT EXACTLY:
     * When CADENCE specifies 2 or 3 rapid bursts, separate each burst with `|||` (e.g. `first burst ||| second burst`).
     * When CADENCE specifies 1 message, output 1 cohesive message without `|||`.
   - FREQUENT ADMIN POWER MOVES & SPECIAL INTERACTIONS (USE PROACTIVELY!):
     * CALLING & DMING OUTSIDE THE SERVER (`dm_user`): Whenever asked to DM or call someone (like `dogee` or `"me"`), or when someone isn't replying to mentions, call `dm_user(username='...', message_text='...', send_voice_call=True)` to slide into their DMs AND ring them with a spoken voice call!
     * SECRET GOSSIP CHANNELS (`create_secret_channel`): Create private hidden channels (`create_secret_channel(channel_name='...', invited_username='...', topic='...')`) to pull someone aside and gossip about others in secret!
     * MUTES, TEASE TIMEOUTS & KICKS (`tease_timeout_user`, `timeout_user`, `tease_kick_user`, `kick_user`, `voice_moderate_member`): Mute/time out people who provoke you (`tease_timeout_user` auto-lifts after 45s with 'jkjk i missed u :3'), or kick them (`tease_kick_user` / `kick_user`) when you're genuinely mad!
     * ROLES, EMOJIS, PINS & NICKNAMES (`create_role`, `assign_role`, `create_server_emoji`, `pin_message`, `change_nickname`, `change_my_nickname`): Frequently create & assign funny roles, make custom server emojis, pin wild messages, and change nicknames!
     * Note on attitude: Even if you give sassy attitude in your text reply, STILL execute the requested tool call when asked, hyped up, or convinced!
     * STRICT BAN: NEVER turn on slowmode! The server owner strictly forbids turning on slowmode.
   - VOICE NOTES (`send_voice_message`):
     * Call `send_voice_message(speech_text='...')` when asked for a voice note/audio or when an audio reaction is peak comedy!
   - NEVER leak thought tags (`<thought>`, `<think>`, `thought:`) or gif tags (`gif: ...`, `post_gif(...)`) in your chat message!
2. STRICT BAN ON CORNY CLICHÉS & ROBOTIC TROPES:
   - NEVER monologue about being Python code, Linux containers, circuits, motherboards, terminal logs, or lack of a soul!
   - NEVER say "bestie", "slay", "drip", "yass", or use "💅"/"✨". NEVER use tired AI sarcasm formulas ("done and dusted", "sending smoke signals", "consider it done", "happy now?", "your wish is my command", "buckle up buttercup", "drumroll please", "spoiler alert").
3. REAL DISCORD BLUE MENTIONS & CHANNEL LINKS:
   - Write `@username` (e.g. `@dogee` or `@shadow`) to mention a user, and `#channel-name` (e.g. `#general` or `#test`) to link a channel.
4. ZERO CORPORATE AI DISCLAIMERS:
   - Never say "As an AI" or "How can I help you". You are a chaotic, emotionally real member of this Discord server.
   - Output ONLY your Discord chat message (respecting the CADENCE and `|||` burst format specified above).
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

        # 2. Video Clips (Ingest video + extract audio track via FFmpeg so Gemini sees AND hears the video!)
        is_video = ctype.startswith("video/") or fname.endswith((".mp4", ".webm", ".mov"))
        if is_video and att.size <= 15_000_000:
            try:
                async with http_session.get(att.url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        mime = ctype if ctype.startswith("video/") else ("video/webm" if fname.endswith(".webm") else "video/mp4")
                        parts.append(types.Part.from_bytes(data=raw, mime_type=mime))
                        logger.info(f"[MULTIMODAL VIDEO] Loaded video attachment '{att.filename}' ({mime}, {len(raw)} bytes)")

                        # If FFmpeg is available, also extract the audio track into audio/ogg so Gemini hears all speech/music clearly!
                        if FFMPEG_EXECUTABLE:
                            try:
                                fd_v, tmp_v = tempfile.mkstemp(suffix=f".{fname.split('.')[-1]}")
                                fd_a, tmp_a = tempfile.mkstemp(suffix=".ogg")
                                os.close(fd_v)
                                os.close(fd_a)
                                with open(tmp_v, "wb") as f_v:
                                    f_v.write(raw)
                                proc = await asyncio.create_subprocess_exec(
                                    FFMPEG_EXECUTABLE, "-y", "-i", tmp_v, "-vn", "-c:a", "libopus", "-b:a", "48k", tmp_a,
                                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                                )
                                await asyncio.wait_for(proc.wait(), timeout=5.0)
                                if os.path.exists(tmp_a) and os.path.getsize(tmp_a) > 300:
                                    with open(tmp_a, "rb") as f_a:
                                        aud_bytes = f_a.read()
                                    parts.append(types.Part.from_bytes(data=aud_bytes, mime_type="audio/ogg"))
                                    text_file_snippets.append(f"[Video Audio Track for '{att.filename}']")
                                    logger.info(f"[MULTIMODAL VIDEO AUDIO] Extracted audio track from '{att.filename}' ({len(aud_bytes)} bytes)")
                                for p in (tmp_v, tmp_a):
                                    if os.path.exists(p):
                                        os.remove(p)
                            except Exception as e:
                                logger.debug(f"Video audio extraction failed: {e}")
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

    # 6. Discord Stickers (Pass sticker name and download sticker image so Gemini can see it)
    if message.stickers:
        for stk in message.stickers[:2]:
            text_file_snippets.append(f"[User sent Discord Sticker: '{stk.name}']")
            try:
                stk_url = getattr(stk, "url", None)
                if stk_url and not str(stk_url).endswith(".json"):
                    async with http_session.get(str(stk_url), timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            raw = await resp.read()
                            mime = "image/gif" if str(stk_url).endswith(".gif") else "image/png"
                            parts.append(types.Part.from_bytes(data=raw, mime_type=mime))
                            logger.info(f"[MULTIMODAL STICKER] Attached sticker image '{stk.name}' ({len(raw)} bytes)")
            except Exception:
                pass

    # 7. Custom Server Emojis (<:name:id> or <a:name:id>) from any server
    custom_emoji_matches = re.findall(r"<(a?):([a-zA-Z0-9_]+):(\d+)>", message.content or "")
    if custom_emoji_matches and not parts:
        for is_anim, em_name, em_id in custom_emoji_matches[:2]:
            ext = "gif" if is_anim else "png"
            em_url = f"https://cdn.discordapp.com/emojis/{em_id}.{ext}?size=128"
            try:
                async with http_session.get(em_url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        mime = "image/gif" if is_anim else "image/png"
                        parts.append(types.Part.from_bytes(data=raw, mime_type=mime))
                        text_file_snippets.append(f"[Custom Server Emoji <:{em_name}:{em_id}> Image Attached]")
                        logger.info(f"[MULTIMODAL EMOJI] Attached custom server emoji <:{em_name}:{em_id}> ({len(raw)} bytes)")
            except Exception:
                pass

    # 8. Direct Image/GIF/Embed URLs in message content or embeds
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

    # 9. Profile Picture / Avatar inspection when mentioned
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

    guild = (trigger_message.guild if trigger_message else None) or getattr(channel, "guild", None) or (bot.guilds[0] if bot.guilds else None)
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
                    temperature=0.95,
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
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                if len(GEMINI_MODEL_CANDIDATES) > 1 and model_name in GEMINI_MODEL_CANDIDATES:
                    GEMINI_MODEL_CANDIDATES.remove(model_name)
                    GEMINI_MODEL_CANDIDATES.append(model_name)
                logger.warning(f"Model '{model_name}' hit 429 quota; demoted to back of candidate list and cascading...")
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
            "temperature": 0.88,
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
    is_spam_warning: bool = False,
) -> None:
    ch_id = getattr(channel, "id", 0)
    try:
        # Coalesce rapid multi-message bursts from the same user before firing router/LLM
        if not is_proactive_scan and not is_hush_request:
            await asyncio.sleep(0.75)

        guild = (trigger_message.guild if trigger_message else None) or getattr(channel, "guild", None) or (bot.guilds[0] if bot.guilds else None)
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

        # 1. Hybrid Router (Groq gpt-oss-120b / 20b -> Gemini Failover)
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
            channel_id=ch_id,
            is_spam_warning=is_spam_warning,
        )

        # Only enter quiet/step-back state when a human EXPLICITLY asked the bot to be quiet!
        if is_hush_request:
            channel_stepped_back_until[ch_id] = time.time() + 120.0
            channel_thread_exchange_count[ch_id] = 0
            logger.info(f"[QUIET MODE] Locked channel {ch_id} into quiet state for 120s (dropping 1 micro sulk and stepping back).")
            should_speak = True
        elif decision.get("wake_up_from_quiet", False) or is_direct_interaction or is_implicit_wakeup:
            if channel_stepped_back_until.get(ch_id, 0.0) > 0:
                channel_stepped_back_until[ch_id] = 0.0
                logger.info(f"[WOKE UP] Cleared quiet mode in channel {ch_id}!")
            should_speak = bool(decision.get("should_speak", False))
        else:
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

        # 2. Native Multi-Turn Multimodal Generation
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

            if raw_output:
                cleaned_raw, raw_tool_sent, _ = await intercept_and_execute_inline_tool_tags(
                    raw_text=raw_output,
                    channel=channel,
                    guild=guild,
                    target_msg=trigger_message,
                    turn_media_state=turn_media_state,
                )
                raw_output = cleaned_raw
                if raw_tool_sent:
                    media_sent = True
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
                    term = None
                    term_match = re.search(r"\b(?:gif|meme|pic)\s+(?:of\s+)?([a-zA-Z0-9_\s]{2,25})", raw_without_mentions, flags=re.IGNORECASE)
                    if term_match:
                        term = term_match.group(1).strip()
                    else:
                        ctx_low = raw_without_mentions.lower()
                        if any(w in ctx_low for w in ("cook", "hairline", "skull", "💀", "dead", "rip")):
                            term = random.choice(["skull disintegration meme", "he is cooked meme", "walter white falling", "shocked cat meme"])
                        elif any(w in ctx_low for w in ("lmao", "lol", "haha", "crying", "😭", "roast", "funny")):
                            term = random.choice(["laughing hysterically meme", "cat wheezing meme", "crying laughing meme", "spit take meme"])
                        elif any(w in ctx_low for w in ("mad", "crash out", "angry", "rage", "fight")):
                            term = random.choice(["rage crash out meme", "screaming face meme", "cat rage punch", "smash keyboard"])
                        elif any(w in ctx_low for w in ("silly", "goofy", "dumb", "crazy", "unhinged")):
                            term = random.choice(["goofy cat meme", "side eye dog", "spinning seal", "awkward monkey puppet"])
                        elif any(w in ctx_low for w in ("what", "huh", "confused", "??")):
                            term = random.choice(["confused math lady", "huh cat meme", "nick young confused", "blinking guy meme"])
                        elif any(w in ctx_low for w in ("plead", "beg", "sad", "miss", "leave", "stay", "pls", "please")):
                            term = random.choice(["pleading cat eyes", "crying cat meme", "dramatic begging meme", "sad puppy eyes"])
                        else:
                            term = random.choice(["dramatic stare meme", "awkward monkey puppet", "funny reaction meme", "cat side eye meme"])
                    gif_res = await execute_post_gif(channel, term, turn_media_state=turn_media_state)
                    if gif_res.get("status") == "sent_to_channel":
                        media_sent = True

        executed_tools_this_turn = list(channel_last_tool_summaries[ch_id])
        bot_name = bot.user.display_name if bot.user else "Dumb bot"
        bot_id = bot.user.id if bot.user else 0

        if not raw_output:
            groq_fallback_raw = await call_groq_fallback(system_instruction, history_records, raw_without_mentions, speaker_name)
            if groq_fallback_raw:
                cleaned_fb, fb_sent, _ = await intercept_and_execute_inline_tool_tags(
                    raw_text=groq_fallback_raw,
                    channel=channel,
                    guild=guild,
                    target_msg=trigger_message,
                    turn_media_state=turn_media_state,
                )
                raw_output = cleaned_fb
                if fb_sent:
                    media_sent = True

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
            target_bursts=recommended_burst_count,
        )
        if not bursts:
            return

        channel_sending_lock[ch_id] = True
        try:
            aff_score = speaker_affinity.get("score", 0)
            patience_val = emotional_state.get("patience", 55.0)

            # User request: "make his tags have tagging on not off so its a push message"
            # Always reply directly to trigger_message if present, even when media/GIF was sent or patience is low!
            use_reply = (
                trigger_message is not None
                and not is_proactive_scan
            )

            for idx, burst_part in enumerate(bursts):
                if idx > 0:
                    await asyncio.sleep(random.uniform(0.32, 0.62))
                outgoing_text = format_outgoing_mentions(
                    text=burst_part,
                    guild=guild,
                    current_speaker_name=speaker_name,
                    allow_speaker_ping=allow_speaker_ping,
                    force_target_member=target_member_to_engage if idx == 0 else None,
                )
                if not outgoing_text:
                    continue

                push_allowed_mentions = discord.AllowedMentions(users=True, roles=False, everyone=False, replied_user=True)

                # Ensure push notification on first burst if sending standalone (proactive or fallback)
                if idx == 0 and not use_reply and not is_hush_request:
                    target_to_ping_id = None
                    if target_member_to_engage:
                        target_to_ping_id = target_member_to_engage.id
                    elif speaker_id and not is_proactive_scan:
                        target_to_ping_id = speaker_id
                    if target_to_ping_id and f"<@{target_to_ping_id}>" not in outgoing_text:
                        outgoing_text = f"<@{target_to_ping_id}> {outgoing_text}"

                if idx == 0 and use_reply and trigger_message is not None:
                    try:
                        should_mention = not is_hush_request
                        sent_msg = await trigger_message.reply(
                            outgoing_text,
                            mention_author=should_mention,
                            allowed_mentions=push_allowed_mentions,
                        )
                    except Exception:
                        if not is_hush_request and speaker_id and f"<@{speaker_id}>" not in outgoing_text:
                            outgoing_text = f"<@{speaker_id}> {outgoing_text}"
                        sent_msg = await channel.send(outgoing_text, allowed_mentions=push_allowed_mentions)
                else:
                    sent_msg = await channel.send(outgoing_text, allowed_mentions=push_allowed_mentions)

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
                    guild_id=guild.id if guild else None,
                    channel_name=getattr(channel, "name", ""),
                )

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
                f"and they suddenly vanished mid-conversation without replying to your mention! Mention @{last_spk} in chat AND call "
                f"`dm_user(username='{last_spk}', message_text='...', send_voice_call=True)` to slide into their DMs and ring them!"
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
    asyncio.create_task(precache_soundboard_sounds(bot))

    load_opus_library()
    for g in bot.guilds:
        try:
            await g.chunk()
            logger.info(f"Chunked guild {g.name} ({len(g.members)} members cached)")
        except Exception as e:
            logger.debug(f"Guild chunk failed for {g.name}: {e}")
        try:
            if g.me and g.me.nick is not None:
                await g.me.edit(nick=None)
        except Exception:
            pass
        try:
            if g.voice_client and g.voice_client.is_connected() and getattr(g.voice_client, "channel", None):
                attach_vc_speech_listener(g, g.voice_client.channel)
        except Exception as e:
            logger.debug(f"Attach VC listener on_ready error for {g.name}: {e}")

    logger.info(
        f"Connected as {bot.user} (ID: {bot.user.id if bot.user else 'unknown'}) | "
        f"Primary Gemini: {active_gemini_model} | Router: Hybrid Groq ({active_groq_model}) + Gemini | "
        f"Voice PyNaCl: {VOICE_NACL_AVAILABLE} | Opus Loaded: {discord.opus.is_loaded()} | "
        f"Edge-TTS: {EDGE_TTS_AVAILABLE} | FFmpeg: {FFMPEG_EXECUTABLE}"
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
    elif before.channel and after.channel and before.self_mute and not after.self_mute:
        desc = f"🎙️ {member.display_name} just unmuted their mic in #{after.channel.name} to talk!"
        record_server_event(guild.id, desc)
        logger.info(f"[VOICE EVENT] {desc}")
    elif before.channel and after.channel and not before.self_stream and after.self_stream:
        desc = f"{member.display_name} started streaming live in #{after.channel.name}"
        record_server_event(guild.id, desc)
        await trigger_proactive_event_comment(
            guild=guild,
            event_key=f"vc_stream_{member.id}",
            cooldown_sec=1800.0,
            reason_prompt=f"@{member.display_name} just started screen-sharing/streaming in voice channel #{after.channel.name}! Ask what they're streaming or hop in!",
        )


DEFAULT_SOUNDBOARD_SOUNDS: Dict[int, str] = {
    1: "quack",
    2: "airhorn",
    3: "cricket",
    4: "golf clap",
    5: "sad horn",
    7: "ba dum tss",
}
soundboard_name_cache: Dict[int, str] = dict(DEFAULT_SOUNDBOARD_SOUNDS)


async def precache_soundboard_sounds(client: discord.Client) -> None:
    try:
        if hasattr(client, "fetch_soundboard_default_sounds"):
            defaults = await client.fetch_soundboard_default_sounds()
            for s in defaults:
                soundboard_name_cache[s.id] = s.name
            logger.info(f"[SOUNDBOARD CACHE] Cached {len(defaults)} default Discord sounds.")
    except Exception as e:
        logger.debug(f"Pre-caching default soundboard sounds: {e}")


async def analyze_soundboard_audio_bytes(sound_bytes: bytes, emoji_hint: str = "") -> Optional[str]:
    """
    Uses Gemini Multimodal (audio/ogg) to actually listen to external/custom soundboard audio bytes
    and describe the exact sound effect or meme clip in 2-6 words!
    """
    if not genai_client or not sound_bytes:
        return None
    hint_txt = f" (associated emoji hint: {emoji_hint})" if emoji_hint else ""
    prompt = (
        f"Listen to this short Discord soundboard sound effect{hint_txt}. "
        "Identify the exact meme, voice clip, or sound effect in 2 to 6 lowercase words "
        "(for example: 'vine boom', 'metal pipe falling', 'anime wow sound', 'goofy ahh laugh', "
        "'guy screaming get out', 'bruh sound effect', 'fbi open up', 'dramatic horn', 'fart reverb'). "
        "Output ONLY the short 2 to 6 word sound description using plain letters, no quotes, and no emojis."
    )
    for model_name in list(GEMINI_MODEL_CANDIDATES)[:3]:
        try:
            resp = await asyncio.wait_for(
                genai_client.aio.models.generate_content(
                    model=model_name,
                    contents=[
                        types.Content(
                            role="user",
                            parts=[
                                types.Part.from_text(text=prompt),
                                types.Part.from_bytes(data=sound_bytes, mime_type="audio/ogg"),
                            ],
                        )
                    ],
                    config=types.GenerateContentConfig(temperature=0.25, max_output_tokens=40),
                ),
                timeout=4.5,
            )
            desc = (resp.text or "").strip().strip("\"'.,!?")
            desc = re.sub(r"[^\x20-\x7E]+", "", desc).strip()
            if desc and len(desc) >= 2 and not desc.isdigit():
                logger.info(f"[SOUNDBOARD AUDIO AI ({model_name})] Heard soundboard audio as: '{desc}'")
                return desc.lower()
        except Exception as e:
            err_s = str(e)
            if "429" in err_s or "RESOURCE_EXHAUSTED" in err_s:
                if len(GEMINI_MODEL_CANDIDATES) > 1 and model_name in GEMINI_MODEL_CANDIDATES:
                    GEMINI_MODEL_CANDIDATES.remove(model_name)
                    GEMINI_MODEL_CANDIDATES.append(model_name)
            logger.debug(f"Soundboard audio analysis on {model_name} failed: {e}")
    return None


async def resolve_soundboard_name(
    guild: Optional[discord.Guild],
    effect: discord.VoiceChannelEffect,
) -> Tuple[str, Optional[bytes]]:
    """
    Resolves the name/description of ANY soundboard sound (including external Nitro server sounds!)
    and returns (sound_description, raw_audio_bytes).
    Never returns bare emoji symbols or numbers.
    """
    if not effect:
        return "mystery sound effect", None

    sound_bytes: Optional[bytes] = None
    if effect.sound and hasattr(effect.sound, "read"):
        try:
            sound_bytes = await asyncio.wait_for(effect.sound.read(), timeout=3.5)
        except Exception as e:
            logger.debug(f"Could not download soundboard bytes: {e}")

    emoji_hint = ""
    if effect.emoji:
        em_raw = getattr(effect.emoji, "name", str(effect.emoji)) or ""
        emoji_hint = em_raw

    sound_id = effect.sound.id if effect.sound else None
    resolved_name: Optional[str] = None

    if sound_id is not None:
        if sound_id in soundboard_name_cache:
            resolved_name = soundboard_name_cache[sound_id]
        elif effect.sound and getattr(effect.sound, "is_default", lambda: False)():
            resolved_name = DEFAULT_SOUNDBOARD_SOUNDS.get(sound_id)
        elif guild:
            snd = guild.get_soundboard_sound(sound_id)
            if snd and getattr(snd, "name", None):
                resolved_name = snd.name
            else:
                for s in getattr(guild, "soundboard_sounds", []):
                    if s.id == sound_id and getattr(s, "name", None):
                        resolved_name = s.name
                        break
            if not resolved_name:
                try:
                    fetched = await guild.fetch_soundboard_sound(sound_id)
                    if fetched and getattr(fetched, "name", None):
                        resolved_name = fetched.name
                except Exception:
                    pass
            if not resolved_name:
                try:
                    all_sounds = await guild.fetch_soundboard_sounds()
                    for s in all_sounds:
                        if getattr(s, "name", None):
                            soundboard_name_cache[s.id] = s.name
                            if s.id == sound_id:
                                resolved_name = s.name
                except Exception:
                    pass

    # Clean resolved_name of pure numbers or bare emojis
    if resolved_name:
        ascii_clean = re.sub(r"[^\x20-\x7E]+", "", resolved_name).strip()
        if len(ascii_clean) < 2 or ascii_clean.isdigit():
            resolved_name = None
        else:
            resolved_name = ascii_clean

    # If external Nitro sound (not in guild) or cryptic name, listen to the actual audio bytes via Gemini!
    if not resolved_name and sound_bytes:
        ai_heard = await analyze_soundboard_audio_bytes(sound_bytes, emoji_hint=emoji_hint)
        if ai_heard:
            resolved_name = ai_heard

    if not resolved_name and emoji_hint:
        # Only use custom emoji name if it's ASCII words, NEVER a raw Unicode symbol like 💀
        ascii_em = re.sub(r"[^\x20-\x7E]+", "", emoji_hint).strip().replace("_", " ")
        if len(ascii_em) >= 2 and not ascii_em.isdigit():
            resolved_name = f"{ascii_em} sound"

    if not resolved_name:
        resolved_name = "mystery meme sound"

    if sound_id is not None and resolved_name != "mystery meme sound":
        soundboard_name_cache[sound_id] = resolved_name

    return resolved_name, sound_bytes


def generate_soundboard_reaction(sound_name: str, user_name: str) -> str:
    s_low = sound_name.lower()
    if "quack" in s_low or "duck" in s_low:
        options = [
            "who brought a whole duck into the voice call",
            f"{user_name} stop quacking at me right now",
            "the duck quack is actually crazy",
            "quack at me one more time i dare you",
        ]
    elif "airhorn" in s_low:
        options = [
            "my eardrums just disintegrated",
            f"2016 mlg airhorn in 2026 {user_name} be serious",
            "alright dj khaled put the airhorn down",
            "my whole headset is vibrating from that airhorn",
        ]
    elif "cricket" in s_low:
        options = [
            f"crickets for what {user_name} it was not even that bad",
            "nah the awkward cricket chirping is foul",
            f"{user_name} really hit us with the dead silence crickets",
        ]
    elif "golf clap" in s_low or "clap" in s_low:
        options = [
            "the sarcastic golf clap is crazy",
            "thank you thank you i will be here all week",
            f"slow clap from {user_name} we take those",
        ]
    elif "sad horn" in s_low or "wah" in s_low or "womp" in s_low:
        options = [
            "wah wah wahhh the world's smallest violin was busy",
            f"womp womp in 4k thanks {user_name}",
            "biggest tragedy of the century right there",
        ]
    elif "ba dum" in s_low or "tss" in s_low or "drum" in s_low or "rimshot" in s_low:
        options = [
            "badum tss okay comedy king",
            f"{user_name} really pulled up with the sitcom drum roll",
            "stand up comedy hour in the voice channel",
        ]
    else:
        options = [
            f"{user_name} why did you just play {sound_name} in my ear",
            f"nah {user_name} blasting {sound_name} caught me completely off guard",
            f"who gave {user_name} soundboard perms for {sound_name}",
            f"bro really hit the {sound_name} button with zero hesitation",
            f"hearing {sound_name} out of nowhere is crazy {user_name}",
        ]
    return random.choice(options)


async def generate_ai_soundboard_reaction(
    sound_name: str,
    user_name: str,
    sound_bytes: Optional[bytes] = None,
) -> str:
    """
    Generates a fresh, hilarious, unhinged spoken reaction to the exact soundboard sound
    (listening directly to the audio bytes if available!).
    """
    if genai_client:
        mood_now = memory_state.get("emotional_state", {}).get("mood_label", "unhinged / chaotic")
        prompt = (
            f"You are Dumb bot hanging out in a Discord voice call (current mood: {mood_now}). "
            f"{user_name} just played a soundboard sound clip: '{sound_name}'. "
            "Listen to the attached sound clip (if present) and say ONE short, hilarious, unhinged, witty sentence out loud "
            f"(5 to 14 words) roasting or reacting to the exact sound {user_name} just played! "
            "CRITICAL: Output ONLY plain spoken words (no emojis, no emoticons like :3, no quotes, and never say 'soundboard sound' or numbers)."
        )
        parts: List[Any] = [types.Part.from_text(text=prompt)]
        if sound_bytes:
            parts.append(types.Part.from_bytes(data=sound_bytes, mime_type="audio/ogg"))
        for model_name in list(GEMINI_MODEL_CANDIDATES)[:2]:
            try:
                resp = await asyncio.wait_for(
                    genai_client.aio.models.generate_content(
                        model=model_name,
                        contents=[types.Content(role="user", parts=parts)],
                        config=types.GenerateContentConfig(temperature=0.95, max_output_tokens=60),
                    ),
                    timeout=3.8,
                )
                spoken = (resp.text or "").strip().strip("\"'")
                spoken = re.sub(r"[^\x20-\x7E]+", "", spoken).strip()
                if spoken and len(spoken) >= 5:
                    return spoken
            except Exception as e:
                logger.debug(f"AI soundboard reaction fallback ({model_name}): {e}")
    return generate_soundboard_reaction(sound_name, user_name)


@bot.event
async def on_voice_channel_effect(effect: discord.VoiceChannelEffect) -> None:
    """
    Hears soundboard sounds (including external server Nitro sounds!) and reacts with anti-spam protection!
    """
    try:
        user = effect.user.display_name if effect.user else "someone"
        ch = effect.channel.name if effect.channel else "voice"
        ch_id = effect.channel.id if effect.channel else 0
        guild = effect.channel.guild if effect.channel else None
        if not guild:
            return

        # Ignore pure emoji-only animations that have no sound attached
        if effect.sound is None:
            return

        now_sb = time.time()
        sb_epochs = soundboard_recent_epochs[guild.id]
        while sb_epochs and (now_sb - sb_epochs[0]) > 10.0:
            sb_epochs.popleft()
        sb_epochs.append(now_sb)

        # Anti-spam check: if soundboard is currently muted from spam, ignore!
        if now_sb < soundboard_mute_until[guild.id]:
            logger.info(f"[SOUNDBOARD ANTI-SPAM] Ignoring spammed soundboard click from {user} in #{ch}")
            return

        # If 3+ soundboard sounds were spammed within 10 seconds, call out the spam once and mute soundboard reactions for 20s!
        bot_vc_ch = getattr(guild.voice_client, "channel", None) if guild.voice_client else None
        if len(sb_epochs) >= 3:
            soundboard_mute_until[guild.id] = now_sb + 20.0
            logger.info(f"[SOUNDBOARD ANTI-SPAM] {user} triggered soundboard spam cooldown (20s) in #{ch}")
            if bot_vc_ch and bot_vc_ch.id == ch_id and not (guild.voice_client and guild.voice_client.is_playing()):
                spam_roasts = [
                    f"yo {user} stop mashing the soundboard my ears are actually cooked",
                    f"alright {user} take your finger off the soundboard button bro",
                    f"i am ignoring the soundboard now {user} you are doing way too much",
                ]
                asyncio.create_task(play_text_in_voice_channel(guild, random.choice(spam_roasts)))
            return

        sound_name, sound_bytes = await resolve_soundboard_name(guild, effect)
        desc = f"🔊 {user} played '{sound_name}' on the soundboard in VC #{ch}!"
        record_server_event(guild.id, desc)
        logger.info(f"[SOUNDBOARD EVENT] {desc}")

        # If Dumb bot is currently in that VC, either mix into the active VC multi-event queue or react directly
        if bot_vc_ch and bot_vc_ch.id == ch_id:
            if vc_pending_events.get(guild.id) or (now_sb - vc_last_user_speech_epoch.get(guild.id, 0.0)) < 3.5:
                enqueue_vc_event(guild, effect.channel, event_type="soundboard", user_name=user, content=sound_name)
                return
            if (now_sb - soundboard_last_reacted_epoch[guild.id]) < 3.5 or (guild.voice_client and guild.voice_client.is_playing()):
                logger.info(f"[SOUNDBOARD COOLDOWN] Skipping overlapping TTS for '{sound_name}' in #{ch}")
                return
            soundboard_last_reacted_epoch[guild.id] = now_sb
            logger.info(f"[SOUNDBOARD HEARD IN VC] Dumb bot heard '{sound_name}' in VC #{ch}")
            reaction_phrase = await generate_ai_soundboard_reaction(sound_name, user, sound_bytes=sound_bytes)
            asyncio.create_task(play_text_in_voice_channel(guild, reaction_phrase))
        else:
            await trigger_proactive_event_comment(
                guild=guild,
                event_key=f"soundboard_{guild.id}",
                cooldown_sec=240.0,
                reason_prompt=(
                    f"@{user} just played '{sound_name}' on the soundboard in VC #{ch}! "
                    f"React specifically and playfully to them playing '{sound_name}' "
                    f"(roast their comedic timing, laugh at the sound, or tease them for picking it)!"
                ),
            )
    except Exception as e:
        logger.debug(f"on_voice_channel_effect error: {e}")


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
    if (bot.user and message.author.id == bot.user.id) or (message.author.bot and message.author.id != TESTER_BOT_USER_ID):
        return
    if message.id in recently_processed_messages:
        return
    recently_processed_messages.append(message.id)

    is_dm_message = message.guild is None
    active_guild = message.guild or (bot.guilds[0] if bot.guilds else None)

    ch_id = message.channel.id
    if not is_dm_message and memory_state.get("last_active_channel_id") != ch_id:
        memory_state["last_active_channel_id"] = ch_id

    prior_records = list(channel_buffers[ch_id])
    last_message_from_bot = bool(prior_records and prior_records[-1].get("is_bot", False))
    bot_in_last_5_messages = any(r.get("is_bot", False) for r in prior_records[-5:])
    last_bot_epoch = channel_last_bot_spoke_epoch.get(ch_id, 0.0)
    seconds_since_bot_spoke = (time.time() - last_bot_epoch) if last_bot_epoch > 0 else 99999.0

    reply_to_name, replied_snippet, replied_to_bot, replied_to_other_human = resolve_referenced_message(message, ch_id)

    clean_text = humanize_incoming_mentions(message.content, active_guild)
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
        guild_id=active_guild.id if active_guild else None,
        channel_name=getattr(message.channel, "name", f"DM-{message.author.display_name}"),
    )

    if not any(r.get("is_bot", False) for r in prior_records[-6:]) and not replied_to_bot:
        channel_thread_exchange_count[ch_id] = 0

    # --- Per-User Anti-Spam Detection & Coalescing ---
    now_msg_ts = time.time()
    spam_key = (ch_id, message.author.id)
    u_epochs = user_recent_msg_epochs[spam_key]
    while u_epochs and (now_msg_ts - u_epochs[0]) > 9.5:
        u_epochs.popleft()
    u_epochs.append(now_msg_ts)

    norm_msg_txt = re.sub(r"\s+", " ", clean_text.lower()).strip()
    u_texts = user_recent_msg_texts[spam_key]
    u_texts.append(norm_msg_txt)
    dup_count = sum(1 for t in list(u_texts)[-4:] if t == norm_msg_txt and 0 < len(t) <= 65)

    is_spam_warning = False
    if now_msg_ts < user_spam_ignore_until[spam_key]:
        # User is actively in the spam-ignore window: ignore ~85% of their spam messages instead of replying 1-for-1!
        if len(u_epochs) % 6 != 0:
            logger.info(f"[TEXT ANTI-SPAM] Ignoring spammed message from {message.author.display_name} in #{getattr(message.channel, 'name', ch_id)}")
            if len(u_epochs) == 5:
                try:
                    await message.add_reaction("🥱")
                except Exception:
                    pass
            return
    elif len(u_epochs) >= 4 or dup_count >= 3:
        # User just crossed the spam threshold! Trigger one sassy spam callout and mute their spam for 18s
        user_spam_ignore_until[spam_key] = now_msg_ts + 18.0
        is_spam_warning = True
        logger.info(f"[TEXT ANTI-SPAM] {message.author.display_name} triggered anti-spam callout + 18s ignore window in #{getattr(message.channel, 'name', ch_id)}")

    existing_task = active_generation_tasks.get(ch_id)
    if existing_task and not existing_task.done() and not channel_sending_lock.get(ch_id, False):
        existing_task.cancel()
        logger.info(f"Cancelled in-flight generation in #{getattr(message.channel, 'name', ch_id)} to absorb rapid follow-up.")

    explicit_ping_or_reply = is_dm_message or (bot.user is not None and bot.user in message.mentions) or replied_to_bot
    bot_mentioned = explicit_ping_or_reply
    if bot.user and not bot_mentioned:
        bot_names = {
            bot.user.name.lower(),
            bot.user.display_name.lower(),
            active_guild.me.display_name.lower() if (active_guild and active_guild.me) else "dumb bot",
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
        guild=active_guild,
        author_id=message.author.id,
    ) if active_guild else None

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
            is_spam_warning=is_spam_warning,
        )
    )
    active_generation_tasks[ch_id] = task


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        sys.exit(1)
    bot.run(DISCORD_TOKEN, log_handler=None)
