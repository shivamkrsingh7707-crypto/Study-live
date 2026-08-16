import os
import json
import time
from flask import Flask, render_template_string, request, session, redirect, url_for, jsonify, Response
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO, emit, join_room, leave_room
from datetime import datetime, date, timedelta
from urllib.parse import quote as url_quote
from sqlalchemy import func, text
from sqlalchemy.exc import OperationalError
import math
import threading

app = Flask(__name__)

# ═════════════════════════════════════════════
# SECRET KEY - Persistent
# ═════════════════════════════════════════════
SECRET_KEY = os.environ.get('SECRET_KEY')
if not SECRET_KEY:
    _key_file = '.secret_key'
    if os.path.exists(_key_file):
        with open(_key_file, 'r') as f:
            SECRET_KEY = f.read().strip()
    else:
        SECRET_KEY = os.urandom(32).hex()
        try:
            with open(_key_file, 'w') as f:
                f.write(SECRET_KEY)
        except Exception:
            pass
app.secret_key = SECRET_KEY
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=365)  # 1 year sessions

# ═════════════════════════════════════════════
# DATABASE - Auto Postgres/SQLite
# ═════════════════════════════════════════════
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///study_tracker.db')
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

IS_POSTGRES = DATABASE_URL.startswith('postgresql://')

# CRITICAL: Connection pool settings for Render free tier
if IS_POSTGRES:
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_pre_ping': True,           # Test connection before use
        'pool_recycle': 280,             # Recycle every ~5 min (Render idles at 5 min)
        'pool_size': 3,                  # Small pool for free tier
        'max_overflow': 5,
        'pool_timeout': 30,
        'connect_args': {
            'connect_timeout': 10,
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 5,
        }
    }
else:
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_pre_ping': True,
        'connect_args': {'check_same_thread': False, 'timeout': 30}
    }

print(f"🗄️  Database: {'PostgreSQL (Production)' if IS_POSTGRES else 'SQLite (Local)'}")

db = SQLAlchemy(app)

# CRITICAL: SocketIO with better config for Render
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode='eventlet' if IS_POSTGRES else 'threading',
    ping_timeout=60,           # Longer timeout for slow connections
    ping_interval=25,          # Keep connection alive
    max_http_buffer_size=1e6,
    logger=False,
    engineio_logger=False,
    allow_upgrades=True,
    transports=['websocket', 'polling']
)


# ═════════════════════════════════════════════
# LIVE ONLINE TRACKING (Thread-safe)
# ═════════════════════════════════════════════

active_sessions = {}
_session_lock = threading.Lock()

def get_live_online_users():
    with _session_lock:
        seen = {}
        for sid, info in list(active_sessions.items()):
            uid = info.get('user_id')
            if uid and uid not in seen:
                seen[uid] = dict(info)
        return list(seen.values())

def get_live_online_count():
    return len(get_live_online_users())

def get_room_online_count(room):
    with _session_lock:
        users_in_room = set()
        for sid, info in list(active_sessions.items()):
            if info.get('room') == room and info.get('user_id'):
                users_in_room.add(info['user_id'])
        return len(users_in_room)


# ═════════════════════════════════════════════
# DATABASE MODELS
# ═════════════════════════════════════════════

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    student_class = db.Column(db.String(50), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_active = db.Column(db.Date, default=date.today)
    last_seen = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    streak = db.Column(db.Integer, default=0)
    avatar_color = db.Column(db.String(20), default='#00E1FD')


class ChapterProgress(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    subject = db.Column(db.String(100), nullable=False, index=True)
    chapter_index = db.Column(db.Integer, nullable=False)
    circle_index = db.Column(db.Integer, nullable=False)
    completed = db.Column(db.Boolean, default=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (
        db.UniqueConstraint('user_id', 'subject', 'chapter_index', 'circle_index'),
    )


class CustomSubject(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    name = db.Column(db.String(100), nullable=False)
    icon = db.Column(db.String(10), default='📘')
    color_from = db.Column(db.String(20), default='#00E1FD')
    color_to = db.Column(db.String(20), default='#C533FF')
    tag = db.Column(db.String(50), default='CUSTOM')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class CustomChapter(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    subject_id = db.Column(db.Integer, db.ForeignKey('custom_subject.id'), nullable=False, index=True)
    name = db.Column(db.String(200), nullable=False)
    order_index = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class ActivityLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    activity_date = db.Column(db.Date, default=date.today, index=True)
    circles_completed = db.Column(db.Integer, default=0)
    __table_args__ = (
        db.UniqueConstraint('user_id', 'activity_date'),
    )


class ExamSchedule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    subject = db.Column(db.String(100), nullable=False)
    exam_date = db.Column(db.Date, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (
        db.UniqueConstraint('user_id', 'subject'),
    )


class ChatMessage(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    user_name = db.Column(db.String(100), nullable=False)
    user_class = db.Column(db.String(50), nullable=False)
    user_avatar_color = db.Column(db.String(20), default='#00E1FD')
    room = db.Column(db.String(50), default='general', index=True)
    message = db.Column(db.String(1000), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    reply_to_id = db.Column(db.Integer, db.ForeignKey('chat_message.id'), nullable=True)


# ═════════════════════════════════════════════
# SAFE DB COMMIT WRAPPER (Auto-retry on failure)
# ═════════════════════════════════════════════

def safe_commit(max_retries=3):
    """Commit with automatic retry on connection errors."""
    for attempt in range(max_retries):
        try:
            db.session.commit()
            return True
        except OperationalError as e:
            db.session.rollback()
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
                continue
            print(f"❌ DB commit failed after {max_retries} tries: {e}")
            return False
        except Exception as e:
            db.session.rollback()
            print(f"❌ DB commit error: {e}")
            return False
    return False


# ═════════════════════════════════════════════
# CURRICULUM
# ═════════════════════════════════════════════

DEFAULT_CURRICULUM = {
    "Science": [
        "Chemical Reactions and Equations", "Acids, Bases and Salts", "Metals and Non-metals",
        "Carbon and its Compounds", "Life Processes", "Control and Coordination",
        "How do Organisms Reproduce?", "Heredity", "Light - Reflection and Refraction",
        "Human Eye and Colourful World", "Electricity", "Magnetic Effects of Electric Current",
        "Our Environment"
    ],
    "Social Science": [
        "Rise of Nationalism in Europe", "Nationalism in India", "The Making of a Global World",
        "Age of Industrialization", "Print Culture & Modern World", "Resources and Development",
        "Forest and Wildlife Resources", "Water Resources", "Agriculture",
        "Minerals and Energy Resources", "Manufacturing Industries", "Lifelines of National Economy",
        "Power Sharing", "Federalism", "Gender, Religion and Caste", "Political Parties",
        "Outcomes of Democracy", "Development", "Sectors of Indian Economy",
        "Money and Credit", "Globalisation & Indian Economy"
    ],
    "Maths": [
        "Real Numbers", "Polynomials", "Pair of Linear Equations", "Quadratic Equations",
        "Arithmetic Progressions", "Triangles", "Coordinate Geometry",
        "Introduction to Trigonometry", "Applications of Trigonometry", "Circles",
        "Areas Related to Circles", "Surface Areas and Volumes", "Statistics", "Probability"
    ],
    "Hindi & English": [
        "A Letter to God (English)", "Nelson Mandela (English)", "Two Stories About Flying (English)",
        "Diary of Anne Frank (English)", "Glimpses of India (English)", "Mijbil the Otter (English)",
        "Madam Rides the Bus (English)", "The Sermon at Benares (English)", "The Proposal (English)",
        "Surdas Ke Pad (Hindi)", "Ram-Lakshman-Parshuram Samvad (Hindi)",
        "Netaji Ka Chashma (Hindi)", "Balgovin Bhagat (Hindi)"
    ]
}

DEFAULT_SUBJECT_META = {
    "Science":         {"icon": "⚗️", "color_from": "#00E1FD", "color_to": "#0077FF", "tag": "PHY · CHM · BIO"},
    "Social Science":  {"icon": "🌍", "color_from": "#C533FF", "color_to": "#7B2FFF", "tag": "HIS · GEO · POL · ECO"},
    "Maths":           {"icon": "📐", "color_from": "#00F260", "color_to": "#0575E6", "tag": "ALG · GEO · TRIG · STAT"},
    "Hindi & English": {"icon": "📚", "color_from": "#FF6B6B", "color_to": "#C533FF", "tag": "LIT · PROSE · POETRY"}
}

COLOR_PRESETS = [
    {"from": "#00E1FD", "to": "#C533FF", "name": "Cyan → Purple"},
    {"from": "#00F260", "to": "#0575E6", "name": "Mint → Blue"},
    {"from": "#FF6B6B", "to": "#C533FF", "name": "Coral → Purple"},
    {"from": "#FFD700", "to": "#FF6B00", "name": "Gold → Orange"},
    {"from": "#00E1FD", "to": "#00F260", "name": "Cyan → Mint"},
    {"from": "#FF00E1", "to": "#7B2FFF", "name": "Pink → Purple"},
]

ICON_PRESETS = ['📘', '📗', '📕', '📙', '📓', '🧪', '🔬', '🧬', '💻', '🎨', '🎵', '⚽', '🌐', '📖', '✏️', '🎯']
AVATAR_COLORS = ['#00E1FD', '#C533FF', '#00F260', '#FFD700', '#FF6B6B', '#FF00E1', '#7B2FFF', '#0575E6']
CIRCLES_PER_CHAPTER = 5
CIRCLE_LABELS = ["Read", "Note", "Prac", "Rev", "Done"]

CHAT_ROOMS = [
    {"id": "general", "name": "General", "icon": "💬", "desc": "Chat about anything"},
    {"id": "science", "name": "Science", "icon": "⚗️", "desc": "Physics, Chem, Bio"},
    {"id": "maths", "name": "Maths", "icon": "📐", "desc": "Algebra, geometry, trig"},
    {"id": "social", "name": "Social", "icon": "🌍", "desc": "History, geo, civics"},
    {"id": "english", "name": "Lang", "icon": "📚", "desc": "Hindi & English lit"},
    {"id": "doubts", "name": "Doubts", "icon": "❓", "desc": "Ask & help others"},
]


# ═════════════════════════════════════════════
# HELPERS (with safe DB access)
# ═════════════════════════════════════════════

def get_current_user():
    if 'user_id' not in session:
        return None
    try:
        return db.session.get(User, session['user_id'])
    except Exception:
        db.session.rollback()
        return None


def touch_user(user):
    if not user:
        return
    try:
        user.last_seen = datetime.utcnow()
        safe_commit()
    except Exception:
        db.session.rollback()


def get_total_user_count():
    try:
        return User.query.count()
    except Exception:
        db.session.rollback()
        return 0


def time_ago(dt):
    if not dt:
        return "just now"
    now = datetime.utcnow()
    diff = now - dt
    secs = diff.total_seconds()
    if secs < 60:
        return "just now"
    mins = int(secs / 60)
    if mins < 60:
        return f"{mins}m ago"
    hrs = int(mins / 60)
    if hrs < 24:
        return f"{hrs}h ago"
    days = int(hrs / 24)
    if days < 7:
        return f"{days}d ago"
    return dt.strftime('%d %b')


def get_full_curriculum(user_id):
    try:
        curriculum = {}
        for name, chapters in DEFAULT_CURRICULUM.items():
            curriculum[name] = {
                "chapters": chapters,
                "meta": DEFAULT_SUBJECT_META[name],
                "is_custom": False,
                "subject_id": None
            }
        customs = CustomSubject.query.filter_by(user_id=user_id).order_by(CustomSubject.created_at).all()
        for cs in customs:
            cchapters = CustomChapter.query.filter_by(subject_id=cs.id).order_by(CustomChapter.order_index, CustomChapter.id).all()
            curriculum[cs.name] = {
                "chapters": [c.name for c in cchapters],
                "meta": {"icon": cs.icon, "color_from": cs.color_from, "color_to": cs.color_to, "tag": cs.tag},
                "is_custom": True,
                "subject_id": cs.id
            }
        return curriculum
    except Exception as e:
        print(f"⚠️ get_full_curriculum error: {e}")
        db.session.rollback()
        return {name: {"chapters": ch, "meta": DEFAULT_SUBJECT_META[name], "is_custom": False, "subject_id": None} 
                for name, ch in DEFAULT_CURRICULUM.items()}


def get_user_progress(user_id):
    try:
        rows = ChapterProgress.query.filter_by(user_id=user_id, completed=True).all()
        return {f"{r.subject}__{r.chapter_index}__{r.circle_index}": True for r in rows}
    except Exception:
        db.session.rollback()
        return {}


def compute_stats(user_id):
    try:
        curriculum = get_full_curriculum(user_id)
        total_circles = sum(len(v['chapters']) * CIRCLES_PER_CHAPTER for v in curriculum.values())
        completed_rows = ChapterProgress.query.filter_by(user_id=user_id, completed=True).count()
        overall_pct = round((completed_rows / total_circles) * 100, 1) if total_circles else 0

        active_chambers = 0
        subject_pcts = {}
        subject_done_circles = {}
        for subject, info in curriculum.items():
            subj_total = len(info['chapters']) * CIRCLES_PER_CHAPTER
            subj_done = ChapterProgress.query.filter_by(user_id=user_id, subject=subject, completed=True).count()
            pct = round((subj_done / subj_total) * 100, 1) if subj_total else 0
            subject_pcts[subject] = pct
            subject_done_circles[subject] = subj_done
            if subj_done > 0:
                active_chambers += 1

        fully_done_chapters = 0
        total_chapters = sum(len(v['chapters']) for v in curriculum.values())
        for subject, info in curriculum.items():
            for ci in range(len(info['chapters'])):
                done = ChapterProgress.query.filter_by(user_id=user_id, subject=subject, chapter_index=ci, completed=True).count()
                if done == CIRCLES_PER_CHAPTER:
                    fully_done_chapters += 1
        syllabus_pct = round((fully_done_chapters / total_chapters) * 100, 1) if total_chapters else 0

        return {
            "overall_pct": overall_pct,
            "active_chambers": active_chambers,
            "subject_pcts": subject_pcts,
            "subject_done_circles": subject_done_circles,
            "syllabus_pct": syllabus_pct,
            "completed_circles": completed_rows,
            "total_circles": total_circles,
            "fully_done_chapters": fully_done_chapters,
            "total_chapters": total_chapters,
            "total_subjects": len(curriculum)
        }
    except Exception as e:
        print(f"⚠️ compute_stats error: {e}")
        db.session.rollback()
        return {"overall_pct": 0, "active_chambers": 0, "subject_pcts": {}, "subject_done_circles": {},
                "syllabus_pct": 0, "completed_circles": 0, "total_circles": 0,
                "fully_done_chapters": 0, "total_chapters": 0, "total_subjects": 4}


def update_streak(user):
    try:
        today = date.today()
        if user.last_active == today:
            return
        if user.last_active == today - timedelta(days=1):
            user.streak = (user.streak or 0) + 1
        else:
            user.streak = 1
        user.last_active = today
        safe_commit()
    except Exception:
        db.session.rollback()


def log_activity(user_id, delta):
    try:
        today = date.today()
        log = ActivityLog.query.filter_by(user_id=user_id, activity_date=today).first()
        if log:
            log.circles_completed = max(0, (log.circles_completed or 0) + delta)
        else:
            if delta > 0:
                log = ActivityLog(user_id=user_id, activity_date=today, circles_completed=delta)
                db.session.add(log)
        safe_commit()
    except Exception:
        db.session.rollback()


def get_7day_data(user_id):
    try:
        today = date.today()
        days = []
        for i in range(6, -1, -1):
            d = today - timedelta(days=i)
            log = ActivityLog.query.filter_by(user_id=user_id, activity_date=d).first()
            count = log.circles_completed if log else 0
            days.append({
                "date": d.strftime('%d %b'),
                "day_label": d.strftime('%a'),
                "count": count,
                "is_today": (d == today)
            })
        return days
    except Exception:
        db.session.rollback()
        return []


def compute_exam_plans(user_id):
    try:
        curriculum = get_full_curriculum(user_id)
        stats = compute_stats(user_id)
        exams = ExamSchedule.query.filter_by(user_id=user_id).all()
        today = date.today()
        plans = []
        for exam in exams:
            if exam.subject not in curriculum:
                continue
            info = curriculum[exam.subject]
            total_ch = len(info['chapters'])
            total_units = total_ch * CIRCLES_PER_CHAPTER
            done_units = stats['subject_done_circles'].get(exam.subject, 0)
            remaining_units = max(0, total_units - done_units)
            remaining_chapters = 0
            for ci in range(total_ch):
                done = ChapterProgress.query.filter_by(user_id=user_id, subject=exam.subject, chapter_index=ci, completed=True).count()
                if done < CIRCLES_PER_CHAPTER:
                    remaining_chapters += 1
            days_left = (exam.exam_date - today).days
            if days_left < 0:
                status = "past"; per_day_units = 0; per_day_chapters = 0; per_week_chapters = 0
            elif days_left == 0:
                status = "today"
                per_day_units = remaining_units
                per_day_chapters = remaining_chapters
                per_week_chapters = remaining_chapters
            else:
                status = "upcoming"
                per_day_units = math.ceil(remaining_units / days_left) if remaining_units > 0 else 0
                per_day_chapters = round(remaining_chapters / days_left, 2) if remaining_chapters > 0 else 0
                per_week_chapters = math.ceil(remaining_chapters / max(days_left / 7, 1)) if remaining_chapters > 0 else 0
            pct = stats['subject_pcts'].get(exam.subject, 0)
            if remaining_units == 0:
                feasibility = "complete"
            elif days_left < 0:
                feasibility = "expired"
            elif per_day_units <= 3:
                feasibility = "easy"
            elif per_day_units <= 7:
                feasibility = "moderate"
            elif per_day_units <= 15:
                feasibility = "tough"
            else:
                feasibility = "urgent"
            plans.append({
                "id": exam.id, "subject": exam.subject,
                "exam_date": exam.exam_date,
                "exam_date_str": exam.exam_date.strftime('%d %b %Y'),
                "exam_day": exam.exam_date.strftime('%A'),
                "days_left": days_left,
                "total_chapters": total_ch,
                "remaining_chapters": remaining_chapters,
                "total_units": total_units,
                "remaining_units": remaining_units,
                "done_units": done_units,
                "per_day_units": per_day_units,
                "per_day_chapters": per_day_chapters,
                "per_week_chapters": per_week_chapters,
                "progress_pct": pct,
                "meta": info['meta'],
                "status": status,
                "feasibility": feasibility
            })
        plans.sort(key=lambda p: p['exam_date'])
        return plans
    except Exception:
        db.session.rollback()
        return []


@app.template_filter('urlencode')
def urlencode_filter(s):
    return url_quote(str(s), safe='')


@app.template_filter('initials')
def initials_filter(name):
    if not name:
        return "?"
    parts = name.strip().split()
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


# ═════════════════════════════════════════════
# HEALTH CHECK ROUTE (Keep Render awake)
# ═════════════════════════════════════════════

@app.route('/health')
def health():
    """Simple health check for uptime monitoring."""
    try:
        db.session.execute(text('SELECT 1'))
        return jsonify({'status': 'ok', 'db': 'connected', 'time': datetime.utcnow().isoformat()})
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/ping')
def ping():
    return 'pong', 200


# ═════════════════════════════════════════════
# BASE STYLE (with better loading UX)
# ═════════════════════════════════════════════

BASE_STYLE = """
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0"/>
<meta name="theme-color" content="#0B0F19"/>
<meta name="apple-mobile-web-app-capable" content="yes"/>
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"/>
<link rel="manifest" href="/manifest.json"/>
<link rel="icon" type="image/svg+xml" href="/icon-192.png"/>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<script>
  tailwind.config = { theme: { extend: {
    colors: { 'app-bg':'#0B0F19','card-bg':'#1A2035','cyan-neon':'#00E1FD','purple-neon':'#C533FF','green-neon':'#00F260','muted':'#8892A4' },
    fontFamily: { sans: ['Inter','system-ui','sans-serif'] }
  }}}
  if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => navigator.serviceWorker.register('/service-worker.js').catch(()=>{}));
  }
</script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet"/>
<style>
  *{box-sizing:border-box;-webkit-tap-highlight-color:transparent;}
  body{background:#0B0F19;font-family:'Inter',sans-serif;color:#E2E8F0;min-height:100vh;overflow-x:hidden;}
  a{text-decoration:none;}
  .glass-nav{background:rgba(11,15,25,0.92);backdrop-filter:blur(24px);border-top:1px solid rgba(0,225,253,0.12);}
  .gradient-text{background:linear-gradient(135deg,#00E1FD,#C533FF);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;}
  .gradient-btn{background:linear-gradient(135deg,#00E1FD,#C533FF);transition:all 0.3s ease;}
  .gradient-btn:hover{opacity:0.92;transform:translateY(-1px);box-shadow:0 8px 25px rgba(0,225,253,0.3);}
  .gradient-btn:disabled{opacity:0.5;cursor:not-allowed;}
  .card-base{background:#1A2035;border:1px solid rgba(255,255,255,0.06);border-radius:16px;transition:all 0.25s ease;}
  .card-glow:hover{box-shadow:0 0 30px rgba(0,225,253,0.08),0 8px 32px rgba(0,0,0,0.4);transform:translateY(-2px);}
  .circle-btn{transition:all 0.2s cubic-bezier(0.34,1.56,0.64,1);cursor:pointer;user-select:none;}
  .circle-btn:hover{transform:scale(1.12);}
  .circle-btn:active{transform:scale(0.94);}
  .nav-icon{transition:all 0.2s ease;}
  .scroll-content{padding-bottom:100px;}
  ::-webkit-scrollbar{width:4px;}
  ::-webkit-scrollbar-track{background:#0B0F19;}
  ::-webkit-scrollbar-thumb{background:#1A2035;border-radius:4px;}
  .input-field{background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.1);border-radius:12px;color:#E2E8F0;outline:none;transition:all 0.2s ease;}
  .input-field:focus{border-color:#00E1FD;box-shadow:0 0 0 3px rgba(0,225,253,0.12);}
  .chapter-row{background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.05);border-radius:14px;transition:all 0.2s ease;}
  .tag-pill{background:rgba(255,255,255,0.06);border-radius:999px;}
  @keyframes fadeInUp{from{opacity:0;transform:translateY(20px)}to{opacity:1;transform:translateY(0)}}
  .fade-in{animation:fadeInUp 0.5s ease forwards;}
  @keyframes msgIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
  .msg-in{animation:msgIn 0.3s ease forwards;}
  @keyframes pulseGlow{0%,100%{opacity:1;transform:scale(1)}50%{opacity:0.5;transform:scale(1.2)}}
  .online-pulse{animation:pulseGlow 2s ease infinite;}
  .subject-card{border-radius:20px;overflow:hidden;position:relative;transition:all 0.3s ease;cursor:pointer;}
  .subject-card:hover{transform:translateY(-4px);box-shadow:0 20px 40px rgba(0,0,0,0.5);}
  .modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,0.7);backdrop-filter:blur(6px);z-index:100;display:none;align-items:center;justify-content:center;padding:16px;}
  .modal-overlay.active{display:flex;animation:fadeInUp 0.3s ease;}
  .modal-content{background:#1A2035;border:1px solid rgba(255,255,255,0.08);border-radius:20px;width:100%;max-width:440px;max-height:90vh;overflow-y:auto;}
  .icon-choice,.color-choice{cursor:pointer;transition:all 0.2s ease;}
  .icon-choice:hover,.color-choice:hover{transform:scale(1.1);}
  .icon-choice.active{background:rgba(0,225,253,0.15)!important;border-color:#00E1FD!important;}
  .color-choice.active{transform:scale(1.15);box-shadow:0 0 0 3px #fff;}
  .bar-chart-bar{transition:all 0.6s cubic-bezier(0.34,1.2,0.64,1);}
  .chapter-item{background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:10px;padding:10px 12px;display:flex;align-items:center;justify-content:space-between;gap:8px;}
  .btn-secondary{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);border-radius:10px;padding:8px 14px;font-size:13px;font-weight:600;color:#E2E8F0;cursor:pointer;transition:all 0.2s;}
  .btn-secondary:hover{background:rgba(255,255,255,0.08);border-color:rgba(0,225,253,0.3);}
  .btn-danger{background:rgba(255,100,100,0.08);border:1px solid rgba(255,100,100,0.3);color:#FF6B6B;}
  .exam-card{border-radius:18px;padding:18px;position:relative;overflow:hidden;}
  .countdown-badge{display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:999px;font-size:11px;font-weight:800;}
  input[type="date"]{color-scheme:dark;}
  .avatar{width:36px;height:36px;border-radius:999px;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:12px;color:#0B0F19;flex-shrink:0;}
  .chat-bubble{border-radius:14px;padding:10px 14px;max-width:78%;word-wrap:break-word;line-height:1.4;font-size:13.5px;}
  .chat-bubble.mine{background:linear-gradient(135deg,#00E1FD,#C533FF);color:#fff;border-bottom-right-radius:4px;}
  .chat-bubble.theirs{background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.08);color:#E2E8F0;border-bottom-left-radius:4px;}
  .chat-bubble.pending{opacity:0.6;}
  .chat-bubble.failed{background:rgba(255,100,100,0.15)!important;border:1px solid rgba(255,100,100,0.4);}
  .chat-scroll{height:calc(100vh - 340px);min-height:300px;overflow-y:auto;padding:8px 12px;scroll-behavior:smooth;}
  .chat-input-bar{position:fixed;bottom:78px;left:0;right:0;background:rgba(11,15,25,0.96);backdrop-filter:blur(20px);border-top:1px solid rgba(255,255,255,0.06);z-index:40;padding:10px 16px;}
  .reply-preview{background:rgba(0,225,253,0.06);border-left:3px solid #00E1FD;border-radius:8px;padding:6px 10px;margin-bottom:8px;font-size:12px;display:flex;align-items:center;justify-content:space-between;}
  .room-pill{transition:all 0.2s ease;}
  .room-pill.active{background:linear-gradient(135deg,rgba(0,225,253,0.18),rgba(197,51,255,0.18));border-color:#00E1FD!important;color:#fff!important;}
  .status-dot{width:8px;height:8px;border-radius:50%;display:inline-block;}
  .status-dot.connected{background:#00F260;box-shadow:0 0 8px rgba(0,242,96,0.5);}
  .status-dot.connecting{background:#FFD700;}
  .status-dot.disconnected{background:#FF6B6B;}
  .page-loader{position:fixed;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,#00E1FD,#C533FF);z-index:9999;transform:translateX(-100%);transition:transform 0.3s;}
  .page-loader.active{transform:translateX(0);}
  .toast{position:fixed;bottom:120px;left:50%;transform:translateX(-50%);background:rgba(26,32,53,0.95);border:1px solid rgba(0,225,253,0.3);border-radius:10px;padding:10px 18px;font-size:13px;font-weight:600;color:#fff;z-index:200;box-shadow:0 8px 24px rgba(0,0,0,0.5);backdrop-filter:blur(20px);display:none;}
  .toast.show{display:block;animation:fadeInUp 0.3s ease;}
</style>
"""


def bottom_nav_html(active):
    icons = {
        "hub": '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>',
        "exam": '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>',
        "chat": '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>',
        "analytics": '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
        "settings": '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M12 1v6m0 6v6m11-7h-6m-6 0H1"/></svg>',
    }
    def item(key, label, href):
        is_active = (key == active)
        wrap_style = "background:linear-gradient(135deg,rgba(0,225,253,0.15),rgba(197,51,255,0.15));border-color:rgba(0,225,253,0.35);" if is_active else "background:rgba(255,255,255,0.04);border-color:rgba(255,255,255,0.08);"
        stroke_color = "#00E1FD" if is_active else "#8892A4"
        text_class = 'text-[9px] font-semibold' if is_active else 'text-[9px] font-medium text-muted'
        text_style = 'color:#00E1FD;' if is_active else ''
        return f'''<a href="{href}" class="nav-icon flex flex-col items-center gap-0.5 flex-1"><div class="w-9 h-9 rounded-2xl flex items-center justify-center border border-transparent" style="{wrap_style}"><span style="color:{stroke_color};display:flex;">{icons[key].replace('stroke-width', f'stroke="{stroke_color}" stroke-width')}</span></div><span class="{text_class}" style="{text_style}">{label}</span></a>'''
    return f'''<nav class="fixed bottom-0 left-0 right-0 glass-nav z-50"><div class="max-w-2xl mx-auto px-3 py-2.5 flex items-center justify-around gap-1">{item("hub","Hub","/")}{item("exam","Exam","/exam-zone")}{item("chat","Chat","/chat")}{item("analytics","Stats","/analytics")}{item("settings","Setup","/settings")}</div></nav><div id="pageLoader" class="page-loader"></div><div id="toast" class="toast"></div><script>function showToast(m,c='#00E1FD'){{const t=document.getElementById('toast');t.style.borderColor=c;t.textContent=m;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),3000);}}document.addEventListener('DOMContentLoaded',()=>{{document.querySelectorAll('a').forEach(a=>{{a.addEventListener('click',(e)=>{{const h=a.getAttribute('href');if(h&&!h.startsWith('#')&&!h.startsWith('javascript')&&!a.target){{document.getElementById('pageLoader').classList.add('active');}}}});}});}});</script>'''


def presence_script():
    return """<script>(function(){if(typeof io==='undefined')return;try{const s=io({transports:['websocket','polling'],reconnection:true,reconnectionDelay:1000,reconnectionAttempts:Infinity});s.on('presence_update',(d)=>{document.querySelectorAll('.js-online-count').forEach(el=>el.textContent=d.online);});}catch(e){}})();</script>"""


# ═════════════════════════════════════════════
# ONBOARDING TEMPLATE
# ═════════════════════════════════════════════

ONBOARDING_TEMPLATE = """
<!DOCTYPE html><html lang="en"><head><title>Study Tracker — Setup</title>""" + BASE_STYLE + """</head><body class="flex items-center justify-center min-h-screen p-4">
<div class="w-full max-w-md fade-in">
<div class="card-base p-8 relative overflow-hidden" style="box-shadow:0 25px 60px rgba(0,0,0,0.5);">
<div class="absolute -top-10 -right-10 w-40 h-40 rounded-full opacity-20" style="background:linear-gradient(135deg,#00E1FD,#C533FF);filter:blur(40px);"></div>
<div class="relative z-10">
<div class="flex items-center gap-3 mb-8">
<div class="w-12 h-12 rounded-2xl flex items-center justify-center text-2xl" style="background:linear-gradient(135deg,#00E1FD22,#C533FF22);border:1px solid rgba(0,225,253,0.3);">🎯</div>
<div><div class="gradient-text font-black text-xl">StudyTracker</div><div class="text-xs text-muted">Class 10 Academic Hub</div></div>
</div>
<h1 class="text-3xl font-black text-white mb-2">Set Up Your<br/><span class="gradient-text">Command Center</span></h1>
<p class="text-muted text-sm mb-8">Join {{ total_users }} student{{ 's' if total_users != 1 else '' }}.</p>
{% if error %}<div class="mb-4 p-3 rounded-xl text-sm" style="background:rgba(255,100,100,0.1);border:1px solid rgba(255,100,100,0.3);color:#FF6B6B;">⚠️ {{ error }}</div>{% endif %}
<form method="POST" action="/setup" class="space-y-4">
<div><label class="block text-xs font-semibold text-muted uppercase mb-2">Your Name</label><input type="text" name="name" placeholder="e.g. Arjun" required maxlength="100" class="input-field w-full px-4 py-3.5 text-sm"/></div>
<div><label class="block text-xs font-semibold text-muted uppercase mb-2">Your Class</label><input type="text" name="student_class" placeholder="e.g. Class 10 — A" required maxlength="50" class="input-field w-full px-4 py-3.5 text-sm"/></div>
<button type="submit" class="gradient-btn w-full py-4 rounded-xl text-white font-bold text-sm">🚀 Launch My Dashboard</button>
</form>
<div class="flex items-center justify-center gap-2 mt-6 text-xs text-muted">
<span class="inline-block w-2 h-2 rounded-full online-pulse" style="background:#00F260;"></span>
<span class="js-online-count">{{ online_count }}</span><span>online now</span>
</div>
</div></div></div>
""" + presence_script() + """
</body></html>
"""


DASHBOARD_TEMPLATE = """
<!DOCTYPE html><html lang="en"><head><title>StudyTracker</title>""" + BASE_STYLE + """</head><body>
<div class="scroll-content"><div class="max-w-2xl mx-auto px-4 pt-6 pb-4">
<div class="flex items-center justify-between mb-4 fade-in">
<a href="/chat" class="flex items-center gap-2 px-3 py-1.5 rounded-full" style="background:rgba(0,242,96,0.08);border:1px solid rgba(0,242,96,0.25);">
<span class="inline-block w-2 h-2 rounded-full online-pulse" style="background:#00F260;"></span>
<span class="text-xs font-bold js-online-count" style="color:#00F260;">{{ online_count }}</span>
<span class="text-xs text-muted">live</span>
</a>
<div class="text-xs text-muted">{{ total_users }} learners</div>
</div>
<div class="flex items-center justify-between mb-6 fade-in">
<div>
<p class="text-xs font-semibold text-muted uppercase mb-1">Welcome 👋</p>
<h1 class="text-2xl font-black text-white">{{ user.name }}</h1>
<div class="flex items-center gap-2 mt-1"><span class="text-xs px-2 py-0.5 rounded-full font-semibold" style="background:rgba(0,225,253,0.12);color:#00E1FD;">{{ user.student_class }}</span></div>
</div>
<div class="avatar" style="background:linear-gradient(135deg,{{ user.avatar_color }},#C533FF);width:44px;height:44px;font-size:14px;">{{ user.name | initials }}</div>
</div>
{% if nearest_exam %}
<a href="/exam-zone" class="card-base p-4 mb-4 block" style="background:linear-gradient(135deg,rgba(255,107,107,0.08),rgba(197,51,255,0.08));border-color:rgba(255,107,107,0.2);">
<div class="flex items-center gap-3">
<div class="text-3xl">📅</div>
<div class="flex-1">
<div class="text-xs text-muted font-semibold uppercase">Next Exam</div>
<div class="text-sm font-bold text-white">{{ nearest_exam.subject }} · <span style="color:#FF6B6B;">{{ nearest_exam.days_left }} days left</span></div>
<div class="text-xs text-muted">Study <span class="font-bold" style="color:#00E1FD;">{{ nearest_exam.per_day_units }} units/day</span></div>
</div>
</div>
</a>
{% endif %}
<div class="card-base p-5 mb-4">
<div class="flex items-center gap-5">
<div class="relative" style="width:100px;height:100px;">
<svg width="100" height="100" viewBox="0 0 100 100" style="transform:rotate(-90deg);">
<circle cx="50" cy="50" r="40" fill="none" stroke="rgba(255,255,255,0.07)" stroke-width="10"/>
<circle cx="50" cy="50" r="40" fill="none" stroke="url(#rg)" stroke-width="10" stroke-linecap="round" stroke-dasharray="{{ ring_dash }} {{ ring_gap }}"/>
<defs><linearGradient id="rg"><stop offset="0%" style="stop-color:#00E1FD"/><stop offset="100%" style="stop-color:#C533FF"/></linearGradient></defs>
</svg>
<div class="absolute inset-0 flex flex-col items-center justify-center">
<span class="text-xl font-black text-white">{{ stats.overall_pct }}%</span>
<span class="text-xs text-muted">Sync</span>
</div>
</div>
<div class="flex-1">
<h2 class="text-lg font-bold text-white mb-0.5">Overall Progress</h2>
<p class="text-xs text-muted mb-3">{{ stats.completed_circles }}/{{ stats.total_circles }} units</p>
<div class="w-full rounded-full h-2" style="background:rgba(255,255,255,0.07);">
<div class="h-2 rounded-full" style="width:{{ stats.overall_pct }}%;background:linear-gradient(90deg,#00E1FD,#C533FF);"></div>
</div>
</div>
</div>
</div>
<div class="card-base p-5 mb-4">
<div class="flex items-center justify-between mb-4">
<div><h2 class="text-sm font-bold text-white">Past 7 Days</h2><p class="text-xs text-muted">Daily activity</p></div>
<div class="text-right"><div class="text-lg font-black gradient-text">{{ week_total }}</div><div class="text-xs text-muted">units</div></div>
</div>
<div class="flex items-end justify-between gap-2" style="height:110px;">
{% for day in week_data %}
{% set bh = (day.count / week_max * 100) if week_max > 0 else 0 %}
<div class="flex flex-col items-center flex-1 gap-1.5">
<div class="w-full flex flex-col justify-end items-center" style="height:80px;">
{% if day.count > 0 %}<span class="text-xs font-bold mb-1" style="color:{% if day.is_today %}#00E1FD{% else %}#E2E8F0{% endif %};font-size:10px;">{{ day.count }}</span>{% endif %}
<div class="bar-chart-bar w-full rounded-t-lg" style="height:{{ bh if bh >= 6 else (6 if day.count > 0 else 3) }}%;background:{% if day.is_today %}linear-gradient(180deg,#00E1FD,#C533FF){% elif day.count > 0 %}linear-gradient(180deg,rgba(0,225,253,0.6),rgba(197,51,255,0.4)){% else %}rgba(255,255,255,0.06){% endif %};min-height:4px;"></div>
</div>
<div class="text-center"><div class="text-xs font-semibold" style="color:{% if day.is_today %}#00E1FD{% else %}#8892A4{% endif %};font-size:10px;">{{ day.day_label }}</div></div>
</div>
{% endfor %}
</div>
</div>
<div class="grid grid-cols-3 gap-3 mb-4">
<div class="card-base p-4 flex flex-col items-center text-center" style="border-radius:14px;"><div class="text-2xl mb-1">🔥</div><div class="text-xl font-black text-white">{{ user.streak or 0 }}</div><div class="text-xs text-muted">Streak</div></div>
<div class="card-base p-4 flex flex-col items-center text-center" style="border-radius:14px;"><div class="text-2xl mb-1">⚡</div><div class="text-xl font-black gradient-text">{{ stats.active_chambers }}/{{ stats.total_subjects }}</div><div class="text-xs text-muted">Active</div></div>
<div class="card-base p-4 flex flex-col items-center text-center" style="border-radius:14px;"><div class="text-2xl mb-1">📊</div><div class="text-xl font-black" style="color:#00F260;">{{ stats.syllabus_pct }}%</div><div class="text-xs text-muted">Chapters</div></div>
</div>
<div class="mb-2">
<div class="flex items-center justify-between mb-3">
<h2 class="text-base font-bold text-white">Subject Chambers</h2>
<button onclick="openAddSubject()" class="text-xs font-bold gradient-text">+ Add Subject</button>
</div>
<div class="grid grid-cols-2 gap-3">
{% for subject, info in curriculum.items() %}
{% set meta = info.meta %}
<a href="/subject/{{ subject | urlencode }}" class="subject-card card-base block">
<div class="p-5">
<div class="flex items-start justify-between mb-4">
<div class="w-11 h-11 rounded-xl flex items-center justify-center text-xl" style="background:linear-gradient(135deg,{{ meta.color_from }}22,{{ meta.color_to }}22);border:1px solid {{ meta.color_from }}33;">{{ meta.icon }}</div>
<div class="text-right"><div class="text-lg font-black text-white">{{ stats.subject_pcts[subject] }}%</div><div class="text-xs text-muted">Done</div></div>
</div>
<div class="flex items-center gap-1.5"><h3 class="text-sm font-bold text-white">{{ subject }}</h3>{% if info.is_custom %}<span class="text-xs" style="color:#00F260;">●</span>{% endif %}</div>
<span class="tag-pill text-xs px-2 py-0.5 font-medium mt-1 inline-block" style="color:{{ meta.color_from }};font-size:10px;">{{ meta.tag }}</span>
<div class="mt-3 w-full rounded-full h-1.5" style="background:rgba(255,255,255,0.07);"><div class="h-1.5 rounded-full" style="width:{{ stats.subject_pcts[subject] }}%;background:linear-gradient(90deg,{{ meta.color_from }},{{ meta.color_to }});"></div></div>
<div class="flex justify-between mt-1.5"><span class="text-muted" style="font-size:10px;">{{ info.chapters | length }} chapters</span><span style="font-size:10px;color:{{ meta.color_from }};">→</span></div>
</div>
</a>
{% endfor %}
<button onclick="openAddSubject()" class="subject-card card-base block text-left" style="border:2px dashed rgba(0,225,253,0.25);background:rgba(0,225,253,0.02);">
<div class="p-5 flex flex-col items-center justify-center h-full min-h-[160px]">
<div class="w-11 h-11 rounded-xl flex items-center justify-center text-2xl mb-3" style="background:linear-gradient(135deg,#00E1FD22,#C533FF22);border:1px solid rgba(0,225,253,0.3);">+</div>
<h3 class="text-sm font-bold text-white">Add Subject</h3>
</div>
</button>
</div>
</div>
<a href="/chat" class="mt-4 block card-base" style="border-radius:16px;background:linear-gradient(135deg,rgba(0,225,253,0.08),rgba(197,51,255,0.08));border:1px solid rgba(0,225,253,0.15);padding:16px 20px;">
<div class="flex items-center gap-3"><span class="text-2xl">💬</span>
<div class="flex-1"><div class="text-sm font-semibold text-white">Join Live Chat</div><div class="text-xs text-muted"><span class="js-online-count">{{ online_count }}</span> online →</div></div></div>
</a>
</div></div>
<div id="addSubjectModal" class="modal-overlay">
<div class="modal-content p-6">
<div class="flex items-center justify-between mb-4"><h2 class="text-lg font-black text-white">Create New Subject</h2><button onclick="closeAddSubject()" class="w-8 h-8 rounded-lg" style="background:rgba(255,255,255,0.05);">✕</button></div>
<form method="POST" action="/add_subject" class="space-y-4">
<div><label class="block text-xs font-semibold text-muted uppercase mb-2">Subject Name</label><input type="text" name="name" required maxlength="100" class="input-field w-full px-4 py-3 text-sm"/></div>
<div><label class="block text-xs font-semibold text-muted uppercase mb-2">Tag</label><input type="text" name="tag" maxlength="50" class="input-field w-full px-4 py-3 text-sm"/></div>
<div><label class="block text-xs font-semibold text-muted uppercase mb-2">Icon</label>
<div class="grid grid-cols-8 gap-2">{% for icon in icon_presets %}<div class="icon-choice w-10 h-10 rounded-xl flex items-center justify-center text-lg border{% if loop.first %} active{% endif %}" style="background:rgba(255,255,255,0.04);border-color:rgba(255,255,255,0.1);" onclick="selectIcon(this,'{{ icon }}')">{{ icon }}</div>{% endfor %}</div>
<input type="hidden" name="icon" id="iconInput" value="{{ icon_presets[0] }}"/></div>
<div><label class="block text-xs font-semibold text-muted uppercase mb-2">Color</label>
<div class="flex gap-3 flex-wrap">{% for c in color_presets %}<div class="color-choice w-10 h-10 rounded-full{% if loop.first %} active{% endif %}" style="background:linear-gradient(135deg,{{ c.from }},{{ c.to }});" onclick="selectColor(this,'{{ c.from }}','{{ c.to }}')"></div>{% endfor %}</div>
<input type="hidden" name="color_from" id="cf" value="{{ color_presets[0].from }}"/><input type="hidden" name="color_to" id="ct" value="{{ color_presets[0].to }}"/></div>
<button type="submit" class="gradient-btn w-full py-3 rounded-xl text-white font-bold text-sm">✨ Create</button>
</form>
</div>
</div>
<script>
function openAddSubject(){document.getElementById('addSubjectModal').classList.add('active');}
function closeAddSubject(){document.getElementById('addSubjectModal').classList.remove('active');}
function selectIcon(e,i){document.querySelectorAll('.icon-choice').forEach(x=>x.classList.remove('active'));e.classList.add('active');document.getElementById('iconInput').value=i;}
function selectColor(e,f,t){document.querySelectorAll('.color-choice').forEach(x=>x.classList.remove('active'));e.classList.add('active');document.getElementById('cf').value=f;document.getElementById('ct').value=t;}
document.getElementById('addSubjectModal').addEventListener('click',e=>{if(e.target.id==='addSubjectModal')closeAddSubject();});
</script>
""" + presence_script() + """
{{ nav_html | safe }}
</body></html>
"""


SUBJECT_TEMPLATE = """
<!DOCTYPE html><html lang="en"><head><title>{{ subject }}</title>""" + BASE_STYLE + """
<script>
const SUBJECT={{ subject | tojson }};
const IS_CUSTOM={{ 'true' if info.is_custom else 'false' }};
const SUBJECT_ID={{ info.subject_id if info.subject_id else 'null' }};
const pendingUpdates=new Map();
async function toggleCircle(ci,ci2,el){
  const f=el.getAttribute('data-filled')==='true';
  const n=!f;
  setC(el,n);
  const key=`${ci}_${ci2}`;
  if(pendingUpdates.has(key))clearTimeout(pendingUpdates.get(key));
  const timeout=setTimeout(async()=>{
    pendingUpdates.delete(key);
    let retries=3;
    while(retries>0){
      try{
        const r=await fetch('/update_progress',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({subject:SUBJECT,chapter_index:ci,circle_index:ci2,completed:n})});
        if(!r.ok)throw new Error('HTTP '+r.status);
        const d=await r.json();
        if(!d.success){setC(el,f);showToast('Save failed','#FF6B6B');return;}
        updateBar(ci);updateSub(d.subject_pct);return;
      }catch(e){
        retries--;
        if(retries===0){setC(el,f);showToast('Network error - please retry','#FF6B6B');return;}
        await new Promise(r=>setTimeout(r,500));
      }
    }
  },200);
  pendingUpdates.set(key,timeout);
}
function setC(el,f){el.setAttribute('data-filled',f?'true':'false');const c=el.querySelector('.circle-check');const l=el.querySelector('.circle-label');if(f){el.style.background='linear-gradient(135deg,#00E1FD,#C533FF)';el.style.borderColor='transparent';el.style.boxShadow='0 0 12px rgba(0,225,253,0.5)';if(c)c.style.opacity='1';if(l)l.style.opacity='0';}else{el.style.background='rgba(255,255,255,0.04)';el.style.borderColor='rgba(255,255,255,0.12)';el.style.boxShadow='none';if(c)c.style.opacity='0';if(l)l.style.opacity='1';}}
function updateBar(ci){const r=document.querySelector(`[data-chapter="${ci}"]`);if(!r)return;const cs=r.querySelectorAll('.circle-btn');let f=0;cs.forEach(c=>{if(c.getAttribute('data-filled')==='true')f++;});const p=(f/cs.length)*100;const b=r.querySelector('.chapter-bar');const pl=r.querySelector('.chapter-pct');if(b)b.style.width=p+'%';if(pl)pl.textContent=Math.round(p)+'%';if(p===100){r.style.borderColor='rgba(0,242,96,0.3)';r.style.background='rgba(0,242,96,0.04)';if(pl){pl.textContent='✓ Done';pl.style.color='#00F260';}}else{r.style.borderColor='rgba(255,255,255,0.05)';r.style.background='rgba(255,255,255,0.02)';if(pl)pl.style.color='{{ meta.color_from }}';}}
function updateSub(p){const e=document.getElementById('subject-pct');const b=document.getElementById('subject-bar');if(e)e.textContent=p+'%';if(b)b.style.width=p+'%';}
function openAddChapter(){document.getElementById('addChapterModal').classList.add('active');}
function closeAddChapter(){document.getElementById('addChapterModal').classList.remove('active');}
async function deleteChapter(ci,cn){if(!IS_CUSTOM){showToast('Cannot delete default','#FF6B6B');return;}if(!confirm(`Delete "${cn}"?`))return;try{const r=await fetch('/delete_chapter',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({subject_id:SUBJECT_ID,chapter_index:ci})});const d=await r.json();if(d.success)location.reload();else showToast('Failed','#FF6B6B');}catch(e){showToast('Network error','#FF6B6B');}}
async function deleteSubject(){if(!confirm(`Delete "${SUBJECT}"?`))return;try{const r=await fetch('/delete_subject',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({subject_id:SUBJECT_ID})});const d=await r.json();if(d.success)window.location.href='/';else showToast('Failed','#FF6B6B');}catch(e){showToast('Network error','#FF6B6B');}}
</script>
</head><body>
<div class="scroll-content"><div class="max-w-2xl mx-auto px-4 pt-6 pb-4">
<div class="flex items-center gap-4 mb-6">
<a href="/" class="w-10 h-10 rounded-xl flex items-center justify-center" style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#8892A4" stroke-width="2.5" stroke-linecap="round"><path d="M19 12H5M12 5l-7 7 7 7"/></svg></a>
<div class="flex-1"><div class="flex items-center gap-2"><span class="text-2xl">{{ meta.icon }}</span><h1 class="text-xl font-black text-white">{{ subject }}</h1>{% if info.is_custom %}<span class="text-xs px-2 py-0.5 rounded-full font-bold" style="background:rgba(0,242,96,0.12);color:#00F260;">CUSTOM</span>{% endif %}</div></div>
</div>
<div class="card-base p-4 mb-5">
<div class="flex items-center justify-between mb-2"><span class="text-sm font-semibold text-white">Subject Sync</span><span id="subject-pct" class="text-lg font-black" style="color:{{ meta.color_from }};">{{ subject_pct }}%</span></div>
<div class="w-full rounded-full h-2" style="background:rgba(255,255,255,0.07);"><div id="subject-bar" class="h-2 rounded-full" style="width:{{ subject_pct }}%;background:linear-gradient(90deg,{{ meta.color_from }},{{ meta.color_to }});"></div></div>
</div>
<div class="flex gap-2 mb-4">
<button onclick="openAddChapter()" class="btn-secondary flex-1 flex items-center justify-center gap-1.5">+ Add Chapter</button>
{% if info.is_custom %}<button onclick="deleteSubject()" class="btn-secondary btn-danger px-4">🗑️</button>{% endif %}
</div>
<div class="space-y-2">
{% for chapter in chapters %}
{% set ci = loop.index0 %}
{% set fc = chapter_fill_counts[ci] %}
{% set cp = ((fc / 5) * 100) | int %}
<div class="chapter-row p-4" data-chapter="{{ ci }}" style="{% if cp == 100 %}border-color:rgba(0,242,96,0.3);background:rgba(0,242,96,0.04);{% endif %}">
<div class="flex items-start justify-between gap-2 mb-3">
<div class="flex items-start gap-2.5 flex-1 min-w-0"><span class="text-xs font-bold mt-0.5" style="color:{{ meta.color_from }};opacity:0.7;">{{ '%02d' % (ci+1) }}</span><span class="text-sm font-semibold text-white">{{ chapter }}</span></div>
<div class="flex items-center gap-2"><span class="chapter-pct text-xs font-bold" style="color:{% if cp == 100 %}#00F260{% else %}{{ meta.color_from }}{% endif %};">{% if cp == 100 %}✓ Done{% else %}{{ cp }}%{% endif %}</span>{% if info.is_custom %}<button onclick="deleteChapter({{ ci }},{{ chapter | tojson }})" class="text-muted text-xs">🗑️</button>{% endif %}</div>
</div>
<div class="flex items-center gap-3">
<div class="flex items-center gap-2 flex-1 flex-wrap">
{% for ci2 in range(5) %}
{% set is_f = progress_map.get(subject + '__' + ci|string + '__' + ci2|string, False) %}
<div class="circle-btn relative w-10 h-10 rounded-full flex items-center justify-center border" data-filled="{{ 'true' if is_f else 'false' }}" onclick="toggleCircle({{ ci }},{{ ci2 }},this)" style="{% if is_f %}background:linear-gradient(135deg,#00E1FD,#C533FF);border-color:transparent;{% else %}background:rgba(255,255,255,0.04);border-color:rgba(255,255,255,0.12);{% endif %}">
<span class="circle-check absolute text-white font-bold" style="font-size:14px;opacity:{% if is_f %}1{% else %}0{% endif %};">✓</span>
<span class="circle-label absolute text-center font-semibold" style="font-size:8px;color:#8892A4;opacity:{% if is_f %}0{% else %}1{% endif %};">{{ circle_labels[ci2] }}</span>
</div>
{% endfor %}
</div>
<div class="w-16"><div class="w-full rounded-full h-1" style="background:rgba(255,255,255,0.07);"><div class="chapter-bar h-1 rounded-full" style="width:{{ cp }}%;background:linear-gradient(90deg,{{ meta.color_from }},{{ meta.color_to }});"></div></div></div>
</div>
</div>
{% endfor %}
</div>
</div></div>
<div id="addChapterModal" class="modal-overlay">
<div class="modal-content p-6">
<div class="flex items-center justify-between mb-4"><h2 class="text-lg font-black text-white">Add Chapter</h2><button onclick="closeAddChapter()" class="w-8 h-8 rounded-lg" style="background:rgba(255,255,255,0.05);">✕</button></div>
<form method="POST" action="/add_chapter" class="space-y-4">
<input type="hidden" name="subject_name" value="{{ subject }}"/>
<div><label class="block text-xs font-semibold text-muted uppercase mb-2">Chapter Name</label><input type="text" name="name" required maxlength="200" class="input-field w-full px-4 py-3 text-sm" autofocus/></div>
<button type="submit" class="gradient-btn w-full py-3 rounded-xl text-white font-bold text-sm">➕ Add</button>
</form>
</div>
</div>
<script>document.getElementById('addChapterModal').addEventListener('click',e=>{if(e.target.id==='addChapterModal')closeAddChapter();});</script>
""" + presence_script() + """
{{ nav_html | safe }}
</body></html>
"""


CHAT_TEMPLATE = """
<!DOCTYPE html><html lang="en"><head><title>Chat</title>""" + BASE_STYLE + """
<script>
const CURRENT_USER_ID={{ user.id }};
const CURRENT_ROOM={{ current_room | tojson }};
let replyToId=null;
let socket=null;
let seenIds=new Set({{ initial_msg_ids | tojson }});
let pendingMessages=new Map();
let reconnectAttempts=0;

function esc(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function initials(n){const p=String(n||'?').trim().split(/\\s+/);if(p.length===1)return p[0].substring(0,2).toUpperCase();return(p[0][0]+p[p.length-1][0]).toUpperCase();}

function renderMessage(m,a=true,tempId=null){
  if(m.id && seenIds.has(m.id))return;
  if(m.id)seenIds.add(m.id);
  const isMine=m.user_id===CURRENT_USER_ID;
  const c=document.createElement('div');
  c.className='flex gap-2 mb-3 '+(a?'msg-in ':'')+(isMine?'flex-row-reverse':'');
  if(tempId)c.setAttribute('data-temp-id',tempId);
  if(m.id)c.setAttribute('data-msg-id',m.id);
  const av=document.createElement('div');
  av.className='avatar';
  av.style.background=`linear-gradient(135deg,${m.user_avatar_color||'#00E1FD'},#C533FF)`;
  av.textContent=initials(m.user_name);
  const bw=document.createElement('div');
  bw.className='flex flex-col '+(isMine?'items-end':'items-start');
  bw.style.maxWidth='calc(100% - 50px)';
  const mt=document.createElement('div');
  mt.className='text-xs text-muted mb-1 px-1 flex items-center gap-1.5';
  mt.innerHTML=isMine?`<span>${esc(m.time_ago||'sending...')}</span><span class="font-semibold" style="color:#00E1FD">You</span>`:`<span class="font-semibold text-white">${esc(m.user_name)}</span><span class="text-[10px] px-1.5 py-0.5 rounded-full" style="background:rgba(0,225,253,0.1);color:#00E1FD;">${esc(m.user_class)}</span><span>${esc(m.time_ago)}</span>`;
  const b=document.createElement('div');
  b.className='chat-bubble '+(isMine?'mine':'theirs')+(tempId?' pending':'');
  let rh='';
  if(m.reply_to)rh=`<div class="reply-preview" style="margin-bottom:6px;"><div><span style="color:#00E1FD;font-weight:700;">↳ ${esc(m.reply_to.user_name)}:</span> <span style="color:#8892A4;">${esc(m.reply_to.message.substring(0,60))}</span></div></div>`;
  b.innerHTML=rh+esc(m.message);
  b.style.cursor='pointer';
  b.addEventListener('click',()=>setReply(m.id,m.user_name,m.message));
  bw.appendChild(mt);bw.appendChild(b);c.appendChild(av);c.appendChild(bw);
  const s=document.getElementById('chatScroll');
  const e=document.getElementById('emptyState');
  if(e)e.remove();
  s.appendChild(c);
}

function replaceTempMessage(tempId,realMsg){
  const el=document.querySelector(`[data-temp-id="${tempId}"]`);
  if(el){
    el.remove();
    pendingMessages.delete(tempId);
  }
  renderMessage(realMsg);
  scrollBot();
}

function markMessageFailed(tempId){
  const el=document.querySelector(`[data-temp-id="${tempId}"]`);
  if(el){
    const b=el.querySelector('.chat-bubble');
    if(b){b.classList.remove('pending');b.classList.add('failed');}
    const mt=el.querySelector('.text-xs');
    if(mt)mt.innerHTML='<span style="color:#FF6B6B;">⚠️ Failed - tap to retry</span>';
    el.style.cursor='pointer';
    el.onclick=()=>{
      const pm=pendingMessages.get(tempId);
      if(pm){
        el.remove();
        pendingMessages.delete(tempId);
        sendMessageInternal(pm.text,pm.replyTo);
      }
    };
  }
}

function setReply(id,un,msg){
  if(!id)return;
  replyToId=id;
  const p=document.getElementById('replyPreview');
  p.style.display='flex';
  p.querySelector('.reply-text').innerHTML=`<span style="color:#00E1FD;font-weight:700;">↳ ${esc(un)}:</span> <span style="color:#8892A4;">${esc(msg.substring(0,60))}</span>`;
  document.getElementById('msgInput').focus();
}

function cancelReply(){
  replyToId=null;
  document.getElementById('replyPreview').style.display='none';
}

function sendMessage(){
  const i=document.getElementById('msgInput');
  const t=i.value.trim();
  if(!t)return;
  i.value='';
  sendMessageInternal(t,replyToId);
  cancelReply();
  i.focus();
}

function sendMessageInternal(text,replyTo){
  const tempId='temp_'+Date.now()+'_'+Math.random();
  const tempMsg={id:null,user_id:CURRENT_USER_ID,user_name:'You',user_class:'',user_avatar_color:'#00E1FD',message:text,time_ago:'sending...',reply_to:null,room:CURRENT_ROOM};
  pendingMessages.set(tempId,{text:text,replyTo:replyTo,timestamp:Date.now()});
  renderMessage(tempMsg,true,tempId);
  scrollBot();
  
  if(!socket||!socket.connected){
    // Try HTTP fallback
    sendViaHTTP(text,replyTo,tempId);
    return;
  }
  
  const timeout=setTimeout(()=>{
    sendViaHTTP(text,replyTo,tempId);
  },5000);
  
  socket.emit('send_message',{room:CURRENT_ROOM,message:text,reply_to_id:replyTo,temp_id:tempId},(ack)=>{
    clearTimeout(timeout);
    if(ack&&ack.success&&ack.message){
      replaceTempMessage(tempId,ack.message);
    }else if(ack&&!ack.success){
      sendViaHTTP(text,replyTo,tempId);
    }
  });
}

async function sendViaHTTP(text,replyTo,tempId){
  try{
    const r=await fetch('/api/send_message',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({room:CURRENT_ROOM,message:text,reply_to_id:replyTo})});
    if(!r.ok)throw new Error('HTTP '+r.status);
    const d=await r.json();
    if(d.success&&d.message){
      replaceTempMessage(tempId,d.message);
    }else{
      markMessageFailed(tempId);
    }
  }catch(e){
    markMessageFailed(tempId);
  }
}

function scrollBot(){const s=document.getElementById('chatScroll');s.scrollTop=s.scrollHeight;}
function isNB(){const s=document.getElementById('chatScroll');return s.scrollHeight-s.scrollTop-s.clientHeight<120;}

function updateStatus(s){
  const d=document.getElementById('connStatus');
  const l=document.getElementById('connLabel');
  if(!d)return;
  d.className='status-dot '+s;
  if(l)l.textContent=s==='connected'?'Live':(s==='connecting'?'Connecting...':'Offline (using HTTP)');
}

document.addEventListener('DOMContentLoaded',()=>{
  const init={{ messages | tojson }};
  init.forEach(m=>renderMessage(m,false));
  scrollBot();
  updateStatus('connecting');
  
  socket=io({
    transports:['websocket','polling'],
    reconnection:true,
    reconnectionDelay:1000,
    reconnectionDelayMax:5000,
    reconnectionAttempts:Infinity,
    timeout:20000
  });
  
  socket.on('connect',()=>{
    updateStatus('connected');
    reconnectAttempts=0;
    socket.emit('join_chat_room',{room:CURRENT_ROOM});
  });
  socket.on('disconnect',()=>updateStatus('disconnected'));
  socket.on('connect_error',()=>{updateStatus('disconnected');reconnectAttempts++;});
  socket.on('reconnecting',()=>updateStatus('connecting'));
  
  socket.on('new_message',(m)=>{
    if(m.room!==CURRENT_ROOM)return;
    const wb=isNB();
    renderMessage(m);
    if(wb||m.user_id===CURRENT_USER_ID)scrollBot();
  });
  
  socket.on('presence_update',(d)=>{
    document.querySelectorAll('.js-online-count').forEach(el=>el.textContent=d.online);
    if(d.room_online!==undefined){const r=document.getElementById('roomOnlineCount');if(r)r.textContent=d.room_online;}
  });
  
  const inp=document.getElementById('msgInput');
  inp.addEventListener('keydown',e=>{
    if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendMessage();}
  });
  
  // Auto-refresh chat every 30 sec as backup
  setInterval(async()=>{
    if(socket&&socket.connected)return;
    try{
      const r=await fetch('/api/chat_messages?room='+encodeURIComponent(CURRENT_ROOM)+'&after='+(Math.max(...Array.from(seenIds),0)));
      const d=await r.json();
      if(d.messages){
        const wb=isNB();
        d.messages.forEach(m=>renderMessage(m));
        if(wb)scrollBot();
      }
    }catch(e){}
  },30000);
});
</script>
</head><body>
<div><div class="max-w-2xl mx-auto">
<div class="px-4 pt-6 pb-3">
<div class="flex items-center gap-3 mb-4">
<a href="/" class="w-10 h-10 rounded-xl flex items-center justify-center" style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#8892A4" stroke-width="2.5" stroke-linecap="round"><path d="M19 12H5M12 5l-7 7 7 7"/></svg></a>
<div class="flex-1">
<div class="flex items-center gap-2"><span class="text-2xl">💬</span><h1 class="text-xl font-black text-white">Live Chat</h1>
<div class="flex items-center gap-1.5 ml-1"><span id="connStatus" class="status-dot connecting"></span><span id="connLabel" class="text-xs text-muted">...</span></div></div>
<div class="flex items-center gap-2 mt-0.5 text-xs">
<span class="inline-block w-2 h-2 rounded-full online-pulse" style="background:#00F260;"></span>
<span style="color:#00F260;"><span class="js-online-count">{{ online_count }}</span> online</span>
<span class="text-muted">·</span><span class="text-muted"><span id="roomOnlineCount">{{ room_online }}</span> here</span>
</div>
</div>
</div>
<div class="flex gap-2 overflow-x-auto pb-2" style="scrollbar-width:none;">
<style>.tabs::-webkit-scrollbar{display:none}</style>
<div class="tabs flex gap-2">
{% for room in chat_rooms %}
<a href="/chat?room={{ room.id }}" class="room-pill flex items-center gap-1.5 px-3 py-2 rounded-full text-xs font-semibold whitespace-nowrap {% if room.id == current_room %}active{% endif %}" style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);color:#8892A4;">
<span>{{ room.icon }}</span><span>{{ room.name }}</span>
</a>
{% endfor %}
</div>
</div>
</div>
<div id="chatScroll" class="chat-scroll">
{% if messages | length == 0 %}
<div class="flex flex-col items-center justify-center h-full text-center px-6" id="emptyState">
<div class="text-5xl mb-3">👋</div><h3 class="text-base font-bold text-white mb-1">Be the first to say hi!</h3>
</div>
{% endif %}
</div>
</div></div>
<div class="chat-input-bar">
<div class="max-w-2xl mx-auto">
<div id="replyPreview" class="reply-preview" style="display:none;">
<div class="reply-text flex-1 min-w-0"></div>
<button onclick="cancelReply()" class="text-muted ml-2" style="font-size:16px;">✕</button>
</div>
<div class="flex items-end gap-2">
<textarea id="msgInput" placeholder="Type a message..." rows="1" maxlength="1000" class="input-field flex-1 px-4 py-3 text-sm resize-none" style="max-height:100px;"></textarea>
<button onclick="sendMessage()" class="gradient-btn w-11 h-11 rounded-xl text-white flex items-center justify-center">
<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
</button>
</div>
</div>
</div>
{{ nav_html | safe }}
</body></html>
"""


EXAM_ZONE_TEMPLATE = """
<!DOCTYPE html><html lang="en"><head><title>Exam Zone</title>""" + BASE_STYLE + """
<script>
async function deleteExam(id){if(!confirm('Remove?'))return;try{const r=await fetch('/delete_exam',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({exam_id:id})});const d=await r.json();if(d.success)location.reload();else showToast('Failed','#FF6B6B');}catch(e){showToast('Network error','#FF6B6B');}}
function openSchedule(){document.getElementById('scheduleModal').classList.add('active');}
function closeSchedule(){document.getElementById('scheduleModal').classList.remove('active');}
</script>
</head><body>
<div class="scroll-content"><div class="max-w-2xl mx-auto px-4 pt-8 pb-4">
<div class="flex items-center gap-4 mb-6">
<a href="/" class="w-10 h-10 rounded-xl flex items-center justify-center" style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#8892A4" stroke-width="2.5" stroke-linecap="round"><path d="M19 12H5M12 5l-7 7 7 7"/></svg></a>
<div class="flex-1"><div class="flex items-center gap-2"><span class="text-2xl">📅</span><h1 class="text-2xl font-black text-white">Exam Zone</h1></div></div>
</div>
<div class="grid grid-cols-3 gap-3 mb-4">
<div class="card-base p-4 flex flex-col items-center text-center" style="border-radius:14px;"><div class="text-2xl mb-1">📆</div><div class="text-xl font-black text-white">{{ exam_plans | length }}</div><div class="text-xs text-muted">Exams</div></div>
<div class="card-base p-4 flex flex-col items-center text-center" style="border-radius:14px;"><div class="text-2xl mb-1">⏳</div><div class="text-xl font-black" style="color:#FF6B6B;">{% if nearest_days is not none %}{{ nearest_days }}{% else %}—{% endif %}</div><div class="text-xs text-muted">Days</div></div>
<div class="card-base p-4 flex flex-col items-center text-center" style="border-radius:14px;"><div class="text-2xl mb-1">🎯</div><div class="text-xl font-black gradient-text">{{ today_target }}</div><div class="text-xs text-muted">Today</div></div>
</div>
<button onclick="openSchedule()" class="gradient-btn w-full py-3.5 rounded-xl text-white font-bold text-sm mb-5">+ Schedule Exam</button>
{% if exam_plans | length == 0 %}
<div class="card-base p-8 text-center"><div class="text-5xl mb-3">🗓️</div><h3 class="text-lg font-black text-white mb-1">No Exams</h3><button onclick="openSchedule()" class="gradient-btn px-6 py-3 rounded-xl text-white font-bold text-sm">📅 Schedule First</button></div>
{% else %}
<div class="space-y-3">
{% for plan in exam_plans %}
{% set meta = plan.meta %}
<div class="card-base exam-card">
<div class="flex items-start justify-between gap-3 mb-3">
<div class="flex items-start gap-3 flex-1 min-w-0">
<div class="w-11 h-11 rounded-xl flex items-center justify-center text-xl" style="background:linear-gradient(135deg,{{ meta.color_from }}22,{{ meta.color_to }}22);border:1px solid {{ meta.color_from }}33;">{{ meta.icon }}</div>
<div class="flex-1 min-w-0"><h3 class="text-base font-black text-white truncate">{{ plan.subject }}</h3><div class="text-xs text-muted">📅 {{ plan.exam_day }}, {{ plan.exam_date_str }}</div></div>
</div>
<div class="text-right"><span class="countdown-badge" style="background:rgba(255,60,60,0.15);color:#FF6B6B;">{{ plan.days_left }}d</span><button onclick="deleteExam({{ plan.id }})" class="text-xs text-muted mt-1 block ml-auto">🗑️</button></div>
</div>
<div class="mb-3"><div class="w-full rounded-full h-2" style="background:rgba(255,255,255,0.07);"><div class="h-2 rounded-full" style="width:{{ plan.progress_pct }}%;background:linear-gradient(90deg,{{ meta.color_from }},{{ meta.color_to }});"></div></div></div>
{% if plan.status != 'past' and plan.feasibility != 'complete' %}
<div class="grid grid-cols-3 gap-2">
<div class="rounded-xl p-3 text-center" style="background:rgba(0,225,253,0.08);border:1px solid rgba(0,225,253,0.2);"><div class="text-xl font-black" style="color:#00E1FD;">{{ plan.per_day_units }}</div><div class="text-muted" style="font-size:10px;">Units/day</div></div>
<div class="rounded-xl p-3 text-center" style="background:rgba(197,51,255,0.08);"><div class="text-xl font-black" style="color:#C533FF;">{{ plan.per_day_chapters }}</div><div class="text-muted" style="font-size:10px;">Ch/day</div></div>
<div class="rounded-xl p-3 text-center" style="background:rgba(0,242,96,0.08);"><div class="text-xl font-black" style="color:#00F260;">{{ plan.per_week_chapters }}</div><div class="text-muted" style="font-size:10px;">Ch/week</div></div>
</div>
{% endif %}
<a href="/subject/{{ plan.subject | urlencode }}" class="block mt-3 text-center py-2 rounded-lg text-xs font-bold" style="background:rgba(255,255,255,0.04);color:{{ meta.color_from }};">Open →</a>
</div>
{% endfor %}
</div>
{% endif %}
</div></div>
<div id="scheduleModal" class="modal-overlay">
<div class="modal-content p-6">
<div class="flex items-center justify-between mb-4"><h2 class="text-lg font-black text-white">Schedule Exam</h2><button onclick="closeSchedule()" class="w-8 h-8 rounded-lg" style="background:rgba(255,255,255,0.05);">✕</button></div>
<form method="POST" action="/add_exam" class="space-y-4">
<div><label class="block text-xs font-semibold text-muted uppercase mb-2">Subject</label><select name="subject" required class="input-field w-full px-4 py-3 text-sm"><option value="">— Choose —</option>{% for subject in all_subjects %}<option value="{{ subject }}">{{ subject }}</option>{% endfor %}</select></div>
<div><label class="block text-xs font-semibold text-muted uppercase mb-2">Date</label><input type="date" name="exam_date" required min="{{ today_iso }}" class="input-field w-full px-4 py-3 text-sm"/></div>
<button type="submit" class="gradient-btn w-full py-3 rounded-xl text-white font-bold text-sm">📌 Schedule</button>
</form>
</div>
</div>
<script>document.getElementById('scheduleModal').addEventListener('click',e=>{if(e.target.id==='scheduleModal')closeSchedule();});</script>
""" + presence_script() + """
{{ nav_html | safe }}
</body></html>
"""


ANALYTICS_TEMPLATE = """
<!DOCTYPE html><html lang="en"><head><title>Analytics</title>""" + BASE_STYLE + """</head><body>
<div class="scroll-content"><div class="max-w-2xl mx-auto px-4 pt-8 pb-4">
<div class="flex items-center gap-4 mb-6">
<a href="/" class="w-10 h-10 rounded-xl flex items-center justify-center" style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#8892A4" stroke-width="2.5" stroke-linecap="round"><path d="M19 12H5M12 5l-7 7 7 7"/></svg></a>
<div><h1 class="text-2xl font-black text-white">Analytics</h1></div>
</div>
<div class="card-base p-5 mb-4">
<h2 class="text-sm font-bold text-white mb-3">7-Day Activity</h2>
<div class="flex items-end justify-between gap-2" style="height:150px;">
{% for day in week_data %}
{% set bh = (day.count / week_max * 100) if week_max > 0 else 0 %}
<div class="flex flex-col items-center flex-1 gap-1.5">
<div class="w-full flex flex-col justify-end items-center" style="height:115px;">
{% if day.count > 0 %}<span class="text-xs font-bold mb-1" style="color:{% if day.is_today %}#00E1FD{% else %}#E2E8F0{% endif %};">{{ day.count }}</span>{% endif %}
<div class="bar-chart-bar w-full rounded-t-lg" style="height:{{ bh if bh >= 6 else (6 if day.count > 0 else 3) }}%;background:{% if day.is_today %}linear-gradient(180deg,#00E1FD,#C533FF){% elif day.count > 0 %}linear-gradient(180deg,rgba(0,225,253,0.6),rgba(197,51,255,0.4)){% else %}rgba(255,255,255,0.06){% endif %};"></div>
</div>
<div class="text-center"><div class="text-xs font-semibold" style="color:{% if day.is_today %}#00E1FD{% else %}#8892A4{% endif %};">{{ day.day_label }}</div></div>
</div>
{% endfor %}
</div>
</div>
<div class="card-base p-5 mb-4">
<h2 class="text-sm font-bold text-white mb-3">Subject Performance</h2>
{% for subject, info in curriculum.items() %}
{% set meta = info.meta %}
<div class="mb-3">
<div class="flex items-center gap-3 mb-2"><span class="text-lg">{{ meta.icon }}</span><span class="text-sm font-semibold text-white flex-1">{{ subject }}</span><span class="text-sm font-black" style="color:{{ meta.color_from }};">{{ stats.subject_pcts[subject] }}%</span></div>
<div class="w-full rounded-full h-2" style="background:rgba(255,255,255,0.07);"><div class="h-2 rounded-full" style="width:{{ stats.subject_pcts[subject] }}%;background:linear-gradient(90deg,{{ meta.color_from }},{{ meta.color_to }});"></div></div>
</div>
{% endfor %}
</div>
</div></div>
""" + presence_script() + """
{{ nav_html | safe }}
</body></html>
"""


SETTINGS_TEMPLATE = """
<!DOCTYPE html><html lang="en"><head><title>Settings</title>""" + BASE_STYLE + """</head><body>
<div class="scroll-content"><div class="max-w-2xl mx-auto px-4 pt-8 pb-4">
<div class="flex items-center gap-4 mb-6">
<a href="/" class="w-10 h-10 rounded-xl flex items-center justify-center" style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#8892A4" stroke-width="2.5" stroke-linecap="round"><path d="M19 12H5M12 5l-7 7 7 7"/></svg></a>
<div><h1 class="text-2xl font-black text-white">Settings</h1></div>
</div>
<div class="card-base p-6 mb-4">
<div class="flex items-center gap-4 mb-4"><div class="avatar" style="width:56px;height:56px;font-size:18px;background:linear-gradient(135deg,{{ user.avatar_color }},#C533FF);">{{ user.name | initials }}</div>
<div><h2 class="text-lg font-black text-white">{{ user.name }}</h2><p class="text-sm text-muted">{{ user.student_class }}</p></div>
</div>
</div>
<div class="card-base p-5 mb-4">
<h3 class="text-sm font-bold text-white mb-3">Avatar Color</h3>
<form method="POST" action="/update_avatar" class="flex items-center gap-2 flex-wrap">
{% for c in avatar_colors %}<button type="submit" name="color" value="{{ c }}" class="w-10 h-10 rounded-full {% if user.avatar_color == c %}ring-4 ring-white{% endif %}" style="background:linear-gradient(135deg,{{ c }},#C533FF);"></button>{% endfor %}
</form>
</div>
<div class="card-base p-5 mb-4">
<h3 class="text-sm font-bold text-white mb-4">Update Profile</h3>
{% if msg %}<div class="mb-3 p-3 rounded-xl text-sm" style="background:rgba(0,242,96,0.1);color:#00F260;">✅ {{ msg }}</div>{% endif %}
<form method="POST" action="/settings" class="space-y-3">
<div><label class="block text-xs font-semibold text-muted uppercase mb-1.5">Name</label><input type="text" name="name" value="{{ user.name }}" required maxlength="100" class="input-field w-full px-4 py-3 text-sm"/></div>
<div><label class="block text-xs font-semibold text-muted uppercase mb-1.5">Class</label><input type="text" name="student_class" value="{{ user.student_class }}" required maxlength="50" class="input-field w-full px-4 py-3 text-sm"/></div>
<button type="submit" class="gradient-btn w-full py-3 rounded-xl text-white font-bold text-sm">Save</button>
</form>
</div>
<div class="card-base p-5" style="border-color:rgba(255,100,100,0.15);">
<h3 class="text-sm font-bold mb-1" style="color:#FF6B6B;">Danger Zone</h3>
<form method="POST" action="/reset" onsubmit="return confirm('Delete all data?');">
<button type="submit" class="w-full py-3 rounded-xl text-sm font-bold" style="background:rgba(255,100,100,0.08);border:1px solid rgba(255,100,100,0.3);color:#FF6B6B;">🗑️ Reset All Data</button>
</form>
</div>
</div></div>
""" + presence_script() + """
{{ nav_html | safe }}
</body></html>
"""


# ═════════════════════════════════════════════
# PWA ROUTES
# ═════════════════════════════════════════════

@app.route('/manifest.json')
def manifest():
    return jsonify({
        "name": "StudyTracker","short_name": "StudyTracker",
        "start_url": "/","display": "standalone","orientation": "portrait",
        "background_color": "#0B0F19","theme_color": "#0B0F19",
        "icons": [{"src": "/icon-192.png", "sizes": "192x192", "type": "image/svg+xml"}]
    })


@app.route('/service-worker.js')
def sw():
    return Response("""
self.addEventListener('install',e=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(clients.claim()));
self.addEventListener('fetch',e=>{if(e.request.method!=='GET')return;e.respondWith(fetch(e.request).catch(()=>caches.match(e.request)));});
""", mimetype='application/javascript')


@app.route('/icon-192.png')
def icon():
    svg = '<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg" width="192" height="192" viewBox="0 0 512 512"><defs><linearGradient id="g" x1="0%" y1="0%" x2="100%" y2="100%"><stop offset="0%" style="stop-color:#00E1FD"/><stop offset="100%" style="stop-color:#C533FF"/></linearGradient></defs><rect width="512" height="512" rx="90" fill="#0B0F19"/><rect x="40" y="40" width="432" height="432" rx="70" fill="url(#g)" opacity="0.2"/><text x="256" y="340" font-size="260" text-anchor="middle" font-family="Arial" font-weight="900">🎯</text></svg>'
    return Response(svg, mimetype='image/svg+xml')


# ═════════════════════════════════════════════
# HTTP ROUTES
# ═════════════════════════════════════════════

@app.before_request
def before_request():
    """Handle DB reconnection before each request."""
    try:
        db.session.execute(text('SELECT 1'))
    except Exception:
        db.session.rollback()


@app.route('/')
def index():
    user = get_current_user()
    if not user:
        return redirect(url_for('onboarding'))
    touch_user(user); update_streak(user)
    stats = compute_stats(user.id)
    curriculum = get_full_curriculum(user.id)
    circ = 251.33
    ring_dash = round((stats['overall_pct'] / 100) * circ, 2)
    ring_gap = round(circ - ring_dash, 2)
    week_data = get_7day_data(user.id)
    week_total = sum(d['count'] for d in week_data)
    week_max = max((d['count'] for d in week_data), default=0)
    exam_plans = compute_exam_plans(user.id)
    nearest_exam = None
    for p in exam_plans:
        if p['status'] in ('upcoming', 'today'):
            nearest_exam = p; break
    return render_template_string(
        DASHBOARD_TEMPLATE, user=user, stats=stats, curriculum=curriculum,
        ring_dash=ring_dash, ring_gap=ring_gap,
        week_data=week_data, week_total=week_total, week_max=week_max,
        icon_presets=ICON_PRESETS, color_presets=COLOR_PRESETS,
        nearest_exam=nearest_exam, online_count=get_live_online_count(),
        total_users=get_total_user_count(), nav_html=bottom_nav_html("hub")
    )


@app.route('/onboarding')
def onboarding():
    if get_current_user():
        return redirect(url_for('index'))
    return render_template_string(ONBOARDING_TEMPLATE, error=None,
        online_count=get_live_online_count(), total_users=get_total_user_count())


@app.route('/setup', methods=['POST'])
def setup():
    name = request.form.get('name', '').strip()
    student_class = request.form.get('student_class', '').strip()
    if not name or not student_class:
        return render_template_string(ONBOARDING_TEMPLATE, error="Please fill all fields.",
            online_count=get_live_online_count(), total_users=get_total_user_count())
    color = AVATAR_COLORS[len(name) % len(AVATAR_COLORS)]
    user = User(name=name[:100], student_class=student_class[:50], streak=1,
                last_active=date.today(), created_at=datetime.utcnow(),
                last_seen=datetime.utcnow(), avatar_color=color)
    db.session.add(user)
    if not safe_commit():
        return render_template_string(ONBOARDING_TEMPLATE, error="Setup failed. Please try again.",
            online_count=get_live_online_count(), total_users=get_total_user_count())
    session['user_id'] = user.id
    session.permanent = True
    return redirect(url_for('index'))


@app.route('/subject/<path:subject_name>')
def subject_page(subject_name):
    user = get_current_user()
    if not user:
        return redirect(url_for('onboarding'))
    touch_user(user)
    curriculum = get_full_curriculum(user.id)
    if subject_name not in curriculum:
        return redirect(url_for('index'))
    info = curriculum[subject_name]
    chapters = info['chapters']
    meta = info['meta']
    progress_map = get_user_progress(user.id)
    chapter_fill_counts = {}
    for ci in range(len(chapters)):
        chapter_fill_counts[ci] = sum(1 for ci2 in range(CIRCLES_PER_CHAPTER)
            if progress_map.get(f"{subject_name}__{ci}__{ci2}", False))
    subj_total = len(chapters) * CIRCLES_PER_CHAPTER
    subj_done = sum(chapter_fill_counts.values())
    subject_pct = round((subj_done / subj_total) * 100, 1) if subj_total else 0
    return render_template_string(SUBJECT_TEMPLATE, user=user, subject=subject_name,
        chapters=chapters, meta=meta, info=info, progress_map=progress_map,
        circle_labels=CIRCLE_LABELS, chapter_fill_counts=chapter_fill_counts,
        subject_pct=subject_pct, nav_html=bottom_nav_html("hub"))


@app.route('/add_subject', methods=['POST'])
def add_subject():
    user = get_current_user()
    if not user:
        return redirect(url_for('onboarding'))
    name = request.form.get('name', '').strip()
    if not name:
        return redirect(url_for('index'))
    curriculum = get_full_curriculum(user.id)
    if name in curriculum:
        return redirect(url_for('index'))
    cs = CustomSubject(user_id=user.id, name=name[:100],
        icon=request.form.get('icon', '📘')[:10],
        color_from=request.form.get('color_from', '#00E1FD')[:20],
        color_to=request.form.get('color_to', '#C533FF')[:20],
        tag=(request.form.get('tag', '').strip() or 'CUSTOM')[:50])
    db.session.add(cs)
    safe_commit()
    return redirect(url_for('subject_page', subject_name=cs.name))


@app.route('/add_chapter', methods=['POST'])
def add_chapter():
    user = get_current_user()
    if not user:
        return redirect(url_for('onboarding'))
    subject_name = request.form.get('subject_name', '').strip()
    name = request.form.get('name', '').strip()
    if not subject_name or not name:
        return redirect(url_for('index'))
    cs = CustomSubject.query.filter_by(user_id=user.id, name=subject_name).first()
    if not cs and subject_name in DEFAULT_CURRICULUM:
        meta = DEFAULT_SUBJECT_META[subject_name]
        cs = CustomSubject(user_id=user.id, name=subject_name, icon=meta['icon'],
            color_from=meta['color_from'], color_to=meta['color_to'], tag=meta['tag'])
        db.session.add(cs)
        try:
            db.session.flush()
            for i, ch in enumerate(DEFAULT_CURRICULUM[subject_name]):
                db.session.add(CustomChapter(subject_id=cs.id, name=ch, order_index=i))
            safe_commit()
        except Exception:
            db.session.rollback()
    if not cs:
        return redirect(url_for('index'))
    max_order = db.session.query(func.max(CustomChapter.order_index)).filter_by(subject_id=cs.id).scalar() or 0
    db.session.add(CustomChapter(subject_id=cs.id, name=name[:200], order_index=max_order + 1))
    safe_commit()
    return redirect(url_for('subject_page', subject_name=cs.name))


@app.route('/delete_chapter', methods=['POST'])
def delete_chapter():
    user = get_current_user()
    if not user:
        return jsonify({'success': False}), 401
    data = request.get_json(silent=True) or {}
    try:
        subject_id = int(data.get('subject_id'))
        chapter_index = int(data.get('chapter_index'))
    except:
        return jsonify({'success': False}), 400
    cs = CustomSubject.query.filter_by(id=subject_id, user_id=user.id).first()
    if not cs:
        return jsonify({'success': False}), 404
    chapters = CustomChapter.query.filter_by(subject_id=cs.id).order_by(CustomChapter.order_index).all()
    if not (0 <= chapter_index < len(chapters)):
        return jsonify({'success': False}), 400
    ChapterProgress.query.filter_by(user_id=user.id, subject=cs.name, chapter_index=chapter_index).delete()
    for p in ChapterProgress.query.filter(ChapterProgress.user_id == user.id,
        ChapterProgress.subject == cs.name, ChapterProgress.chapter_index > chapter_index).all():
        p.chapter_index -= 1
    db.session.delete(chapters[chapter_index])
    for i, ch in enumerate([c for j, c in enumerate(chapters) if j != chapter_index]):
        ch.order_index = i
    if not safe_commit():
        return jsonify({'success': False}), 500
    return jsonify({'success': True})


@app.route('/delete_subject', methods=['POST'])
def delete_subject():
    user = get_current_user()
    if not user:
        return jsonify({'success': False}), 401
    data = request.get_json(silent=True) or {}
    try:
        subject_id = int(data.get('subject_id'))
    except:
        return jsonify({'success': False}), 400
    cs = CustomSubject.query.filter_by(id=subject_id, user_id=user.id).first()
    if not cs:
        return jsonify({'success': False}), 404
    ExamSchedule.query.filter_by(user_id=user.id, subject=cs.name).delete()
    ChapterProgress.query.filter_by(user_id=user.id, subject=cs.name).delete()
    CustomChapter.query.filter_by(subject_id=cs.id).delete()
    db.session.delete(cs)
    if not safe_commit():
        return jsonify({'success': False}), 500
    return jsonify({'success': True})


@app.route('/update_progress', methods=['POST'])
def update_progress():
    user = get_current_user()
    if not user:
        return jsonify({'success': False}), 401
    touch_user(user)
    data = request.get_json(silent=True) or {}
    subject = data.get('subject')
    try:
        chapter_index = int(data.get('chapter_index'))
        circle_index = int(data.get('circle_index'))
    except:
        return jsonify({'success': False}), 400
    completed = bool(data.get('completed', False))
    curriculum = get_full_curriculum(user.id)
    if subject not in curriculum:
        return jsonify({'success': False}), 400
    if not (0 <= chapter_index < len(curriculum[subject]['chapters'])):
        return jsonify({'success': False}), 400
    if not (0 <= circle_index < CIRCLES_PER_CHAPTER):
        return jsonify({'success': False}), 400
    existing = ChapterProgress.query.filter_by(user_id=user.id, subject=subject,
        chapter_index=chapter_index, circle_index=circle_index).first()
    delta = 0
    if existing:
        prev = existing.completed
        existing.completed = completed
        existing.updated_at = datetime.utcnow()
        if not prev and completed: delta = 1
        elif prev and not completed: delta = -1
    else:
        db.session.add(ChapterProgress(user_id=user.id, subject=subject,
            chapter_index=chapter_index, circle_index=circle_index, completed=completed))
        if completed: delta = 1
    if not safe_commit():
        return jsonify({'success': False}), 500
    if delta != 0:
        log_activity(user.id, delta)
    chapters = curriculum[subject]['chapters']
    subj_total = len(chapters) * CIRCLES_PER_CHAPTER
    subj_done = ChapterProgress.query.filter_by(user_id=user.id, subject=subject, completed=True).count()
    subject_pct = round((subj_done / subj_total) * 100, 1) if subj_total else 0
    return jsonify({'success': True, 'subject_pct': subject_pct})


@app.route('/exam-zone')
def exam_zone():
    user = get_current_user()
    if not user:
        return redirect(url_for('onboarding'))
    touch_user(user); update_streak(user)
    exam_plans = compute_exam_plans(user.id)
    curriculum = get_full_curriculum(user.id)
    scheduled = {p['subject'] for p in exam_plans}
    all_subjects = [s for s in curriculum.keys() if s not in scheduled]
    today_target = sum(p['per_day_units'] for p in exam_plans if p['status'] in ('upcoming', 'today'))
    nearest_days = None
    for p in exam_plans:
        if p['status'] in ('upcoming', 'today'):
            nearest_days = p['days_left']; break
    return render_template_string(EXAM_ZONE_TEMPLATE, user=user, exam_plans=exam_plans,
        all_subjects=all_subjects, today_iso=date.today().isoformat(),
        today_target=today_target, nearest_days=nearest_days, nav_html=bottom_nav_html("exam"))


@app.route('/add_exam', methods=['POST'])
def add_exam():
    user = get_current_user()
    if not user:
        return redirect(url_for('onboarding'))
    subject = request.form.get('subject', '').strip()
    exam_date_str = request.form.get('exam_date', '').strip()
    if not subject or not exam_date_str:
        return redirect(url_for('exam_zone'))
    try:
        exam_dt = datetime.strptime(exam_date_str, '%Y-%m-%d').date()
    except:
        return redirect(url_for('exam_zone'))
    curriculum = get_full_curriculum(user.id)
    if subject not in curriculum:
        return redirect(url_for('exam_zone'))
    existing = ExamSchedule.query.filter_by(user_id=user.id, subject=subject).first()
    if existing:
        existing.exam_date = exam_dt
    else:
        db.session.add(ExamSchedule(user_id=user.id, subject=subject, exam_date=exam_dt))
    safe_commit()
    return redirect(url_for('exam_zone'))


@app.route('/delete_exam', methods=['POST'])
def delete_exam():
    user = get_current_user()
    if not user:
        return jsonify({'success': False}), 401
    data = request.get_json(silent=True) or {}
    try:
        exam_id = int(data.get('exam_id'))
    except:
        return jsonify({'success': False}), 400
    exam = ExamSchedule.query.filter_by(id=exam_id, user_id=user.id).first()
    if not exam:
        return jsonify({'success': False}), 404
    db.session.delete(exam)
    if not safe_commit():
        return jsonify({'success': False}), 500
    return jsonify({'success': True})


@app.route('/chat')
def chat():
    user = get_current_user()
    if not user:
        return redirect(url_for('onboarding'))
    touch_user(user); update_streak(user)
    room_id = request.args.get('room', 'general')
    if not any(r['id'] == room_id for r in CHAT_ROOMS):
        room_id = 'general'
    try:
        rows = ChatMessage.query.filter_by(room=room_id).order_by(ChatMessage.id.desc()).limit(50).all()
        rows.reverse()
    except Exception:
        db.session.rollback()
        rows = []
    messages = []
    for m in rows:
        msg = {"id": m.id, "user_id": m.user_id, "user_name": m.user_name,
            "user_class": m.user_class, "user_avatar_color": m.user_avatar_color or '#00E1FD',
            "message": m.message, "time_ago": time_ago(m.created_at), "reply_to": None, "room": m.room}
        if m.reply_to_id:
            try:
                parent = db.session.get(ChatMessage, m.reply_to_id)
                if parent:
                    msg["reply_to"] = {"id": parent.id, "user_name": parent.user_name, "message": parent.message}
            except Exception:
                db.session.rollback()
        messages.append(msg)
    initial_msg_ids = [m['id'] for m in messages]
    return render_template_string(CHAT_TEMPLATE, user=user, messages=messages,
        initial_msg_ids=initial_msg_ids, chat_rooms=CHAT_ROOMS, current_room=room_id,
        online_count=get_live_online_count(), room_online=get_room_online_count(room_id),
        nav_html=bottom_nav_html("chat"))


# HTTP fallback for chat when WebSocket fails
@app.route('/api/send_message', methods=['POST'])
def api_send_message():
    user = get_current_user()
    if not user:
        return jsonify({'success': False, 'error': 'not authenticated'}), 401
    touch_user(user)
    data = request.get_json(silent=True) or {}
    room = data.get('room', 'general')
    if not any(r['id'] == room for r in CHAT_ROOMS):
        return jsonify({'success': False, 'error': 'invalid room'}), 400
    message = (data.get('message') or '').strip()
    if not message:
        return jsonify({'success': False, 'error': 'empty'}), 400
    if len(message) > 1000:
        message = message[:1000]
    reply_to_id = data.get('reply_to_id')
    reply_to = None
    if reply_to_id:
        try:
            reply_to = int(reply_to_id)
            parent = db.session.get(ChatMessage, reply_to)
            if not parent or parent.room != room:
                reply_to = None
        except:
            reply_to = None
    msg = ChatMessage(user_id=user.id, user_name=user.name, user_class=user.student_class,
        user_avatar_color=user.avatar_color or '#00E1FD', room=room, message=message, reply_to_id=reply_to)
    db.session.add(msg)
    if not safe_commit():
        return jsonify({'success': False, 'error': 'db error'}), 500
    payload = {"id": msg.id, "user_id": msg.user_id, "user_name": msg.user_name,
        "user_class": msg.user_class, "user_avatar_color": msg.user_avatar_color,
        "message": msg.message, "time_ago": "just now", "room": msg.room, "reply_to": None}
    if reply_to:
        try:
            parent = db.session.get(ChatMessage, reply_to)
            if parent:
                payload["reply_to"] = {"id": parent.id, "user_name": parent.user_name, "message": parent.message}
        except:
            pass
    # Broadcast via socket if available
    try:
        socketio.emit('new_message', payload, room=room)
    except Exception:
        pass
    return jsonify({'success': True, 'message': payload})


@app.route('/api/chat_messages')
def api_chat_messages():
    user = get_current_user()
    if not user:
        return jsonify({'messages': []})
    room = request.args.get('room', 'general')
    try:
        after = int(request.args.get('after', 0))
    except:
        after = 0
    try:
        rows = ChatMessage.query.filter(ChatMessage.room == room, ChatMessage.id > after).order_by(ChatMessage.id).limit(20).all()
    except Exception:
        db.session.rollback()
        rows = []
    messages = []
    for m in rows:
        msg = {"id": m.id, "user_id": m.user_id, "user_name": m.user_name,
            "user_class": m.user_class, "user_avatar_color": m.user_avatar_color or '#00E1FD',
            "message": m.message, "time_ago": time_ago(m.created_at), "reply_to": None, "room": m.room}
        messages.append(msg)
    return jsonify({'messages': messages})


@app.route('/analytics')
def analytics():
    user = get_current_user()
    if not user:
        return redirect(url_for('onboarding'))
    touch_user(user); update_streak(user)
    stats = compute_stats(user.id)
    curriculum = get_full_curriculum(user.id)
    week_data = get_7day_data(user.id)
    week_max = max((d['count'] for d in week_data), default=0)
    return render_template_string(ANALYTICS_TEMPLATE, user=user, stats=stats,
        curriculum=curriculum, week_data=week_data, week_max=week_max,
        nav_html=bottom_nav_html("analytics"))


@app.route('/settings', methods=['GET', 'POST'])
def settings():
    user = get_current_user()
    if not user:
        return redirect(url_for('onboarding'))
    touch_user(user)
    msg = None
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        student_class = request.form.get('student_class', '').strip()
        if name and student_class:
            user.name = name[:100]
            user.student_class = student_class[:50]
            if safe_commit():
                msg = "Updated!"
    return render_template_string(SETTINGS_TEMPLATE, user=user, msg=msg,
        avatar_colors=AVATAR_COLORS, nav_html=bottom_nav_html("settings"))


@app.route('/update_avatar', methods=['POST'])
def update_avatar():
    user = get_current_user()
    if not user:
        return redirect(url_for('onboarding'))
    color = request.form.get('color', '#00E1FD')[:20]
    if color in AVATAR_COLORS:
        user.avatar_color = color
        safe_commit()
    return redirect(url_for('settings'))


@app.route('/reset', methods=['POST'])
def reset():
    user = get_current_user()
    if not user:
        return redirect(url_for('onboarding'))
    try:
        ChapterProgress.query.filter_by(user_id=user.id).delete()
        ActivityLog.query.filter_by(user_id=user.id).delete()
        ExamSchedule.query.filter_by(user_id=user.id).delete()
        ChatMessage.query.filter_by(user_id=user.id).delete()
        for cs in CustomSubject.query.filter_by(user_id=user.id).all():
            CustomChapter.query.filter_by(subject_id=cs.id).delete()
            db.session.delete(cs)
        db.session.delete(user)
        safe_commit()
    except Exception:
        db.session.rollback()
    session.clear()
    return redirect(url_for('onboarding'))


# ═════════════════════════════════════════════
# SOCKETIO
# ═════════════════════════════════════════════

def broadcast_presence():
    try:
        users = get_live_online_users()
        users_public = [{"name": u['name'], "student_class": u['student_class'],
            "avatar_color": u.get('avatar_color', '#00E1FD')} for u in users]
        socketio.emit('presence_update', {'online': len(users), 'users': users_public})
    except Exception:
        pass


@socketio.on('connect')
def on_connect():
    user_id = session.get('user_id')
    if not user_id:
        return False
    try:
        with app.app_context():
            user = db.session.get(User, user_id)
            if not user:
                return False
            with _session_lock:
                active_sessions[request.sid] = {
                    'user_id': user.id, 'name': user.name,
                    'student_class': user.student_class,
                    'avatar_color': user.avatar_color or '#00E1FD',
                    'room': None, 'connected_at': datetime.utcnow()
                }
            user.last_seen = datetime.utcnow()
            safe_commit()
    except Exception:
        db.session.rollback()
    broadcast_presence()


@socketio.on('disconnect')
def on_disconnect():
    with _session_lock:
        info = active_sessions.pop(request.sid, None)
    if info and info.get('room'):
        try:
            socketio.emit('user_left', {'user_id': info['user_id'], 'name': info['name']}, room=info['room'])
        except Exception:
            pass
    broadcast_presence()


@socketio.on('join_chat_room')
def on_join_chat_room(data):
    sid = request.sid
    with _session_lock:
        if sid not in active_sessions:
            return
        info = active_sessions[sid]
        prev_room = info.get('room')
    room = data.get('room', 'general')
    if not any(r['id'] == room for r in CHAT_ROOMS):
        room = 'general'
    if prev_room and prev_room != room:
        try:
            leave_room(prev_room)
        except Exception:
            pass
    join_room(room)
    with _session_lock:
        if sid in active_sessions:
            active_sessions[sid]['room'] = room
    try:
        users = get_live_online_users()
        emit('presence_update', {'online': len(users), 'room_online': get_room_online_count(room),
            'users': [{"name": u['name'], "student_class": u['student_class'],
                "avatar_color": u.get('avatar_color', '#00E1FD')} for u in users]})
    except Exception:
        pass


@socketio.on('send_message')
def on_send_message(data):
    sid = request.sid
    with _session_lock:
        info = active_sessions.get(sid)
    if not info:
        return {'success': False, 'error': 'not connected'}
    room = data.get('room', info.get('room') or 'general')
    if not any(r['id'] == room for r in CHAT_ROOMS):
        return {'success': False, 'error': 'invalid room'}
    message = (data.get('message') or '').strip()
    if not message:
        return {'success': False, 'error': 'empty'}
    if len(message) > 1000:
        message = message[:1000]
    reply_to_id = data.get('reply_to_id')
    temp_id = data.get('temp_id')
    try:
        with app.app_context():
            user = db.session.get(User, info['user_id'])
            if not user:
                return {'success': False, 'error': 'user not found'}
            reply_to = None
            if reply_to_id:
                try:
                    reply_to = int(reply_to_id)
                    parent = db.session.get(ChatMessage, reply_to)
                    if not parent or parent.room != room:
                        reply_to = None
                except:
                    reply_to = None
            msg = ChatMessage(user_id=user.id, user_name=user.name, user_class=user.student_class,
                user_avatar_color=user.avatar_color or '#00E1FD', room=room, message=message, reply_to_id=reply_to)
            db.session.add(msg)
            user.last_seen = datetime.utcnow()
            if not safe_commit():
                return {'success': False, 'error': 'db failed'}
            payload = {"id": msg.id, "user_id": msg.user_id, "user_name": msg.user_name,
                "user_class": msg.user_class, "user_avatar_color": msg.user_avatar_color,
                "message": msg.message, "time_ago": "just now", "room": msg.room, "reply_to": None}
            if reply_to:
                parent = db.session.get(ChatMessage, reply_to)
                if parent:
                    payload["reply_to"] = {"id": parent.id, "user_name": parent.user_name, "message": parent.message}
    except Exception as e:
        db.session.rollback()
        return {'success': False, 'error': str(e)}
    try:
        socketio.emit('new_message', payload, room=room)
    except Exception:
        pass
    return {'success': True, 'message': payload, 'temp_id': temp_id}


# ═════════════════════════════════════════════
# DB INIT
# ═════════════════════════════════════════════

def init_db():
    with app.app_context():
        try:
            db.create_all()
            print("✅ DB tables ensured")
        except Exception as e:
            print(f"⚠️ create_all: {e}")
        try:
            from sqlalchemy import inspect
            inspector = inspect(db.engine)
            if 'user' not in inspector.get_table_names():
                return
            existing_cols = [c['name'] for c in inspector.get_columns('user')]
            needed = {
                'created_at': 'TIMESTAMP', 'last_active': 'DATE',
                'last_seen': 'TIMESTAMP', 'streak': 'INTEGER DEFAULT 0',
                'avatar_color': "VARCHAR(20) DEFAULT '#00E1FD'"
            }
            for col, ct in needed.items():
                if col not in existing_cols:
                    tref = '"user"' if IS_POSTGRES else 'user'
                    try:
                        with db.engine.begin() as conn:
                            conn.execute(text(f"ALTER TABLE {tref} ADD COLUMN {col} {ct}"))
                        print(f"✅ Added: {col}")
                    except Exception as e:
                        print(f"⚠️ Skip {col}: {e}")
        except Exception as e:
            print(f"⚠️ Migration: {e}")


init_db()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug_mode = not IS_POSTGRES
    print(f"🚀 StudyTracker on http://0.0.0.0:{port}")
    socketio.run(app, host='0.0.0.0', port=port, debug=debug_mode,
        allow_unsafe_werkzeug=True, use_reloader=False)
