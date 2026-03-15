from flask import Flask, render_template, request, redirect, url_for, flash, session
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from datetime import timedelta
from flask_mail import Mail, Message
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer
from dotenv import load_dotenv
import requests
import stripe
import sqlite3
import csv
import io
import re
import os

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-fallback-key')
app.permanent_session_lifetime = timedelta(days=30)

# ─── Internal access ──────────────────────────────────────────────────────────
INTERNAL_PASSWORD = os.environ.get('INTERNAL_PASSWORD', 'football2024')

# ─── Mail config ──────────────────────────────────────────────────────────────
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME', '')
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD', '')
app.config['MAIL_DEFAULT_SENDER'] = os.environ.get('MAIL_USERNAME', '')
mail = Mail(app)
serializer = URLSafeTimedSerializer(app.secret_key)

# ─── Stripe config ────────────────────────────────────────────────────────────
STRIPE_SECRET_KEY = os.environ.get('STRIPE_SECRET_KEY', '')
STRIPE_PUBLISHABLE_KEY = os.environ.get('STRIPE_PUBLISHABLE_KEY', '')
STRIPE_PRICE_ID = os.environ.get('STRIPE_PRICE_ID', '')
stripe.api_key = STRIPE_SECRET_KEY

_sk_preview = STRIPE_SECRET_KEY[:10] if STRIPE_SECRET_KEY else 'NOT SET'
print(f">>> STRIPE_SECRET_KEY starts with: {_sk_preview}", flush=True)
print(f">>> STRIPE_PRICE_ID: {STRIPE_PRICE_ID or 'NOT SET'}", flush=True)

# ─── Database ─────────────────────────────────────────────────────────────────
# Use /data for Render persistent disk, fallback to local for dev
if os.path.isdir('/data'):
    DB_PATH = '/data/users.db'
else:
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'football_scout.db')
print(f">>> Database path: {DB_PATH}", flush=True)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        subscribed INTEGER DEFAULT 0,
        stripe_customer_id TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        description TEXT,
        video_url TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.commit()
    conn.close()

init_db()

ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin2024')

def to_embed_url(url):
    """Convert YouTube/Vimeo URLs to embeddable format."""
    # YouTube: various formats
    m = re.search(r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([\w-]+)', url)
    if m:
        return f'https://www.youtube.com/embed/{m.group(1)}'
    # Vimeo
    m = re.search(r'(?:vimeo\.com/)(\d+)', url)
    if m:
        return f'https://player.vimeo.com/video/{m.group(1)}'
    return url

# ─── Flask-Login ──────────────────────────────────────────────────────────────
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'landing'

class User(UserMixin):
    def __init__(self, id, email, password, subscribed, stripe_customer_id, created_at):
        self.id = id
        self.email = email
        self.password = password
        self.subscribed = bool(subscribed)
        self.stripe_customer_id = stripe_customer_id
        self.created_at = created_at

@login_manager.user_loader
def load_user(user_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
    conn.close()
    if row:
        return User(**dict(row))
    return None

def subscription_required(f):
    """Decorator: user must be logged in AND subscribed, OR have internal access."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('internal_access'):
            return f(*args, **kwargs)
        if not current_user.is_authenticated:
            return redirect(url_for('landing'))
        if not current_user.subscribed:
            return redirect(url_for('landing'))
        return f(*args, **kwargs)
    return decorated

# ─── Scouting: down/distance breakpoints ────────────────────────────────────

# (medium_min, long_min) thresholds per down.
# Short  = dist < medium_min
# Medium = medium_min <= dist < long_min
# Long   = dist >= long_min
# Boundary always belongs to the HIGHER range.
DOWN_THRESHOLDS = {
    1: (6, 10),
    2: (5, 10),
    3: (4, 9),
    4: (3, 8),
}

DOWN_LABELS = {
    1: {"Short": "1–5", "Medium": "6–9",  "Long": "10+"},
    2: {"Short": "1–4", "Medium": "5–9",  "Long": "10+"},
    3: {"Short": "1–3", "Medium": "4–8",  "Long": "9+"},
    4: {"Short": "1–2", "Medium": "3–7",  "Long": "8+"},
}


# ─── Strategy: ratings parsing & matchup logic ──────────────────────────────

POSITIONS = ['QB', 'RB', 'WR', 'TE', 'OL', 'DL', 'LB', 'DB']

# Maps raw text tokens → canonical stat key
STAT_MAP = {
    'a': 'A',   'agility': 'A',   'agl': 'A',
    'spd': 'SPD', 'speed': 'SPD',
    'str': 'STR', 'strength': 'STR', 'strn': 'STR',
    'blk': 'BLK', 'blocking': 'BLK', 'block': 'BLK',
    'tkl': 'TKL', 'tackling': 'TKL', 'tckl': 'TKL',
    'tot': 'TOT', 'total': 'TOT', 'overall': 'TOT', 'ovr': 'TOT',
}

RECOMMENDATIONS = {
    'run_block': {
        'major': 'Commit to the run — your OL will dominate at the point of attack',
        'slight': 'Run game is viable — mix in zone and gap schemes',
        'avoid': 'Avoid direct run blocking — use misdirection, counters, or screens',
    },
    'power_run': {
        'major': 'Pound it inside — you win the strength battle up front',
        'slight': 'Inside runs are sound on short yardage and goal line',
        'avoid': 'Avoid power runs — spread the field and use quickness instead',
    },
    'outside_run': {
        'major': 'Attack the edges — your RB will outrun their LBs to the corner',
        'slight': 'Sweeps and tosses have real upside, especially on early downs',
        'avoid': 'Stay away from outside runs — their LBs have the speed to close',
    },
    'inside_run': {
        'major': 'Run between the tackles — your RB wins every collision',
        'slight': 'Mix in inside runs on favorable downs and short yardage',
        'avoid': 'Avoid inside runs — their LBs win the physical matchup',
    },
    'open_field': {
        'major': 'Use screens and draws — your RB is dangerous in space vs their DBs',
        'slight': 'Checkdowns and RB flats are solid safety valves',
        'avoid': 'Limit RB routes into the open field — their DBs have the speed advantage',
    },
    'deep_pass': {
        'major': 'Go deep early and often — your WRs will create separation downfield',
        'slight': 'Test deep coverage on 1st down to keep the defense honest',
        'avoid': 'Avoid deep shots — their DBs have the speed to recover and contest',
    },
    'route_run': {
        'major': 'Work the intermediate routes aggressively — your WRs win at the break',
        'slight': 'Crossing routes and option routes should create consistent separation',
        'avoid': 'Their DBs can mirror routes — use speed releases and stack concepts',
    },
    'te_mismatch': {
        'major': 'Target your TE on seams and crosses — massive speed edge vs their LBs',
        'slight': 'TE seam routes and drag routes are a reliable check-down option',
        'avoid': 'Their LBs can cover your TE — spread with extra WRs instead',
    },
    'qb_pressure': {
        'major': 'QB has time to work — use longer developing routes and play-action',
        'slight': 'Pressure is manageable — mix quick throws with deeper concepts',
        'avoid': 'Expect heavy DL pressure — prioritize quick game, hot routes, and RB outlets',
    },
}


def parse_ratings(text):
    """Parse a pasted WhatIfSports ratings block into {POS: {stat: avg_value}}.

    Handles individual player rows (Name / Year / Pos / stats...).
    Finds the header row containing stat names, identifies the Pos column,
    reads every player row, then averages stats per position group.
    """
    pos_set = set(POSITIONS)
    lines = [l for l in text.splitlines() if l.strip()]

    # Find the header row — must contain a 'pos' token AND at least 3 stat tokens
    header_idx = None
    pos_col = None
    stat_cols = []  # list of (col_index, canonical_stat)

    for i, line in enumerate(lines):
        tokens = re.split(r'\t', line)  # WhatIfSports tables are tab-separated
        if len(tokens) < 3:
            continue
        normed = [STAT_MAP.get(t.strip().lower()) for t in tokens]
        stat_count = sum(1 for n in normed if n)
        # Look for 'pos' column header
        pos_col_candidate = next(
            (j for j, t in enumerate(tokens) if t.strip().lower() == 'pos'), None
        )
        if stat_count >= 3 and pos_col_candidate is not None:
            header_idx = i
            pos_col = pos_col_candidate
            stat_cols = [(j, normed[j]) for j in range(len(normed)) if normed[j]]
            break

    if header_idx is None:
        return {}

    # Accumulate stats per position
    accum = {}  # {pos: {stat: [values]}}
    for line in lines[header_idx + 1:]:
        tokens = re.split(r'\t', line)
        if len(tokens) <= pos_col:
            continue
        # Strip asterisks and whitespace from all tokens
        tokens = [t.strip().lstrip('*').strip() for t in tokens]
        pos = tokens[pos_col].upper()
        if pos not in pos_set:
            continue
        if pos not in accum:
            accum[pos] = {}
        for col_j, stat in stat_cols:
            if col_j < len(tokens):
                try:
                    val = int(float(tokens[col_j]))
                    accum[pos].setdefault(stat, []).append(val)
                except (ValueError, TypeError):
                    pass

    # Average each stat per position
    ratings = {}
    for pos, stats in accum.items():
        ratings[pos] = {stat: round(sum(vals) / len(vals)) for stat, vals in stats.items()}

    return ratings


def _stat(ratings, pos, stat):
    """Return a stat value or None."""
    return ratings.get(pos, {}).get(stat)


def _tier(edge):
    """Return (css_class, icon, short_label) for an edge value."""
    if edge is None:
        return 'unknown', '—', 'No data'
    if edge >= 10:
        return 'major',  '✅', 'Major Advantage'
    if edge >= 5:
        return 'slight', '⚠️', 'Slight Advantage'
    if edge >= -4:
        return 'even',   '–',  'Even Matchup'
    return     'avoid',  '❌', 'Avoid'


def compute_matchups(offense_r, defense_r):
    """Return sorted list of matchup dicts, best edge first.

    offense_r = YOUR team's ratings (OL, RB, WR, TE are what matter)
    defense_r = OPPONENT team's ratings (DL, LB, DB are what matter)
    Positive edge = YOUR team has the advantage.
    Each row is a single stat vs single stat comparison — no composites.
    """

    def edge(off_pos, off_stat, def_pos, def_stat):
        off = _stat(offense_r, off_pos, off_stat)
        dfn = _stat(defense_r, def_pos, def_stat)
        return (off - dfn) if (off is not None and dfn is not None) else None

    raw = [
        ('outside_run', 'Outside Run',   'Your RB Spd vs Their LB Spd',   edge('RB', 'SPD', 'LB', 'SPD')),
        ('inside_run',  'Inside Run',    'Your RB Str vs Their LB Str',    edge('RB', 'STR', 'LB', 'STR')),
        ('run_block',   'Run Blocking',  'Your OL Blk vs Their DL Tkl',    edge('OL', 'BLK', 'DL', 'TKL')),
        ('power_run',   'Power Run',     'Your OL Str vs Their DL Str',    edge('OL', 'STR', 'DL', 'STR')),
        ('open_field',  'Open Field',    'Your RB Spd vs Their DB Spd',    edge('RB', 'SPD', 'DB', 'SPD')),
        ('deep_pass',   'Deep Pass',     'Your WR Spd vs Their DB Spd',    edge('WR', 'SPD', 'DB', 'SPD')),
        ('route_run',   'Route Running', 'Your WR Agi vs Their DB Agi',    edge('WR', 'A',   'DB', 'A')),
        ('te_mismatch', 'TE Mismatch',   'Your TE Spd vs Their LB Spd',    edge('TE', 'SPD', 'LB', 'SPD')),
    ]

    # QB Protection: 65 minus their DL SPD — low DL speed = high positive number
    their_dl_spd = _stat(defense_r, 'DL', 'SPD')
    qb_edge = (65 - their_dl_spd) if their_dl_spd is not None else None
    raw.append(('qb_pressure', 'QB Protection', 'Their DL Spd (65 − Their DL Spd)', qb_edge))

    matchups = []
    for key, label, desc, e in raw:
        css, icon, short = _tier(e)
        rec = RECOMMENDATIONS.get(key, {}).get(css, '')
        matchups.append({
            'key': key, 'label': label, 'desc': desc,
            'edge': e, 'tier': css, 'icon': icon,
            'short': short, 'rec': rec,
        })

    matchups.sort(key=lambda m: -(m['edge'] if m['edge'] is not None else -999))
    return matchups


INDIVIDUAL_EDGE_DEFS = [
    {
        'key': 'rb_spd_lb',
        'label': 'RB Speed vs LB Speed',
        'off': ('RB', 'SPD'), 'def': ('LB', 'SPD'),
        'adv_text': (
            "Your running backs are significantly faster than their linebackers. "
            "When your RB reaches the edge or breaks through the first level, the LBs simply won't close in time. "
            "Outside runs, tosses, and RB screens in space become high-percentage plays."
        ),
        'adv_rec': "Toss sweeps, outside zone, and RB screen passes — attack the perimeter on early downs.",
        'dan_text': (
            "Their linebackers are faster than your running backs, limiting your outside run game. "
            "Sweeps and tosses will get run down before reaching the corner. "
            "Keep carries between the tackles and use your passing game to create space."
        ),
        'dan_rec': "Avoid outside runs — stick to inside zone and quick dump-offs to RB.",
    },
    {
        'key': 'rb_spd_db',
        'label': 'RB Speed vs DB Speed',
        'off': ('RB', 'SPD'), 'def': ('DB', 'SPD'),
        'adv_text': (
            "Your running backs have a speed edge over their defensive backs in the open field. "
            "Once your RB clears the second level, safeties and corners won't run him down. "
            "Draw plays, delay routes, and swing passes become big-play opportunities."
        ),
        'adv_rec': "Draw plays, RB delay routes, and swing passes into open space.",
        'dan_text': (
            "Their defensive backs can run down your RB in the open field. "
            "Big plays on screens and outside runs will be cut short by their speed in pursuit. "
            "Get the ball out quickly and take the safe checkdown rather than hoping for yards after contact."
        ),
        'dan_rec': "Limit RB in open space — quick inside runs and safe checkdowns only.",
    },
    {
        'key': 'rb_str_lb',
        'label': 'RB Strength vs LB Strength',
        'off': ('RB', 'STR'), 'def': ('LB', 'STR'),
        'adv_text': (
            "Your running backs are physically stronger than their linebackers. "
            "Your RB will win contact situations inside the tackles and break through arm tackles at the second level. "
            "Power runs and short-yardage situations heavily favor your offense."
        ),
        'adv_rec': "Power runs between the guards — inside zone and lead plays on any down and distance.",
        'dan_text': (
            "Their linebackers are physically stronger than your running backs. "
            "Your RB will struggle to break tackles and pick up yards after contact in power situations. "
            "Use quickness and misdirection rather than trying to run through defenders."
        ),
        'dan_rec': "Avoid power runs — use counters, traps, and misdirection instead.",
    },
    {
        'key': 'ol_blk_dl',
        'label': 'OL Blocking vs DL Tackling',
        'off': ('OL', 'BLK'), 'def': ('DL', 'TKL'),
        'adv_text': (
            "Your offensive linemen are significantly better at blocking than their defensive linemen are at making tackles. "
            "Your OL will consistently seal defenders and create running lanes. "
            "This is a foundational advantage that makes the entire run game viable on any down."
        ),
        'adv_rec': "Commit to the run — any gap scheme works. OL will dominate the point of attack.",
        'dan_text': (
            "Their defensive linemen are better tacklers than your OL are blockers. "
            "Running lanes will be filled quickly and short runs will get stuffed at the line. "
            "Look to the passing game to move the ball efficiently."
        ),
        'dan_rec': "Abandon the run early — use quick passing and movement to neutralize their DL.",
    },
    {
        'key': 'ol_str_dl',
        'label': 'OL Strength vs DL Strength',
        'off': ('OL', 'STR'), 'def': ('DL', 'STR'),
        'adv_text': (
            "Your offensive line wins the strength battle at the point of attack. "
            "In short yardage and goal line situations your OL will drive their DL off the ball. "
            "Power runs and QB sneaks are near-automatic when the strength gap is this large."
        ),
        'adv_rec': "Power runs, QB sneaks, and goal line packages — go physical at the line.",
        'dan_text': (
            "Their defensive line is physically stronger than your offensive line. "
            "Expect struggles in short yardage and power run situations. "
            "Get the ball out quickly in the passing game before their DL can engage your linemen."
        ),
        'dan_rec': "Avoid power runs and slow-developing plays — quick game and screens only.",
    },
    {
        'key': 'wr_spd_db',
        'label': 'WR Speed vs DB Speed',
        'off': ('WR', 'SPD'), 'def': ('DB', 'SPD'),
        'adv_text': (
            "Your wide receivers have a clear speed advantage over their cornerbacks and safeties. "
            "Vertical routes will consistently put your WRs behind coverage downfield. "
            "Your QB should look deep on first and second down to exploit this mismatch."
        ),
        'adv_rec': "Go routes, post routes, and vertical concepts — attack deep coverage every series.",
        'dan_text': (
            "Their defensive backs are faster than your wide receivers. "
            "Deep routes will be covered and your WRs will struggle to create separation vertically. "
            "Use short and intermediate routes with quick releases to get the ball out before coverage closes."
        ),
        'dan_rec': "Short and intermediate routes only — slants, crossers, and quick outs.",
    },
    {
        'key': 'te_spd_lb',
        'label': 'TE Speed vs LB Speed',
        'off': ('TE', 'SPD'), 'def': ('LB', 'SPD'),
        'adv_text': (
            "Your tight end is significantly faster than their linebackers in coverage. "
            "Linebackers simply cannot run with a fast TE over the middle or down the seam. "
            "This is one of the most exploitable mismatches in football — target your TE relentlessly."
        ),
        'adv_rec': "TE seam routes, crossing routes, and TE leak plays — make them pay every drive.",
        'dan_text': (
            "Their linebackers have the speed to cover your tight end. "
            "TE seam routes and crossers won't create the separation you need. "
            "Use your TE as a blocker or target him only on quick routes in traffic."
        ),
        'dan_rec': "Use TE as a blocker — route tree should favor WR options instead.",
    },
    {
        'key': 'te_str_lb',
        'label': 'TE Strength vs LB Strength',
        'off': ('TE', 'STR'), 'def': ('LB', 'STR'),
        'adv_text': (
            "Your tight end is stronger than their linebackers in physical matchups. "
            "Your TE will win contested catches and break through arm tackles after the catch. "
            "Use your TE as both a power blocker on run plays and a physical receiving threat on short routes."
        ),
        'adv_rec': "TE blocking on power runs, and short TE routes over the middle in traffic.",
        'dan_text': (
            "Their linebackers are stronger than your tight end. "
            "Your TE will get jammed at the line of scrimmage and lose contested catch situations. "
            "Keep your TE in to help with pass protection or use him only on quick releases."
        ),
        'dan_rec': "Keep TE in protection — don't rely on TE as a primary receiving option.",
    },
]

# Special threshold check — not a vs comparison
DL_SPD_SAFE_THRESHOLD = 55

INDIVIDUAL_EDGE_LABELS = {e['key']: e for e in INDIVIDUAL_EDGE_DEFS}


def find_individual_edges(offense_r, defense_r, your_team, opponent_team):
    """Return dicts with 'advantages' and 'dangers' lists for the highlights sections."""
    results = []

    for defn in INDIVIDUAL_EDGE_DEFS:
        off = _stat(offense_r, defn['off'][0], defn['off'][1])
        dfn = _stat(defense_r, defn['def'][0], defn['def'][1])
        if off is None or dfn is None:
            continue
        edge = off - dfn
        results.append({
            'key': defn['key'],
            'label': defn['label'],
            'edge': edge,
            'adv_text': defn['adv_text'],
            'adv_rec': defn['adv_rec'],
            'dan_text': defn['dan_text'],
            'dan_rec': defn['dan_rec'],
        })

    # Special: their DL SPD threshold check
    dl_spd = _stat(defense_r, 'DL', 'SPD')
    if dl_spd is not None and dl_spd < DL_SPD_SAFE_THRESHOLD:
        results.append({
            'key': 'dl_spd_thresh',
            'label': f'Their DL Speed ({dl_spd}) — Below Pressure Threshold',
            'edge': DL_SPD_SAFE_THRESHOLD - dl_spd,  # positive = your advantage
            'adv_text': (
                f"Their defensive line has an average speed of only {dl_spd}, well below the threshold where pass rush becomes dangerous. "
                "Your quarterback will have time to survey the field and work through progressions. "
                "Longer-developing routes that normally carry risk are now viable calls."
            ),
            'adv_rec': "Play-action deep routes, 7-step drops, and sprint-out passes — take full advantage of slow pass rush.",
            'dan_text': '',
            'dan_rec': '',
        })

    advantages = sorted(
        [r for r in results if r['edge'] >= 10],
        key=lambda x: -x['edge']
    )[:5]

    dangers = sorted(
        [r for r in results if r['edge'] <= -10],
        key=lambda x: x['edge']
    )[:5]

    return advantages, dangers


def build_game_plan(matchups):
    """Build the 5-row game plan summary."""
    by_key = {m['key']: m for m in matchups}

    def avg_edge(*keys):
        vals = [by_key[k]['edge'] for k in keys
                if k in by_key and by_key[k]['edge'] is not None]
        return round(sum(vals) / len(vals)) if vals else None

    rows = [
        ('Run Outside',    avg_edge('outside_run')),
        ('Run Inside',     avg_edge('power_run', 'inside_run')),
        ('Pass Short',     avg_edge('te_mismatch', 'route_run')),
        ('Pass Deep',      avg_edge('deep_pass')),
        ('QB Protection',  avg_edge('qb_pressure')),
    ]

    plan = []
    for play, e in rows:
        css, icon, short = _tier(e)
        plan.append({'play': play, 'edge': e, 'tier': css, 'icon': icon, 'short': short})
    return plan


# ─── Scouting helpers ────────────────────────────────────────────────────────

def sheets_url_to_csv(url):
    if "/export" in url:
        return url
    if "/edit" in url:
        base = url.split("/edit")[0]
        gid_part = ""
        if "gid=" in url:
            gid = url.split("gid=")[1].split("&")[0].split("#")[0]
            gid_part = f"&gid={gid}"
        return f"{base}/export?format=csv{gid_part}"
    if "/pub" in url:
        base = url.split("/pub")[0]
        return f"{base}/export?format=csv"
    return url + "/export?format=csv"


def get_range_label(down, distance):
    med_min, long_min = DOWN_THRESHOLDS[down]
    if distance >= long_min:
        return "Long"
    if distance >= med_min:
        return "Medium"
    return "Short"


def analyze_text(team, csv_text):
    reader = csv.DictReader(io.StringIO(csv_text))
    return _analyze_rows(team, list(reader))


def analyze(team, csv_url):
    resp = requests.get(csv_url, timeout=15)
    resp.raise_for_status()
    return analyze_text(team, resp.text)


def _bucket_plays(filtered_plays, col_func):
    """Run down/distance bucketing on a list of filtered plays. Returns results dict."""
    results = {}
    for down in [1, 2, 3, 4]:
        buckets = {b: {"runs": 0, "total": 0} for b in ["Short", "Medium", "Long"]}

        for r in filtered_plays:
            try:
                dn = int(r.get(col_func("Dwn"), "0").strip())
            except ValueError:
                continue
            if dn != down:
                continue
            try:
                distance = int(float(r.get(col_func("Dist"), "0").strip()))
            except ValueError:
                continue

            ot = r.get(col_func("OT"), "").strip()
            bucket = get_range_label(down, distance)
            buckets[bucket]["total"] += 1
            if ot == "Rn":
                buckets[bucket]["runs"] += 1

        rows_out = []
        for label in ["Short", "Medium", "Long"]:
            b = buckets[label]
            total, runs = b["total"], b["runs"]
            run_pct = round((runs / total) * 100) if total > 0 else None
            anomaly = run_pct is not None and (run_pct >= 85 or run_pct <= 15)
            rows_out.append({
                "label": label,
                "range": DOWN_LABELS[down][label],
                "total": total, "runs": runs,
                "run_pct": run_pct, "anomaly": anomaly,
            })

        results[down] = rows_out

    return results


def _formation_strategy_note(formation_name, plays, col_func):
    """Generate a 2-3 sentence strategy note for a formation based on its tendencies."""
    total = len(plays)
    if total == 0:
        return ""
    runs = sum(1 for r in plays if r.get(col_func("OT"), "").strip() == "Rn")
    run_pct = (runs / total) * 100

    # Check early down (1st & 2nd) run tendency
    early = [r for r in plays if r.get(col_func("Dwn"), "").strip() in ("1", "2")]
    early_runs = sum(1 for r in early if r.get(col_func("OT"), "").strip() == "Rn")
    early_pct = (early_runs / len(early)) * 100 if early else 0

    # Check 3rd down pass tendency
    third = [r for r in plays if r.get(col_func("Dwn"), "").strip() == "3"]
    third_passes = sum(1 for r in third if r.get(col_func("OT"), "").strip() == "Ps")
    third_pass_pct = (third_passes / len(third)) * 100 if third else 0

    parts = []
    if run_pct >= 80:
        parts.append(f"This formation is a strong run indicator — {run_pct:.0f}% run rate overall.")
        parts.append("Expect heavy run game when they line up here. Stack the box and force them to prove they can throw.")
    elif run_pct <= 20:
        parts.append(f"This is a pass-heavy formation — only {run_pct:.0f}% run rate.")
        parts.append("Drop into coverage and bring pressure. They are not running out of this look.")
    elif early_pct >= 70:
        parts.append(f"They run {early_pct:.0f}% of the time on early downs out of this formation.")
        parts.append("Look run first on 1st and 2nd down, then expect them to throw on 3rd.")
    elif third_pass_pct >= 80 and third:
        parts.append(f"On 3rd down out of this formation they pass {third_pass_pct:.0f}% of the time.")
        parts.append("This is a passing down look — bring extra pressure and play the pass.")
    else:
        parts.append(f"Balanced formation — {run_pct:.0f}% run, {100 - run_pct:.0f}% pass.")
        parts.append("They mix it up out of this look. Play assignment football and read your keys.")

    return ' '.join(parts)


def _analyze_rows(team, rows):
    if rows:
        col_map = {k.strip(): k for k in rows[0].keys()}
    else:
        col_map = {}

    # Print column headers to console for debugging
    try:
        print(f"\n>>> CSV COLUMN HEADERS (raw keys): {list(rows[0].keys()) if rows else 'NO ROWS'}", flush=True)
        print(f">>> CSV COLUMN HEADERS (stripped): {list(col_map.keys())}", flush=True)
        print(f">>> col_map mapping: {col_map}", flush=True)
    except BrokenPipeError:
        pass

    def col(name):
        return col_map.get(name, name)

    # Detect formation column — try common names
    form_col = None
    for candidate in ['OForm', 'oform', 'OFORM', 'Form', 'Formation', 'FORM', 'FORMATION', 'form', 'formation']:
        if candidate in col_map:
            form_col = col_map[candidate]
            break

    try:
        print(f">>> Formation column found: {form_col!r}", flush=True)
        if form_col:
            unique_vals = {}
            for r in rows:
                v = r.get(form_col, "").strip()
                unique_vals[v] = unique_vals.get(v, 0) + 1
            print(f">>> ALL formation values (before filtering): {sorted(unique_vals.items(), key=lambda x: -x[1])}", flush=True)
        else:
            print(">>> NO formation column detected! Checking all columns for formation-like data...", flush=True)
            for key in (rows[0].keys() if rows else []):
                sample_vals = set()
                for r in rows[:20]:
                    sample_vals.add(r.get(key, "").strip())
                print(f">>>   Column {key!r} sample values: {sample_vals}", flush=True)
    except BrokenPipeError:
        pass

    filtered = [
        r for r in rows
        if r.get(col("Offense"), "").strip().lower() == team.lower()
        and r.get(col("OT"), "").strip() in ("Rn", "Ps")
    ]

    total_plays = len(filtered)

    # Overall results (all formations combined)
    results = _bucket_plays(filtered, col)

    # Per-formation breakdown
    formations = {}
    all_formation_names = []
    if form_col:
        # Group plays by formation
        form_groups = {}
        for r in filtered:
            fname = r.get(form_col, "").strip()
            if fname:
                form_groups.setdefault(fname, []).append(r)

        # All formation names sorted by play count (for subtitle)
        all_formation_names = [f for f, _ in sorted(form_groups.items(), key=lambda x: -len(x[1]))]

        try:
            print(f">>> Formations found: {[(f, len(p)) for f, p in form_groups.items()]}", flush=True)
        except BrokenPipeError:
            pass

        # Only include formations with 20+ plays for detailed breakdown
        for fname, fplays in sorted(form_groups.items(), key=lambda x: -len(x[1])):
            if len(fplays) >= 20:
                formations[fname] = {
                    'results': _bucket_plays(fplays, col),
                    'play_count': len(fplays),
                    'strategy_note': _formation_strategy_note(fname, fplays, col),
                }

        try:
            print(f">>> Formations with 20+ plays (sent to template): {list(formations.keys())}", flush=True)
            print(f">>> Formations dict size: {len(formations)}", flush=True)
        except BrokenPipeError:
            pass

    return total_plays, results, formations, all_formation_names


# ─── Halftime Advisor ────────────────────────────────────────────────────────

def _names(text):
    """Extract capitalized 2-word names from a line."""
    return re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b', text)


def _split_player_entries(text_block):
    """Split a block of text into individual player entries by splitting on
    position abbreviations (WR, RB, TE, QB, OL, DL, LB, DB, K, P).

    Handles cases where names and positions run together without spaces, e.g.
    "Justin RiveraWR5 catches 55 yards" → ("Justin Rivera", "WR", "5 catches 55 yards")
    """
    POSITIONS = ['WR', 'RB', 'TE', 'QB', 'OL', 'DL', 'LB', 'DB']
    # Split on position abbreviations — they may be glued to the preceding name
    # Pattern: split before each position code that is followed by digits or end
    parts = re.split(r'(?<=[a-zA-Z])((?:WR|RB|TE|QB|OL|DL|LB|DB|K|P)(?=\s*\d|\s*$|\s+\d))', text_block)

    players = []
    i = 0
    while i < len(parts):
        if i + 1 < len(parts) and parts[i + 1].strip() in POSITIONS + ['K', 'P']:
            name_raw = parts[i].strip()
            pos = parts[i + 1].strip()
            # Get the stat portion — everything up to the next player entry
            stat_text = ''
            if i + 2 < len(parts):
                stat_text = parts[i + 2].strip()
                # stat_text might contain the next player's name at the end
                # We'll handle that by only taking the numeric prefix
            players.append((name_raw, pos, stat_text))
            i += 3
        else:
            i += 1

    return players


def _parse_player_stat_line(name_raw, pos, stat_text, team_name):
    """Parse a single player's stat text into a structured dict.

    Returns dict with: name, pos, team, and stat fields depending on context.
    Stat text examples:
      "5 55"          → catches=5, yards=55 (receiving)
      "12 45 3.8"     → carries=12, yards=45, ypc=3.8 (rushing)
      "15 25 180 2 1" → comp=15, att=25, yards=180, td=2, int=1 (passing)
      "8 3 2"         → tackles=8, solo=3, sacks=2 (defense)
    """
    # Clean up name — remove trailing digits/spaces that got included
    name_clean = re.sub(r'\d+$', '', name_raw).strip()
    # Also strip any trailing position abbreviation that might be glued
    name_clean = re.sub(r'(WR|RB|TE|QB|OL|DL|LB|DB|K|P)$', '', name_clean).strip()

    nums = re.findall(r'[\d.]+', stat_text)
    nums_int = []
    for n in nums:
        try:
            if '.' in n:
                nums_int.append(float(n))
            else:
                nums_int.append(int(n))
        except ValueError:
            pass

    return {
        'name': name_clean,
        'pos': pos,
        'team': team_name,
        'nums': nums_int,
    }


def parse_box_score(text, your_team, opp_team):
    """Return (your_stats, their_stats, box_players) from pasted box score.

    box_players is a list of dicts with individual player stats parsed from
    receiving, rushing, passing, and defense tables.
    """
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    your_stats, their_stats = {}, {}
    your_first = True  # column order default

    for line in lines:
        ll = line.lower()
        yi = ll.find(your_team.lower())
        ti = ll.find(opp_team.lower())
        if yi != -1 and ti != -1:
            your_first = yi < ti
            break

    STAT_KEYS = [
        ('total_yards',  r'total\s*yards?'),
        ('pass_yards',   r'pass(?:ing)?\s*yards?|net\s*pass'),
        ('rush_yards',   r'rush(?:ing)?\s*yards?|net\s*rush'),
        ('ypc',          r'yards?\s*(?:per|/)\s*(?:rush|carry)'),
        ('ypa',          r'yards?\s*(?:per|/)\s*(?:pass|attempt|comp)'),
        ('third_down',   r'3rd\s*down|third\s*down'),
        ('top',          r'time\s*of\s*poss'),
        ('first_downs',  r'first\s*downs?'),
        ('turnovers',    r'turnovers?'),
        ('score',        r'score|points'),
    ]

    for line in lines:
        ll = line.lower()
        for key, pat in STAT_KEYS:
            if re.search(pat, ll):
                tokens = re.split(r'\t|  +', line)
                nums = [re.search(r'\d+:\d+|\d+/\d+|\d+\.\d+|\d+', t) for t in tokens]
                nums = [m.group(0) for m in nums if m]
                if len(nums) >= 2:
                    a, b = nums[-2], nums[-1]
                    your_stats[key], their_stats[key] = (a, b) if your_first else (b, a)
                break

    # ── Parse individual player stat tables ─────────────────────────────────
    box_players = []  # list of {name, pos, team, category, stats...}
    full_text = '\n'.join(lines)

    # Detect table sections by headers
    SECTION_PATTERNS = [
        ('receiving', r'receiving', ['catches', 'yards']),
        ('rushing',   r'rushing',   ['carries', 'yards']),
        ('passing',   r'passing',   ['comp', 'att', 'yards']),
        ('defense',   r'defense|tackles|defensive', ['tackles']),
    ]

    current_team_name = None
    current_section = None

    for line in lines:
        ll = line.lower().strip()

        # Detect team ownership
        if your_team.lower() in ll and opp_team.lower() not in ll:
            current_team_name = your_team
        elif opp_team.lower() in ll and your_team.lower() not in ll:
            current_team_name = opp_team

        # Detect section headers
        for sec_name, sec_pat, _ in SECTION_PATTERNS:
            if re.search(sec_pat, ll) and not re.search(r'\d', ll[:10]):
                current_section = sec_name
                break

        if not current_team_name or not current_section:
            continue

        # Try to extract player entries from this line
        # Look for lines with a position abbreviation and numbers
        pos_match = re.search(r'(WR|RB|TE|QB|OL|DL|LB|DB|K|P)', line)
        if not pos_match:
            continue

        # Split on position abbreviations to handle concatenated entries
        entries = _split_player_entries(line)
        if not entries:
            # Try simpler single-player parse: "Name POS num num num..."
            m = re.match(r'^(.+?)\s*(WR|RB|TE|QB|OL|DL|LB|DB|K|P)\s+([\d\s.]+)$', line.strip())
            if m:
                entries = [(m.group(1), m.group(2), m.group(3))]

        for name_raw, pos, stat_text in entries:
            parsed = _parse_player_stat_line(name_raw, pos, stat_text, current_team_name)
            nums = parsed['nums']
            name = parsed['name']
            if not name or len(name) < 3:
                continue

            player = {
                'name': name, 'pos': pos, 'team': current_team_name,
                'category': current_section,
            }

            if current_section == 'receiving' and len(nums) >= 2:
                player['catches'] = int(nums[0])
                player['yards'] = int(nums[1])
            elif current_section == 'rushing' and len(nums) >= 2:
                player['carries'] = int(nums[0])
                player['yards'] = int(nums[1])
                if len(nums) >= 3:
                    player['ypc'] = float(nums[2])
            elif current_section == 'passing' and len(nums) >= 4:
                # Format: Comp Att Pct Yards TD Int ...
                # nums[0]=comp, nums[1]=att, nums[2]=pct (skip), nums[3]=yards
                player['comp'] = int(nums[0])
                player['att'] = int(nums[1])
                player['yards'] = int(nums[3])
                if len(nums) >= 5:
                    player['td'] = int(nums[4])
                if len(nums) >= 6:
                    player['int'] = int(nums[5])
            elif current_section == 'passing' and len(nums) == 3:
                # Fallback if no pct column: Comp Att Yards
                player['comp'] = int(nums[0])
                player['att'] = int(nums[1])
                player['yards'] = int(nums[2])
            elif current_section == 'defense' and len(nums) >= 1:
                player['tackles'] = int(nums[0])
                if len(nums) >= 2:
                    player['solo'] = int(nums[1])

            box_players.append(player)

    # Debug output
    try:
        print("\n--- BOX SCORE PLAYER PARSING ---", flush=True)
        for p in box_players:
            print(f"  [{p['team']}] {p['name']} ({p['pos']}) — {p['category']}: {p}", flush=True)
        print("--- END BOX SCORE PLAYERS ---\n", flush=True)
    except BrokenPipeError:
        pass

    return your_stats, their_stats, box_players


def parse_game_log(text, your_team, opp_team):
    """Parse play-by-play text into structured play data for both teams.

    Expects lines like:
      "Duane Cruz rushes up the middle for 6 yards"
      "Jake Smith completes a pass to Mike Jones (Short) for 12 yards"
      "pass is incomplete to Ray Brown (Medium)"
      "Jake Smith drops the pass"
      "pass is overthrown"
    """
    your_runs, their_runs = {}, {}           # dir -> [yards, ...]
    your_passes, their_passes = {}, {}       # depth -> {att, comp, yards:[]}
    your_players, their_players = {}, {}     # name -> total_yards
    your_3rds, their_3rds = [], []
    scores = []
    on_3rd = False

    # Detailed per-player stat tracking
    # name -> {team, role, rush_yards, rush_att, rec_yards, rec_att, rec_comp}
    player_stats = {}

    RUN_DIRS = [
        ('left end',    [r'left\s+end', r'outside\s+left', r'wide\s+left']),
        ('right end',   [r'right\s+end', r'outside\s+right', r'wide\s+right']),
        ('up the middle', [r'up\s+(?:the\s+)?middle']),
        ('inside',      [r'\binside\b', r'between\s+the\s+guards']),
        ('left',        [r'\bleft\b']),
        ('right',       [r'\bright\b']),
    ]

    current_team = None  # 'yours' | 'theirs' | None

    def _ensure_stat(name, team_label, role='RB'):
        if name not in player_stats:
            player_stats[name] = {
                'team': team_label, 'role': role,
                'rush_yards': 0, 'rush_att': 0,
                'rec_yards': 0, 'rec_att': 0, 'rec_comp': 0,
                'pass_yards': 0, 'pass_comp': 0, 'pass_att': 0,
                'tackles': 0,
            }
        else:
            if not player_stats[name]['team']:
                player_stats[name]['team'] = team_label
            # Ensure new fields exist on old entries
            for f in ('pass_yards', 'pass_comp', 'pass_att', 'tackles'):
                if f not in player_stats[name]:
                    player_stats[name][f] = 0

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        ll = line.lower()

        # Detect 3rd down situation
        if re.search(r'3rd\s*(?:and|&)\s*\d+', ll):
            on_3rd = True

        # Track scores
        if 'touchdown' in ll:
            scores.append(line)
        elif re.search(r'field\s*goal', ll):
            scores.append(line)

        # Update possession based on team name appearing
        is_yours  = your_team.lower() in ll
        is_theirs = opp_team.lower() in ll
        if is_yours and not is_theirs:
            current_team = 'yours'
        elif is_theirs and not is_yours:
            current_team = 'theirs'

        if current_team is None:
            continue

        team_label = your_team if current_team == 'yours' else opp_team
        runs    = your_runs    if current_team == 'yours' else their_runs
        passes  = your_passes  if current_team == 'yours' else their_passes
        players = your_players if current_team == 'yours' else their_players
        thirds  = your_3rds    if current_team == 'yours' else their_3rds

        # ── RUSH detection: line contains 'rushes' ──────────────────────────
        if 'rushes' in ll:
            # Extract ball carrier — first capitalized name before 'rushes'
            carrier_m = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+rushes', line)
            if not carrier_m:
                continue
            carrier = carrier_m.group(1)

            # Extract direction
            direction = None
            for dlabel, patterns in RUN_DIRS:
                if any(re.search(p, ll) for p in patterns):
                    direction = dlabel
                    break
            if direction is None:
                continue  # Skip plays with no recognizable direction

            # Extract yards
            yds_m = re.search(r'(\d+)\s*yard', ll)
            yards = int(yds_m.group(1)) if yds_m else 0
            if re.search(r'no\s+gain|loss\s+of', ll):
                yards = 0

            runs.setdefault(direction, []).append(yards)
            players[carrier] = players.get(carrier, 0) + yards

            _ensure_stat(carrier, team_label, 'RB')
            player_stats[carrier]['rush_yards'] += yards
            player_stats[carrier]['rush_att'] += 1
            player_stats[carrier]['role'] = 'RB'

            if on_3rd:
                thirds.append({'success': yards > 0, 'type': 'rush'})
                on_3rd = False

        # ── PASS COMPLETION: 'completes a pass' ────────────────────────────
        elif 'completes a pass' in ll:
            # Extract passer — name before 'completes'
            passer_m = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+completes', line)
            passer = passer_m.group(1) if passer_m else None

            # Extract receiver — name after 'to'
            recv_m = re.search(r'\bto\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)', line)
            receiver = recv_m.group(1) if recv_m else None

            # Extract route depth from parenthesized word: (Short), (Medium), (Long)
            depth_m = re.search(r'\((Short|Medium|Long|Deep)\)', line, re.IGNORECASE)
            depth = depth_m.group(1).lower() if depth_m else None

            # Extract yards
            yds_m = re.search(r'(\d+)\s*yard', ll)
            yards = int(yds_m.group(1)) if yds_m else 0

            if depth:
                passes.setdefault(depth, {'att': 0, 'comp': 0, 'yards': []})
                passes[depth]['att'] += 1
                passes[depth]['comp'] += 1
                passes[depth]['yards'].append(yards)

            if receiver:
                players[receiver] = players.get(receiver, 0) + yards
                _ensure_stat(receiver, team_label, 'WR')
                player_stats[receiver]['rec_yards'] += yards
                player_stats[receiver]['rec_att'] += 1
                player_stats[receiver]['rec_comp'] += 1
                player_stats[receiver]['role'] = 'WR'

            if passer:
                _ensure_stat(passer, team_label, 'QB')
                player_stats[passer]['role'] = 'QB'

            if on_3rd:
                thirds.append({'success': True, 'type': 'pass', 'depth': depth})
                on_3rd = False

        # ── PASS INCOMPLETION: 'incomplete', 'overthrown', 'drops the pass'
        elif re.search(r'incomplete|pass\s+is\s+overthrown|drops\s+the\s+pass', ll):
            # Extract route depth from parenthesized word
            depth_m = re.search(r'\((Short|Medium|Long|Deep)\)', line, re.IGNORECASE)
            depth = depth_m.group(1).lower() if depth_m else None

            if depth:
                passes.setdefault(depth, {'att': 0, 'comp': 0, 'yards': []})
                passes[depth]['att'] += 1

            # Track targeted receiver if present
            recv_m = re.search(r'\bto\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)', line)
            if recv_m:
                receiver = recv_m.group(1)
                _ensure_stat(receiver, team_label, 'WR')
                player_stats[receiver]['rec_att'] += 1
                player_stats[receiver]['role'] = 'WR'

            # Track passer
            passer_m = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+(?:pass|throw)', line)
            if passer_m:
                passer = passer_m.group(1)
                _ensure_stat(passer, team_label, 'QB')
                player_stats[passer]['role'] = 'QB'

            if on_3rd:
                thirds.append({'success': False, 'type': 'pass', 'depth': depth})
                on_3rd = False

    # ── Console debug output ────────────────────────────────────────────────
    try:
        print("\n" + "="*70, flush=True)
        print("HALFTIME PARSER DEBUG OUTPUT", flush=True)
        print("="*70, flush=True)

        print(f"\n--- RUSHING ({your_team}) ---", flush=True)
        for d, yds_list in your_runs.items():
            avg = sum(yds_list)/len(yds_list) if yds_list else 0
            print(f"  {d}: {len(yds_list)} carries, {sum(yds_list)} yds, {avg:.1f} ypc", flush=True)

        print(f"\n--- RUSHING ({opp_team}) ---", flush=True)
        for d, yds_list in their_runs.items():
            avg = sum(yds_list)/len(yds_list) if yds_list else 0
            print(f"  {d}: {len(yds_list)} carries, {sum(yds_list)} yds, {avg:.1f} ypc", flush=True)

        print(f"\n--- PASSING ({your_team}) ---", flush=True)
        for d, v in your_passes.items():
            avg = sum(v['yards'])/v['comp'] if v['comp'] else 0
            print(f"  {d}: {v['comp']}/{v['att']} comp, {sum(v['yards'])} yds, {avg:.1f} yds/comp", flush=True)

        print(f"\n--- PASSING ({opp_team}) ---", flush=True)
        for d, v in their_passes.items():
            avg = sum(v['yards'])/v['comp'] if v['comp'] else 0
            print(f"  {d}: {v['comp']}/{v['att']} comp, {sum(v['yards'])} yds, {avg:.1f} yds/comp", flush=True)

        print(f"\n--- TOP PERFORMERS ({your_team}) ---", flush=True)
        for name, yds in sorted(your_players.items(), key=lambda x: -x[1])[:5]:
            info = player_stats.get(name, {})
            print(f"  {name} ({info.get('role','?')}) — {yds} yds | rush: {info.get('rush_att',0)} att/{info.get('rush_yards',0)} yds | rec: {info.get('rec_comp',0)}/{info.get('rec_att',0)} for {info.get('rec_yards',0)} yds", flush=True)

        print(f"\n--- TOP PERFORMERS ({opp_team}) ---", flush=True)
        for name, yds in sorted(their_players.items(), key=lambda x: -x[1])[:5]:
            info = player_stats.get(name, {})
            print(f"  {name} ({info.get('role','?')}) — {yds} yds | rush: {info.get('rush_att',0)} att/{info.get('rush_yards',0)} yds | rec: {info.get('rec_comp',0)}/{info.get('rec_att',0)} for {info.get('rec_yards',0)} yds", flush=True)

        print("="*70 + "\n", flush=True)
    except BrokenPipeError:
        pass

    return {
        'your_runs': your_runs, 'their_runs': their_runs,
        'your_passes': your_passes, 'their_passes': their_passes,
        'your_players': your_players, 'their_players': their_players,
        'your_3rds': your_3rds, 'their_3rds': their_3rds,
        'scores': scores, 'player_stats': player_stats,
    }


_OUTSIDE_DIRS = {'left end', 'right end', 'left', 'right'}
_INSIDE_DIRS = {'up the middle', 'inside'}


def _group_runs(run_data):
    """Group run data into outside vs inside. Returns (outside_yds_list, inside_yds_list)."""
    outside, inside = [], []
    for d, v in run_data.items():
        if d in _OUTSIDE_DIRS:
            outside.extend(v)
        elif d in _INSIDE_DIRS:
            inside.extend(v)
    return outside, inside


def _player_tag(name, player_stats, fallback_team='', fallback_role=''):
    """Return 'Name (ROLE, Team)' using tracked stat data."""
    info = player_stats.get(name)
    if info:
        return f"{name} ({info['role']}, {info['team']})"
    if fallback_team:
        role = fallback_role or 'PLAYER'
        return f"{name} ({role}, {fallback_team})"
    return name


def _player_statline(name, pstats):
    """Return a formatted stat line string for a player."""
    info = pstats.get(name, {})
    parts = []
    if info.get('rush_att', 0) > 0:
        ypc = info['rush_yards'] / info['rush_att'] if info['rush_att'] else 0
        parts.append(f"{info['rush_att']} carries, {info['rush_yards']} yards, {ypc:.1f} ypc")
    if info.get('rec_comp', 0) > 0:
        ypr = info['rec_yards'] / info['rec_comp'] if info['rec_comp'] else 0
        parts.append(f"{info['rec_comp']} catches, {info['rec_yards']} yards, {ypr:.1f} ypc")
    return ' | '.join(parts) if parts else 'no stats recorded'


def build_halftime_report(your_team, opp_team, your_stats, their_stats, plays, box_players=None):
    """Return {summary, win_bullets, your_performers, their_performers}."""
    pstats = plays.get('player_stats', {})
    box_players = box_players or []

    # Merge box_players into pstats where box data is richer
    for bp in box_players:
        name = bp['name']
        team = bp['team']
        pos = bp['pos']
        cat = bp.get('category', '')

        if name not in pstats:
            pstats[name] = {
                'team': team, 'role': pos,
                'rush_yards': 0, 'rush_att': 0,
                'rec_yards': 0, 'rec_att': 0, 'rec_comp': 0,
                'pass_yards': 0, 'pass_comp': 0, 'pass_att': 0,
                'tackles': 0,
            }

        ps = pstats[name]
        ps['team'] = team
        ps['role'] = pos
        # Ensure all fields exist
        for field in ['pass_yards', 'pass_comp', 'pass_att', 'tackles']:
            if field not in ps:
                ps[field] = 0

        if cat == 'receiving':
            catches = bp.get('catches', 0)
            yards = bp.get('yards', 0)
            # Use box score data if it has more catches (more complete)
            if catches > ps.get('rec_comp', 0):
                ps['rec_comp'] = catches
                ps['rec_att'] = max(catches, ps.get('rec_att', 0))
                ps['rec_yards'] = yards
        elif cat == 'rushing':
            carries = bp.get('carries', 0)
            yards = bp.get('yards', 0)
            if carries > ps.get('rush_att', 0):
                ps['rush_att'] = carries
                ps['rush_yards'] = yards
        elif cat == 'passing':
            comp = bp.get('comp', 0)
            att = bp.get('att', 0)
            yards = bp.get('yards', 0)
            if att > ps.get('pass_att', 0):
                ps['pass_comp'] = comp
                ps['pass_att'] = att
                ps['pass_yards'] = yards
        elif cat == 'defense':
            tackles = bp.get('tackles', 0)
            if tackles > ps.get('tackles', 0):
                ps['tackles'] = tackles

    # ── Section 1: First Half Summary (color-commentator paragraph) ─────────
    sentences = []

    # Score and margin
    ys, ts = your_stats.get('score'), their_stats.get('score')
    yi, ti = 0, 0
    if ys and ts:
        try:
            yi, ti = int(ys), int(ts)
            margin = abs(yi - ti)
            if yi > ti:
                sentences.append(f"We're at the half and {your_team} holds a {yi}–{ti} lead over {opp_team}, up by {margin}.")
            elif ti > yi:
                sentences.append(f"We're at the half and {opp_team} has taken a {ti}–{yi} lead, putting {your_team} down by {margin}.")
            else:
                sentences.append(f"We're at the half and it's all knotted up {yi}–{ti} between {your_team} and {opp_team}.")
        except ValueError:
            pass

    # Total yards
    yt, tt = your_stats.get('total_yards'), their_stats.get('total_yards')
    if yt and tt:
        sentences.append(f"{your_team} has racked up {yt} total yards while {opp_team} has put together {tt}.")

    # What's working offensively — YOUR team runs
    your_run_data = plays['your_runs']
    your_pass_data = plays['your_passes']
    if your_run_data:
        y_outside, y_inside = _group_runs(your_run_data)
        y_out_avg = sum(y_outside) / len(y_outside) if y_outside else 0
        y_in_avg = sum(y_inside) / len(y_inside) if y_inside else 0
        ytop_rush = [n for n, info in pstats.items() if info.get('role') == 'RB' and info.get('team') == your_team and info.get('rush_att', 0) > 0]
        rush_note = ""
        if ytop_rush:
            top_r = max(ytop_rush, key=lambda n: pstats[n].get('rush_yards', 0))
            rush_note = f", led by {_player_tag(top_r, pstats, your_team, 'RB')}"
        if y_outside and y_out_avg >= y_in_avg:
            sentences.append(
                f"On the ground, {your_team} has been effective running outside at {y_out_avg:.1f} ypc on {len(y_outside)} carries{rush_note}."
            )
        elif y_inside:
            sentences.append(
                f"On the ground, {your_team} has been effective running inside at {y_in_avg:.1f} ypc on {len(y_inside)} carries{rush_note}."
            )

    # YOUR team passes
    if your_pass_data:
        best_pd = max(your_pass_data, key=lambda d: sum(your_pass_data[d]['yards']) if your_pass_data[d]['comp'] > 0 else 0)
        pd = your_pass_data[best_pd]
        if pd['comp'] > 0:
            pavg = sum(pd['yards']) / pd['comp']
            ytop_rec = [n for n, info in pstats.items() if info.get('role') == 'WR' and info.get('team') == your_team and info.get('rec_comp', 0) > 0]
            rec_note = ""
            if ytop_rec:
                top_wr = max(ytop_rec, key=lambda n: pstats[n].get('rec_yards', 0))
                rec_note = f" with {_player_tag(top_wr, pstats, your_team, 'WR')} as the primary target"
            sentences.append(
                f"Through the air, {best_pd} passes have been the bread and butter — {pd['comp']}/{pd['att']} for {pavg:.0f} yards per completion{rec_note}."
            )

    # What's NOT working — YOUR team
    if your_run_data:
        y_outside, y_inside = _group_runs(your_run_data)
        y_out_avg = sum(y_outside) / len(y_outside) if y_outside else 0
        y_in_avg = sum(y_inside) / len(y_inside) if y_inside else 0
        if y_outside and y_inside:
            if y_out_avg < y_in_avg and y_out_avg < 3.0:
                sentences.append(
                    f"What is not working for {your_team}: outside runs have been a dead end, averaging just {y_out_avg:.1f} ypc on {len(y_outside)} attempts."
                )
            elif y_in_avg < y_out_avg and y_in_avg < 3.0:
                sentences.append(
                    f"What is not working for {your_team}: inside runs have been stuffed, averaging just {y_in_avg:.1f} ypc on {len(y_inside)} attempts."
                )

    # What's working — OPPONENT
    their_run_data = plays['their_runs']
    their_pass_data = plays['their_passes']
    opp_parts = []
    if their_run_data:
        t_outside, t_inside = _group_runs(their_run_data)
        t_out_avg = sum(t_outside) / len(t_outside) if t_outside else 0
        t_in_avg = sum(t_inside) / len(t_inside) if t_inside else 0
        ttop_rush = [n for n, info in pstats.items() if info.get('role') == 'RB' and info.get('team') == opp_team and info.get('rush_att', 0) > 0]
        rush_note = ""
        if ttop_rush:
            top_r = max(ttop_rush, key=lambda n: pstats[n].get('rush_yards', 0))
            rush_note = f" behind {_player_tag(top_r, pstats, opp_team, 'RB')}"
        if t_outside and t_out_avg >= t_in_avg:
            opp_parts.append(f"{opp_team} has found success running outside at {t_out_avg:.1f} ypc{rush_note}")
        elif t_inside:
            opp_parts.append(f"{opp_team} has found success running inside at {t_in_avg:.1f} ypc{rush_note}")

    if their_pass_data:
        best_td = max(their_pass_data, key=lambda d: sum(their_pass_data[d]['yards']) if their_pass_data[d]['comp'] > 0 else 0)
        td = their_pass_data[best_td]
        if td['comp'] > 0:
            tavg = sum(td['yards']) / td['comp']
            ttop_rec = [n for n, info in pstats.items() if info.get('role') == 'WR' and info.get('team') == opp_team and info.get('rec_comp', 0) > 0]
            rec_note = ""
            if ttop_rec:
                top_wr = max(ttop_rec, key=lambda n: pstats[n].get('rec_yards', 0))
                rec_note = f", targeting {_player_tag(top_wr, pstats, opp_team, 'WR')}"
            opp_parts.append(f"their {best_td} passing game has connected {td['comp']}/{td['att']}{rec_note}")

    if opp_parts:
        sentences.append('; '.join(opp_parts) + '.')

    # Top performers on each side
    ytop_all = sorted(plays['your_players'].items(), key=lambda x: -x[1])
    ttop_all = sorted(plays['their_players'].items(), key=lambda x: -x[1])
    perf_parts = []
    if ytop_all:
        name, yds = ytop_all[0]
        perf_parts.append(f"{_player_tag(name, pstats, your_team)} leads all {your_team} players with {yds} yards")
    if ttop_all:
        name, yds = ttop_all[0]
        perf_parts.append(f"{_player_tag(name, pstats, opp_team)} paces {opp_team} with {yds} yards")
    if perf_parts:
        sentences.append(', while '.join(perf_parts) + '.')

    # Third-down comparison
    y3, t3 = your_stats.get('third_down'), their_stats.get('third_down')
    if y3 or t3:
        chunk = []
        if y3: chunk.append(f"{your_team} is {y3} on third down")
        if t3: chunk.append(f"{opp_team} is {t3}")
        sentences.append('; '.join(chunk) + '.')

    # What the game is hinging on
    if ys and ts:
        try:
            margin = abs(yi - ti)
            if margin <= 7:
                sentences.append(f"This game is hinging on third-down execution and whoever can control the tempo coming out of the locker room.")
            elif yi > ti:
                sentences.append(f"This game hinges on whether {your_team} can keep the foot on the gas and prevent {opp_team} from finding a rhythm in the second half.")
            else:
                sentences.append(f"This game hinges on whether {your_team} can make the adjustments needed to claw back into it before the deficit grows.")
        except (ValueError, TypeError):
            pass
    else:
        sentences.append("This game is hinging on which coaching staff makes the better halftime adjustments.")

    summary = ' '.join(sentences) if sentences else (
        "Stats could not be fully parsed — recommendations below are drawn from available play-by-play data."
    )

    # ── Top Performers section ──────────────────────────────────────────────
    def _build_offense_performers(team_name):
        """Top 3 offensive skill players by yards (RB rush, WR/TE/RB rec, QB pass)."""
        candidates = []
        for name, info in pstats.items():
            if info.get('team') != team_name:
                continue
            role = info.get('role', '')
            # RB by rushing yards
            if role == 'RB' and info.get('rush_att', 0) > 0:
                ypc = info['rush_yards'] / info['rush_att']
                candidates.append((name, info['rush_yards'], info,
                    f"{info['rush_att']} carries, {info['rush_yards']} yards, {ypc:.1f} ypc"))
            # WR/TE/RB by receiving yards
            if info.get('rec_comp', 0) > 0:
                ypr = info['rec_yards'] / info['rec_comp']
                label = f"{info['rec_comp']} catches, {info['rec_yards']} yards, {ypr:.1f} ypc"
                # Only add as receiver if not already added as rusher, or if rec yards > rush yards
                if role != 'RB' or info.get('rec_yards', 0) > info.get('rush_yards', 0):
                    candidates.append((name, info['rec_yards'], info, label))
                elif role == 'RB':
                    # RB who also catches — add receiving as secondary
                    candidates.append((name, info['rec_yards'], info, label))
            # QB by passing yards
            if role == 'QB' and info.get('pass_att', 0) > 0:
                candidates.append((name, info['pass_yards'], info,
                    f"{info['pass_comp']}/{info['pass_att']} passing, {info['pass_yards']} yards"))

        # Deduplicate — keep the entry with the highest yards per player
        seen = {}
        for entry in candidates:
            name = entry[0]
            if name not in seen or entry[1] > seen[name][1]:
                seen[name] = entry
        candidates = list(seen.values())
        candidates.sort(key=lambda x: -x[1])

        cards = []
        for name, yds, info, statline in candidates[:3]:
            tag = _player_tag(name, pstats, team_name)
            role = info.get('role', '')
            if role == 'RB' and info.get('rush_att', 0) > 0:
                ypc = info['rush_yards'] / info['rush_att']
                if ypc >= 5.0:
                    note = f"Averaging {ypc:.1f} ypc — breaking tackles and finding lanes consistently."
                elif ypc >= 3.5:
                    note = f"Solid {ypc:.1f} ypc — grinding out tough yards and keeping the chains moving."
                else:
                    note = f"Only {ypc:.1f} ypc — the run blocking hasn't given him much room to work with."
            elif role in ('WR', 'TE') and info.get('rec_comp', 0) > 0:
                ypr = info['rec_yards'] / info['rec_comp']
                if ypr >= 15.0:
                    note = f"Averaging {ypr:.1f} yards per catch — a big-play threat all half."
                elif ypr >= 8.0:
                    note = f"Reliable at {ypr:.1f} yards per catch — getting open consistently."
                else:
                    note = f"Short-area work at {ypr:.1f} yards per catch — used on check-downs and screens."
            elif role == 'QB':
                note = f"Running the offense — {info.get('pass_yards', 0)} passing yards in the first half."
            else:
                note = f"Has contributed {yds} yards in the first half."
            cards.append({'name': name, 'tag': tag, 'statline': statline, 'note': note, 'side': 'offense'})
        return cards

    def _build_defense_performers(team_name):
        """Top 3 defensive players by tackles (DL, LB, DB all eligible)."""
        candidates = []
        for name, info in pstats.items():
            if info.get('team') != team_name:
                continue
            role = info.get('role', '')
            if role in ('DL', 'LB', 'DB') and info.get('tackles', 0) > 0:
                tackles = info['tackles']
                solo = info.get('solo', 0)
                statline = f"{tackles} tackles"
                if solo:
                    statline += f" ({solo} solo)"
                candidates.append((name, tackles, info, statline))
        candidates.sort(key=lambda x: -x[1])
        cards = []
        for name, tackles, info, statline in candidates[:3]:
            tag = _player_tag(name, pstats, team_name)
            if tackles >= 8:
                note = "All over the field — making plays in every phase of the defense."
            elif tackles >= 5:
                note = "Active and disruptive — consistently around the ball."
            else:
                note = "Contributing in a role capacity on defense."
            cards.append({'name': name, 'tag': tag, 'statline': statline, 'note': note, 'side': 'defense'})
        return cards

    your_off_performers = _build_offense_performers(your_team)
    your_def_performers = _build_defense_performers(your_team)
    their_off_performers = _build_offense_performers(opp_team)
    their_def_performers = _build_defense_performers(opp_team)
    your_performers = your_off_performers + your_def_performers
    their_performers = their_off_performers + their_def_performers

    # ── Section 2: What To Do To Win The Second Half ────────────────────────
    win_bullets = []

    # ── RUN GAME ────────────────────────────────────────────────────────────
    run_data = plays['your_runs']
    if run_data:
        gp_outside, gp_inside = _group_runs(run_data)
        gp_out_avg = sum(gp_outside) / len(gp_outside) if gp_outside else 0
        gp_in_avg = sum(gp_inside) / len(gp_inside) if gp_inside else 0

        if gp_outside and gp_inside:
            if gp_out_avg > gp_in_avg:
                win_bullets.append(
                    f"▶ Run outside — averaging {gp_out_avg:.1f} ypc vs {gp_in_avg:.1f} inside, attack the perimeter on 1st and 2nd down"
                )
            else:
                win_bullets.append(
                    f"▶ Run inside — averaging {gp_in_avg:.1f} ypc vs {gp_out_avg:.1f} outside, pound it between the tackles"
                )
        elif gp_outside:
            win_bullets.append(
                f"▶ Run outside — averaging {gp_out_avg:.1f} ypc, attack the perimeter on 1st and 2nd down"
            )
        elif gp_inside:
            win_bullets.append(
                f"▶ Run inside — averaging {gp_in_avg:.1f} ypc, pound it between the tackles on early downs"
            )

        # Name the best RB
        your_rbs = [(n, info) for n, info in pstats.items()
                    if info.get('team') == your_team and info.get('role') == 'RB' and info.get('rush_att', 0) > 0]
        if your_rbs:
            best_rb_name, best_rb_info = max(your_rbs, key=lambda x: x[1]['rush_yards'])
            rb_tag = _player_tag(best_rb_name, pstats, your_team, 'RB')
            win_bullets.append(
                f"▶ Give {rb_tag} the bulk of carries — {best_rb_info['rush_yards']} yards on {best_rb_info['rush_att']} carries"
            )

    # ── PASSING GAME ────────────────────────────────────────────────────────
    # Get ALL receivers (WR + TE) sorted by yards
    your_receivers = [(n, info) for n, info in pstats.items()
                      if info.get('team') == your_team
                      and info.get('role') in ('WR', 'TE')
                      and info.get('rec_comp', 0) > 0]
    your_receivers.sort(key=lambda x: x[1]['rec_yards'], reverse=True)

    if your_receivers:
        top_rec_name, top_rec_info = your_receivers[0]
        top_rec_tag = _player_tag(top_rec_name, pstats, your_team)
        win_bullets.append(
            f"▶ Keep targeting {top_rec_tag} — {top_rec_info['rec_comp']} catches for {top_rec_info['rec_yards']} yards, he is your most reliable weapon"
        )

        # Look for a TE mismatch
        te_receivers = [(n, info) for n, info in your_receivers if info.get('role') == 'TE']
        if te_receivers:
            te_name, te_info = te_receivers[0]
            if te_info['rec_yards'] >= 20:
                te_tag = _player_tag(te_name, pstats, your_team, 'TE')
                if te_name != top_rec_name:
                    win_bullets.append(
                        f"▶ Your TE {te_tag} has {te_info['rec_yards']} yards — keep attacking linebackers with him underneath"
                    )

        # Look for underused weapon — has yards but few touches
        for rec_name, rec_info in your_receivers[1:]:
            if rec_info['rec_comp'] <= 2 and rec_info['rec_yards'] >= 15:
                rec_tag = _player_tag(rec_name, pstats, your_team)
                ypr = rec_info['rec_yards'] / rec_info['rec_comp'] if rec_info['rec_comp'] > 0 else 0
                win_bullets.append(
                    f"▶ Get {rec_tag} more involved — only {rec_info['rec_comp']} touches but showing {ypr:.0f} yards per catch potential"
                )
                break

    # ── THIRD DOWN ──────────────────────────────────────────────────────────
    thirds = plays['your_3rds']
    if thirds:
        conv = [t for t in thirds if t['success']]
        pass3 = [t for t in conv if t.get('type') == 'pass']
        rush3 = [t for t in conv if t.get('type') == 'rush']
        if pass3 and len(pass3) >= len(rush3):
            if your_receivers:
                best_3rd_target = _player_tag(your_receivers[0][0], pstats, your_team)
                win_bullets.append(
                    f"▶ On 3rd down keep throwing the ball — passing converted {len(pass3)} times in the first half, look for {best_3rd_target} in those situations"
                )
            else:
                win_bullets.append(
                    f"▶ On 3rd down keep throwing the ball — passing converted {len(pass3)} times in the first half, stick with what is working"
                )
        elif rush3:
            your_rbs2 = [(n, info) for n, info in pstats.items()
                        if info.get('team') == your_team and info.get('role') == 'RB' and info.get('rush_att', 0) > 0]
            if your_rbs2:
                rb_name = max(your_rbs2, key=lambda x: x[1]['rush_yards'])[0]
                rb_3rd_tag = _player_tag(rb_name, pstats, your_team, 'RB')
                win_bullets.append(
                    f"▶ On 3rd and short hand it to {rb_3rd_tag} — the run game converted {len(rush3)} times in the first half, trust your offensive line"
                )
            else:
                win_bullets.append(
                    f"▶ On 3rd and short keep running the ball — the ground game converted {len(rush3)} times in the first half"
                )

    # ── SCORE SITUATION ────────────────────────────────────────────────────
    if ys and ts:
        try:
            deficit = ti - yi
            if deficit > 0 and deficit <= 8:
                win_bullets.append(
                    f"▶ You are within striking distance — score first in the third quarter and this game changes"
                )
            elif deficit > 8:
                win_bullets.append(
                    f"▶ You are down {deficit} — go aggressive immediately, cannot afford to trade punts"
                )
            elif deficit == 0:
                win_bullets.append(
                    f"▶ It is tied — come out of halftime and set the tempo, score first and make {opp_team} chase you"
                )
            elif abs(deficit) >= 14:
                win_bullets.append(
                    f"▶ You have the lead — establish the run early in the third to control clock and force {opp_team} to chase"
                )
            else:
                win_bullets.append(
                    f"▶ You have the lead — keep doing what got you here, stay aggressive and do not let {opp_team} hang around"
                )
        except (ValueError, TypeError):
            pass
    else:
        win_bullets.append(f"▶ Come out of halftime with energy — establish the run early and force {opp_team} to adjust to you")

    if not win_bullets:
        win_bullets = ["Paste play-by-play data to generate specific second-half recommendations."]

    return {
        'summary': summary,
        'win_bullets': win_bullets,
        'your_performers': your_performers,
        'their_performers': their_performers,
    }


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.route("/internal")
def internal_access():
    session.permanent = True
    session['internal_access'] = True
    return redirect(url_for('index'))

@app.route("/")
def landing():
    if session.get('internal_access'):
        return redirect(url_for('index'))
    if current_user.is_authenticated and current_user.subscribed:
        return redirect(url_for('index'))
    return render_template("landing.html")

@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "GET":
        return render_template("signup.html")
    email = request.form.get("email", "").strip().lower()
    password = request.form.get("password", "").strip()
    if not email or not password:
        return render_template("signup.html", error="Email and password are required.")
    if len(password) < 6:
        return render_template("signup.html", error="Password must be at least 6 characters.")
    conn = get_db()
    existing = conn.execute('SELECT id FROM users WHERE email = ?', (email,)).fetchone()
    if existing:
        conn.close()
        return render_template("signup.html", error="An account with that email already exists.")
    hashed = generate_password_hash(password, method='pbkdf2:sha256')
    cursor = conn.execute('INSERT INTO users (email, password) VALUES (?, ?)', (email, hashed))
    user_id = cursor.lastrowid
    conn.commit()
    row = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
    conn.close()
    user = User(**dict(row))
    login_user(user)
    return redirect(url_for('checkout'))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html")
    email = request.form.get("email", "").strip().lower()
    password = request.form.get("password", "").strip()
    conn = get_db()
    row = conn.execute('SELECT * FROM users WHERE email = ?', (email,)).fetchone()
    conn.close()
    if not row or not check_password_hash(row['password'], password):
        return render_template("login.html", error="Invalid email or password.")
    user = User(**dict(row))
    login_user(user)
    if user.subscribed:
        return redirect(url_for('index'))
    return redirect(url_for('checkout'))

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for('landing'))

@app.route("/checkout")
@login_required
def checkout():
    try:
        session = stripe.checkout.Session.create(
            customer_email=current_user.email,
            payment_method_types=['card'],
            line_items=[{'price': STRIPE_PRICE_ID, 'quantity': 1}],
            mode='subscription',
            success_url=request.host_url.rstrip('/') + url_for('payment_success') + '?session_id={CHECKOUT_SESSION_ID}',
            cancel_url=request.host_url.rstrip('/') + url_for('payment_cancel'),
            metadata={'user_id': str(current_user.id)},
        )
        return redirect(session.url, code=303)
    except Exception as e:
        return render_template("login.html", error=f"Payment error: {e}")

@app.route("/success")
@login_required
def payment_success():
    session_id = request.args.get('session_id')
    if session_id:
        try:
            session = stripe.checkout.Session.retrieve(session_id)
            customer_id = session.get('customer')
            conn = get_db()
            conn.execute('UPDATE users SET subscribed = 1, stripe_customer_id = ? WHERE id = ?',
                         (customer_id, current_user.id))
            conn.commit()
            conn.close()
        except Exception:
            pass
    return redirect(url_for('index'))

@app.route("/cancel")
@login_required
def payment_cancel():
    return render_template("cancel.html")

@app.route("/account")
@login_required
def account():
    return render_template("account.html", user=current_user)

@app.route("/manage-subscription")
@login_required
def manage_subscription():
    if not current_user.stripe_customer_id:
        return redirect(url_for('account'))
    try:
        session = stripe.billing_portal.Session.create(
            customer=current_user.stripe_customer_id,
            return_url=request.host_url.rstrip('/') + url_for('account'),
        )
        return redirect(session.url, code=303)
    except Exception as e:
        return render_template("account.html", user=current_user, error=f"Could not open billing portal: {e}")

@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    sent = False
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        conn = get_db()
        user = conn.execute('SELECT id FROM users WHERE email = ?', (email,)).fetchone()
        conn.close()
        if user and app.config['MAIL_USERNAME']:
            token = serializer.dumps(email, salt='password-reset')
            reset_url = request.host_url.rstrip('/') + url_for('reset_password', token=token)
            try:
                msg = Message('Football Scout — Password Reset',
                              recipients=[email])
                msg.html = f'''<p>You requested a password reset for Football Scout.</p>
                <p><a href="{reset_url}">Click here to reset your password</a></p>
                <p>This link expires in 1 hour. If you didn't request this, ignore this email.</p>'''
                mail.send(msg)
            except Exception as e:
                print(f">>> Mail send error: {e}", flush=True)
        sent = True
    return render_template("forgot_password.html", sent=sent)

@app.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    try:
        email = serializer.loads(token, salt='password-reset', max_age=3600)
    except Exception:
        return render_template("forgot_password.html", sent=False, error="This reset link is invalid or has expired.")
    if request.method == "GET":
        return render_template("reset_password.html", token=token)
    password = request.form.get("password", "").strip()
    if len(password) < 6:
        return render_template("reset_password.html", token=token, error="Password must be at least 6 characters.")
    hashed = generate_password_hash(password, method='pbkdf2:sha256')
    conn = get_db()
    conn.execute('UPDATE users SET password = ? WHERE email = ?', (hashed, email))
    conn.commit()
    conn.close()
    return redirect(url_for('login'))

@app.route("/cancel-subscription", methods=["POST"])
@login_required
def cancel_subscription():
    if current_user.stripe_customer_id:
        try:
            subscriptions = stripe.Subscription.list(customer=current_user.stripe_customer_id, status='active')
            for sub in subscriptions.data:
                stripe.Subscription.cancel(sub.id)
            conn = get_db()
            conn.execute('UPDATE users SET subscribed = 0 WHERE id = ?', (current_user.id,))
            conn.commit()
            conn.close()
        except Exception:
            pass
    return redirect(url_for('account'))

@app.route("/scout", methods=["GET"])
@subscription_required
def index():
    return render_template("index.html")


@app.route("/analyze", methods=["POST"])
@subscription_required
def analyze_route():
    team = request.form.get("team", "").strip()
    sheets_url = request.form.get("sheets_url", "").strip()
    uploaded_file = request.files.get("csv_file")
    error = None
    total_plays = 0
    results = {}

    formations = {}
    all_formation_names = []

    if not team:
        error = "Please enter a team name."
    elif uploaded_file and uploaded_file.filename:
        try:
            csv_text = uploaded_file.read().decode("utf-8-sig")
            total_plays, results, formations, all_formation_names = analyze_text(team, csv_text)
            if total_plays == 0:
                error = f"No plays found for '{team}' in the uploaded file."
        except Exception as e:
            error = f"Error processing file: {e}"
    elif sheets_url:
        csv_url = sheets_url_to_csv(sheets_url)
        try:
            total_plays, results, formations, all_formation_names = analyze(team, csv_url)
            if total_plays == 0:
                error = f"No plays found for '{team}'. Check the team name and that the sheet is publicly accessible."
        except requests.exceptions.RequestException as e:
            error = f"Could not fetch the sheet: {e}"
        except Exception as e:
            error = f"Error processing data: {e}"
    else:
        error = "Please provide a Google Sheets URL or upload a CSV file."

    return render_template(
        "index.html",
        team=team, sheets_url=sheets_url,
        total_plays=total_plays, results=results,
        formations=formations,
        all_formation_names=all_formation_names,
        error=error, submitted=True,
    )


@app.route("/strategy", methods=["POST"])
@subscription_required
def strategy_route():
    opponent_team        = request.form.get("opponent_team", "").strip()
    your_team            = request.form.get("your_team", "").strip()
    opponent_ratings_raw = request.form.get("opponent_ratings", "").strip()
    your_ratings_raw     = request.form.get("your_ratings", "").strip()

    error = None
    matchups = []
    game_plan = []
    advantages = []
    dangers = []

    if not opponent_team or not your_team:
        error = "Please enter both team names."
    elif not opponent_ratings_raw or not your_ratings_raw:
        error = "Please paste ratings for both teams."
    else:
        # Your Team  = OFFENSE (OL, RB, WR, TE)
        # Opponent   = DEFENSE (DL, LB, DB)
        offense_r = parse_ratings(your_ratings_raw)
        defense_r = parse_ratings(opponent_ratings_raw)

        if not defense_r:
            error = f"Could not parse {opponent_team} ratings. Check the format."
        elif not offense_r:
            error = f"Could not parse {your_team} ratings. Check the format."
        else:
            matchups           = compute_matchups(offense_r, defense_r)
            game_plan          = build_game_plan(matchups)
            advantages, dangers = find_individual_edges(
                offense_r, defense_r, your_team, opponent_team
            )

    return render_template(
        "strategy.html",
        opponent_team=opponent_team,
        your_team=your_team,
        opponent_ratings_raw=opponent_ratings_raw,
        your_ratings_raw=your_ratings_raw,
        matchups=matchups,
        game_plan=game_plan,
        advantages=advantages,
        dangers=dangers,
        error=error,
    )


@app.route("/halftime-advisor", methods=["GET"])
@subscription_required
def halftime_advisor():
    return render_template("halftime.html", your_team='', opp_team='', box_raw='', gamelog_raw='', report={}, error=None)

@app.route("/halftime", methods=["POST"])
@subscription_required
def halftime_route():
    your_team    = request.form.get("ht_your_team",    "").strip()
    opp_team     = request.form.get("ht_opp_team",     "").strip()
    box_raw      = request.form.get("ht_box_score",    "").strip()
    gamelog_raw  = request.form.get("ht_game_log",     "").strip()

    try:
        print(f"\n>>> HALFTIME game_log first 100 chars: {gamelog_raw[:100]!r}", flush=True)
    except BrokenPipeError:
        pass
    with open("/tmp/football_debug.txt", "w") as fh:
        fh.write(f"box_score full:\n{box_raw}\n\n{'='*70}\n\ngame_log first 500:\n{gamelog_raw[:500]}\n")

    error   = None
    report  = {}

    if not your_team or not opp_team:
        error = "Please enter both team names."
    elif not box_raw and not gamelog_raw:
        error = "Please paste at least a box score or game log."
    else:
        your_stats, their_stats, box_players = parse_box_score(box_raw, your_team, opp_team)
        plays = parse_game_log(gamelog_raw, your_team, opp_team)
        report = build_halftime_report(your_team, opp_team, your_stats, their_stats, plays, box_players)

    return render_template(
        "halftime.html",
        your_team=your_team, opp_team=opp_team,
        box_raw=box_raw, gamelog_raw=gamelog_raw,
        report=report, error=error,
    )


@app.route("/training")
@subscription_required
def training():
    conn = get_db()
    videos = conn.execute('SELECT * FROM videos ORDER BY created_at DESC').fetchall()
    conn.close()
    return render_template("training.html", videos=videos)

@app.route("/admin", methods=["GET", "POST"])
def admin():
    action = request.form.get("action", "")
    if action == "add" and request.method == "POST":
        title = request.form.get("title", "").strip()
        description = request.form.get("description", "").strip()
        video_url = request.form.get("video_url", "").strip()
        if title and video_url:
            embed_url = to_embed_url(video_url)
            conn = get_db()
            conn.execute('INSERT INTO videos (title, description, video_url) VALUES (?, ?, ?)',
                         (title, description, embed_url))
            conn.commit()
            conn.close()
    elif action == "delete" and request.method == "POST":
        video_id = request.form.get("video_id")
        if video_id:
            conn = get_db()
            conn.execute('DELETE FROM videos WHERE id = ?', (video_id,))
            conn.commit()
            conn.close()

    conn = get_db()
    videos = conn.execute('SELECT * FROM videos ORDER BY created_at DESC').fetchall()
    conn.close()
    return render_template("admin.html", videos=videos)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5003))
    app.run(host='0.0.0.0', port=port)
