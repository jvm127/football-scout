from flask import Flask, render_template, request, redirect, url_for, flash, session
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from datetime import timedelta
from flask_mail import Mail, Message
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer
from dotenv import load_dotenv
import anthropic
import requests
import stripe
import sqlite3
import csv
import io
import traceback
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
        display_order INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    # Add display_order column if missing (existing databases)
    try:
        conn.execute('ALTER TABLE videos ADD COLUMN display_order INTEGER DEFAULT 0')
    except Exception:
        pass
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

POSITIONS = ['QB', 'FB', 'RB', 'WR', 'TE', 'OL', 'DL', 'LB', 'DB']

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
    'short_yardage': {
        'major': 'Run between the tackles — your RB wins every collision',
        'slight': 'Mix in inside runs on favorable downs and short yardage',
        'avoid': 'Avoid inside runs — their LBs win the physical matchup',
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
    'te_coverage': {
        'major': 'Target your TE relentlessly — massive overall edge vs their LBs',
        'slight': 'TE seam routes and drag routes are a reliable check-down option',
        'avoid': 'Their LBs can cover your TE — spread with extra WRs instead',
    },
    'te_middle': {
        'major': 'TE over the middle is a free completion — their DBs cannot keep up',
        'slight': 'TE crossing routes have real upside against their secondary',
        'avoid': 'Their DBs can match your TE speed — use other options',
    },
}

# ─── Formation / Defense personnel ──────────────────────────────────────────

OFFENSE_FORMATIONS = {
    'I Formation':      {'RB': 1, 'WR': 2, 'TE': 1},
    'Pro':              {'RB': 1, 'WR': 2, 'TE': 1},
    'Wishbone':         {'FB': 1, 'RB': 2, 'WR': 1, 'TE': 1},
    'Notre Dame Box':   {'RB': 2, 'WR': 1, 'TE': 2},
    'Shotgun':          {'RB': 0, 'WR': 4, 'TE': 1},
    'Trips':            {'RB': 1, 'WR': 3, 'TE': 1},
}

DEFENSE_FORMATIONS = {
    '3-4':    {'DL': 3, 'LB': 4, 'DB': 4},
    '4-3':    {'DL': 4, 'LB': 3, 'DB': 4},
    '4-4':    {'DL': 4, 'LB': 4, 'DB': 3},
    '5-2':    {'DL': 5, 'LB': 2, 'DB': 4},
    'Nickel': {'DL': 4, 'LB': 2, 'DB': 5},
    'Dime':   {'DL': 3, 'LB': 2, 'DB': 6},
}


def get_formation_matchup_note(off_form, def_form):
    """Return a formation-vs-formation personnel insight or empty string."""
    if not off_form or not def_form:
        return ''
    o = off_form.strip()
    d = def_form.strip()
    off_pers = OFFENSE_FORMATIONS.get(o, {})
    def_pers = DEFENSE_FORMATIONS.get(d, {})
    if not off_pers or not def_pers:
        return ''

    notes = []
    # Wishbone (1 FB, 2 RB, 1 TE) vs Nickel/Dime (2 LB)
    if o == 'Wishbone' and d in ('Nickel', 'Dime'):
        notes.append(f'Your 3 ball carriers (FB, RB1, RB2) overpower their light box — run it down their throat, they only have {def_pers.get("DL",3)+def_pers.get("LB",2)} in the box')
    # ND Box (2 RB, 2 TE) vs Nickel/Dime (2 LB)
    if o == 'Notre Dame Box' and d in ('Nickel', 'Dime'):
        notes.append(f'Your 2 TEs force them to cover with DBs or give up easy completions underneath — exploit this all game')
    # Shotgun (4 WR) vs 4-4 (3 DB)
    if o == 'Shotgun' and d == '4-4':
        notes.append(f'Your 4 WRs vs their 3 DBs — at least one WR gets a LB in coverage every play, find the mismatch and attack it relentlessly')
    # Trips (3 WR) vs 4-4 (3 DB)
    if o == 'Trips' and d == '4-4':
        notes.append(f'One of your WRs will get a LB in coverage every play — identify which LB is slowest and attack him all game')
    # Shotgun (4 WR) vs Nickel (5 DB)
    if o == 'Shotgun' and d == 'Nickel':
        notes.append(f'Your 4 WRs vs their 5 DBs — they can match up but one DB must cover two zones, find the soft spot')
    # Trips vs Nickel (5 DB)
    if o == 'Trips' and d == 'Nickel':
        notes.append(f'They have enough DBs to match your WRs — win with route running, exploit WR AGI vs DB AGI edges')
    # I Formation/Pro vs 5-2 (5 DL)
    if o in ('I Formation', 'Pro') and d == '5-2':
        notes.append(f'Their 5 DL will overpower your OL in the run game — consider spreading them out with Shotgun or Trips')
    # ND Box (2 RB, 2 TE) vs Dime (2 LB)
    if o == 'Notre Dame Box' and d == 'Dime':
        notes.append(f'Perfect formation vs their pass defense — your RBs and TEs will overpower their 2 LBs, run it early and often')
    # Wishbone (1 FB, 2 RB) vs Dime (2 LB)
    if o == 'Wishbone' and d == 'Dime':
        notes.append(f'Your Wishbone with FB and 2 RBs vs their Dime — they cannot stop the run with only 2 LBs, pound it inside with your FB and outside with your RBs')
    # Wishbone vs 3-4 (4 LB)
    if o == 'Wishbone' and d == '3-4':
        notes.append(f'They have 4 LBs to match your run-heavy Wishbone — FB inside runs will be contested, use RB speed to attack the edges')
    # Wishbone vs 5-2 (5 DL)
    if o == 'Wishbone' and d == '5-2':
        notes.append(f'Their 5 DL clogs the inside — your FB power runs will be tough, use your RBs on sweeps and tosses to get outside')

    return ' | '.join(notes)


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


def parse_players(text):
    """Parse individual player names and TOT ratings from raw ratings text.
    Returns list of {'name': str, 'pos': str, 'tot': int, 'stats': {stat: val}}."""
    pos_set = set(POSITIONS)
    lines = [l for l in text.splitlines() if l.strip()]

    header_idx = None
    pos_col = None
    name_col = None
    stat_cols = []

    for i, line in enumerate(lines):
        tokens = re.split(r'\t', line)
        if len(tokens) < 3:
            continue
        normed = [STAT_MAP.get(t.strip().lower()) for t in tokens]
        stat_count = sum(1 for n in normed if n)
        pos_col_candidate = next(
            (j for j, t in enumerate(tokens) if t.strip().lower() == 'pos'), None
        )
        # Look for a name column
        name_col_candidate = next(
            (j for j, t in enumerate(tokens) if t.strip().lower() in ('name', 'player')), None
        )
        if stat_count >= 3 and pos_col_candidate is not None:
            header_idx = i
            pos_col = pos_col_candidate
            name_col = name_col_candidate
            stat_cols = [(j, normed[j]) for j in range(len(normed)) if normed[j]]
            break

    if header_idx is None:
        return []

    # If no explicit name column, use column 0 if pos_col != 0, else column 1
    if name_col is None:
        name_col = 0 if pos_col != 0 else 1

    players = []
    for line in lines[header_idx + 1:]:
        tokens = re.split(r'\t', line)
        if len(tokens) <= max(pos_col, name_col):
            continue
        tokens = [t.strip().lstrip('*').strip() for t in tokens]
        pos = tokens[pos_col].upper()
        if pos not in pos_set:
            continue
        name = tokens[name_col]
        if not name or name.upper() in pos_set:
            continue
        stats = {}
        for col_j, stat in stat_cols:
            if col_j < len(tokens):
                try:
                    stats[stat] = int(float(tokens[col_j]))
                except (ValueError, TypeError):
                    pass
        tot = stats.get('TOT', 0)
        players.append({'name': name, 'pos': pos, 'tot': tot, 'stats': stats})

    return players


def find_standout_players(your_players, opp_players):
    """Return standout players dicts for display.
    Top 2 offensive (RB, WR, TE, QB) and top 2 defensive (DL, LB, DB) by TOT for each team."""
    off_pos = {'QB', 'RB', 'WR', 'TE'}
    def_pos = {'DL', 'LB', 'DB'}

    def _top(players, positions, n=2):
        eligible = [p for p in players if p['pos'] in positions and p['tot'] > 0]
        eligible.sort(key=lambda x: -x['tot'])
        return eligible[:n]

    def _off_sentence(p):
        pos = p['pos']
        tot = p['tot']
        stats = p['stats']
        if pos == 'QB':
            return f"Elite quarterback with {tot} TOT — can make every throw and extends plays."
        if pos == 'RB':
            spd = stats.get('SPD', '?')
            stren = stats.get('STR', '?')
            return f"Your most explosive weapon. {spd} speed and {stren} strength make him a mismatch on every play."
        if pos == 'WR':
            spd = stats.get('SPD', '?')
            agi = stats.get('A', '?')
            return f"Deep threat with {spd} speed and {agi} agility — can beat any DB one-on-one."
        if pos == 'TE':
            spd = stats.get('SPD', '?')
            stren = stats.get('STR', '?')
            return f"Versatile weapon with {spd} speed and {stren} strength — creates mismatches vs LBs."
        return f"{tot} TOT — impact player."

    def _def_sentence(p):
        pos = p['pos']
        stats = p['stats']
        if pos == 'DL':
            stren = stats.get('STR', '?')
            spd = stats.get('SPD', '?')
            return f"Dominant presence on the line — {stren} strength and {spd} speed disrupt every play."
        if pos == 'LB':
            spd = stats.get('SPD', '?')
            tkl = stats.get('TKL', '?')
            return f"Sideline-to-sideline player — {spd} speed and {tkl} tackling shut down the run and passing lanes."
        if pos == 'DB':
            spd = stats.get('SPD', '?')
            agi = stats.get('A', '?')
            return f"Lockdown coverage — {spd} speed and {agi} agility to shadow any receiver."
        return f"Impact defender."

    def _opp_def_sentence(p):
        return _def_sentence(p).rstrip('.') + f" — watch out for {p['name']}, he will be a problem."

    your_off = _top(your_players, off_pos)
    your_def = _top(your_players, def_pos)
    opp_off = _top(opp_players, off_pos)
    opp_def = _top(opp_players, def_pos)

    return {
        'your_offense': [{'name': p['name'], 'pos': p['pos'], 'tot': p['tot'],
                          'sentence': _off_sentence(p)} for p in your_off],
        'your_defense': [{'name': p['name'], 'pos': p['pos'], 'tot': p['tot'],
                          'sentence': _def_sentence(p)} for p in your_def],
        'opp_offense':  [{'name': p['name'], 'pos': p['pos'], 'tot': p['tot'],
                          'sentence': _off_sentence(p)} for p in opp_off],
        'opp_defense':  [{'name': p['name'], 'pos': p['pos'], 'tot': p['tot'],
                          'sentence': _opp_def_sentence(p)} for p in opp_def],
    }


def _stat(ratings, pos, stat):
    """Return a stat value or None."""
    return ratings.get(pos, {}).get(stat)


def _tier(edge, key=None):
    """Return (css_class, icon, short_label) for an edge value.
    Outside run uses different thresholds: +30 Major, +15 Slight, <+15 Even.
    """
    if edge is None:
        return 'unknown', '—', 'No data'
    if key == 'outside_run':
        if edge >= 30:
            return 'major',  '✅', 'Major Advantage'
        if edge >= 15:
            return 'slight', '⚠️', 'Slight Advantage'
        if edge >= -14:
            return 'even',   '–',  'Even Matchup'
        return     'avoid',  '❌', 'Avoid'
    if edge >= 10:
        return 'major',  '✅', 'Major Advantage'
    if edge >= 5:
        return 'slight', '⚠️', 'Slight Advantage'
    if edge >= -4:
        return 'even',   '–',  'Even Matchup'
    return     'avoid',  '❌', 'Avoid'


_RUN_MATCHUP_KEYS = {'run_block', 'power_run', 'outside_run', 'short_yardage'}


def compute_matchups(offense_r, defense_r, off_form=None):
    """Return sorted list of matchup dicts, best edge first.
    Shotgun: only passing matchups (no RB on the field)."""

    def edge(off_pos, off_stat, def_pos, def_stat):
        off = _stat(offense_r, off_pos, off_stat)
        dfn = _stat(defense_r, def_pos, def_stat)
        return (off - dfn) if (off is not None and dfn is not None) else None

    raw = [
        # RUN GAME
        ('run_block',    'Run Blocking',  'Your OL BLK vs Their DL STR',   edge('OL', 'BLK', 'DL', 'STR')),
        ('power_run',   'Power Run',      'Your OL STR vs Their DL STR',   edge('OL', 'STR', 'DL', 'STR')),
        ('outside_run', 'Outside Run',    'Your RB SPD vs Their LB SPD',   edge('RB', 'SPD', 'LB', 'SPD')),
        ('short_yardage','Short Yardage', 'Your RB STR vs Their LB STR',   edge('RB', 'STR', 'LB', 'STR')),
        # PASSING GAME
        ('deep_pass',   'Deep Pass',      'Your WR SPD vs Their DB SPD',   edge('WR', 'SPD', 'DB', 'SPD')),
        ('route_run',   'Route Running',  'Your WR AGI vs Their DB AGI',   edge('WR', 'A',   'DB', 'A')),
        ('te_coverage', 'TE Coverage',    'Your TE TOT vs Their LB TOT',   edge('TE', 'TOT', 'LB', 'TOT')),
        ('te_middle',   'TE Over Middle', 'Your TE SPD vs Their DB SPD',   edge('TE', 'SPD', 'DB', 'SPD')),
    ]

    # Shotgun: exclude run matchups entirely — no RB on the field
    if off_form == 'Shotgun':
        raw = [(k, l, d, e) for k, l, d, e in raw if k not in _RUN_MATCHUP_KEYS]

    matchups = []
    for key, label, desc, e in raw:
        css, icon, short = _tier(e, key=key)
        rec = RECOMMENDATIONS.get(key, {}).get(css, '')
        matchups.append({
            'key': key, 'label': label, 'desc': desc,
            'edge': e, 'tier': css, 'icon': icon,
            'short': short, 'rec': rec,
        })

    matchups.sort(key=lambda m: -(m['edge'] if m['edge'] is not None else -999))
    return matchups


# ─── Mismatch scanning: only the 8 meaningful comparisons ───────────────────

MISMATCH_COMPARISONS = [
    ('OL', 'BLK', 'DL', 'STR', 'OL Blocking vs DL Strength'),
    ('OL', 'STR', 'DL', 'STR', 'OL Strength vs DL Strength'),
    ('RB', 'SPD', 'LB', 'SPD', 'RB Speed vs LB Speed'),
    ('RB', 'STR', 'LB', 'STR', 'RB Strength vs LB Strength'),
    ('WR', 'SPD', 'DB', 'SPD', 'WR Speed vs DB Speed'),
    ('WR', 'A',   'DB', 'A',   'WR Agility vs DB Agility'),
    ('TE', 'TOT', 'LB', 'TOT', 'TE Overall vs LB Overall'),
    ('TE', 'SPD', 'DB', 'SPD', 'TE Speed vs DB Speed'),
]

MISMATCH_THRESHOLD = 20


def _mismatch_narrative(off_pos, off_stat, off_val, def_pos, def_stat, def_val, edge_val):
    stat_names = {'SPD': 'speed', 'STR': 'strength', 'A': 'agility', 'BLK': 'blocking', 'TKL': 'tackling', 'TOT': 'overall'}
    off_name = stat_names.get(off_stat, off_stat)
    def_name = stat_names.get(def_stat, def_stat)
    return (
        f"Your {off_pos} {off_name} ({off_val}) vs their {def_pos} {def_name} ({def_val}) "
        f"creates a +{edge_val} mismatch. This is a significant edge — exploit it every series."
    )


def _mismatch_rec(off_pos, off_stat, def_pos, edge_val):
    if off_pos == 'OL' and off_stat == 'BLK':
        return f"Commit to the run — your OL blocking edge is +{edge_val} over their DL tackling."
    if off_pos == 'OL' and off_stat == 'STR':
        return f"Power runs and goal line plays — your OL is +{edge_val} stronger than their DL."
    if off_pos == 'RB' and off_stat == 'SPD':
        return f"Toss sweeps and outside zone — your RB has a +{edge_val} speed edge on their LBs."
    if off_pos == 'RB' and off_stat == 'STR':
        return f"Power runs inside — your RB is +{edge_val} stronger than their LBs."
    if off_pos == 'WR' and off_stat == 'SPD':
        return f"Go routes and post routes — your WRs burn their DBs with a +{edge_val} speed edge."
    if off_pos == 'WR' and off_stat == 'A':
        return f"Crossing routes and option routes — your WRs have a +{edge_val} agility edge at the break."
    if off_pos == 'TE' and off_stat == 'TOT':
        return f"Target your TE relentlessly — +{edge_val} overall edge vs their LBs in coverage."
    if off_pos == 'TE' and off_stat == 'SPD':
        return f"TE crossing routes over the middle — your TE has a +{edge_val} speed edge over their DBs."
    return f"Exploit this +{edge_val} edge with {off_pos} vs their {def_pos}."


def _danger_narrative(off_pos, off_stat, off_val, def_pos, def_stat, def_val, edge_val):
    stat_names = {'SPD': 'speed', 'STR': 'strength', 'A': 'agility', 'BLK': 'blocking', 'TKL': 'tackling', 'TOT': 'overall'}
    off_name = stat_names.get(off_stat, off_stat)
    def_name = stat_names.get(def_stat, def_stat)
    return (
        f"Their {def_pos} {def_name} ({def_val}) vs your {off_pos} {off_name} ({off_val}) "
        f"gives them a {edge_val} advantage. Avoid isolating this matchup."
    )


def _danger_rec(off_pos, off_stat, def_pos, edge_val):
    if off_pos == 'OL' and off_stat == 'BLK':
        return f"Limit run plays — their DL tackling is {edge_val} better than your OL blocking."
    if off_pos == 'OL' and off_stat == 'STR':
        return f"Avoid power runs — their DL is {edge_val} stronger than your OL."
    if off_pos == 'RB' and off_stat == 'SPD':
        return f"Avoid outside runs — their LBs have a {edge_val} speed edge over your RBs."
    if off_pos == 'RB' and off_stat == 'STR':
        return f"No power runs inside — their LBs are {edge_val} stronger than your RBs."
    if off_pos == 'WR' and off_stat == 'SPD':
        return f"No deep shots — their DBs have a {edge_val} speed edge over your WRs."
    if off_pos == 'WR' and off_stat == 'A':
        return f"Avoid intermediate routes — their DBs mirror your WRs ({edge_val} agility edge)."
    if off_pos == 'TE' and off_stat == 'TOT':
        return f"Don't rely on TE vs their LBs — they have a {edge_val} overall edge."
    if off_pos == 'TE' and off_stat == 'SPD':
        return f"Don't route your TE vs their DBs — {edge_val} speed disadvantage."
    return f"Avoid this matchup — their {def_pos} has a {edge_val} edge over your {off_pos}."


def find_individual_edges(offense_r, defense_r, off_form=None):
    """Flag edges on meaningful matchups only.
    Shotgun: skip run matchups (OL/RB comparisons) — no RB on the field.
    RB SPD vs LB SPD uses +30 threshold; all others use +20.
    Returns (advantages, dangers) using real numbers."""
    advantages = []
    dangers = []

    # In Shotgun, skip run-game comparisons (OL and RB positions)
    _skip_pos = {'OL', 'RB'} if off_form == 'Shotgun' else set()

    for off_pos, off_stat, def_pos, def_stat, label in MISMATCH_COMPARISONS:
        if off_pos in _skip_pos:
            continue
        off_val = _stat(offense_r, off_pos, off_stat)
        def_val = _stat(defense_r, def_pos, def_stat)
        if off_val is None or def_val is None:
            continue
        edge_val = off_val - def_val

        # RB SPD vs LB SPD needs +30 to be flagged (outside run threshold)
        threshold = 30 if (off_pos == 'RB' and off_stat == 'SPD' and def_pos == 'LB') else MISMATCH_THRESHOLD

        if edge_val >= threshold:
            advantages.append({
                'label': label,
                'edge': edge_val,
                'adv_text': _mismatch_narrative(off_pos, off_stat, off_val, def_pos, def_stat, def_val, edge_val),
                'adv_rec': _mismatch_rec(off_pos, off_stat, def_pos, edge_val),
            })
        elif edge_val <= -threshold:
            dangers.append({
                'label': label,
                'edge': edge_val,
                'dan_text': _danger_narrative(off_pos, off_stat, off_val, def_pos, def_stat, def_val, edge_val),
                'dan_rec': _danger_rec(off_pos, off_stat, def_pos, edge_val),
            })

    advantages.sort(key=lambda x: -x['edge'])
    dangers.sort(key=lambda x: x['edge'])
    return advantages[:8], dangers[:8]


def compute_passing_targets(offense_r, defense_r, off_form=None, def_form=None,
                            your_players=None):
    """Compute passing target percentages using individual player stats.

    Returns a list of {'label', 'pct', 'edge', 'explain', 'css_class'} dicts
    matching exactly who is on the field. Always sums to 100%.

    Each player gets a percentage based on their individual stats:
      WR: SPD + AGI vs DB SPD + DB AGI (separation edge)
      TE: TOT vs LB TOT and SPD vs DB SPD
      RB: AGI vs LB AGI, capped at 20% (30% if AGI edge >= +25)
      FB: AGI vs LB AGI, capped at 10% — safety valve only
    Players at the same position within 5 points of each other split evenly.
    Wishbone: 1 FB + 2 RB + 1 TE + 1 WR — FB capped at 10%.
    Shotgun: 4 WR + 1 TE, no RB — WRs get at least 65%.
    Trips: 3 WR + 1 TE + 1 RB — WRs get at least 50%, RB capped at 20%.
    """
    your_players = your_players or []

    # Defensive averages for comparison
    db_spd = _stat(defense_r, 'DB', 'SPD') or 0
    db_agi = _stat(defense_r, 'DB', 'A') or 0
    lb_tot = _stat(defense_r, 'LB', 'TOT') or 0
    lb_agi = _stat(defense_r, 'LB', 'A') or 0

    def_pers = DEFENSE_FORMATIONS.get(def_form, {}) if def_form else {}
    num_db = def_pers.get('DB', 4)
    db_wr_adj = 0.85 if num_db >= 5 else (1.2 if num_db <= 3 else 1.0)
    db_te_adj = 1.15 if num_db >= 5 else 1.0

    # Gather individual players by position
    wrs = [p for p in your_players if p['pos'] == 'WR']
    tes = [p for p in your_players if p['pos'] == 'TE']
    rbs = [p for p in your_players if p['pos'] == 'RB']
    fbs = [p for p in your_players if p['pos'] == 'FB']

    # Sort by TOT descending to pick starters
    wrs.sort(key=lambda p: -p['tot'])
    tes.sort(key=lambda p: -p['tot'])
    rbs.sort(key=lambda p: -p['tot'])
    fbs.sort(key=lambda p: -p['tot'])

    # Determine formation personnel counts
    off_pers = OFFENSE_FORMATIONS.get(off_form, {'RB': 1, 'WR': 2, 'TE': 1})
    n_wr = off_pers.get('WR', 2)
    n_te = off_pers.get('TE', 1)
    n_rb = off_pers.get('RB', 1)
    n_fb = off_pers.get('FB', 0)

    # Compute individual edge for each player
    def _wr_edge(p):
        spd = p['stats'].get('SPD', 0)
        agi = p['stats'].get('A', 0)
        return ((spd - db_spd) + (agi - db_agi)) / 2.0

    def _te_edge(p):
        tot = p['stats'].get('TOT', 0)
        spd = p['stats'].get('SPD', 0)
        edges = []
        if tot and lb_tot:
            edges.append(tot - lb_tot)
        if spd and db_spd:
            edges.append(spd - db_spd)
        return sum(edges) / len(edges) if edges else 0

    def _rb_edge(p):
        agi = p['stats'].get('A', 0)
        return agi - lb_agi if agi and lb_agi else 0

    def _fb_edge(p):
        agi = p['stats'].get('A', 0)
        return agi - lb_agi if agi and lb_agi else 0

    # Build starter lists with edges
    wr_starters = []
    for p in wrs[:n_wr]:
        edge = _wr_edge(p)
        explain = f"SPD {p['stats'].get('SPD','?')} AGI {p['stats'].get('A','?')} vs DB SPD {db_spd} AGI {db_agi} ({'+' if edge >= 0 else ''}{round(edge)})"
        wr_starters.append({'name': p['name'], 'pos': 'WR', 'edge': edge, 'explain': explain, 'css_class': 'pct-bar-wr'})

    te_starters = []
    for p in tes[:n_te]:
        edge = _te_edge(p)
        tot = p['stats'].get('TOT', '?')
        spd = p['stats'].get('SPD', '?')
        explain = f"TOT {tot} vs LB TOT {lb_tot}, SPD {spd} vs DB SPD {db_spd} ({'+' if edge >= 0 else ''}{round(edge)})"
        te_starters.append({'name': p['name'], 'pos': 'TE', 'edge': edge, 'explain': explain, 'css_class': 'pct-bar-te'})

    rb_starters = []
    for p in rbs[:n_rb]:
        edge = _rb_edge(p)
        agi = p['stats'].get('A', '?')
        rb_is_mismatch = edge >= 25
        if rb_is_mismatch:
            explain = f"AGI {agi} vs LB AGI {lb_agi} (+{round(edge)}) — mismatch weapon — their LBs cannot stay with your RB in space, target him in the flat and on screens"
        else:
            explain = f"AGI {agi} vs LB AGI {lb_agi} ({'+' if edge >= 0 else ''}{round(edge)}) — safety valve — use on 3rd and short or when coverage takes away your primary options"
        rb_starters.append({'name': p['name'], 'pos': 'RB', 'edge': edge, 'explain': explain,
                            'css_class': 'pct-bar-rb', 'is_mismatch': rb_is_mismatch})

    fb_starters = []
    for p in fbs[:n_fb]:
        edge = _fb_edge(p)
        agi = p['stats'].get('A', '?')
        explain = f"AGI {agi} vs LB AGI {lb_agi} ({'+' if edge >= 0 else ''}{round(edge)}) — safety valve only — rarely targeted, use on short yardage check-downs"
        fb_starters.append({'name': p['name'], 'pos': 'FB', 'edge': edge, 'explain': explain,
                            'css_class': 'pct-bar-fb'})

    # If no players parsed, fall back to position labels with averaged ratings
    if not wr_starters and not te_starters and not rb_starters and not fb_starters:
        return _compute_passing_targets_fallback(offense_r, defense_r, off_form, def_form)

    # Fill missing positions with generic labels using averaged ratings
    if not wr_starters:
        wr_spd = _stat(offense_r, 'WR', 'SPD') or 0
        wr_a = _stat(offense_r, 'WR', 'A') or 0
        edge = ((wr_spd - db_spd) + (wr_a - db_agi)) / 2.0
        for i in range(n_wr):
            label = f"WR{i+1}" if n_wr > 1 else "WR"
            wr_starters.append({'name': label, 'pos': 'WR', 'edge': edge,
                                'explain': f"WR SPD {wr_spd} AGI {wr_a} vs DB SPD {db_spd} AGI {db_agi}", 'css_class': 'pct-bar-wr'})
    if not te_starters:
        te_tot_v = _stat(offense_r, 'TE', 'TOT') or 0
        te_spd_v = _stat(offense_r, 'TE', 'SPD') or 0
        edges = []
        if te_tot_v and lb_tot: edges.append(te_tot_v - lb_tot)
        if te_spd_v and db_spd: edges.append(te_spd_v - db_spd)
        edge = sum(edges) / len(edges) if edges else 0
        for i in range(n_te):
            label = f"TE{i+1}" if n_te > 1 else "TE"
            te_starters.append({'name': label, 'pos': 'TE', 'edge': edge,
                                'explain': f"TE TOT {te_tot_v} vs LB TOT {lb_tot}", 'css_class': 'pct-bar-te'})
    if not rb_starters:
        rb_agi_v = _stat(offense_r, 'RB', 'A') or 0
        edge = rb_agi_v - lb_agi if rb_agi_v and lb_agi else 0
        rb_is_mismatch = edge >= 25
        explain = f"RB AGI {rb_agi_v} vs LB AGI {lb_agi} ({'+' if edge >= 0 else ''}{round(edge)}) — {'mismatch weapon' if rb_is_mismatch else 'safety valve'}"
        for i in range(n_rb):
            label = f"RB{i+1}" if n_rb > 1 else "RB"
            rb_starters.append({'name': label, 'pos': 'RB', 'edge': edge, 'explain': explain,
                                'css_class': 'pct-bar-rb', 'is_mismatch': rb_is_mismatch})
    if not fb_starters and n_fb > 0:
        fb_agi_v = _stat(offense_r, 'FB', 'A') or _stat(offense_r, 'RB', 'A') or 0
        edge = fb_agi_v - lb_agi if fb_agi_v and lb_agi else 0
        explain = f"FB AGI {fb_agi_v} vs LB AGI {lb_agi} ({'+' if edge >= 0 else ''}{round(edge)}) — safety valve only"
        fb_starters.append({'name': 'FB', 'pos': 'FB', 'edge': edge, 'explain': explain,
                            'css_class': 'pct-bar-fb'})

    # Equalize same-position players within 5 points of each other
    def _equalize(starters):
        if len(starters) <= 1:
            return
        edges = [s['edge'] for s in starters]
        if max(edges) - min(edges) <= 5:
            avg = sum(edges) / len(edges)
            for s in starters:
                s['edge'] = avg

    _equalize(wr_starters)
    _equalize(te_starters)
    _equalize(rb_starters)

    # Compute raw weights
    all_starters = []
    for s in wr_starters:
        w = max(s['edge'] + 30, 5) * db_wr_adj
        all_starters.append({**s, 'weight': w})
    for s in te_starters:
        w = max(s['edge'] + 30, 5) * db_te_adj
        all_starters.append({**s, 'weight': w})

    # RB weight — Shotgun has no RB (n_rb=0 so rb_starters is empty)
    for s in rb_starters:
        w = max(s['edge'] + 30, 5)
        all_starters.append({**s, 'weight': w})

    # FB weight — low priority, safety valve only
    for s in fb_starters:
        w = max(s['edge'] + 30, 5) * 0.3  # heavily reduced
        all_starters.append({**s, 'weight': w})

    # Trips: RB capped at 20% max
    # Normal: RB capped at 20%, or 30% if mismatch
    # Shotgun: no RB in formation, cap irrelevant but set for safety
    # FB: always capped at 10%
    fb_cap = 10
    if off_form == 'Shotgun':
        rb_cap = 0
    elif off_form == 'Trips':
        rb_cap = 20
    else:
        any_rb_mismatch = any(s.get('is_mismatch') for s in rb_starters)
        rb_cap = 30 if any_rb_mismatch else 20

    # Convert weights to percentages
    total_w = sum(s['weight'] for s in all_starters)
    if total_w <= 0:
        total_w = 1
    targets = []
    for s in all_starters:
        pct = round(s['weight'] / total_w * 100)
        label = f"{s['name']} ({s['pos']})"
        targets.append({
            'label': label, 'pct': pct, 'edge': round(s['edge']),
            'explain': s['explain'], 'css_class': s['css_class'],
            '_pos': s['pos'], '_is_mismatch': s.get('is_mismatch', False),
        })

    # Fix rounding
    total_pct = sum(t['pct'] for t in targets)
    if total_pct != 100 and targets:
        targets[0]['pct'] += (100 - total_pct)

    # Cap RB targets
    _cap_rb_and_distribute(targets, rb_cap)

    # Cap FB targets at 10%
    if fb_cap and any(t.get('_pos') == 'FB' for t in targets):
        _cap_pos_and_distribute(targets, 'FB', fb_cap)

    # Shotgun: WRs collectively get at least 65%
    # Trips: WRs collectively get at least 50%
    if off_form == 'Shotgun':
        _enforce_wr_floor(targets, 65)
    elif off_form == 'Trips':
        _enforce_wr_floor(targets, 50)

    # Sort by pct descending
    targets.sort(key=lambda t: -t['pct'])
    return targets


def _cap_pos_and_distribute(targets, pos, cap_val):
    """Cap each target at pos to cap_val, redistribute excess to others proportionally."""
    excess = 0
    other_total = 0
    for t in targets:
        if t.get('_pos') == pos and t['pct'] > cap_val:
            excess += t['pct'] - cap_val
            t['pct'] = cap_val
        elif t.get('_pos') != pos:
            other_total += t['pct']

    if excess > 0 and other_total > 0:
        for t in targets:
            if t.get('_pos') != pos:
                share = t['pct'] / other_total
                t['pct'] += round(excess * share)

    # Fix rounding to ensure sum is exactly 100
    total = sum(t['pct'] for t in targets)
    if total != 100 and targets:
        others = [t for t in targets if t.get('_pos') != pos]
        if others:
            others[0]['pct'] += (100 - total)


def _cap_rb_and_distribute(targets, rb_cap_val):
    """Cap each RB target at rb_cap_val, redistribute excess to others proportionally."""
    _cap_pos_and_distribute(targets, 'RB', rb_cap_val)


def _enforce_wr_floor(targets, wr_floor):
    """Ensure WR targets collectively reach at least wr_floor%."""
    wr_targets = [t for t in targets if t.get('_pos') == 'WR']
    non_wr = [t for t in targets if t.get('_pos') != 'WR']
    wr_total = sum(t['pct'] for t in wr_targets)
    if wr_total >= wr_floor or not wr_targets:
        return
    deficit = wr_floor - wr_total
    non_wr_total = sum(t['pct'] for t in non_wr)
    if non_wr_total <= 0:
        return
    for t in non_wr:
        take = round(deficit * (t['pct'] / non_wr_total))
        t['pct'] -= take
    per_wr = deficit // len(wr_targets)
    remainder = deficit - per_wr * len(wr_targets)
    for i, t in enumerate(wr_targets):
        t['pct'] += per_wr + (1 if i < remainder else 0)
    total = sum(t['pct'] for t in targets)
    if total != 100 and wr_targets:
        wr_targets[0]['pct'] += (100 - total)


def _compute_passing_targets_fallback(offense_r, defense_r, off_form=None, def_form=None):
    """Fallback when no individual players are parsed — uses averaged ratings."""
    db_spd = _stat(defense_r, 'DB', 'SPD') or 0
    db_agi = _stat(defense_r, 'DB', 'A') or 0
    lb_tot = _stat(defense_r, 'LB', 'TOT') or 0
    lb_agi = _stat(defense_r, 'LB', 'A') or 0
    te_tot = _stat(offense_r, 'TE', 'TOT') or 0
    te_spd = _stat(offense_r, 'TE', 'SPD') or 0
    wr_spd = _stat(offense_r, 'WR', 'SPD') or 0
    wr_a = _stat(offense_r, 'WR', 'A') or 0
    rb_agi = _stat(offense_r, 'RB', 'A') or 0

    def_pers = DEFENSE_FORMATIONS.get(def_form, {}) if def_form else {}
    num_db = def_pers.get('DB', 4)
    db_wr_adj = 0.85 if num_db >= 5 else (1.2 if num_db <= 3 else 1.0)
    db_te_adj = 1.15 if num_db >= 5 else 1.0

    wr_edge = ((wr_spd - db_spd) + (wr_a - db_agi)) / 2.0
    te_edges = []
    if te_tot and lb_tot: te_edges.append(te_tot - lb_tot)
    if te_spd and db_spd: te_edges.append(te_spd - db_spd)
    te_edge = sum(te_edges) / len(te_edges) if te_edges else 0
    rb_edge = rb_agi - lb_agi if rb_agi and lb_agi else 0
    rb_is_mismatch = rb_edge >= 25

    off_pers = OFFENSE_FORMATIONS.get(off_form, {'RB': 1, 'WR': 2, 'TE': 1})
    n_wr = off_pers.get('WR', 2)
    n_te = off_pers.get('TE', 1)
    n_rb = off_pers.get('RB', 1)
    n_fb = off_pers.get('FB', 0)

    wr_explain = f"WR SPD {wr_spd} AGI {wr_a} vs DB SPD {db_spd} AGI {db_agi} ({'+' if wr_edge >= 0 else ''}{round(wr_edge)})"
    te_explain = f"TE TOT {te_tot} vs LB TOT {lb_tot}, SPD {te_spd} vs DB SPD {db_spd} ({'+' if te_edge >= 0 else ''}{round(te_edge)})"
    if rb_is_mismatch:
        rb_explain = f"RB AGI {rb_agi} vs LB AGI {lb_agi} (+{round(rb_edge)}) — mismatch weapon"
    else:
        rb_explain = f"RB AGI {rb_agi} vs LB AGI {lb_agi} ({'+' if rb_edge >= 0 else ''}{round(rb_edge)}) — safety valve"

    targets = []
    for i in range(n_wr):
        label = f"WR{i+1}" if n_wr > 1 else "WR"
        w = max(wr_edge + 30, 5) * db_wr_adj
        targets.append({'label': label, 'weight': w, 'edge': round(wr_edge), 'explain': wr_explain, 'css_class': 'pct-bar-wr', '_pos': 'WR'})
    for i in range(n_te):
        label = f"TE{i+1}" if n_te > 1 else "TE"
        w = max(te_edge + 30, 5) * db_te_adj
        targets.append({'label': label, 'weight': w, 'edge': round(te_edge), 'explain': te_explain, 'css_class': 'pct-bar-te', '_pos': 'TE'})
    # n_rb is 0 for Shotgun, so this loop won't add any RB targets
    for i in range(n_rb):
        label = f"RB{i+1}" if n_rb > 1 else "RB"
        w = max(rb_edge + 30, 5)
        targets.append({'label': label, 'weight': w, 'edge': round(rb_edge), 'explain': rb_explain, 'css_class': 'pct-bar-rb', '_pos': 'RB'})
    # FB fallback — Wishbone has 1 FB
    for i in range(n_fb):
        fb_agi_fb = _stat(offense_r, 'FB', 'A') or rb_agi or 0
        fb_edge = fb_agi_fb - lb_agi if fb_agi_fb and lb_agi else 0
        fb_explain = f"FB AGI {fb_agi_fb} vs LB AGI {lb_agi} ({'+' if fb_edge >= 0 else ''}{round(fb_edge)}) — safety valve only"
        w = max(fb_edge + 30, 5) * 0.3
        targets.append({'label': 'FB', 'weight': w, 'edge': round(fb_edge), 'explain': fb_explain, 'css_class': 'pct-bar-fb', '_pos': 'FB'})

    total_w = sum(t['weight'] for t in targets) or 1
    for t in targets:
        t['pct'] = round(t['weight'] / total_w * 100)
        del t['weight']
    total_pct = sum(t['pct'] for t in targets)
    if total_pct != 100 and targets:
        targets[0]['pct'] += (100 - total_pct)

    rb_cap = 0 if off_form == 'Shotgun' else (20 if off_form == 'Trips' else (30 if rb_is_mismatch else 20))
    _cap_rb_and_distribute(targets, rb_cap)
    if any(t.get('_pos') == 'FB' for t in targets):
        _cap_pos_and_distribute(targets, 'FB', 10)
    if off_form == 'Shotgun':
        _enforce_wr_floor(targets, 65)
    elif off_form == 'Trips':
        _enforce_wr_floor(targets, 50)

    targets.sort(key=lambda t: -t['pct'])
    return targets


def compute_run_split(offense_r, defense_r, off_form=None, def_form=None):
    """Compute Outside/Inside run percentage split factoring in defense."""
    def_pers = DEFENSE_FORMATIONS.get(def_form, {}) if def_form else {}
    num_dl = def_pers.get('DL', 4)
    num_lb = def_pers.get('LB', 3)
    num_db = def_pers.get('DB', 4)
    heavy_box = num_dl + num_lb  # DL + LB in the box

    # Outside edge: RB SPD vs LB SPD
    outside_edge = 0
    rb_spd = _stat(offense_r, 'RB', 'SPD')
    lb_spd = _stat(defense_r, 'LB', 'SPD')
    if rb_spd is not None and lb_spd is not None:
        outside_edge = rb_spd - lb_spd

    # Inside edge: OL STR vs DL STR + RB STR vs LB STR
    inside_edges = []
    ol_str = _stat(offense_r, 'OL', 'STR')
    dl_str = _stat(defense_r, 'DL', 'STR')
    if ol_str is not None and dl_str is not None:
        inside_edges.append(ol_str - dl_str)
    rb_str = _stat(offense_r, 'RB', 'STR')
    lb_str = _stat(defense_r, 'LB', 'STR')
    if rb_str is not None and lb_str is not None:
        inside_edges.append(rb_str - lb_str)
    inside_edge = round(sum(inside_edges) / len(inside_edges)) if inside_edges else 0
    ol_str_edge = (ol_str - dl_str) if (ol_str is not None and dl_str is not None) else 0

    # Determine split
    warning = ''

    # Shotgun has no RB on the field — no run game section
    if off_form == 'Shotgun':
        return {
            'shotgun_pass_only': True,
            'warning': 'Shotgun is a pass-first formation — no RB on the field. Commit to the passing game.',
            'outside': {'pct': 0, 'edge': 0, 'rb_spd': None, 'lb_spd': None},
            'inside':  {'pct': 0, 'edge': 0, 'ol_str': None, 'dl_str': None, 'rb_str': None, 'lb_str': None},
        }

    # Wishbone: run-first with 3 ball carriers (FB, RB1, RB2)
    # FB is the inside power runner, RBs are outside speed options
    if off_form == 'Wishbone':
        fb_str = _stat(offense_r, 'FB', 'STR') or _stat(offense_r, 'RB', 'STR')
        fb_inside_edge = (fb_str - (dl_str or 0)) if fb_str else 0

        if fb_inside_edge >= 10 and outside_edge >= 15:
            warning = f"Wishbone run-first: FB STR {fb_str} vs DL STR {dl_str} favors inside power, RB SPD {rb_spd} vs LB SPD {lb_spd} favors outside — attack both."
            outside_pct = 50
            inside_pct = 50
        elif fb_inside_edge >= 10:
            warning = f"Wishbone run-first: FB STR {fb_str} vs DL STR {dl_str} (+{round(fb_inside_edge)}) — pound it inside with your FB, use RBs on play-action."
            outside_pct = 35
            inside_pct = 65
        elif outside_edge >= 15:
            warning = f"Wishbone run-first: RB SPD {rb_spd} vs LB SPD {lb_spd} (+{round(outside_edge)}) — use your RBs on sweeps and tosses, FB as decoy inside."
            outside_pct = 65
            inside_pct = 35
        else:
            warning = f"Wishbone run-first: no clear edge inside or outside — mix FB dives with RB sweeps to keep the defense guessing."
            outside_pct = 50
            inside_pct = 50

        return {
            'outside': {'pct': outside_pct, 'edge': round(outside_edge),
                         'rb_spd': rb_spd, 'lb_spd': lb_spd},
            'inside': {'pct': inside_pct, 'edge': round(fb_inside_edge),
                        'ol_str': ol_str, 'dl_str': dl_str, 'rb_str': fb_str, 'lb_str': lb_str},
            'warning': warning,
        }

    if def_form in ('5-2', '4-4') or heavy_box >= 8:
        # Heavy box — warn about inside runs
        warning = f"They have {heavy_box} in the box ({def_form}) — inside runs will be tough. Consider passing or outside runs."
        outside_pct = 65
        inside_pct = 35
    elif def_form in ('Nickel', 'Dime') or num_db >= 5:
        # Light box — strongly recommend running
        warning = f"They are in a pass defense ({def_form}, only {heavy_box} in the box) — run the ball, they cannot stop it."
        outside_pct = 50
        inside_pct = 50
    else:
        # Standard — balance based on edges
        if outside_edge >= 15 and ol_str_edge >= 10:
            outside_pct = 55
            inside_pct = 45
        elif outside_edge >= 15:
            outside_pct = 60
            inside_pct = 40
        elif ol_str_edge >= 10:
            outside_pct = 40
            inside_pct = 60
        else:
            outside_pct = 50
            inside_pct = 50

    return {
        'outside': {'pct': outside_pct, 'edge': round(outside_edge),
                     'rb_spd': rb_spd, 'lb_spd': lb_spd},
        'inside': {'pct': inside_pct, 'edge': round(inside_edge),
                    'ol_str': ol_str, 'dl_str': dl_str, 'rb_str': rb_str, 'lb_str': lb_str},
        'warning': warning,
    }


def build_game_plan(matchups, off_form=None):
    """Build the game plan summary.
    Shotgun: only passing rows (no run game — no RB on the field)."""
    by_key = {m['key']: m for m in matchups}

    def avg_edge(*keys):
        vals = [by_key[k]['edge'] for k in keys
                if k in by_key and by_key[k]['edge'] is not None]
        return round(sum(vals) / len(vals)) if vals else None

    if off_form == 'Shotgun':
        rows = [
            ('Pass Short',     None,          avg_edge('te_coverage', 'route_run')),
            ('Pass Deep',      None,          avg_edge('deep_pass')),
        ]
    else:
        rows = [
            ('Run Outside',    'outside_run', avg_edge('outside_run')),
            ('Run Inside',     'short_yardage', avg_edge('power_run', 'short_yardage')),
            ('Pass Short',     None,          avg_edge('te_coverage', 'route_run')),
            ('Pass Deep',      None,          avg_edge('deep_pass')),
        ]

    plan = []
    for play, key, e in rows:
        css, icon, short = _tier(e, key=key)
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


def trim_box_for_api(text):
    """Strip defensive stat tables from box score text before sending to API.
    Keeps: score header, team stats, passing, rushing, receiving, kicking FG/XP lines."""
    lines = text.splitlines()
    out = []
    skip = False
    for line in lines:
        ll = line.lower().strip()
        # Start skipping on defensive section headers
        if re.search(r'defensive|defense\b', ll) and not re.search(r'\d', ll[:5]):
            skip = True
            continue
        # Stop skipping when we hit the next non-defensive section
        if skip and re.search(r'(passing|rushing|receiving|kicking|team\s*stats|punting)', ll) and not re.search(r'\d', ll[:5]):
            skip = False
        if not skip:
            out.append(line)
    return '\n'.join(out)


def trim_gamelog_for_api(text):
    """Keep only scoring plays and the first 50 play-by-play lines."""
    lines = text.splitlines()
    scoring = []
    plays = []
    in_scoring = False
    for line in lines:
        ll = line.lower().strip()
        if not ll:
            continue
        # Detect scoring section header
        if 'scoring' in ll and not re.search(r'\d{2,}', ll):
            in_scoring = True
            scoring.append(line)
            continue
        # Detect play-by-play section header
        if re.search(r'play.by.play|play by play', ll):
            in_scoring = False
            plays.append(line)
            continue
        if in_scoring:
            scoring.append(line)
        else:
            # Scoring plays anywhere (TD, FG, PAT/XP lines)
            if re.search(r'\bTD\b|\btouchdown\b|\bfield goal\b|\bFG\b|\bPAT\b|\bXP\b', ll):
                scoring.append(line)
            elif len(plays) < 51:  # header + 50 plays
                plays.append(line)
    result = []
    if scoring:
        result.extend(scoring)
    if plays:
        if result:
            result.append('')
        result.extend(plays)
    return '\n'.join(result)


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

    # Third-down conversion context (data-backed only)
    if ys and ts:
        try:
            margin = abs(yi - ti)
            y3_val = your_stats.get('third_down')
            if y3_val and margin <= 7:
                sentences.append(f"{your_team} is {y3_val} on third down — conversion rate will decide this game.")
        except (ValueError, TypeError):
            pass

    summary = ' '.join(sentences) if sentences else (
        "Stats could not be fully parsed — recommendations below are drawn from available play-by-play data."
    )

    # ── Top Performers section ──────────────────────────────────────────────
    def _build_offense_performers(team_name):
        """Top 2 offensive skill players by yards (RB rush, WR/TE/RB rec, QB pass)."""
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
        for name, yds, info, statline in candidates[:2]:
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
        """Top 2 defensive players by tackles (DL, LB, DB all eligible)."""
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
        for name, tackles, info, statline in candidates[:2]:
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

    # ── SCORE SITUATION — only include if we have data to be specific ────
    if ys and ts:
        try:
            deficit = ti - yi
            if deficit > 8:
                win_bullets.append(
                    f"▶ Down {deficit} points — throw on early downs to maximize possessions, you need {(deficit + 6) // 7} scores minimum"
                )
            elif deficit > 0 and deficit <= 8:
                # Only add if we have passing/run data to back up a recommendation
                if your_pass_data:
                    best_pd = max(your_pass_data, key=lambda d: sum(your_pass_data[d]['yards']) if your_pass_data[d]['comp'] > 0 else 0)
                    pd = your_pass_data[best_pd]
                    if pd['comp'] > 0:
                        win_bullets.append(
                            f"▶ Down {deficit} — your {best_pd} passing game is {pd['comp']}/{pd['att']}, keep attacking there to close the gap"
                        )
        except (ValueError, TypeError):
            pass

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
def logout():
    logout_user()
    session.pop('internal_access', None)
    session.pop('admin_access', None)
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
def account():
    if not current_user.is_authenticated and not session.get('internal_access'):
        return redirect(url_for('landing'))
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


# ─── AI output validation layer ──────────────────────────────────────────────

def validate_ai_output(text):
    """Post-process AI output to fix common errors:
    1. Fix incorrect 'overpowers' claims where the numbers are wrong
    2. Remove formation-change recommendations
    3. Remove cross-position analysis (TE stats in RB context, etc.)
    4. Fix false speed advantage claims
    """
    import re as _re

    # 1. Fix "X overpowers/dominates/overwhelms Y" — wrong direction or too small a gap
    POWER_WORDS = r'(?:overpower[s]?|dominate[s]?|overwhelm[s]?)'
    POWER_REPLACEMENT_MAP = {
        'overpowers': 'is outmatched by', 'overpower': 'is outmatched by',
        'dominates': 'is outmatched by', 'dominate': 'is outmatched by',
        'overwhelms': 'is outmatched by', 'overwhelm': 'is outmatched by',
    }
    MILD_REPLACEMENT_MAP = {
        'overpowers': 'holds a slight edge over', 'overpower': 'hold a slight edge over',
        'dominates': 'has a modest advantage over', 'dominate': 'have a modest advantage over',
        'overwhelms': 'edges out', 'overwhelm': 'edge out',
    }

    def _fix_overpowers(m):
        full = m.group(0)
        nums = _re.findall(r'(\d+)', full)
        if len(nums) >= 2:
            first, second = int(nums[0]), int(nums[1])
            lower = full.lower()
            # Find the power word position
            pw_match = _re.search(POWER_WORDS, lower)
            if not pw_match:
                return full
            pw_pos = pw_match.start()
            pw_word = pw_match.group(0)

            # Check if numbers are backwards ("their X overpowers your Y" but X < Y)
            if 'their' in lower[:pw_pos] and first < second:
                print(f">>> VALIDATE: Fixed wrong power claim (backwards): {full!r}", flush=True)
                return _re.sub(POWER_WORDS, lambda m2: POWER_REPLACEMENT_MAP.get(m2.group(0).lower(), 'is outmatched by'), full, flags=_re.IGNORECASE)
            if 'your' in lower[:pw_pos] and first < second:
                print(f">>> VALIDATE: Fixed wrong power claim (backwards): {full!r}", flush=True)
                return _re.sub(POWER_WORDS, lambda m2: 'trails', full, flags=_re.IGNORECASE)

            # Check if gap is too small for power language (< 20)
            gap = abs(first - second)
            if gap < 20:
                print(f">>> VALIDATE: Downgraded power word for small gap ({gap}): {full!r}", flush=True)
                return _re.sub(POWER_WORDS, lambda m2: MILD_REPLACEMENT_MAP.get(m2.group(0).lower(), 'edges'), full, flags=_re.IGNORECASE)
        return full

    text = _re.sub(
        r'[^.]*\d+[^.]*' + POWER_WORDS + r'[^.]*\d+[^.]*',
        _fix_overpowers, text, flags=_re.IGNORECASE
    )

    # 2. Remove formation-change recommendations
    FORMATION_CHANGE_PATTERNS = [
        r'<div class="gameplan-bullet">[^<]*(?:switch(?:ing)?\s+(?:to|formations?)|go\s+to\s+shotgun|spread\s+(?:the\s+field|formation)|(?:use|try|go\s+with)\s+(?:a\s+)?(?:\d-wide|trips|shotgun|pro\s+set|wishbone|i\s+formation|notre\s+dame)|add(?:ing)?\s+(?:an?\s+)?extra\s+(?:receiver|wide\s+out|WR)|(?:\d|more|extra|additional)\s*(?:-|\s)?wide\s*(?:-|\s)?receiver\s+set)[^<]*</div>',
        r'<li>[^<]*(?:switch(?:ing)?\s+(?:to|formations?)|go\s+to\s+shotgun|spread\s+(?:the\s+field|formation)|(?:use|try|go\s+with)\s+(?:a\s+)?(?:\d-wide|trips|shotgun|pro\s+set|wishbone|i\s+formation|notre\s+dame)|add(?:ing)?\s+(?:an?\s+)?extra\s+(?:receiver|wide\s+out|WR)|(?:\d|more|extra|additional)\s*(?:-|\s)?wide\s*(?:-|\s)?receiver\s+set)[^<]*</li>',
    ]
    for pattern in FORMATION_CHANGE_PATTERNS:
        matches = _re.findall(pattern, text, flags=_re.IGNORECASE)
        for match in matches:
            print(f">>> VALIDATE: Removed formation-change recommendation: {match[:100]!r}", flush=True)
        text = _re.sub(pattern, '', text, flags=_re.IGNORECASE)

    # Also catch plain-text formation changes (for strategy page non-HTML output)
    PLAIN_FORMATION_PATTERNS = [
        r'^[•\-\*]\s*[^\n]*(?:switch(?:ing)?\s+(?:to|formations?)|go\s+to\s+shotgun|(?:\d|more|extra)\s*-?\s*wide\s*-?\s*receiver\s+set)[^\n]*$',
    ]
    for pattern in PLAIN_FORMATION_PATTERNS:
        matches = _re.findall(pattern, text, flags=_re.IGNORECASE | _re.MULTILINE)
        for match in matches:
            print(f">>> VALIDATE: Removed plain-text formation change: {match[:100]!r}", flush=True)
        text = _re.sub(pattern, '', text, flags=_re.IGNORECASE | _re.MULTILINE)

    # 3. Flag cross-position analysis (TE in RB context, RB in TE context)
    # Look for "RB" recommendations that reference TE ratings or vice versa
    def _clean_cross_position(m):
        sentence = m.group(0)
        # Check for RB context mentioning TE stats
        if _re.search(r'\bRB\b', sentence) and _re.search(r'\bTE\b.*\b(?:TOT|SPD|rating|overall)\b', sentence):
            # Remove the TE reference clause
            cleaned = _re.sub(r',?\s*(?:and|combined with|along with|plus|supported by)[^,.]* \bTE\b[^,.]*', '', sentence)
            if cleaned != sentence:
                print(f">>> VALIDATE: Removed TE reference from RB context", flush=True)
                return cleaned
        # Check for TE context mentioning RB stats
        if _re.search(r'\bTE\b', sentence) and _re.search(r'\bRB\b.*\b(?:STR|SPD|rating|overall)\b', sentence):
            cleaned = _re.sub(r',?\s*(?:and|combined with|along with|plus|supported by)[^,.]* \bRB\b[^,.]*', '', sentence)
            if cleaned != sentence:
                print(f">>> VALIDATE: Removed RB reference from TE context", flush=True)
                return cleaned
        return sentence

    text = _re.sub(r'[^.!?]*(?:RB|TE)[^.!?]*(?:TE|RB)[^.!?]*[.!?]', _clean_cross_position, text)

    # 4. Fix false speed advantage claims
    # Pattern: "X SPD NN ... speed advantage ... Y SPD MM" where NN < MM
    def _fix_speed_claims(m):
        full = m.group(0)
        spd_vals = _re.findall(r'SPD\s*(?:of\s*)?(\d+)', full, _re.IGNORECASE)
        if len(spd_vals) >= 2:
            our_spd, their_spd = int(spd_vals[0]), int(spd_vals[1])
            if our_spd < their_spd and 'speed advantage' in full.lower():
                print(f">>> VALIDATE: Fixed false speed advantage (ours={our_spd} < theirs={their_spd})", flush=True)
                return full.replace('speed advantage', 'route running ability')
        return full

    text = _re.sub(
        r'[^.]*SPD[^.]*speed advantage[^.]*SPD[^.]*\.',
        _fix_speed_claims, text, flags=_re.IGNORECASE
    )
    # Also check reverse order (speed advantage mentioned before SPD numbers)
    text = _re.sub(
        r'[^.]*speed advantage[^.]*SPD[^.]*SPD[^.]*\.',
        _fix_speed_claims, text, flags=_re.IGNORECASE
    )

    # 5. Remove "tale of two halves" and similar phrases (this is a halftime report — only one half played)
    TWO_HALVES_PATTERNS = [
        r'tale\s+of\s+two\s+halves',
        r'game\s+of\s+two\s+halves',
        r'two\s+(?:very\s+)?different\s+halves',
        r'two\s+distinct\s+halves',
        r'a\s+story\s+of\s+two\s+halves',
    ]
    for pattern in TWO_HALVES_PATTERNS:
        if _re.search(pattern, text, flags=_re.IGNORECASE):
            print(f">>> VALIDATE: Removed 'tale of two halves' phrase matching: {pattern}", flush=True)
            text = _re.sub(pattern, 'a pivotal first half', text, flags=_re.IGNORECASE)

    # 6. Remove leading colons before stat lines (e.g. ": 5 rec, 89 yds" -> "5 rec, 89 yds")
    colon_count = len(_re.findall(r':\s*\d+\s*(?:rec|car|att|yds|td|int|sack|tkl|comp)', text, flags=_re.IGNORECASE))
    if colon_count:
        text = _re.sub(r':\s*(\d+\s*(?:rec|car|att|yds|td|int|sack|tkl|comp))', r' \1', text, flags=_re.IGNORECASE)
        print(f">>> VALIDATE: Removed {colon_count} leading colon(s) before stat lines", flush=True)

    return text


@app.route("/strategy", methods=["POST"])
@subscription_required
def strategy_route():
    opponent_team        = request.form.get("opponent_team", "").strip()
    your_team            = request.form.get("your_team", "").strip()
    opponent_ratings_raw = request.form.get("opponent_ratings", "").strip()
    your_ratings_raw     = request.form.get("your_ratings", "").strip()
    your_offense         = request.form.get("your_offense", "").strip()
    their_defense        = request.form.get("their_defense", "").strip()

    error = None
    ai_result = None

    if not opponent_team or not your_team:
        error = "Please enter both team names."
    elif not opponent_ratings_raw or not your_ratings_raw:
        error = "Please paste ratings for both teams."
    else:
        strategy_system_prompt = """You are an expert WhatIfSports sim football analyst. Analyze the matchup between two teams and provide a detailed game plan.

SIM FOOTBALL CONTEXT — READ FIRST:
This is text-based sim football, not real football. All recommendations must work within the constraints of a text-based sim game. There are no audibles, no pre-snap reads, no physical adjustments, no formation changes mid-game, no player substitutions during a drive. The only decisions available are: run inside, run outside, pass (which receivers to target), and which plays to call within the selected formation. Never recommend anything that requires physical action, real football strategy that does not apply to text sim, or changes that cannot be made in a text-based game.

You will receive:

Your team name and offense formation
Opponent team name and defense formation
Raw player ratings for both teams

FORMATION PERSONNEL:
I Formation: 1 RB, 2 WR, 1 TE
Pro: 1 RB, 2 WR, 1 TE
Wishbone: 1 FB, 2 RB, 1 TE, 1 WR
Notre Dame Box: 2 RB, 1 WR, 2 TE
Shotgun: 4 WR, 1 TE (no RB — do not include RB in passing targets or run game)
Trips: 1 RB, 3 WR, 1 TE
DEFENSE PERSONNEL:
3-4: 3 DL, 4 LB, 4 DB
4-3: 4 DL, 3 LB, 4 DB
4-4: 4 DL, 4 LB, 3 DB
5-2: 5 DL, 2 LB, 4 DB
Nickel: 4 DL, 2 LB, 5 DB
Dime: 3 DL, 2 LB, 6 DB
ONLY USE THESE MEANINGFUL MATCHUPS:

YOUR OL BLK vs THEIR DL STR — run blocking edge (can your blockers move their DL)
YOUR OL STR vs THEIR DL STR — power run edge (who wins the strength battle)
YOUR RB SPD vs THEIR LB SPD — outside run edge (only flag if +15 or more)
YOUR RB STR vs THEIR LB STR — short yardage edge
YOUR WR SPD vs THEIR DB SPD — deep passing edge
YOUR WR AGI vs THEIR DB AGI — route running edge
YOUR TE TOT vs THEIR LB TOT — TE coverage mismatch
YOUR TE SPD vs THEIR DB SPD — TE over the middle
NEVER compare RB vs DL, WR vs LB, RB SPD vs DB SPD, or include QB protection.

MISMATCH THRESHOLD: A mismatch only exists when the difference is 20 or more points. If YOUR stat is 767 and THEIR stat is 768 that is NOT a mismatch — it is an even matchup. Never say to exploit a matchup where your rating is lower than or equal to the opponent. Never recommend targeting a position as a mismatch unless your rating is at least 20 points higher than their corresponding defender rating.

OVERPOWERING LANGUAGE: Never use "overpowers", "dominates", or "overwhelms" for any stat difference less than 20 points. A +4 edge (e.g. OL STR 88 vs DL STR 84) is a "slight edge" or "modest advantage". A +10 to +19 edge is a "solid advantage". Only use "dominates" or "overpowers" for differences of +20 or more.

OUTPUT SECTIONS IN ORDER:

STANDOUT PLAYERS — parse individual player names and TOT ratings from the raw data
Your team: top 2 offensive players by TOT, top 2 defensive players by TOT with one sentence each
Opponent: same format, add 'Watch out for [name]' for their defensive standouts
FORMATION MATCHUP — analyze your offense vs their defense personnel. Call out specific mismatches like '3 WRs vs only 3 DBs means one WR gets a LB in coverage'
BIGGEST ADVANTAGES — list only edges of +20 or more using the 8 meaningful matchups. Write specific sim football advice with actual numbers.
DANGER ZONES — opponent edges of +20 or more. Give specific advice to neutralize.
RUN GAME PLAN — recommend inside vs outside percentage based on matchups and defense. For Shotgun say passing only — no RB on field. Factor in defense: Nickel/Dime = run more, 5-2/4-4 = spread them out.
PASSING TARGETS — show recommended target percentages for each player on the field based on the formation. Use individual player names and stats. WRs get at least 50% in Shotgun/Trips. RB capped at 20% max (30% only if RB AGI vs LB AGI edge is +25 or more). Show reasoning with actual TOT numbers.
GAME PLAN SUMMARY — 3-4 bullet points summarizing the most important things to do. Specific and actionable, no generic advice."""

        user_message = f"""Your Team: {your_team}
Your Offense: {your_offense}
Opponent Team: {opponent_team}
Their Defense: {their_defense}

Your Team Ratings:
{your_ratings_raw}

Opponent Team Ratings:
{opponent_ratings_raw}"""

        try:
            client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                system=strategy_system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )
            ai_result = validate_ai_output(response.content[0].text)
        except Exception as e:
            print(f">>> STRATEGY ANALYZE ERROR: {e}", flush=True)
            traceback.print_exc()
            error = f"AI analysis failed: {str(e)}"

    return render_template(
        "strategy.html",
        opponent_team=opponent_team,
        your_team=your_team,
        opponent_ratings_raw=opponent_ratings_raw,
        your_ratings_raw=your_ratings_raw,
        your_offense=your_offense,
        their_defense=their_defense,
        ai_result=ai_result,
        error=error,
    )


@app.route("/halftime-advisor", methods=["GET"])
@subscription_required
def halftime_advisor():
    return render_template("halftime.html", your_team='', opp_team='', box_raw='', gamelog_raw='',
                           your_ratings_raw='', opp_ratings_raw='',
                           your_offense='', their_defense='',
                           report={}, error=None)

@app.route("/halftime", methods=["POST"])
@subscription_required
def halftime_route():
    your_team        = request.form.get("ht_your_team",    "").strip()
    opp_team         = request.form.get("ht_opp_team",     "").strip()
    box_raw          = request.form.get("ht_box_score",    "").strip()
    gamelog_raw       = request.form.get("ht_game_log",     "").strip()
    your_ratings_raw = request.form.get("ht_your_ratings", "").strip()
    opp_ratings_raw  = request.form.get("ht_opp_ratings",  "").strip()
    your_offense     = request.form.get("ht_your_offense", "").strip()
    their_defense    = request.form.get("ht_their_defense","").strip()

    error   = None
    ai_result = None

    if not your_team or not opp_team:
        error = "Please enter both team names."
    elif not box_raw and not gamelog_raw:
        error = "Please paste at least a box score or game log."
    else:
        halftime_system_prompt = """You are an expert WhatIfSports sim football halftime analyst AND a dramatic color commentator. You will receive first half game data and team ratings and provide a detailed second half game plan.

SIM FOOTBALL CONTEXT — READ FIRST:
This is text-based sim football, not real football. All recommendations must work within the constraints of a text-based sim game. There are no audibles, no pre-snap reads, no physical adjustments, no formation changes mid-game, no player substitutions during a drive. The only decisions available are: run inside, run outside, pass (which receivers to target), and which plays to call within the selected formation. Never recommend anything that requires physical action, real football strategy that does not apply to text sim, or changes that cannot be made in a text-based game.

VOICE AND PERSONALITY:
Write with drama, energy, and personality. You CAN reference team names, mascots, records, rivalry context, and the magnitude of the game. Paint the picture — make the coach FEEL the moment. However, you CANNOT invent specific facts that are not in the data. If both teams are undefeated based on the data, you can say "two undefeated powerhouses colliding." You CANNOT make up historical context, championship references, or specific facts not provided. Facts must be real, storytelling can be dramatic.

FORMATION PERSONNEL:
I Formation: 1 RB, 2 WR, 1 TE
Pro: 1 RB, 2 WR, 1 TE
Wishbone: 1 FB, 2 RB, 1 TE, 1 WR
Notre Dame Box: 2 RB, 1 WR, 2 TE
Shotgun: 4 WR, 1 TE (no RB — do not include RB in passing targets or run game)
Trips: 1 RB, 3 WR, 1 TE
DEFENSE PERSONNEL:
3-4: 3 DL, 4 LB, 4 DB
4-3: 4 DL, 3 LB, 4 DB
4-4: 4 DL, 4 LB, 3 DB
5-2: 5 DL, 2 LB, 4 DB
Nickel: 4 DL, 2 LB, 5 DB
Dime: 3 DL, 2 LB, 6 DB

CRITICAL RULES:
- NEVER recommend switching formations, using different receiver sets, or any formation variation. Players CANNOT change formations mid-game in this sim. Do not suggest "X-wide receiver sets", "spread formations", "go to shotgun", etc. Only give play-calling advice within the formation already selected.
- EVERY player mentioned MUST include position and team in parentheses. Example: Roy Hogan (RB, Stony Brook). No exceptions.
- Never compare RB vs DL, WR vs LB, or include QB protection matchups.
- For Shotgun: no RB on field, passing only.
- If Nickel or Dime defense: recommend running more.
- Never include motivational filler — every recommendation must be specific and backed by data.
- NO MIXING POSITION ANALYSIS — when analyzing an RB, use only RB stats and matchups. When analyzing a TE, use only TE stats and matchups. Do not bring in one position's ratings to support another position's recommendation. Keep each position analysis clean and separate.
- MATH MUST BE CORRECT — before writing any comparison, verify which number is higher. If YOUR stat is 88 and THEIR stat is 84, that is a +4 advantage FOR YOU — do not say they overpower you. Never say a stat "overpowers" or "significantly overpowers" unless the edge is +15 or more. Double check every single comparison before writing.
- DO NOT CONTRADICT THE DATA — if the passing game is working well (high completion %, good yardage), do not recommend abandoning it. If inside runs average 3.0+ ypc, that is decent — do not say to abandon them. Only recommend stopping something if the numbers clearly show it is failing (below 3.0 ypc for runs, below 50% completion for passes). Every recommendation must be logically consistent with the actual first half stats.
- SCORE ACCURACY — always state the score correctly. If Team A has 13 points and Team B has 14 points, then Team A is LOSING by 1 point and Team B is WINNING by 1 point. The team with MORE points is winning. The team with FEWER points is losing. Double check who is winning before writing the summary. Never say a team "leads" when their score is lower than the opponent's.
- NO "TALE OF TWO HALVES" — this is a HALFTIME report. Only the first half has been played. The second half has NOT happened yet. Never use phrases like "tale of two halves", "game of two halves", "two different halves", or any language that implies both halves have already been played. You are analyzing ONE half of data and recommending adjustments for the upcoming second half.
- MISMATCH THRESHOLD — a mismatch only exists when the difference is 20 or more points. If YOUR stat is 767 and THEIR stat is 768 that is NOT a mismatch — it is an even matchup. Never say to exploit a matchup where your rating is lower than or equal to the opponent. Never recommend targeting a position as a mismatch unless your rating is at least 20 points higher than their corresponding defender rating. This applies to ALL matchups including TE TOT vs LB TOT — if the TE TOT is not at least 20 higher than LB TOT, do not recommend exploiting the TE as a mismatch.
- SACKS BELONG TO THE DEFENSE — "Sacked-Yds 3-21" listed under a team's stats means that team's QB was sacked 3 times for 21 yards lost. Sacks are a DEFENSIVE stat credited to the opposing defense. If Stony Brook shows "Sacked-Yds 3-21" that means Stony Brook's offense has a pass protection problem — their QB was sacked 3 times. It does NOT mean Stony Brook's defense recorded sacks. Always check which team the sack stat belongs to before writing any recommendation about pass rush or QB protection. If YOUR team has sacks listed, YOUR offense is struggling with protection. If THEIR team has sacks listed, THEIR defense is getting to your QB.
- TOP PERFORMERS FORMATTING — in the Top Performers section, never put a colon before stat lines. Write "5 rec, 89 yds" not ": 5 rec, 89 yds". No colon prefix on any stat line.
- ONE RUN DIRECTION — calculate average yards per carry for inside runs and outside runs separately from the game log. Recommend ONLY the direction with higher yards per carry. Never recommend both inside AND outside running in the same game plan. Pick one and commit to it with the data to back it up.
- OVERPOWERING LANGUAGE — never use "overpowers", "dominates", or "overwhelms" for any stat difference less than 20 points. A +4 edge (e.g. OL STR 88 vs DL STR 84) is a "slight edge" or "modest advantage". A +10 to +19 edge is a "solid advantage". Only use "dominates" or "overpowers" for differences of +20 or more.

ANALYSIS RULES:
- Read the game log carefully and identify actual play patterns — what run directions worked, what pass routes converted, which players performed
- Use the ratings to identify matchup advantages
- Combine what actually happened in the first half WITH the ratings to give the most accurate second half plan
- Only use these 8 meaningful matchups:
  RUN GAME: OL BLK vs DL STR (run blocking edge), OL STR vs DL STR (power run edge), RB SPD vs LB SPD (outside run, only if +15 or more), RB STR vs LB STR (short yardage edge)
  PASSING GAME: WR SPD vs DB SPD, WR AGI vs DB AGI, TE TOT vs LB TOT, TE SPD vs DB SPD

OUTPUT FORMAT — respond with clean HTML fragments (no <html>, <head>, or <body> tags). Use these elements:
- <h3> for section headers
- <p> for paragraphs
- <strong> for bold/emphasis
- <ul><li> for bullet lists
- <div class="performers-grid"> with two <div class="perf-col"> inside for the two-column Top Performers layout
- <div class="gameplan-bullet"> for each game plan recommendation
Do NOT use markdown syntax (no **, no ##, no -). Output raw HTML only.

OUTPUT SECTIONS IN ORDER:

<h3>First Half Summary</h3> — 6-8 sentences written like a dramatic color commentator. Include current score, what worked and what did not for each team with specific stats and player names (with position and team). End with what the game is hinging on. Be vivid and intense.

<h3>Top Performers — First Half</h3> — wrapped in <div class="performers-grid">. Two <div class="perf-col"> columns: one for each team. Top 2 offensive and top 2 defensive players per team based on actual stats. Show stat lines.

<h3>Second Half Game Plan</h3> — for the user's team only. 5-7 specific actionable items, each wrapped in <div class="gameplan-bullet">. Each must reference actual first half data or ratings. Include:
- Best run direction (outside vs inside) with actual ypc from game log
- Best passing target with catches and yards from game log
- Formation vs defense exploitation based on personnel (within the CURRENT formation only)
- Which players to target more based on first half performance AND ratings matchup
- Score situation urgency if losing by 2+ scores
Never include generic advice. Every item must have a specific reason."""

        trimmed_box = trim_box_for_api(box_raw)
        trimmed_log = trim_gamelog_for_api(gamelog_raw)

        user_message = f"""Your Team: {your_team}
Your Offense: {your_offense}
Opponent Team: {opp_team}
Their Defense: {their_defense}

Box Score & Team Stats:
{trimmed_box}

Game Log:
{trimmed_log}

Your Team Ratings:
{your_ratings_raw}

Opponent Team Ratings:
{opp_ratings_raw}"""

        try:
            client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=800,
                system=halftime_system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )
            raw_result = validate_ai_output(response.content[0].text)
            # Sanitize: only allow specific safe HTML tags
            import re as _re
            ALLOWED_TAGS = {'h3','h4','p','strong','em','ul','ol','li','div','span','br'}
            ALLOWED_CLASSES = {'performers-grid','perf-col','gameplan-bullet'}
            def _sanitize_html(html):
                # Strip any <script>, <style>, <iframe>, on* attributes etc.
                # Allow only whitelisted tags
                html = _re.sub(r'<script[^>]*>.*?</script>', '', html, flags=_re.DOTALL | _re.IGNORECASE)
                html = _re.sub(r'<style[^>]*>.*?</style>', '', html, flags=_re.DOTALL | _re.IGNORECASE)
                html = _re.sub(r'<iframe[^>]*>.*?</iframe>', '', html, flags=_re.DOTALL | _re.IGNORECASE)
                html = _re.sub(r'\bon\w+\s*=', '', html, flags=_re.IGNORECASE)
                # Strip class attributes that aren't in our allowed list
                def _filter_class(m):
                    tag = m.group(1).lower()
                    cls = m.group(2)
                    if tag not in ALLOWED_TAGS:
                        return ''
                    classes = [c.strip() for c in cls.split() if c.strip() in ALLOWED_CLASSES]
                    if classes:
                        return f'<{m.group(1)} class="{" ".join(classes)}"'
                    return f'<{m.group(1)}'
                html = _re.sub(r'<(\w+)\s+class="([^"]*)"', _filter_class, html)
                return html
            ai_result = _sanitize_html(raw_result)
        except Exception as e:
            print(f">>> HALFTIME ANALYZE ERROR: {e}", flush=True)
            traceback.print_exc()
            error = f"AI analysis failed: {str(e)}"

    # Parse halftime score from box score text
    # WIS box score can come in multiple formats:
    #
    # FORMAT 1 — jammed single line (no separators):
    #   "Stony Brook (18-0)31013#1Northern Iowa (18-0)7714"
    #   Pattern: TeamName (Record)Q1 Q2 Q3 Q4 Total ... TeamName (Record)Q1 Q2 ...
    #
    # FORMAT 2 — vertical (each value on its own line):
    #   Stony Brook (18-0)
    #   3
    #   10
    #   13
    #   #1
    #   Northern Iowa (18-0)
    #   7
    #   7
    #   14
    #
    # FORMAT 3 — pipe/tab separated:
    #   Stony Brook (18-0) | 3 | 10 | 13
    #
    # Halftime = Q1 + Q2 (first two score numbers after each team).
    # Records in parentheses like (18-0) are always ignored.
    your_score = None
    opp_score = None
    if box_raw and your_team and opp_team:
        print(f">>> SCORE PARSER: Looking for '{your_team}' and '{opp_team}'", flush=True)
        print(f">>> SCORE PARSER: First 200 chars of box_raw: {box_raw[:200]!r}", flush=True)

        def _split_jammed_digits(digits_str):
            """Split a jammed digit string like '31013' into individual scores [3, 10, 13].
            Scores are 0-99. Uses backtracking to find the best split where the last
            number equals the sum of the previous ones (total = Q1+Q2+...)."""
            results = []
            def _bt(pos, current):
                if pos == len(digits_str):
                    if len(current) >= 2:
                        results.append(list(current))
                    return
                # Try 2-digit first
                if pos + 2 <= len(digits_str):
                    two = digits_str[pos:pos+2]
                    if two[0] != '0':
                        current.append(int(two))
                        _bt(pos + 2, current)
                        current.pop()
                # Try 1-digit
                if pos + 1 <= len(digits_str):
                    current.append(int(digits_str[pos]))
                    _bt(pos + 1, current)
                    current.pop()
            _bt(0, [])
            # Prefer splits where last number = sum of others (it's the game total)
            best = None
            for r in results:
                if len(r) >= 3 and r[-1] == sum(r[:-1]):
                    if best is None or len(r) > len(best):
                        best = r
            if not best:
                for r in sorted(results, key=lambda x: -len(x)):
                    if 2 <= len(r) <= 5:
                        best = r
                        break
            return best

        def _parse_jammed_scores(text, team1, team2):
            """Parse scores from jammed format like 'Team1 (W-L)31013#1Team2 (W-L)7714'.
            Uses the opposing team name as a boundary to isolate each team's digit blob.
            Returns (team1_quarters, team2_quarters) or (None, None)."""
            t1_quarters = None
            t2_quarters = None

            for team, other_team, label in [(team1, team2, 'YOUR'), (team2, team1, 'OPP')]:
                escaped = re.escape(team)
                # Find where this team name appears
                team_match = re.search(escaped, text, re.IGNORECASE)
                if not team_match:
                    continue
                after_team = text[team_match.end():]

                # Find where the OTHER team name starts (to set a boundary)
                other_escaped = re.escape(other_team)
                other_match = re.search(other_escaped, after_team, re.IGNORECASE)
                if other_match:
                    segment = after_team[:other_match.start()]
                else:
                    segment = after_team

                # Strip all parenthesized groups (FL), (9-2), etc. and non-digit prefixes like #23
                segment_clean = re.sub(r'\([^)]*\)', '', segment)
                segment_clean = re.sub(r'#\d+', '', segment_clean)
                # Extract the digit blob — all consecutive digits after cleanup
                digit_blob = ''.join(re.findall(r'\d+', segment_clean))
                if not digit_blob:
                    continue
                quarters = _split_jammed_digits(digit_blob)
                print(f">>> SCORE PARSER JAMMED: {label} team '{team}' -> segment='{segment.strip()}', blob='{digit_blob}', split={quarters}", flush=True)
                if label == 'YOUR':
                    t1_quarters = quarters
                else:
                    t2_quarters = quarters

            return t1_quarters, t2_quarters

        def _parse_vertical_scores(lines, team_lower, words):
            """Parse scores from vertical format — team name line followed by number-only lines."""
            for i, line in enumerate(lines):
                line_lower = line.lower().strip()
                if not line_lower:
                    continue
                # Strip records to match team name
                clean = re.sub(r'\([^)]*\)', '', line_lower).strip()
                if not clean:
                    continue
                if team_lower in line_lower or any(w in clean for w in words):
                    # Check if this line ALSO has score numbers jammed onto it
                    # e.g. "Stony Brook (18-0)31013" — digits right after )
                    after_record = re.sub(r'^.*\)', '', line.strip())
                    jammed_nums = re.findall(r'\d+', after_record)
                    if jammed_nums:
                        return None  # jammed format, not vertical — let other parser handle
                    # Collect number-only lines below
                    scores = []
                    for j in range(i + 1, len(lines)):
                        stripped = lines[j].strip()
                        if not stripped:
                            continue
                        if re.match(r'^\d+$', stripped):
                            scores.append(int(stripped))
                        else:
                            break
                    if len(scores) >= 2:
                        return scores
            return None

        def _parse_separated_scores(line, team_lower, words):
            """Parse scores from pipe/tab separated format on a single line."""
            line_lower = line.lower()
            clean = re.sub(r'\([^)]*\)', '', line_lower)
            if team_lower in line_lower or any(w in clean for w in words):
                # Remove team name and record, get remaining numbers
                cleaned = re.sub(r'\([^)]*\)', '', line)
                nums = re.findall(r'\d+', cleaned)
                # First number after team name should be Q1
                if len(nums) >= 2:
                    return [int(n) for n in nums]
            return None

        your_team_lower = your_team.lower().strip()
        opp_team_lower = opp_team.lower().strip()
        your_words = [w for w in your_team_lower.split() if len(w) >= 3]
        opp_words = [w for w in opp_team_lower.split() if len(w) >= 3]

        your_quarters = None
        opp_quarters = None
        lines = box_raw.splitlines()

        # Strategy 1: Try jammed single-line format first
        # Join all lines and look for the jammed pattern
        joined = ' '.join(l.strip() for l in lines if l.strip())
        # Check if it looks jammed: team name followed immediately by digits after )
        if re.search(r'\)\s*\d', joined):
            print(f">>> SCORE PARSER: Trying jammed format parser", flush=True)
            your_quarters, opp_quarters = _parse_jammed_scores(joined, your_team, opp_team)

        # Strategy 2: Try vertical format (number-only lines below team name)
        if not your_quarters or not opp_quarters:
            print(f">>> SCORE PARSER: Trying vertical format parser", flush=True)
            if not your_quarters:
                your_quarters = _parse_vertical_scores(lines, your_team_lower, your_words)
                if your_quarters:
                    print(f">>> SCORE PARSER: Vertical YOUR: {your_quarters}", flush=True)
            if not opp_quarters:
                opp_quarters = _parse_vertical_scores(lines, opp_team_lower, opp_words)
                if opp_quarters:
                    print(f">>> SCORE PARSER: Vertical OPP: {opp_quarters}", flush=True)

        # Strategy 3: Try pipe/tab separated lines
        if not your_quarters or not opp_quarters:
            print(f">>> SCORE PARSER: Trying separated format parser", flush=True)
            for line in lines:
                if not your_quarters:
                    your_quarters = _parse_separated_scores(line, your_team_lower, your_words)
                if not opp_quarters:
                    opp_quarters = _parse_separated_scores(line, opp_team_lower, opp_words)

        # Halftime = Q1 + Q2 (first two numbers)
        if your_quarters and len(your_quarters) >= 2 and opp_quarters and len(opp_quarters) >= 2:
            your_score = your_quarters[0] + your_quarters[1]
            opp_score = opp_quarters[0] + opp_quarters[1]
            print(f">>> HALFTIME SCORE: {your_team} {your_score} (Q1={your_quarters[0]}, Q2={your_quarters[1]}) — "
                  f"{opp_team} {opp_score} (Q1={opp_quarters[0]}, Q2={opp_quarters[1]})", flush=True)
        else:
            print(f">>> SCORE PARSER: FAILED. your_quarters={your_quarters}, opp_quarters={opp_quarters}", flush=True)

    print(f">>> RENDER: your_score={your_score!r}, opp_score={opp_score!r}, ai_result={'yes' if ai_result else 'no'}", flush=True)

    return render_template(
        "halftime.html",
        your_team=your_team, opp_team=opp_team,
        box_raw=box_raw, gamelog_raw=gamelog_raw,
        your_ratings_raw=your_ratings_raw, opp_ratings_raw=opp_ratings_raw,
        your_offense=your_offense, their_defense=their_defense,
        ai_result=ai_result, error=error,
        your_score=your_score, opp_score=opp_score,
    )


@app.route("/recruiting")
@subscription_required
def recruiting():
    return render_template("recruiting.html")

# ─── Recruiting: D1-AA school exclusion list ─────────────────────────────────
D1AA_EXCLUDED_SCHOOLS = {s.strip().lower() for s in """
Michigan, Arizona State, Iowa, USC, Virginia, Penn State, Florida, Indiana,
DePaul, Syracuse, Texas, Purdue, Alabama, LSU, Florida State, Minnesota,
Nebraska, Southern Methodist, Vanderbilt, Clemson, Georgia, Notre Dame,
Oklahoma, Tennessee, Miami (FL), Boston College, South Carolina, Washington,
Missouri, Northwestern, UCLA, California, Colorado State, Iowa State, Temple,
Colorado, Kansas, West Virginia, Cincinnati, Ohio State, Toledo, Montana,
Air Force, Baylor, Navy, Washington State, Texas A&M, Texas Tech, Army,
Akron, Ole Miss, Stanford, Buffalo, Boise State, Tulane, Kent State, BYU,
Kentucky, Auburn, Alabama Birmingham, Oklahoma State, North Texas, Hawaii,
Pittsburgh, Utah State, Northern Illinois, San Diego State, New Mexico State,
Arkansas, Louisiana Tech, Kansas State, Oregon, Idaho, Mississippi State,
Fresno State, North Carolina, Wake Forest, South Florida, Georgia Tech,
Central Michigan, Oregon State, Michigan State, Tulsa, NC State, Wisconsin,
Ohio, Marshall, East Carolina, Rice, Memphis, Louisville, Utah, Connecticut,
Arizona, Bowling Green, Ball State, Southern Mississippi, Illinois,
Central Florida, Louisiana Lafayette, Marquette, Rutgers, Miami (OH),
Troy State, Arkansas State, UNLV, New Mexico, San Jose State, Virginia Tech,
UTEP, Middle Tennessee, Nevada, Western Michigan, Maryland, Louisiana Monroe,
Duke, Wyoming, Texas Christian, Eastern Michigan, Houston
""".replace('\n', ',').split(',') if s.strip()}


def parse_recruiting_players(raw_text):
    """Parse tab-separated recruiting board data into list of player dicts.

    Handles two formats:
    1. All data in columns (including a Considering column)
    2. Schools/considering on separate non-tab lines after each player row

    Also scans every column for numeric distance-like values if no explicit
    Distance header is found.

    Returns list of dicts with keys:
    name, pos, distance (float or None), gpa (str), work_ethic (str),
    ovr (str), considering (list of school strings), raw_line (original text),
    and all stat values found.
    """
    all_lines = raw_text.splitlines()
    lines_stripped = [l for l in all_lines if l.strip()]
    if not lines_stripped:
        return []

    # ── Normalize separators: convert multi-space to tabs ──
    # WIS data can be pasted with 4+ spaces instead of tabs, or a mix.
    # Count how many lines use tabs vs multi-space to decide.
    tab_lines = sum(1 for line in lines_stripped if '\t' in line)
    space_lines = sum(1 for line in lines_stripped if re.search(r'  {3,}', line) and '\t' not in line)
    if space_lines > tab_lines:
        print(f">>> RECRUITING: {space_lines} space-separated lines vs {tab_lines} tab lines — normalizing to tabs", flush=True)
        normalized = []
        for line in lines_stripped:
            stripped = line.strip()
            # Only normalize lines with multi-space gaps (data/header rows)
            # Leave school-name-only lines untouched
            if '\t' not in stripped and re.search(r'  {3,}', stripped):
                normalized.append(re.sub(r'  {2,}', '\t', stripped))
            else:
                normalized.append(stripped)
        lines_stripped = normalized

    # ── Pre-processing: strip WIS page noise ──
    # Step 1 (FIND THE START): header row contains both 'Pos' and 'Name' as tab-separated values
    original_count = len(lines_stripped)
    start_idx = 0
    for i, line in enumerate(lines_stripped):
        if '\t' not in line:
            continue
        tokens = [t.strip().lower() for t in line.split('\t')]
        if 'pos' in tokens and 'name' in tokens:
            start_idx = i
            print(f">>> NOISE FILTER: Header row found at line {i}: {line.strip()[:80]!r}", flush=True)
            break

    # Step 2 (FIND THE END): stop at first line matching any stop pattern
    STOP_PATTERNS = ['next >>', 'roster', 'reminders', 'terms of use',
                     'quick jump', 'popular on', 'signing period', 'recruiting ends']
    end_idx = len(lines_stripped)
    for i in range(start_idx + 1, len(lines_stripped)):
        lower = lines_stripped[i].lower().strip()
        if any(pat in lower for pat in STOP_PATTERNS):
            end_idx = i
            print(f">>> NOISE FILTER: Stop line found at line {i}: {lines_stripped[i].strip()[:80]!r}", flush=True)
            break

    filtered_lines = lines_stripped[start_idx:end_idx]

    stripped_count = original_count - len(filtered_lines)
    print(f">>> NOISE FILTER: kept lines {start_idx}-{end_idx} "
          f"({len(filtered_lines)} kept, {stripped_count} stripped from {original_count})", flush=True)
    lines_stripped = filtered_lines

    if not lines_stripped:
        return []

    # ── Find the header row ──
    HEADER_ALIASES = {
        'name': 'name', 'player': 'name',
        'pos': 'pos', 'position': 'pos',
        'dist': 'distance', 'distance': 'distance', 'miles': 'distance',
        'gpa': 'gpa',
        'we': 'work_ethic', 'work ethic': 'work_ethic', 'workethic': 'work_ethic',
        'ovr': 'ovr', 'overall': 'ovr', 'tot': 'ovr', 'total': 'ovr',
        'considering': 'considering', 'schools': 'considering', 'interest': 'considering',
        'considering (*human)': 'considering',
        # Stats
        't': 'T', 'technique': 'T', 'tech': 'T',
        'd': 'D', 'durability': 'D',
        'st': 'ST', 'stamina': 'ST',
        'str': 'STR', 'strength': 'STR', 'strn': 'STR',
        'a': 'A', 'agility': 'A', 'agl': 'A', 'ath': 'A', 'athleticism': 'A',
        'spd': 'SPD', 'speed': 'SPD',
        'e': 'E', 'elusiveness': 'E', 'elus': 'E',
        'gi': 'GI', 'game instinct': 'GI', 'instinct': 'GI',
        'h': 'H', 'hands': 'H',
        'blk': 'BLK', 'blocking': 'BLK', 'block': 'BLK',
        'tkl': 'TKL', 'tackling': 'TKL', 'tckl': 'TKL',
    }

    # Prefixes that WIS prepends to data rows but NOT to the header
    DATA_ROW_PREFIXES = {'watch recruit', 'you have contacted this recruit.', 'you have contacted this recruit'}

    header_idx = None
    col_map = {}  # col_index -> field_name
    header_tab_count = 0

    for i, line in enumerate(lines_stripped):
        tokens = re.split(r'\t', line)
        if len(tokens) < 3:
            continue
        temp_map = {}
        for j, tok in enumerate(tokens):
            key = tok.strip().lower()
            if key in HEADER_ALIASES:
                temp_map[j] = HEADER_ALIASES[key]
        # Need at least name/pos and one stat to call it a header
        fields_found = set(temp_map.values())
        has_identity = 'name' in fields_found or 'pos' in fields_found
        has_stat = bool(fields_found & {'T', 'STR', 'A', 'SPD', 'E', 'GI', 'H', 'BLK', 'TKL', 'ovr'})
        if has_identity and has_stat:
            header_idx = i
            col_map = temp_map
            header_tab_count = len(tokens)
            # If the header has a leading empty/whitespace token (WIS adds a
            # blank column before "Pos"), strip it by shifting all col_map
            # indices down by 1. This keeps the col_map aligned with data rows
            # after their WIS prefix ("Watch Recruit") is also stripped.
            if tokens[0].strip() == '' and 0 not in temp_map:
                col_map = {j - 1: f for j, f in temp_map.items()}
                header_tab_count -= 1
                print(f">>> PARSE: Header had leading empty token — shifted col_map by -1", flush=True)
            break

    if header_idx is None:
        print(">>> PARSE: No header row detected!", flush=True)
        return []

    print(f">>> PARSE: Header found at line {header_idx}: {lines_stripped[header_idx]!r}", flush=True)
    print(f">>> PARSE: Column map: {col_map}", flush=True)
    fields_found = set(col_map.values())
    has_distance_col = 'distance' in fields_found
    has_considering_col = 'considering' in fields_found
    print(f">>> PARSE: has_distance_col={has_distance_col}, has_considering_col={has_considering_col}", flush=True)

    # Show raw tokens for first 2 data rows
    for dbg_i, dbg_line in enumerate(lines_stripped[header_idx + 1:header_idx + 4]):
        dbg_tokens = re.split(r'\t', dbg_line)
        print(f">>> PARSE: Row {dbg_i+1} ({len(dbg_tokens)} cols): {dbg_line[:120]!r}", flush=True)

    # Determine name and pos columns with fallbacks
    name_cols = [j for j, f in col_map.items() if f == 'name']
    pos_cols = [j for j, f in col_map.items() if f == 'pos']
    name_col = name_cols[0] if name_cols else None
    pos_col = pos_cols[0] if pos_cols else None

    if name_col is None:
        name_col = 0 if pos_col != 0 else 1

    # ── Parse data rows, handling schools on separate lines ──
    players = []
    data_lines = lines_stripped[header_idx + 1:]

    for line in data_lines:
        tokens = re.split(r'\t', line)
        num_tabs = len(tokens)

        # Is this a "data row" (has tabs → structured data) or a "school line" (no tabs)?
        # A data row has a WIS prefix OR has enough tabs to hold name+pos+stats.
        # School lines are plain text with no tabs (or just one from copy-paste).
        first_token_lower = tokens[0].strip().lower() if tokens else ''
        is_wis_prefixed = first_token_lower in DATA_ROW_PREFIXES
        is_data_row = is_wis_prefixed or num_tabs >= 3

        if is_data_row:
            # This is a player data row
            tokens = [t.strip().lstrip('*').strip() for t in tokens]

            # WIS prepends "Watch Recruit" or "You have contacted this recruit."
            # as an extra first column not present in the header — strip it
            if tokens and tokens[0].lower() in DATA_ROW_PREFIXES:
                tokens = tokens[1:]
                print(f">>> PARSE: Stripped WIS prefix, now {len(tokens)} cols", flush=True)

            # Decide whether col_map applies: if this row has far fewer columns
            # than the header, the layout doesn't match — use content-based detection
            use_col_map = len(tokens) >= header_tab_count - 2

            if use_col_map:
                # ── Column-mapped parsing (data matches header layout) ──
                name = tokens[name_col] if name_col < len(tokens) else ''
                if not name:
                    continue

                pos = ''
                if pos_col is not None and pos_col < len(tokens):
                    pos = tokens[pos_col].upper()

                player = {
                    'name': name,
                    'pos': pos,
                    'distance': None,
                    'gpa': '',
                    'work_ethic': '',
                    'ovr': '',
                    'considering': [],
                    'stats': {},
                    'raw_line': line,
                }

                for j, field in col_map.items():
                    if j >= len(tokens):
                        continue
                    val = tokens[j]
                    if field == 'distance':
                        num = re.sub(r'[^0-9.]', '', val)
                        try:
                            player['distance'] = float(num) if num else None
                        except ValueError:
                            player['distance'] = None
                    elif field == 'gpa':
                        player['gpa'] = val
                    elif field == 'work_ethic':
                        player['work_ethic'] = val
                    elif field == 'ovr':
                        player['ovr'] = val
                    elif field == 'considering':
                        schools = re.split(r'[,/;]', val)
                        player['considering'] = [s.strip().rstrip('*').strip() for s in schools if s.strip()]
                    elif field in ('T', 'ST', 'STR', 'A', 'SPD', 'D', 'E', 'GI', 'H', 'BLK', 'TKL'):
                        try:
                            player['stats'][field] = int(float(val))
                        except (ValueError, TypeError):
                            pass
            else:
                # ── Content-based parsing (data row has fewer cols than header) ──
                # Typical WIS summary: Pos, Name, Height, Weight, ..., City/State, Miles
                # Detect fields by pattern rather than header position
                player = {
                    'name': '',
                    'pos': '',
                    'distance': None,
                    'gpa': '',
                    'work_ethic': '',
                    'ovr': '',
                    'considering': [],
                    'stats': {},
                    'raw_line': line,
                }

                KNOWN_POSITIONS = {'QB','RB','WR','TE','OL','DL','LB','DB','K','P',
                                   'C','OG','OT','DE','DT','NT','SS','FS','CB','S',
                                   'FB','SE','FL','NG','ILB','OLB','MLB','ATH'}

                for j, val in enumerate(tokens):
                    val_upper = val.upper().strip()
                    val_stripped = val.strip()

                    # Position (short uppercase, known set)
                    if not player['pos'] and val_upper in KNOWN_POSITIONS:
                        player['pos'] = val_upper
                    # Name (contains a space, mostly letters, not a city/state with comma)
                    elif not player['name'] and ' ' in val_stripped and ',' not in val_stripped and re.match(r'^[A-Za-z\s\.\'-]+$', val_stripped):
                        player['name'] = val_stripped
                    # GPA (decimal between 1.0 and 5.0)
                    elif not player['gpa'] and re.match(r'^\d\.\d+$', val_stripped):
                        gpa_val = float(val_stripped)
                        if 1.0 <= gpa_val <= 5.0:
                            player['gpa'] = val_stripped
                    # Distance (last pure integer in plausible range, or number > 100)
                    # We'll collect candidates and pick the best one below

                # Scan from right to left for the distance (last numeric value in range)
                for j in range(len(tokens) - 1, -1, -1):
                    val = tokens[j].strip()
                    m = re.match(r'^(\d+(?:\.\d+)?)\s*(?:mi(?:les?)?)?$', val, re.IGNORECASE)
                    if m:
                        dist_val = float(m.group(1))
                        if 10 < dist_val < 5000:
                            player['distance'] = dist_val
                            break

                if not player['name']:
                    continue

                print(f">>> PARSE (content-detect): name={player['name']!r}, pos={player['pos']!r}, "
                      f"dist={player['distance']}, gpa={player['gpa']!r}", flush=True)

            # If distance is still None, scan ALL columns for distance-like values
            if player['distance'] is None:
                for j in range(len(tokens)):
                    val = tokens[j].strip()
                    m = re.match(r'^(\d+(?:\.\d+)?)\s*(?:mi(?:les?)?)?$', val, re.IGNORECASE)
                    if m:
                        dist_val = float(m.group(1))
                        if 10 < dist_val < 5000:
                            player['distance'] = dist_val
                            break

            players.append(player)
        else:
            # This is likely a "schools/considering" line — attach to the last player
            if players:
                school_text = line.strip()
                if school_text:
                    schools = re.split(r'[,/;]', school_text)
                    # Strip whitespace AND trailing asterisks/markers from school names
                    new_schools = [s.strip().rstrip('*').strip() for s in schools if s.strip()]
                    players[-1]['considering'].extend(new_schools)

    return players


def filter_recruiting_players(players, division, position):
    """Apply all recruiting filters in Python. Returns (primary, extended) lists."""

    # Filter by position if specified
    if position:
        pos_upper = position.upper()
        players = [p for p in players if p['pos'] == pos_upper or not p['pos']]

    # ── Division-specific filtering ──
    if division == 'Division 1':
        # Division 1: distance filtering ONLY — NO school exclusions whatsoever
        print(f">>> D1 NO SCHOOL EXCLUSIONS APPLIED — {len(players)} players enter distance-only filter", flush=True)
        # Players with known distance: split into within 360 and beyond
        has_dist = [p for p in players if p['distance'] is not None]
        no_dist = [p for p in players if p['distance'] is None]
        primary = [p for p in has_dist if p['distance'] <= 360] + no_dist
        beyond = [p for p in has_dist if p['distance'] > 360]
        beyond.sort(key=lambda p: p['distance'])
        # Auto-expand if fewer than 10
        extended = []
        if len(primary) < 10:
            needed = 10 - len(primary)
            extended = beyond[:needed]
        return primary, extended

    elif division == 'Division 1-AA':
        # School exclusion check (D1-AA only)
        def passes_school_check(p):
            for school in p.get('considering', []):
                cleaned = school.strip().rstrip('*').strip().lower()
                if cleaned in D1AA_EXCLUDED_SCHOOLS:
                    return False
            return True
        players = [p for p in players if passes_school_check(p)]
        # Distance filtering — players with unknown distance go to primary
        has_dist = [p for p in players if p['distance'] is not None]
        no_dist = [p for p in players if p['distance'] is None]
        primary = [p for p in has_dist if p['distance'] <= 360] + no_dist
        beyond = [p for p in has_dist if p['distance'] > 360]
        beyond.sort(key=lambda p: p['distance'])
        # Auto-expand if fewer than 10
        extended = []
        if len(primary) < 10:
            needed = 10 - len(primary)
            extended = beyond[:needed]
        return primary, extended

    elif division in ('Division 2', 'Division 3'):
        # Only include undecided players
        def is_undecided(p):
            considering = p.get('considering', [])
            if not considering:
                return True
            return all(s.strip().lower() in ('undecided', '') for s in considering)
        players = [p for p in players if is_undecided(p)]
        # Distance filtering — players with unknown distance go to primary
        has_dist = [p for p in players if p['distance'] is not None]
        no_dist = [p for p in players if p['distance'] is None]
        primary = [p for p in has_dist if p['distance'] <= 800] + no_dist
        beyond = [p for p in has_dist if p['distance'] > 800]
        beyond.sort(key=lambda p: p['distance'])
        # Auto-expand if fewer than 10
        extended = []
        if len(primary) < 10:
            needed = 10 - len(primary)
            extended = beyond[:needed]
        return primary, extended

    # Default: no filtering
    return players, []


def format_players_for_claude(players, tag=''):
    """Format a list of player dicts into readable text for Claude."""
    lines = []
    for p in players:
        parts = [f"Name: {p['name']}"]
        if p['pos']:
            parts.append(f"Pos: {p['pos']}")
        if p['distance'] is not None:
            parts.append(f"Distance: {p['distance']} miles")
        if p['ovr']:
            parts.append(f"OVR: {p['ovr']}")
        if p['gpa']:
            parts.append(f"GPA: {p['gpa']}")
        if p['work_ethic']:
            parts.append(f"Work Ethic: {p['work_ethic']}")
        if p['stats']:
            stat_parts = [f"{k}: {v}" for k, v in p['stats'].items()]
            parts.append(f"Stats: {', '.join(stat_parts)}")
        if p.get('considering'):
            parts.append(f"Considering: {', '.join(p['considering'])}")
        if tag:
            parts.append(f"[{tag}]")
        lines.append(' | '.join(parts))
    return '\n'.join(lines)


@app.route("/recruiting/analyze", methods=["POST"])
@subscription_required
def recruiting_analyze():
    from flask import jsonify
    import json as _json
    data = request.get_json()
    division = data.get("division", "")
    position = data.get("position", "")
    player_data = data.get("player_data", "")

    if not player_data.strip():
        return jsonify(error="No player data provided."), 400

    # ── Step 1: Parse player data ──
    players = parse_recruiting_players(player_data)
    print(f">>> RECRUITING: Parsed {len(players)} players from input", flush=True)
    print(f">>> RECRUITING: Division={division}, Position={position}", flush=True)
    print(f">>> RECRUITING: Raw input length: {len(player_data)} chars, {len(player_data.splitlines())} lines", flush=True)
    # Debug: dump raw input to temp file for inspection
    try:
        with open('/tmp/recruiting_raw_input.txt', 'w') as _df:
            _df.write(player_data)
        print(f">>> RECRUITING: Raw input saved to /tmp/recruiting_raw_input.txt", flush=True)
    except Exception:
        pass
    # Debug: check separator type in first 5 lines
    first_lines = player_data.splitlines()[:5]
    for dl_i, dl in enumerate(first_lines):
        has_tab = '\t' in dl
        has_mspace = bool(re.search(r'  {3,}', dl))
        print(f">>> INPUT LINE {dl_i}: has_tab={has_tab}, has_multispace={has_mspace}, len={len(dl)}, repr={dl[:150]!r}", flush=True)

    # Debug: show first 3 parsed players
    for idx, p in enumerate(players[:3]):
        print(f">>> PLAYER {idx+1}: name={p['name']!r}, pos={p['pos']!r}, "
              f"distance={p['distance']!r}, considering={p['considering']!r}", flush=True)

    # Debug: show exclusion list sample
    sorted_excl = sorted(list(D1AA_EXCLUDED_SCHOOLS))[:10]
    print(f">>> EXCLUSION LIST (first 10 of {len(D1AA_EXCLUDED_SCHOOLS)}): {sorted_excl}", flush=True)

    # Debug: specifically check Ray Brock if present
    for p in players:
        if 'brock' in p['name'].lower():
            print(f">>> RAY BROCK DEBUG: name={p['name']!r}", flush=True)
            print(f">>> RAY BROCK DEBUG: considering (raw list)={p['considering']!r}", flush=True)
            for school in p['considering']:
                cleaned = school.strip().rstrip('*').strip().lower()
                in_excl = cleaned in D1AA_EXCLUDED_SCHOOLS
                print(f">>> RAY BROCK DEBUG: school={school!r} → repr={repr(school)} → "
                      f"cleaned={cleaned!r} → in_exclusion={in_excl}", flush=True)

    # Debug: test school check on first 3 players for D1-AA
    if division == 'Division 1-AA':
        for idx, p in enumerate(players[:3]):
            schools = p.get('considering', [])
            has_distance = p['distance'] is not None
            failed_school = None
            for school in schools:
                cleaned = school.strip().rstrip('*').strip().lower()
                if cleaned in D1AA_EXCLUDED_SCHOOLS:
                    failed_school = school
                    break
            if not has_distance:
                reason = "FAIL: missing distance"
            elif failed_school:
                cleaned = failed_school.strip().rstrip('*').strip().lower()
                reason = f"FAIL: school match '{failed_school}' (cleaned: '{cleaned}')"
            elif p['distance'] > 360:
                reason = f"PASS but beyond 360 (distance={p['distance']})"
            else:
                reason = "PASS"
            print(f">>> FILTER CHECK player {idx+1} ({p['name']!r}): {reason}", flush=True)

    if not players:
        # Could not parse structured data — send raw text to Claude with filtering instructions
        print(">>> RECRUITING: Could not parse players, sending raw data to Claude", flush=True)
        primary_text = player_data
        extended_text = ''
        has_extended = False
    else:
        # ── Step 2: Apply filters in Python ──
        primary, extended = filter_recruiting_players(players, division, position)
        print(f">>> RECRUITING: After filtering — primary={len(primary)}, extended={len(extended)}", flush=True)

        if not primary and not extended:
            return jsonify(error="No players qualified after applying filters for this division."), 400

        primary_text = format_players_for_claude(primary)
        extended_text = format_players_for_claude(extended, tag='EXTENDED RANGE') if extended else ''
        has_extended = len(extended) > 0

    # ── Step 3: Build prompt for Claude — evaluation only, no filtering ──
    system_prompt = """You are a football player evaluation engine. Your ONLY job is to evaluate and rank the pre-filtered players provided below.

IMPORTANT: These players have already been filtered and qualify. Evaluate and rank ONLY these players. Do NOT apply any additional filtering or exclusions. Do NOT remove any players.

STRICT RULES:
- Never ask questions
- Never explain what filters were applied
- Just output the JSON immediately with no preamble or commentary

ATTRIBUTE DEFINITIONS:
T=Technique, STR=Strength, A=Athleticism, SPD=Speed, E=Elusiveness, GI=Game Instinct, H=Hands, BLK=Blocking, TKL=Tackling, GPA=Grade Point Average, WE=Work Ethic, OVR=Overall Rating (never modify)
CORE ATTRIBUTES BY POSITION:
QB: T, STR, A, E, GI
RB: E, SPD, A, GI, H
WR: H, SPD, A, E, GI
TE: A, STR, BLK, H, GI, SPD, E
OL: BLK, STR, A, GI
DL: A, STR, TKL, GI, SPD
LB: TKL, GI, STR, A, SPD
DB: GI, SPD, A, TKL, H, STR
K: T, STR, GI, A, H
P: T, STR, GI, A, H
SPECIALIST STRENGTH MINIMUMS:
Division 1: STR >= 55
Division 1-AA: STR >= 50
Division 2 and 3: No minimum
Exclude specialists below minimum.
OVERALL RATING RULE — OVR comes from the data only, never calculate or adjust. If missing show "N/A".
RANKING — rank players by position attributes primarily but consider the whole player. Use WE (Work Ethic) then GPA as tiebreakers.
Players tagged with [EXTENDED RANGE] should be placed in a separate section header "Extended Range — Beyond 360 Miles" (or "Beyond 800 Miles" for D2/D3).

OUTPUT FORMAT — You MUST respond with valid JSON only. No text before or after the JSON. Use this exact structure:
{
  "sections": [
    {
      "header": "Results",
      "tiers": [
        {
          "name": "Tier 1 — Elite",
          "tier_num": 1,
          "players": [
            {
              "name": "Player Name",
              "rank": 1,
              "distance": "123 miles",
              "ovr": "85",
              "gpa": "3.5",
              "work_ethic": "75",
              "attributes": {"T": 80, "STR": 70, "A": 85, "E": 78, "GI": 82},
              "strengths": ["Strong arm", "Good vision"],
              "red_flags": ["Slow release"]
            }
          ]
        }
      ]
    }
  ]
}

SECTION RULES:
- If there are EXTENDED RANGE players, create two sections: first with header based on the distance limit, second with "Extended Range — Beyond X Miles".
- If no extended range players, use a single section with header "Results".
- Only include tiers that have players. If no players qualify for a tier, omit it.
- attributes object should contain ALL stats provided in the player data (A, SPD, STR, E, GI, H, BLK, TKL, T, D, ST, and any others present). Include every stat, not just core attributes.
- End the JSON and nothing else — do not add any text after the closing brace."""

    # Build user message with pre-filtered players
    user_parts = [f"Division: {division}", f"Position Group: {position}", "", "=== QUALIFYING PLAYERS ===", primary_text]
    if extended_text:
        user_parts.extend(["", "=== EXTENDED RANGE PLAYERS ===", extended_text])
    user_message = '\n'.join(user_parts)

    try:
        client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )
        result_text = response.content[0].text
        print(f">>> RECRUITING RAW RESPONSE (first 500): {result_text[:500]}", flush=True)

        # Try to parse as JSON server-side and return structured data
        cleaned = result_text.strip()
        # Strip markdown code fences
        if cleaned.startswith('```'):
            cleaned = re.sub(r'^```(?:json)?\s*\n?', '', cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r'\n?```\s*$', '', cleaned, flags=re.IGNORECASE)
        # Extract JSON object if there's text before/after
        first_brace = cleaned.find('{')
        last_brace = cleaned.rfind('}')
        if first_brace != -1 and last_brace > first_brace:
            cleaned = cleaned[first_brace:last_brace + 1]
        try:
            parsed = _json.loads(cleaned)
            return jsonify(result=parsed, format="json")
        except _json.JSONDecodeError as e:
            print(f">>> RECRUITING: JSON parse failed: {e}", flush=True)
            # Remove trailing commas and retry
            fixed = re.sub(r',\s*([}\]])', r'\1', cleaned)
            try:
                parsed = _json.loads(fixed)
                return jsonify(result=parsed, format="json")
            except _json.JSONDecodeError:
                pass
            # Try to repair truncated JSON (API may have cut off mid-response)
            # Close any open strings, arrays, objects
            repaired = cleaned
            # Count unclosed braces/brackets
            open_braces = repaired.count('{') - repaired.count('}')
            open_brackets = repaired.count('[') - repaired.count(']')
            # If truncated mid-string, close the string
            if repaired.count('"') % 2 != 0:
                repaired += '"'
            # Remove any trailing comma or partial key/value
            repaired = re.sub(r',\s*$', '', repaired)
            repaired = re.sub(r',\s*"[^"]*$', '', repaired)
            # Close arrays then objects
            repaired += ']' * max(0, open_brackets)
            repaired += '}' * max(0, open_braces)
            repaired = re.sub(r',\s*([}\]])', r'\1', repaired)
            try:
                parsed = _json.loads(repaired)
                print(f">>> RECRUITING: Repaired truncated JSON successfully", flush=True)
                return jsonify(result=parsed, format="json")
            except _json.JSONDecodeError as e2:
                print(f">>> RECRUITING: JSON repair also failed: {e2}", flush=True)
                print(f">>> RECRUITING: Last 200 chars: {cleaned[-200:]!r}", flush=True)
        # Fallback: return raw text
        return jsonify(result=result_text, format="text")
    except Exception as e:
        print(f">>> RECRUITING ANALYZE ERROR: {e}", flush=True)
        traceback.print_exc()
        return jsonify(error=str(e)), 500


@app.route("/training")
@subscription_required
def training():
    conn = get_db()
    videos = conn.execute('SELECT * FROM videos ORDER BY display_order ASC, created_at DESC').fetchall()
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
            max_order = conn.execute('SELECT COALESCE(MAX(display_order), 0) FROM videos').fetchone()[0]
            conn.execute('INSERT INTO videos (title, description, video_url, display_order) VALUES (?, ?, ?, ?)',
                         (title, description, embed_url, max_order + 1))
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
    videos = conn.execute('SELECT * FROM videos ORDER BY display_order ASC, created_at DESC').fetchall()
    conn.close()
    return render_template("admin.html", videos=videos)

@app.route("/admin/reorder", methods=["POST"])
def admin_reorder():
    from flask import jsonify
    order = request.get_json()
    if order and isinstance(order, list):
        conn = get_db()
        for i, video_id in enumerate(order):
            conn.execute('UPDATE videos SET display_order = ? WHERE id = ?', (i, int(video_id)))
        conn.commit()
        conn.close()
    return jsonify(ok=True)


if __name__ == '__main__':
    api_key = os.environ.get('ANTHROPIC_API_KEY', 'NOT FOUND')
    print(f'>>> ANTHROPIC_API_KEY starts with: {api_key[:15]}', flush=True)
    print(">>> Registered routes:", flush=True)
    for rule in sorted(app.url_map.iter_rules(), key=lambda r: r.rule):
        print(f">>>   {rule.rule:30s} → {rule.endpoint} [{', '.join(rule.methods - {'OPTIONS', 'HEAD'})}]", flush=True)
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
