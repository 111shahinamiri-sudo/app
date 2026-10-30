# ============================================================
# PART 1: Imports, Config, Database
# ============================================================
from flask import Flask, jsonify, request, render_template_string, session, redirect, url_for, Response
import serial
import serial.tools.list_ports
import threading
import time
import sqlite3
import csv
import io
import json
import os
import logging
import urllib.request
import urllib.parse
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta

app = Flask(__name__)

# --- FIX #19: secret_key امن ---
_secret = os.environ.get('FLASK_SECRET_KEY')
if not _secret:
    _secret = 'hoseinipour_secret_change_me'
    logging.warning("FLASK_SECRET_KEY not set! Using insecure default. Set env var in production.")
app.secret_key = _secret

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

DB_NAME = 'furnace_data.db'
TIME_OFFSET_SECONDS = 0
CONFIG = {"temp_unit": "C", "temp_offset": 0.0, "safety_limit": 1200.0, "tg_token": "", "tg_chat_id": ""}

MAX_FURNACES = 22
OFFLINE_TIMEOUT = 15.0
POLL_INTERVAL = 0.05
RX_TIMEOUT = 0.15

ser = None
found_port_name = "قطع"
serial_lock = threading.Lock()
# --- FIX #13/#14: قفل فرمان‌های کنترلی برای جلوگیری از race با polling ---
command_lock = threading.Lock()
serial_rx_buffer = ""


def get_db():
    conn = sqlite3.connect(DB_NAME, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS temp_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, time TEXT, furnace_id INTEGER,
        temp REAL, target_temp REAL, cycle_id INTEGER DEFAULT 0,
        relay INTEGER DEFAULT 0, sensor_err INTEGER DEFAULT 0)''')
    # --- FIX #17: cycle_id به bake_history اضافه شد ---
    c.execute('''CREATE TABLE IF NOT EXISTS bake_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, furnace_id INTEGER,
        start_time TEXT, end_time TEXT, status TEXT, total_steps INTEGER,
        cycle_id INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS system_state (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute("SELECT * FROM users WHERE username='admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users (username, password) VALUES (?, ?)",
                  ('admin', generate_password_hash('1234')))
    conn.commit()
    conn.close()


init_db()


def get_system_time():
    return datetime.now() + timedelta(seconds=TIME_OFFSET_SECONDS)

# ============================================================
# PART 2: Telegram, State Save/Load, Auth (Login/Logout)
# ============================================================

# --- FIX #23/#39: تلگرام در thread جداگانه تا polling را بلاک نکند ---
def _send_telegram_sync(text):
    if not CONFIG['tg_token'] or not CONFIG['tg_chat_id']:
        return
    try:
        url = f"https://api.telegram.org/bot{CONFIG['tg_token']}/sendMessage"
        data = urllib.parse.urlencode({'chat_id': CONFIG['tg_chat_id'], 'text': text}).encode('utf-8')
        urllib.request.urlopen(url, data=data, timeout=5)
    except Exception as e:
        log.error(f"Telegram Error: {e}")


def send_telegram_message(text):
    threading.Thread(target=_send_telegram_sync, args=(text,), daemon=True).start()


furnaces = {i: {
    "mode": "MANUAL", "profile": [], "step": 0, "step_timer": 0,
    "start_time": 0, "total_duration": 0, "temp": 0.0,
    "last_seen": 0, "relay": "OFF", "steps": [], "remaining_time": 0,
    "cycle_id": 0, "paused_elapsed": 0, "remaining_from_arduino": False,
    "sensor_err": False, "is_online": False, "config_sent": False,
    "last_arduino_step": -1, "live_target": 0.0, "_last_logged": {}
} for i in range(1, MAX_FURNACES + 1)}
active_furnaces_count = 0


def save_state():
    try:
        conn = get_db()
        c = conn.cursor()
        state_data = {
            "active_furnaces_count": active_furnaces_count,
            "furnaces": {str(k): {
                "mode": v["mode"], "profile": v["profile"], "step": v["step"],
                "step_timer": v["step_timer"], "start_time": v["start_time"],
                "total_duration": v["total_duration"], "steps": v["steps"],
                "cycle_id": v["cycle_id"], "paused_elapsed": v["paused_elapsed"]
            } for k, v in furnaces.items()}
        }
        c.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES (?, ?)",
                  ('current_state', json.dumps(state_data)))
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"save_state: {e}")


def load_state():
    global active_furnaces_count, TIME_OFFSET_SECONDS, CONFIG
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT value FROM system_state WHERE key='current_state'")
        row = c.fetchone()
        c.execute("SELECT value FROM system_state WHERE key='time_offset'")
        offset_row = c.fetchone()
        if offset_row:
            TIME_OFFSET_SECONDS = int(offset_row[0])
        c.execute("SELECT value FROM system_state WHERE key='config'")
        config_row = c.fetchone()
        if config_row:
            CONFIG.update(json.loads(config_row[0]))
        conn.close()
        if row:
            state_data = json.loads(row[0])
            active_furnaces_count = state_data.get("active_furnaces_count", 0)
            for k, v in state_data.get("furnaces", {}).items():
                if int(k) in furnaces:
                    furnaces[int(k)].update(v)
    except Exception as e:
        log.error(f"load_state: {e}")


LOGIN_PAGE = """<!DOCTYPE html><html lang="fa" dir="rtl"><head><meta charset="UTF-8"><title>ورود</title><style>body { background: #0f172a; color: white; font-family: Tahoma; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; } .login-box { background: #1e293b; padding: 40px; border-radius: 15px; border: 1px solid #334155; text-align: center; width: 320px; } h2 { color: #38bdf8; } input { width: 90%; padding: 12px; margin: 10px 0; border-radius: 8px; border: 1px solid #475569; background: #0f172a; color: white; } button { width: 100%; padding: 12px; background: #0ea5e9; color: white; border: none; border-radius: 8px; font-weight: bold; cursor: pointer; } .error { color: #ef4444; background: rgba(239, 68, 68, 0.1); padding: 10px; border-radius: 5px; margin-bottom: 15px; }</style></head><body><div class="login-box"><h2>🏭 ورود به سیستم</h2>{% if error %}<div class="error">{{ error }}</div>{% endif %}<form method="POST"><input type="text" name="username" placeholder="نام کاربری" required><input type="password" name="password" placeholder="رمز عبور" required><button type="submit">ورود</button></form></div></body></html>"""


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT password FROM users WHERE username=?", (username,))
        user = c.fetchone()
        conn.close()
        if user and check_password_hash(user[0], password):
            session['logged_in'] = True
            session['username'] = username
            return redirect(url_for('index'))
        else:
            error = 'نام کاربری یا رمز عبور اشتباه است.'
            time.sleep(0.5)
    return render_template_string(LOGIN_PAGE, error=error)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# ============================================================
# PART 3: Users, System Config, Time, Backup, Cleanup
# ============================================================

# --- FIX #22: فقط admin می‌تواند کاربران را مدیریت کند ---
@app.route('/manage_users', methods=['POST'])
@login_required
def manage_users():
    if session.get('username') != 'admin':
        return jsonify({"status": "error", "msg": "فقط مدیر سیستم مجاز است."})
    d = request.json
    new_user = d.get('username')
    new_pass = d.get('password')
    if not new_user or not new_pass:
        return jsonify({"status": "error", "msg": "نام کاربری و رمز الزامی است."})
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO users (username, password) VALUES (?, ?)",
                  (new_user, generate_password_hash(new_pass)))
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)})


def _broadcast_setcfg():
    if ser is None:
        return
    for fid in range(1, active_furnaces_count + 1):
        try:
            with command_lock:
                with serial_lock:
                    ser.write(f"{fid},SETCFG:{CONFIG['temp_offset']},{CONFIG['safety_limit']}\n".encode())
                    ser.flush()
            time.sleep(0.1)
            furnaces[fid]['config_sent'] = True
        except Exception as e:
            log.error(f"SETCFG broadcast error: {e}")


@app.route('/save_system_config', methods=['POST'])
@login_required
def save_system_config():
    global CONFIG
    new_config = request.json or {}
    try:
        off = float(new_config.get('temp_offset', 0))
        lim = float(new_config.get('safety_limit', 1200))
    except (ValueError, TypeError):
        return jsonify({"status": "error", "msg": "مقادیر عددی نامعتبر"})
    if off < -50 or off > 50:
        return jsonify({"status": "error", "msg": "آفست باید بین -50 تا 50 باشد"})
    if lim < 100 or lim > 1400:
        return jsonify({"status": "error", "msg": "حد ایمنی باید بین 100 تا 1400 باشد"})
    CONFIG.update(new_config)
    CONFIG['temp_offset'] = off
    CONFIG['safety_limit'] = lim
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('config', ?)",
                  (json.dumps(CONFIG),))
        conn.commit()
        conn.close()
        _broadcast_setcfg()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)})


@app.route('/set_time', methods=['POST'])
@login_required
def set_time():
    global TIME_OFFSET_SECONDS
    dt_str = request.json.get('datetime')
    try:
        user_time = datetime.strptime(dt_str, '%Y-%m-%dT%H:%M')
        TIME_OFFSET_SECONDS = int((user_time - datetime.now()).total_seconds())
        conn = get_db()
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('time_offset', ?)",
                  (str(TIME_OFFSET_SECONDS),))
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)})


@app.route('/backup_data')
@login_required
def backup_data():
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT * FROM bake_history")
        bake_rows = [dict(zip([col[0] for col in c.description], row)) for row in c.fetchall()]
        c.execute("SELECT * FROM temp_logs")
        temp_rows = [dict(zip([col[0] for col in c.description], row)) for row in c.fetchall()]
        c.execute("SELECT * FROM system_state")
        state_rows = {row[0]: row[1] for row in c.fetchall()}
        conn.close()
        return Response(
            json.dumps({"bake_history": bake_rows, "temp_logs": temp_rows, "system_state": state_rows},
                       ensure_ascii=False, indent=2),
            mimetype="application/json",
            headers={"Content-disposition": "attachment; filename=furnace_backup.json"})
    except Exception as e:
        return f"خطا در پشتیبان‌گیری: {e}", 500


@app.route('/cleanup_old_logs', methods=['POST'])
@login_required
def cleanup_old_logs():
    try:
        days = int(request.json.get('days', 30))
        if days < 1:
            days = 1
        if days > 3650:
            days = 3650
    except (ValueError, TypeError):
        return jsonify({"status": "error", "msg": "تعداد روز نامعتبر"})
    try:
        cutoff = (get_system_time() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM temp_logs WHERE time < ?", (cutoff,))
        count = c.fetchone()[0]
        c.execute("DELETE FROM temp_logs WHERE time < ?", (cutoff,))
        conn.commit()
        # --- FIX #16: VACUUM خارج از تراکنش ---
        try:
            conn.isolation_level = None
            conn.execute("VACUUM")
            conn.isolation_level = ''
        except Exception as ve:
            log.warning(f"VACUUM failed: {ve}")
        conn.close()
        return jsonify({"status": "ok", "deleted": count, "cutoff": cutoff})
    except Exception as e:
        log.error(f"cleanup_old_logs: {e}")
        return jsonify({"status": "error", "msg": str(e)})


@app.route('/manifest.json')
def manifest():
    return jsonify({"name": "سیستم کوره‌ها", "short_name": "کوره‌ها", "start_url": "/",
                    "display": "standalone", "background_color": "#0f172a", "theme_color": "#0ea5e9"})


@app.route('/sw.js')
def sw():
    return Response(
        "self.addEventListener('fetch', (e) => { e.respondWith(caches.match(e.request).then((r) => r || fetch(e.request))); });",
        mimetype='application/javascript')

# ============================================================
# PART 4: Serial Auto-Connect, Arduino Parser
# ============================================================

def auto_connect():
    global ser, found_port_name, serial_rx_buffer
    with serial_lock:
        if ser is not None and ser.is_open:
            return
        for port in serial.tools.list_ports.comports():
            if any(k in port.description for k in ["Arduino", "CH340", "USB", "UART", "Serial"]):
                try:
                    s = serial.Serial()
                    s.port = port.device
                    s.baudrate = 9600
                    s.timeout = 0.05
                    s.write_timeout = 0.3
                    s.open()
                    # --- FIX #11: dtr/rts بعد از open تنظیم می‌شود ---
                    try:
                        s.dtr = False
                        s.rts = False
                    except Exception:
                        pass
                    time.sleep(0.5)
                    s.reset_input_buffer()
                    ser = s
                    found_port_name = port.device
                    serial_rx_buffer = ""
                    log.info(f"اتصال برقرار شد: {port.device}")
                    return
                except Exception as e:
                    log.debug(f"خطا در باز کردن {port.device}: {e}")
                    ser = None
        found_port_name = "قطع"


auto_connect()
load_state()


def parse_arduino_line(line):
    line = line.strip()
    if not line:
        return None
    parts = line.split(",")
    if len(parts) < 6:
        return None
    try:
        r_id = int(parts[0].strip())
    except ValueError:
        return None
    if r_id not in furnaces:
        return None
    result = {"id": r_id}
    try:
        for p in parts[1:]:
            p = p.strip()
            if ":" not in p:
                continue
            k, v = p.split(":", 1)
            k = k.strip().upper()
            v = v.strip()
            if k == "TEMP":
                if v.upper() == "ERR":
                    result['sensor_err'] = True
                    result['temp'] = 0.0
                else:
                    result['sensor_err'] = False
                    result['temp'] = float(v)
            elif k == "RELAY":
                result['relay'] = v.upper()
            elif k == "STATE":
                code = v.upper()
                if code == "RUN":
                    result['mode'] = "RUNNING"
                elif code == "PAU":
                    result['mode'] = "PAUSED"
                elif code == "FIN":
                    result['mode'] = "FINISHED"
                elif code == "ERR":
                    result['mode'] = "ERROR"
                else:
                    result['mode'] = "MANUAL"
            elif k == "STEP":
                # --- FIX #2: پشتیبانی از عدد منفی/مثبت ---
                try:
                    result['step'] = int(v)
                except ValueError:
                    result['step'] = 0
            elif k == "REM":
                # --- FIX #15: پشتیبانی از + و - ---
                try:
                    result['remaining'] = max(0, int(v))
                except ValueError:
                    result['remaining'] = 0
            elif k == "STEPS":
                try:
                    result['steps_total'] = int(v)
                except ValueError:
                    result['steps_total'] = 0
            elif k == "TGT":
                try:
                    result['target_temp'] = float(v)
                except ValueError:
                    pass
    except (ValueError, IndexError) as e:
        log.warning(f"Parse value error: {e} | line={line}")
        return None
    return result

# ============================================================
# PART 5: Apply Status, Serial Polling Loop (sim)
# ============================================================

def apply_arduino_status(parsed):
    fid = parsed['id']
    f = furnaces[fid]
    prev_mode = f['mode']
    f['sensor_err'] = parsed.get('sensor_err', False)
    f['temp'] = parsed.get('temp', 0.0)
    f['relay'] = parsed.get('relay', 'OFF')
    f['mode'] = parsed.get('mode', 'MANUAL')
    step_val = parsed.get('step', 0)
    f['step'] = max(0, step_val - 1) if step_val > 0 else 0
    if 'remaining' in parsed:
        f['remaining_time'] = parsed['remaining']
        f['remaining_from_arduino'] = True
    if 'target_temp' in parsed:
        f['live_target'] = parsed['target_temp']
    f['last_seen'] = time.time()
    f['is_online'] = True
    f['last_arduino_step'] = f['step']

    if f['mode'] == 'RUNNING' and prev_mode != 'RUNNING' and f['start_time'] == 0:
        f['start_time'] = time.time()

    if f['mode'] == 'FINISHED' and prev_mode != 'FINISHED':
        try:
            now_str = get_system_time().strftime('%Y-%m-%d %H:%M:%S')
            conn = get_db()
            c = conn.cursor()
            c.execute("UPDATE bake_history SET end_time=?, status=? WHERE furnace_id=? AND end_time IS NULL",
                      (now_str, 'پایان یافته (کامل)', fid))
            conn.commit()
            conn.close()
            send_telegram_message(f"✅ پخت کوره {fid} با موفقیت به پایان رسید.")
        except Exception as e:
            log.error(f"DB Finish Error: {e}")

    if f['mode'] == 'ERROR' and prev_mode != 'ERROR':
        send_telegram_message(f"⚠️ خطای ایمنی در کوره {fid}! (دما یا سنسور)")
        log.warning(f"Furnace {fid} entered ERROR state")


def sim():
    global ser, serial_rx_buffer
    poll_index = 1

    while True:
        if ser is None or not ser.is_open:
            auto_connect()
            if ser is None:
                for f in furnaces.values():
                    f['is_online'] = False
                    f['relay'] = 'OFF'
                time.sleep(1)
                continue

        if active_furnaces_count == 0:
            time.sleep(0.5)
            continue

        fid = poll_index
        poll_index += 1
        if poll_index > active_furnaces_count:
            poll_index = 1

        if ser is None or not ser.is_open:
            time.sleep(0.1)
            continue

        try:
            # --- FIX #13: اگر command_lock قفل است، polling را رد کن ---
            if not command_lock.acquire(blocking=False):
                time.sleep(0.05)
                continue
            try:
                with serial_lock:
                    if ser.in_waiting > 0:
                        ser.reset_input_buffer()
                        serial_rx_buffer = ""  # --- FIX #12 ---
                    ser.write(f"{fid},GET\n".encode())
                    ser.flush()
            finally:
                command_lock.release()

            deadline = time.time() + RX_TIMEOUT
            while time.time() < deadline:
                if ser is None or not ser.is_open:
                    break
                with serial_lock:
                    n = ser.in_waiting
                    if n > 0:
                        raw = ser.read(n)
                        serial_rx_buffer += raw.decode('utf-8', errors='ignore')
                if '\n' in serial_rx_buffer:
                    break
                time.sleep(0.01)

            while '\n' in serial_rx_buffer:
                line, serial_rx_buffer = serial_rx_buffer.split('\n', 1)
                parsed = parse_arduino_line(line)
                if parsed:
                    apply_arduino_status(parsed)

            if len(serial_rx_buffer) > 1024:
                serial_rx_buffer = ""

        except PermissionError:
            log.warning(f"PermissionError on port (furnace {fid}).")
            time.sleep(0.5)
        except Exception as e:
            log.error(f"Serial error (furnace {fid}): {e}")
            try:
                if ser and ser.is_open:
                    ser.close()
            except Exception:
                pass
            ser = None
            serial_rx_buffer = ""

# ============================================================
# PART 6: DB Logger
# ============================================================

def log_temp_to_db(f_id, temp, target, cycle_id=0, relay=0, sensor_err=0):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "INSERT INTO temp_logs (time, furnace_id, temp, target_temp, cycle_id, relay, sensor_err) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (get_system_time().strftime('%Y-%m-%d %H:%M:%S'),
             f_id, temp, target, cycle_id, relay, sensor_err))
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"log_temp_to_db failed: {e}")


def db_logger():
    while True:
        time.sleep(5)
        for fid, f in furnaces.items():
            if f['mode'] in ['RUNNING', 'PAUSED'] and f.get('is_online'):
                target = 0
                if f['profile'] and f['step'] < len(f['profile']):
                    step_cfg = f['profile'][f['step']]
                    if step_cfg.get('ramp', True):
                        # --- FIX #4: fallback به step_cfg['temp'] اگر live_target صفر بود ---
                        target = f.get('live_target') or step_cfg.get('temp', 0)
                    else:
                        target = step_cfg.get('temp', 0)

                last = f.get('_last_logged', {})
                last_temp = last.get('temp', None)
                last_relay = last.get('relay', None)
                last_target = last.get('target', None)

                temp_changed = (last_temp is None) or (abs(f['temp'] - last_temp) >= 1.0)
                relay_changed = (last_relay is None) or (f['relay'] != last_relay)
                target_changed = (last_target is None) or (abs(target - last_target) >= 1.0)

                if temp_changed or relay_changed or target_changed:
                    log_temp_to_db(fid, f['temp'], target,
                                   f.get('cycle_id', 0),
                                   1 if f['relay'] == 'ON' else 0,
                                   1 if f['sensor_err'] else 0)
                    f['_last_logged'] = {'temp': f['temp'], 'relay': f['relay'], 'target': target}

# ============================================================
# PART 7: Furnace Control Routes (Start/Pause/Resume/Stop/Manage)
# ============================================================

def _send_profile_to_arduino(f_id, profile):
    """ارسال SETCFG + PROFILE + START به آردوینو به صورت اتمیک."""
    if ser is None or not ser.is_open:
        return False
    try:
        # --- FIX #14: کل عملیات تحت command_lock تا polling با آن قاطی نشود ---
        with command_lock:
            with serial_lock:
                ser.write(f"{f_id},SETCFG:{CONFIG['temp_offset']},{CONFIG['safety_limit']}\n".encode())
                ser.flush()
            time.sleep(0.2)

            profile_str = "".join([
                f"{int(s['temp'])},{int(s['time'])},{1 if s.get('ramp', True) else 0};"
                for s in profile
            ])
            with serial_lock:
                ser.write(f"{f_id},PROFILE:{profile_str}\n".encode())
                ser.flush()
            time.sleep(0.2)

            with serial_lock:
                ser.write(f"{f_id},START\n".encode())
                ser.flush()
            time.sleep(0.3)
        return True
    except Exception as e:
        log.error(f"send_profile: {e}")
        return False


@app.route('/add_furnace', methods=['POST'])
@login_required
def add_furnace():
    global active_furnaces_count
    if active_furnaces_count < MAX_FURNACES:
        active_furnaces_count += 1
        save_state()
    return jsonify({"status": "ok", "count": active_furnaces_count})


@app.route('/remove_furnace', methods=['POST'])
@login_required
def remove_furnace():
    global active_furnaces_count
    if active_furnaces_count > 0:
        # ریست کامل state کوره حذف‌شده
        furnaces[active_furnaces_count].update({
            "mode": "MANUAL", "profile": [], "step": 0, "start_time": 0,
            "total_duration": 0, "steps": [], "cycle_id": 0,
            "config_sent": False, "last_arduino_step": -1,
            "remaining_from_arduino": False, "remaining_time": 0,
            "live_target": 0.0, "paused_elapsed": 0, "_last_logged": {}
        })
        active_furnaces_count -= 1
        save_state()
    return jsonify({"status": "ok", "count": active_furnaces_count})


@app.route('/update_steps', methods=['POST'])
@login_required
def update_steps():
    d = request.json
    try:
        fid = int(d['id'])
    except (ValueError, KeyError, TypeError):
        return jsonify({"status": "error", "msg": "شناسه نامعتبر"})
    if fid not in furnaces:
        return jsonify({"status": "error", "msg": "کوره یافت نشد"})
    # --- FIX #42: تغییر پروفایل در حین اجرا ممنوع ---
    if furnaces[fid]['mode'] in ('RUNNING', 'PAUSED'):
        return jsonify({"status": "error", "msg": "در حین اجرا نمی‌توان پروفایل را تغییر داد"})
    steps = d.get('steps', [])
    normalized = [{
        "temp": int(s.get('temp', 0)),
        "time": int(s.get('time', 0)),
        "ramp": bool(s.get('ramp', True))
    } for s in steps]
    furnaces[fid]['steps'] = normalized
    save_state()
    return jsonify({"status": "ok"})


@app.route('/start', methods=['POST'])
@login_required
def start():
    d = request.json
    try:
        f_id = int(d['id'])
    except (ValueError, KeyError, TypeError):
        return jsonify({"status": "error", "msg": "شناسه کوره نامعتبر"})
    profile = d.get('profile', [])
    if not profile:
        return jsonify({"status": "error", "msg": "پروفایل خالی است"})
    if ser is None or not ser.is_open:
        return jsonify({"status": "error", "msg": "ارتباط با آردوینو برقرار نیست"})
    if not furnaces[f_id].get('is_online'):
        return jsonify({"status": "error", "msg": "کوره آفلاین است"})

    profile = [{
        "temp": int(s.get('temp', 0)),
        "time": int(s.get('time', 0)),
        "ramp": bool(s.get('ramp', True))
    } for s in profile]

    total_mins = sum(step['time'] for step in profile)
    cycle_id = int(time.time() * 1000)

    furnaces[f_id].update({
        "mode": "RUNNING", "profile": profile, "step": 0,
        "start_time": time.time(), "total_duration": total_mins * 60,
        "cycle_id": cycle_id,
        "paused_elapsed": 0, "config_sent": False,
        "last_arduino_step": -1, "live_target": 0.0,
        "remaining_from_arduino": False, "remaining_time": 0,
        "_last_logged": {}  # --- FIX #8: پاک کردن کش لاگ ---
    })

    try:
        now_str = get_system_time().strftime('%Y-%m-%d %H:%M:%S')
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "INSERT INTO bake_history (furnace_id, start_time, end_time, status, total_steps, cycle_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (f_id, now_str, None, 'در حال اجرا', len(profile), cycle_id))
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"DB Start Error: {e}")

    save_state()
    ok = _send_profile_to_arduino(f_id, profile)
    if not ok:
        # rollback state در صورت عدم ارسال به آردوینو
        furnaces[f_id].update({
            "mode": "MANUAL", "profile": [], "step": 0,
            "start_time": 0, "total_duration": 0,
            "remaining_time": 0, "live_target": 0.0
        })
        # بستن رکورد باز در bake_history
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("UPDATE bake_history SET end_time=?, status=? WHERE cycle_id=? AND end_time IS NULL",
                      (get_system_time().strftime('%Y-%m-%d %H:%M:%S'), 'خطا در ارسال', cycle_id))
            conn.commit()
            conn.close()
        except Exception as e:
            log.error(f"DB Rollback Error: {e}")
        save_state()
        return jsonify({"status": "error", "msg": "ارسال به آردوینو ناموفق"})
    return jsonify({"status": "ok"})


@app.route('/pause', methods=['POST'])
@login_required
def pause():
    d = request.json
    try:
        f_id = int(d['id'])
    except (ValueError, KeyError, TypeError):
        return jsonify({"status": "error", "msg": "شناسه نامعتبر"})
    if f_id not in furnaces:
        return jsonify({"status": "error", "msg": "کوره یافت نشد"})
    if furnaces[f_id]['mode'] == 'RUNNING':
        furnaces[f_id]['paused_elapsed'] = time.time() - furnaces[f_id]['start_time']
        furnaces[f_id]['mode'] = 'PAUSED'
        save_state()
        if ser and ser.is_open:
            try:
                with command_lock:
                    with serial_lock:
                        ser.write(f"{f_id},PAUSE\n".encode())
                        ser.flush()
                time.sleep(0.2)
            except Exception as e:
                log.error(f"pause write: {e}")
    return jsonify({"status": "ok"})


@app.route('/resume', methods=['POST'])
@login_required
def resume():
    d = request.json
    try:
        f_id = int(d['id'])
    except (ValueError, KeyError, TypeError):
        return jsonify({"status": "error", "msg": "شناسه نامعتبر"})
    if f_id not in furnaces:
        return jsonify({"status": "error", "msg": "کوره یافت نشد"})
    if furnaces[f_id]['mode'] != 'PAUSED':
        return jsonify({"status": "error", "msg": "فقط در حالت مکث"})
    if not furnaces[f_id].get('is_online'):
        return jsonify({"status": "error", "msg": "کوره آفلاین است"})
    furnaces[f_id]['mode'] = 'RUNNING'
    furnaces[f_id]['start_time'] = time.time() - furnaces[f_id].get('paused_elapsed', 0)
    save_state()
    if ser and ser.is_open:
        try:
            with command_lock:
                with serial_lock:
                    ser.write(f"{f_id},RESUME\n".encode())
                    ser.flush()
            time.sleep(0.2)
        except Exception as e:
            log.error(f"resume write: {e}")
    return jsonify({"status": "ok"})


@app.route('/stop', methods=['POST'])
@login_required
def stop():
    d = request.json
    try:
        f_id = int(d['id'])
    except (ValueError, KeyError, TypeError):
        return jsonify({"status": "error", "msg": "شناسه نامعتبر"})
    if f_id not in furnaces:
        return jsonify({"status": "error", "msg": "کوره یافت نشد"})

    was_running = furnaces[f_id]['mode'] in ['RUNNING', 'PAUSED']
    furnaces[f_id].update({
        "mode": "MANUAL", "step": 0, "start_time": 0,
        "paused_elapsed": 0, "relay": "OFF",
        "config_sent": False, "last_arduino_step": -1,
        "live_target": 0.0, "remaining_from_arduino": False,
        "remaining_time": 0, "_last_logged": {}
    })

    # --- FIX #6: هر رکورد بازی بسته می‌شود، نه فقط was_running ---
    try:
        now_str = get_system_time().strftime('%Y-%m-%d %H:%M:%S')
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE bake_history SET end_time=?, status=? WHERE furnace_id=? AND end_time IS NULL",
                  (now_str, 'متوقف شده (دستی)' if was_running else 'بسته شده', f_id))
        rows_affected = c.rowcount
        conn.commit()
        conn.close()
        if was_running and rows_affected > 0:
            send_telegram_message(f"⏹ کوره {f_id} به صورت دستی متوقف شد.")
    except Exception as e:
        log.error(f"DB Stop Error: {e}")

    save_state()
    if ser and ser.is_open:
        try:
            with command_lock:
                with serial_lock:
                    ser.write(f"{f_id},STOP\n".encode())
                    ser.flush()
            time.sleep(0.2)
        except Exception as e:
            log.error(f"stop write: {e}")
    return jsonify({"status": "ok"})

# ============================================================
# PART 8: Status Endpoint, History, Chart, Export
# ============================================================

@app.route('/history_all')
@login_required
def history_all():
    """لیست پخت‌های اخیر با زمان شروع واقعی از bake_history."""
    res = []
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "SELECT furnace_id, start_time, end_time, status, cycle_id "
            "FROM bake_history ORDER BY id DESC LIMIT 200"
        )
        rows = c.fetchall()
        conn.close()
        for r in rows:
            f_id = r[0]
            live_mode = furnaces.get(f_id, {}).get('mode', 'MANUAL')
            # اگر رکورد باز است و کوره هنوز RUNNING/PAUSED است، mode زنده را نشان بده
            if r[2] is None and live_mode in ('RUNNING', 'PAUSED', 'ERROR'):
                display_mode = live_mode
            else:
                display_mode = 'FINISHED' if r[2] is not None else live_mode
            res.append({
                "furnace_id": f_id,
                "start_time": r[1],          # --- FIX #38: زمان واقعی شروع ---
                "end_time": r[2],
                "status": r[3],
                "cycle_id": r[4],
                "mode": display_mode,
            })
    except Exception as e:
        log.error(f"history_all: {e}")
    return jsonify(res)


@app.route('/chart_history/<int:furnace_id>')
@login_required
def chart_history(furnace_id):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "SELECT cycle_id FROM temp_logs WHERE furnace_id=? AND cycle_id > 0 "
            "ORDER BY id DESC LIMIT 1",
            (furnace_id,))
        row = c.fetchone()
        if not row:
            conn.close()
            return jsonify({"points": [], "profile": [],
                            "total_duration": 0, "cycle_id": 0})
        cycle_id = row[0]
        c.execute(
            "SELECT time, temp, target_temp, relay, sensor_err FROM temp_logs "
            "WHERE furnace_id=? AND cycle_id=? ORDER BY id ASC",
            (furnace_id, cycle_id))
        rows = c.fetchall()
        conn.close()

        points = []
        if rows:
            try:
                first_time = datetime.strptime(rows[0][0], '%Y-%m-%d %H:%M:%S')
            except Exception:
                first_time = None
            for r in rows:
                try:
                    current_time = datetime.strptime(r[0], '%Y-%m-%d %H:%M:%S')
                    elapsed = (current_time - first_time).total_seconds() if first_time else 0
                except Exception:
                    elapsed = 0
                points.append({
                    "t": elapsed,
                    "v": r[1],
                    "target": r[2],
                    "r": "ON" if r[3] == 1 else "OFF",
                    "err": bool(r[4])
                })
        profile = furnaces.get(furnace_id, {}).get('profile', [])
        total_duration = furnaces.get(furnace_id, {}).get('total_duration', 0)
        return jsonify({"points": points, "profile": profile,
                        "total_duration": total_duration, "cycle_id": cycle_id})
    except Exception as e:
        log.error(f"chart_history error: {e}")
        return jsonify({"points": [], "profile": [],
                        "total_duration": 0, "cycle_id": 0})


def _csv_stream():
    """--- FIX #24: streaming CSV ---"""
    yield "\ufeff"  # BOM برای اکسل فارسی
    yield "شماره کوره,زمان شروع,زمان پایان,وضعیت پخت,تعداد مراحل\n"
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "SELECT furnace_id, start_time, end_time, status, total_steps "
            "FROM bake_history ORDER BY id DESC")
        for row in c.fetchall():
            f_id, st, et, status, steps = row
            yield f"کوره {f_id},{st or '-'},{et or 'در حال اجرا...'},{status},{steps}\n"
        conn.close()
    except Exception as e:
        log.error(f"csv_stream: {e}")
        yield f"خطا در تولید گزارش,{e},,,\n"


@app.route('/export_csv')
@login_required
def export_csv():
    return Response(
        _csv_stream(),
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=furnace_bake_history.csv"})


@app.route('/status')
@login_required
def status():
    now = time.time()
    any_connected = False

    # --- به‌روزرسانی is_online ---
    for fid in range(1, active_furnaces_count + 1):
        f = furnaces[fid]
        f['is_online'] = (now - f['last_seen']) < OFFLINE_TIMEOUT
        if f['is_online']:
            any_connected = True
    for f_id, f_data in furnaces.items():
        if f_id > active_furnaces_count:
            f_data['is_online'] = False

    # --- محاسبه elapsed / remaining ---
    for f_id, f_data in furnaces.items():
        if f_id > active_furnaces_count:
            continue

        if f_data['mode'] == 'ERROR':
            f_data['elapsed'] = 0
            f_data['remaining_time'] = 0
            continue

        # --- FIX #9: elapsed برای FINISHED ثابت می‌ماند ---
        if f_data['mode'] == 'FINISHED':
            f_data['elapsed'] = f_data.get('total_duration', 0) or 0
        elif f_data['mode'] == 'PAUSED':
            f_data['elapsed'] = f_data.get('paused_elapsed', 0)
        elif f_data['mode'] == 'RUNNING' and f_data['start_time'] > 0:
            f_data['elapsed'] = max(0, now - f_data['start_time'])
        else:
            f_data['elapsed'] = 0

        # --- محاسبه remaining ---
        if f_data['mode'] in ('RUNNING', 'PAUSED'):
            if f_data.get('remaining_from_arduino'):
                pass  # Arduino مقدار دقیق را فرستاده
            elif f_data['profile'] and f_data['step'] < len(f_data['profile']):
                step_cfg = f_data['profile'][f_data['step']]
                step_total = step_cfg['time'] * 60
                prev_time = sum(s['time'] * 60 for s in f_data['profile'][:f_data['step']])
                f_data['remaining_time'] = max(0, step_total - max(0, f_data['elapsed'] - prev_time))
            else:
                f_data['remaining_time'] = 0
        else:
            f_data['remaining_time'] = 0
            f_data['remaining_from_arduino'] = False

    return jsonify({
        "furnaces": furnaces,
        "serial_connected": any_connected,
        "com_port": found_port_name if any_connected else "قطع",
        "active_count": active_furnaces_count,
        "config": CONFIG,
        "server_time": get_system_time().strftime('%Y-%m-%d %H:%M:%S')
    })

# ============================================================
# PART 9: HTML Template (head, styles, header, tabs)
# ============================================================

HTML_PAGE = r"""
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<link rel="manifest" href="/manifest.json">
<meta name="theme-color" content="#0f172a">
<title>پنل کوره‌ها</title>
<style>
:root { --bg-dark: #0f172a; --panel-bg: #1e293b; --border-color: #334155; --primary: #0ea5e9; --success: #22c55e; --danger: #ef4444; --warning: #f59e0b; --text-main: #f8fafc; --text-muted: #94a3b8; --chart-bg: #0b1120; }
body.light-mode { --bg-dark: #f1f5f9; --panel-bg: #ffffff; --border-color: #e2e8f0; --text-main: #1e293b; --text-muted: #64748b; --chart-bg: #ffffff; }
body { background-color: var(--bg-dark); color: var(--text-main); font-family: Tahoma; margin: 0; transition: background-color 0.3s, color 0.3s; }
.header { background: var(--panel-bg); padding: 15px 25px; border-bottom: 2px solid var(--border-color); display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;}
.header h2 { margin: 0; color: var(--primary); }
.tabs { background: var(--bg-dark); padding: 10px; display: flex; gap: 8px; overflow-x: auto; border-bottom: 1px solid var(--border-color); }
.tab-btn { padding: 10px 18px; background: var(--panel-bg); color: var(--text-muted); border: 1px solid var(--border-color); cursor: pointer; border-radius: 8px; font-weight: bold; white-space: nowrap;}
.tab-btn.active { background: var(--primary); color: white; border-color: var(--primary); }
.tab-content { display: none; padding: 20px; max-width: 1200px; margin: 0 auto; }
.tab-content.active { display: block; }
.card { background: var(--panel-bg); border: 1px solid var(--border-color); padding: 20px; border-radius: 12px; margin-bottom: 20px; }
button { padding: 10px 15px; border: none; color: white; cursor: pointer; border-radius: 8px; font-weight: bold; transition: 0.2s;}
button:hover { transform: translateY(-2px); opacity: 0.9; }
.green { background: var(--success); } .red { background: var(--danger); } .blue { background: var(--primary); } .orange { background: var(--warning); color: black; } .gray { background: #64748b; } .yellow { background: #eab308; color: black; }
.chart-container { position: relative; width: 100%; height: 350px; margin-top: 15px; background: var(--chart-bg); border-radius: 8px; border: 1px solid var(--border-color); overflow: hidden; }
canvas { width: 100%; height: 100%; display: block; }
.ov-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 15px; }
.ov-card { background: var(--panel-bg); border: 1px solid var(--border-color); padding: 15px; border-radius: 12px; cursor: pointer; }
.ov-card .chart-container { height: 120px; margin-top: 10px; }
.badge { padding: 4px 10px; border-radius: 20px; font-size: 12px; font-weight: bold; display: inline-flex; align-items: center; gap: 5px;}
.bg-secondary { background: #475569; color: white; } .bg-warning { background: var(--warning); color: black; } .bg-success { background: var(--success); color: white;} .bg-danger { background: var(--danger); color: white;}
.badge-step { background-color: #eab308; color: black; padding: 4px 10px; border-radius: 20px; font-size: 12px; font-weight: bold; display: none; }
.badge-temp { background-color: var(--primary); color: white; padding: 4px 12px; border-radius: 20px; font-size: 14px; font-weight: bold; display: inline-flex; align-items: center; }
.relay-on { background: rgba(239,68,68,0.2); color: var(--danger); border: 1px solid var(--danger); padding: 4px 10px; border-radius: 20px; font-size: 12px;}
.relay-off { background: rgba(100,116,139,0.2); color: var(--text-muted); border: 1px solid var(--text-muted); padding: 4px 10px; border-radius: 20px; font-size: 12px;}
.conn-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; margin-right: 5px; }
.conn-ok { background: var(--success); } .conn-fail { background: var(--danger); }
table { width: 100%; border-collapse: collapse; margin-top: 10px; }
th, td { padding: 10px; border-bottom: 1px solid var(--border-color); text-align: center; }
th { background: var(--bg-dark); color: var(--primary); }
.step-row { background: var(--bg-dark); padding: 10px 15px; border-radius: 8px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center; border: 1px solid var(--border-color); flex-wrap: wrap; gap: 8px;}
.step-row input[type="number"] { width: 70px; }
.ramp-toggle { display: flex; align-items: center; gap: 6px; cursor: pointer; padding: 6px 12px; border-radius: 6px; background: var(--panel-bg); border: 1px solid var(--border-color); font-size: 13px; user-select: none;}
.ramp-toggle input[type="checkbox"] { width: auto; max-width: none; margin: 0; cursor: pointer; transform: scale(1.3);}
.ramp-toggle.on { border-color: var(--primary); background: rgba(14,165,233,0.15); color: var(--primary); }
input[type="number"], input[type="password"], input[type="text"], input[type="datetime-local"] { background: var(--panel-bg); color: var(--text-main); border: 1px solid var(--border-color); padding: 8px; border-radius: 6px; text-align: center; width: 100%; max-width: 200px; font-family: Tahoma;}
input[type="datetime-local"] { text-align: left; direction: ltr; }
#top-clock { font-family: monospace; font-size: 16px; color: var(--primary); font-weight: bold; margin-left: 15px; direction: ltr;}
.color-picker-box { display: flex; align-items: center; gap: 10px; background: var(--bg-dark); padding: 10px 15px; border-radius: 8px; border: 1px solid var(--border-color); margin-bottom: 10px; flex-wrap:wrap;}
input[type="color"] { -webkit-appearance: none; border: none; width: 40px; height: 40px; border-radius: 5px; cursor: pointer; padding: 0; background: none; }
input[type="color"]::-webkit-color-swatch-wrapper { padding: 0; }
input[type="color"]::-webkit-color-swatch { border: 2px solid var(--border-color); border-radius: 5px; }
.config-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 15px; }
.config-item { background: var(--bg-dark); padding: 15px; border-radius: 8px; border: 1px solid var(--border-color); }
.config-item label { display: block; margin-bottom: 8px; font-weight: bold; color: var(--text-muted); }
.help-text { font-size: 12px; color: var(--text-muted); margin-top: 5px; display: block; }
@media (max-width: 768px) { .header { flex-direction: column; gap: 10px; text-align: center; } .header > div:last-child { flex-wrap: wrap; justify-content: center; } .tabs { padding: 5px; } .tab-btn { padding: 8px 12px; font-size: 12px; } .tab-content { padding: 10px; } .card { padding: 15px; } .ov-grid { grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); } .ov-card { padding: 10px; } .step-row { flex-direction: column; align-items: flex-start; gap: 10px; } .step-row > div { width: 100%; justify-content: space-between; } input[type="number"], input[type="password"] { width: 60px; } .chart-container { height: 250px; } }
</style>
</head>
<body>
<div id="alarm-banner" style="display:none; background:var(--danger); text-align:center; padding:10px; font-weight:bold; font-size:18px;">⚠️ ارتباط شبکه قطع است ⚠️</div>
<div id="safety-banner" style="display:none; background:var(--warning); color:black; text-align:center; padding:10px; font-weight:bold; font-size:18px;">🔥 هشدار: دمای ایمنی رد شده است! 🔥</div>

<div class="header">
    <h2 id="txt-title">🏭 سیستم مدیریت کوره‌ها</h2>
    <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap; justify-content:center;">
        <span id="top-clock">در حال دریافت زمان...</span>
        <span>| <span id="txt-status">وضعیت</span>: <span id="global-com-status" style="color:var(--danger); font-weight:bold;">در حال بررسی...</span></span>
        <span>| <span id="txt-user">کاربر</span>: <b>{{ session['username'] }}</b></span>
    </div>
    <div style="display:flex; gap:10px; align-items: center; flex-wrap:wrap;">
        <button class="blue" onclick="location.reload()" id="txt-refresh">🔄 رفرش</button>
        <a href="/export_csv" class="green" style="padding:8px 15px; text-decoration:none; border-radius:8px; font-weight:bold;" id="txt-excel">📊 اکسل</a>
        <button class="gray" onclick="showTab('settings')" id="txt-settings">⚙️ تنظیمات</button>
        <a href="/logout" class="red" style="padding:8px 15px; text-decoration:none; border-radius:8px; font-weight:bold;" id="txt-logout">خروج</a>
    </div>
</div>

<!-- --- FIX #30/#31: تب‌ها با id ثابت --- -->
<div class="tabs" id="tabs-container">
    <button class="tab-btn active" id="tab-btn-overview" onclick="showTab('overview')" data-tab="overview"><span id="txt-overview">🌐 مشاهده همه</span></button>
    <button class="tab-btn orange" id="tab-btn-compare" onclick="showTab('compare')" data-tab="compare"><span id="txt-compare">📊 مقایسه کوره‌ها</span></button>
</div>

<div id="content-container">
    <div id="tab-overview" class="tab-content active"><div class="ov-grid" id="overview-grid"></div></div>

    <div id="tab-compare" class="tab-content">
        <div class="card">
            <h3 id="txt-compare-title">مقایسه کوره‌ها در زمان واقعی</h3>
            <div class="chart-container" style="height:500px;"><canvas id="compare-cv"></canvas></div>
        </div>
    </div>

    <div id="tab-settings" class="tab-content">
        <div class="card">
            <h3 id="txt-appearance">تنظیمات ظاهری</h3>
            <div style="display:flex; gap:15px; align-items:center; flex-wrap:wrap; margin-bottom:15px;">
                <button class="blue" onclick="toggleTheme()" id="theme-btn">🌙 تغییر به حالت روشن (روز)</button>
                <button class="orange" onclick="requestNotificationPermission()" id="txt-notif-btn">🔔 فعال‌سازی اعلان‌های گوشی</button>
                <button class="purple" style="background:#8b5cf6;" onclick="toggleLanguage()" id="lang-btn">🇬🇧 English</button>
            </div>
            <div class="color-picker-box"><span style="width:180px; display:inline-block;" id="txt-bg-color">رنگ پس‌زمینه سایت:</span><input type="color" id="bg-color-picker" value="#0f172a"><button class="green" onclick="changeBgColor()" id="txt-apply-bg">اعمال</button><button class="gray" onclick="resetBgColor()" style="font-size:12px;" id="txt-reset">پیش‌فرض</button></div>
            <div class="color-picker-box"><span style="width:180px; display:inline-block;" id="txt-chart-color">رنگ پس‌زمینه نمودارها:</span><input type="color" id="chart-color-picker" value="#0b1120"><button class="green" onclick="changeChartColor()" id="txt-apply-chart">اعمال</button><button class="gray" onclick="resetChartColor()" style="font-size:12px;" id="txt-reset-chart">پیش‌فرض</button></div>
        </div>

        <div class="card">
            <h3 id="txt-safety-title">تنظیمات کوره و ایمنی</h3>
            <div class="config-grid">
                <div class="config-item"><label id="txt-temp-unit">واحد نمایش دما</label><select id="cfg-temp-unit" style="padding:8px; border-radius:6px; background:var(--panel-bg); color:var(--text-main); border:1px solid var(--border-color);"><option value="C">سلسیوس (°C)</option><option value="F">فارنهایت (°F)</option></select></div>
                <div class="config-item"><label id="txt-calibration">کالیبراسیون دما (آفست)</label><input type="number" id="cfg-temp-offset" step="0.1" placeholder="مثلاً 2.5 یا -1.0"><span class="help-text">این مقدار به همه ۲۲ کوره فرستاده می‌شود.</span></div>
                <div class="config-item"><label id="txt-safety-limit">حد ایمنی دما (قطع خودکار)</label><input type="number" id="cfg-safety-limit" placeholder="مثلاً 1200"><span class="help-text">اگر ارتباط قطع شود، آردوینو با این عدد کوره را خاموش می‌کند.</span></div>
            </div>
        </div>

        <div class="card">
            <h3 id="txt-tg-title">تنظیمات هشدار تلگرام</h3>
            <div class="config-grid">
                <div class="config-item"><label id="txt-tg-token">توکن ربات تلگرام</label><input type="text" id="cfg-tg-token" placeholder="مثلاً 123456:ABC-DEF..."><span class="help-text" id="txt-tg-help">از @BotFather در تلگرام دریافت کنید.</span></div>
                <div class="config-item"><label id="txt-tg-chat">Chat ID تلگرام</label><input type="text" id="cfg-tg-chat" placeholder="مثلاً 123456789"><span class="help-text" id="txt-tg-chat-help">با ارسال پیام به ربات و باز کردن لینک getUpdates دریافت کنید.</span></div>
            </div>
            <button class="blue" style="margin-top:15px;" onclick="saveSystemConfig()" id="txt-save-config">💾 ذخیره تمام تنظیمات</button>
        </div>

        <div class="card"><h3 id="txt-time-title">تنظیمات تاریخ و ساعت سیستم</h3><div style="display:flex; gap:10px; align-items: center; flex-wrap:wrap;"><input type="datetime-local" id="time-input"><button class="blue" onclick="saveTime()" id="txt-save-time">ذخیره زمان</button></div></div>

        <div class="card"><h3 id="txt-users-title">مدیریت کاربران (افزودن یا تغییر رمز)</h3><div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;"><input type="text" id="new-username" placeholder="نام کاربری جدید"><input type="password" id="new-password" placeholder="رمز عبور جدید"><button class="blue" onclick="manageUsers()" id="txt-save-user">ذخیره کاربر</button></div></div>

        <div class="card"><h3 id="txt-backup-title">پشتیبان‌گیری و مدیریت داده‌ها</h3><p style="color:var(--text-muted); font-size:12px;" id="txt-backup-help">فایل پشتیبان شامل تمام تاریخچه پخت‌ها و تنظیمات است.</p><a href="/backup_data" class="gray" style="display:inline-block; padding:10px 15px; text-decoration:none; border-radius:8px; font-weight:bold;" id="txt-download-backup">💾 دانلود فایل پشتیبان (JSON)</a><a href="/export_csv" class="green" style="display:inline-block; padding:10px 15px; text-decoration:none; border-radius:8px; font-weight:bold; margin-right:10px;" id="txt-download-excel">📊 دانلود گزارش اکسل</a></div>

        <div class="card">
            <h3 id="txt-cleanup-title">🧹 پاک‌سازی لاگ‌های قدیمی</h3>
            <p style="color:var(--text-muted); font-size:12px;" id="txt-cleanup-help">لاگ‌های دما به مرور زمان زیاد می‌شوند. با این دکمه، لاگ‌های قدیمی‌تر از تعداد روز مشخص را حذف کنید. (تاریخچه پخت‌ها حفظ می‌شود)</p>
            <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-top:10px;">
                <label style="color:var(--text-muted);" id="txt-cleanup-days-label">حذف لاگ‌های قدیمی‌تر از:</label>
                <input type="number" id="cleanup-days" value="30" min="1" max="3650" style="width:100px;">
                <span style="color:var(--text-muted);" id="txt-cleanup-days-unit">روز</span>
                <button class="red" onclick="cleanupOldLogs()" id="txt-cleanup-btn">🗑 پاک‌سازی</button>
            </div>
        </div>

        <div class="card"><h3 id="txt-furnaces-title">مدیریت کوره‌ها</h3><button class="orange" onclick="requestNewFurnace()" id="txt-add-furnace">+ افزودن کوره</button><button class="red" onclick="requestRemoveFurnace()" id="txt-remove-furnace">➖ حذف کوره</button></div>

        <div class="card"><h3 id="txt-history-title">گزارش پخت‌های اخیر</h3><table id="history-table"><thead><tr><th id="txt-th-furnace">کوره</th><th id="txt-th-start">زمان شروع</th><th id="txt-th-status">وضعیت</th></tr></thead><tbody id="history-body"><tr><td colspan="3" style="color:var(--text-muted);" id="txt-loading">در حال دریافت اطلاعات...</td></tr></tbody></table></div>
    </div>
</div>
"""

# (ادامه HTML_PAGE در پارت 10 به صورت <script>...</script></body></html> اضافه می‌شود)

# ============================================================
# PART 10-A: JS — Translations, Language, Theme, Settings
# ============================================================

HTML_PAGE += r"""
<script>
// ================== ترجمه‌ها ==================
var translations = {
  fa: { title:"🏭 سیستم مدیریت کوره‌ها", status:"وضعیت", user:"کاربر",
    refresh:"🔄 رفرش", excel:"📊 اکسل", settings:"⚙️ تنظیمات", logout:"خروج",
    overview:"🌐 مشاهده همه", compare:"📊 مقایسه کوره‌ها",
    compare_title:"مقایسه کوره‌ها در زمان واقعی", appearance:"تنظیمات ظاهری",
    theme_dark:"🌙 تغییر به حالت روشن (روز)", theme_light:"☀️ تغییر به حالت تاریک (شب)",
    notif_btn:"🔔 فعال‌سازی اعلان‌های گوشی", lang_btn:"🇬🇧 English",
    bg_color:"رنگ پس‌زمینه سایت:", apply:"اعمال", reset:"پیش‌فرض",
    chart_color:"رنگ پس‌زمینه نمودارها:", safety_title:"تنظیمات کوره و ایمنی",
    temp_unit:"واحد نمایش دما", calibration:"کالیبراسیون دما (آفست)",
    safety_limit:"حد ایمنی دما (قطع خودکار)", tg_title:"تنظیمات هشدار تلگرام",
    tg_token:"توکن ربات تلگرام", tg_help:"از @BotFather در تلگرام دریافت کنید.",
    tg_chat:"Chat ID تلگرام", tg_chat_help:"با ارسال پیام به ربات و باز کردن لینک getUpdates دریافت کنید.",
    save_config:"💾 ذخیره تمام تنظیمات", time_title:"تنظیمات تاریخ و ساعت سیستم",
    save_time:"ذخیره زمان", users_title:"مدیریت کاربران (افزودن یا تغییر رمز)",
    save_user:"ذخیره کاربر", backup_title:"پشتیبان‌گیری و مدیریت داده‌ها",
    backup_help:"فایل پشتیبان شامل تمام تاریخچه پخت‌ها و تنظیمات است.",
    download_backup:"💾 دانلود فایل پشتیبان (JSON)", download_excel:"📊 دانلود گزارش اکسل",
    furnaces_title:"مدیریت کوره‌ها", add_furnace:"+ افزودن کوره", remove_furnace:"➖ حذف کوره",
    history_title:"گزارش پخت‌های اخیر", th_furnace:"کوره", th_start:"زمان شروع",
    th_status:"وضعیت", loading:"در حال دریافت اطلاعات...",
    ramp_label:"شیب", ramp_on:"شیب (آرام)", ramp_off:"سریع+نگه‌داشتن",
    step:"مرحله", minute:"دقیقه", temp:"دما", add_step:"+ افزودن مرحله",
    save_profile:"💾 ذخیره پروفایل", load_profile:"📂 فراخوانی پروفایل",
    start:"▶ شروع از اول", pause:"⏸ مکث / ادامه", stop:"⏹ توقف کامل",
    step_running:"مرحله در حال اجرا", heater_on:"المنت روشن", heater_off:"المنت خاموش",
    past:"گذشته", remaining:"باقیمانده", download_chart:"📥 دانلود نمودار",
    cleanup_title:"🧹 پاک‌سازی لاگ‌های قدیمی",
    cleanup_help:"لاگ‌های دما به مرور زمان زیاد می‌شوند. با این دکمه، لاگ‌های قدیمی‌تر از تعداد روز مشخص را حذف کنید. (تاریخچه پخت‌ها حفظ می‌شود)",
    cleanup_days_label:"حذف لاگ‌های قدیمی‌تر از:", cleanup_days_unit:"روز", cleanup_btn:"🗑 پاک‌سازی",
    no_furnace_started:"هنوز کوره‌ای شروع نشده است. از تب کوره‌ها یک برنامه را استارت کنید.",
    only_running:"(فقط کوره‌های در حال اجرا در این نمودار نمایش داده می‌شوند)" },
  en: { title:"🏭 Furnace Management System", status:"Status", user:"User",
    refresh:"🔄 Refresh", excel:"📊 Excel", settings:"⚙️ Settings", logout:"Logout",
    overview:"🌐 Overview", compare:"📊 Compare Furnaces",
    compare_title:"Real-time Furnace Comparison", appearance:"Appearance Settings",
    theme_dark:"🌙 Switch to Light Mode", theme_light:"☀️ Switch to Dark Mode",
    notif_btn:"🔔 Enable Phone Notifications", lang_btn:"🇮🇷 فارسی",
    bg_color:"Site Background Color:", apply:"Apply", reset:"Reset",
    chart_color:"Chart Background Color:", safety_title:"Furnace & Safety Settings",
    temp_unit:"Temperature Unit", calibration:"Temperature Calibration (Offset)",
    safety_limit:"Safety Limit (Auto-Stop)", tg_title:"Telegram Alert Settings",
    tg_token:"Telegram Bot Token", tg_help:"Get it from @BotFather in Telegram.",
    tg_chat:"Telegram Chat ID", tg_chat_help:"Send a message to the bot and check the getUpdates link.",
    save_config:"💾 Save All Settings", time_title:"System Date & Time Settings",
    save_time:"Save Time", users_title:"User Management (Add or Change Password)",
    save_user:"Save User", backup_title:"Backup & Data Management",
    backup_help:"The backup file contains all bake history and settings.",
    download_backup:"💾 Download Backup (JSON)", download_excel:"📊 Download Excel Report",
    furnaces_title:"Furnace Management", add_furnace:"+ Add Furnace", remove_furnace:"➖ Remove Furnace",
    history_title:"Recent Bake History", th_furnace:"Furnace", th_start:"Start Time",
    th_status:"Status", loading:"Loading data...",
    ramp_label:"Ramp", ramp_on:"Ramp (slow)", ramp_off:"Fast+Hold",
    step:"Step", minute:"min", temp:"Temp", add_step:"+ Add Step",
    save_profile:"💾 Save Profile", load_profile:"📂 Load Profile",
    start:"▶ Start", pause:"⏸ Pause/Resume", stop:"⏹ Stop",
    step_running:"Running step", heater_on:"Heater ON", heater_off:"Heater OFF",
    past:"Elapsed", remaining:"Remaining", download_chart:"📥 Download Chart",
    cleanup_title:"🧹 Cleanup Old Logs",
    cleanup_help:"Temperature logs grow over time. Delete logs older than a specified number of days. (Bake history is preserved)",
    cleanup_days_label:"Delete logs older than:", cleanup_days_unit:"days", cleanup_btn:"🗑 Cleanup",
    no_furnace_started:"No data yet. Start a furnace from its tab.",
    only_running:"(Only running furnaces are displayed here)" }
};

var currentLang = localStorage.getItem('furnace_lang') || 'fa';
var globalConfig = null;
var data = {};
var prevData = {};
var activeFurnaces = 0;
var audioCtx = null;
var alarmInterval = null;
var furnaces = {};
var stepDebounceTimers = {};

// ================== زبان و ترجمه ==================
function applyLanguage(lang) {
  currentLang = lang;
  localStorage.setItem('furnace_lang', lang);
  var t = translations[lang];
  document.documentElement.dir = (lang === 'fa') ? 'rtl' : 'ltr';
  document.documentElement.lang = lang;
  var map = {
    'txt-title':'title','txt-status':'status','txt-user':'user','txt-refresh':'refresh',
    'txt-excel':'excel','txt-settings':'settings','txt-logout':'logout',
    'txt-overview':'overview','txt-compare':'compare','txt-compare-title':'compare_title',
    'txt-appearance':'appearance','txt-notif-btn':'notif_btn','lang-btn':'lang_btn',
    'txt-bg-color':'bg_color','txt-apply-bg':'apply','txt-reset':'reset',
    'txt-chart-color':'chart_color','txt-apply-chart':'apply','txt-reset-chart':'reset',
    'txt-safety-title':'safety_title','txt-temp-unit':'temp_unit',
    'txt-calibration':'calibration','txt-safety-limit':'safety_limit',
    'txt-tg-title':'tg_title','txt-tg-token':'tg_token','txt-tg-help':'tg_help',
    'txt-tg-chat':'tg_chat','txt-tg-chat-help':'tg_chat_help',
    'txt-save-config':'save_config','txt-time-title':'time_title','txt-save-time':'save_time',
    'txt-users-title':'users_title','txt-save-user':'save_user',
    'txt-backup-title':'backup_title','txt-backup-help':'backup_help',
    'txt-download-backup':'download_backup','txt-download-excel':'download_excel',
    'txt-furnaces-title':'furnaces_title','txt-add-furnace':'add_furnace',
    'txt-remove-furnace':'remove_furnace','txt-history-title':'history_title',
    'txt-th-furnace':'th_furnace','txt-th-start':'th_start','txt-th-status':'th_status',
    'txt-loading':'loading','txt-cleanup-title':'cleanup_title','txt-cleanup-help':'cleanup_help',
    'txt-cleanup-days-label':'cleanup_days_label','txt-cleanup-days-unit':'cleanup_days_unit',
    'txt-cleanup-btn':'cleanup_btn'
  };
  for (var id in map) {
    var el = document.getElementById(id);
    if (el) el.innerText = t[map[id]];
  }
  var isLight = document.body.classList.contains('light-mode');
  var themeBtn = document.getElementById('theme-btn');
  if (themeBtn) themeBtn.innerHTML = isLight ? t.theme_light : t.theme_dark;

  for (var i = 1; i <= activeFurnaces; i++) {
    var btn = document.getElementById('btn-' + i);
    if (btn) btn.innerText = (lang === 'fa') ? 'کوره ' + i : 'Furnace ' + i;
    var ovLabel = document.getElementById('ov-label-' + i);
    if (ovLabel) ovLabel.innerText = (lang === 'fa') ? 'کوره ' + i : 'Furnace ' + i;
    var elapsedLbl = document.getElementById('lbl-elapsed-' + i);
    if (elapsedLbl) elapsedLbl.innerText = t.past;
    var remainingLbl = document.getElementById('lbl-remaining-' + i);
    if (remainingLbl) remainingLbl.innerText = t.remaining;
    var dlBtn = document.getElementById('dl-chart-' + i);
    if (dlBtn) dlBtn.innerText = t.download_chart;
  }
  if (globalConfig) {
    var tu = document.getElementById('cfg-temp-unit'); if (tu) tu.value = globalConfig.temp_unit || 'C';
    var to = document.getElementById('cfg-temp-offset'); if (to) to.value = globalConfig.temp_offset || 0;
    var sl = document.getElementById('cfg-safety-limit'); if (sl) sl.value = globalConfig.safety_limit || 1200;
    var tt = document.getElementById('cfg-tg-token'); if (tt) tt.value = globalConfig.tg_token || '';
    var tc = document.getElementById('cfg-tg-chat'); if (tc) tc.value = globalConfig.tg_chat_id || '';
  }
}

function toggleLanguage() { applyLanguage(currentLang === 'fa' ? 'en' : 'fa'); redrawAllCharts(); }

function requestNotificationPermission() {
  if (!("Notification" in window)) {
    alert(currentLang === 'fa' ? "مرورگر شما از اعلان پشتیبانی نمی‌کند." : "Browser does not support notifications.");
    return;
  }
  Notification.requestPermission().then(function(permission) {
    if (permission === "granted") {
      alert(currentLang === 'fa' ? "اعلان‌ها فعال شدند!" : "Notifications enabled!");
      if ('serviceWorker' in navigator) navigator.serviceWorker.register('/sw.js');
    }
  });
}

// ================== تم و رنگ ==================
function initTheme() {
  var currentTheme = localStorage.getItem('furnace_theme') || 'dark';
  if (currentTheme === 'light') document.body.classList.add('light-mode');
  var customBg = localStorage.getItem('furnace_bg_color');
  if (customBg) {
    document.documentElement.style.setProperty('--bg-dark', customBg);
    var bcp = document.getElementById('bg-color-picker'); if (bcp) bcp.value = customBg;
  }
  var customChartBg = localStorage.getItem('furnace_chart_color');
  if (customChartBg) {
    document.documentElement.style.setProperty('--chart-bg', customChartBg);
    var ccp = document.getElementById('chart-color-picker'); if (ccp) ccp.value = customChartBg;
  }
  applyLanguage(currentLang);
}

function toggleTheme() {
  document.body.classList.toggle('light-mode');
  var isLight = document.body.classList.contains('light-mode');
  localStorage.setItem('furnace_theme', isLight ? 'light' : 'dark');
  var t = translations[currentLang];
  var themeBtn = document.getElementById('theme-btn');
  if (themeBtn) themeBtn.innerHTML = isLight ? t.theme_light : t.theme_dark;
  if (!localStorage.getItem('furnace_bg_color')) {
    var defaultBg = isLight ? '#f1f5f9' : '#0f172a';
    document.documentElement.style.setProperty('--bg-dark', defaultBg);
    var bcp = document.getElementById('bg-color-picker'); if (bcp) bcp.value = defaultBg;
  }
  if (!localStorage.getItem('furnace_chart_color')) {
    var defaultChart = isLight ? '#ffffff' : '#0b1120';
    document.documentElement.style.setProperty('--chart-bg', defaultChart);
    var ccp = document.getElementById('chart-color-picker'); if (ccp) ccp.value = defaultChart;
  }
  redrawAllCharts();
}

function changeBgColor() {
  var color = document.getElementById('bg-color-picker').value;
  document.documentElement.style.setProperty('--bg-dark', color);
  localStorage.setItem('furnace_bg_color', color);
}
function resetBgColor() {
  localStorage.removeItem('furnace_bg_color');
  var isLight = document.body.classList.contains('light-mode');
  var defaultBg = isLight ? '#f1f5f9' : '#0f172a';
  document.documentElement.style.setProperty('--bg-dark', defaultBg);
  document.getElementById('bg-color-picker').value = defaultBg;
}
function changeChartColor() {
  var color = document.getElementById('chart-color-picker').value;
  document.documentElement.style.setProperty('--chart-bg', color);
  localStorage.setItem('furnace_chart_color', color);
  redrawAllCharts();
}
function resetChartColor() {
  localStorage.removeItem('furnace_chart_color');
  var isLight = document.body.classList.contains('light-mode');
  var defaultChart = isLight ? '#ffffff' : '#0b1120';
  document.documentElement.style.setProperty('--chart-bg', defaultChart);
  document.getElementById('chart-color-picker').value = defaultChart;
  redrawAllCharts();
}
function redrawAllCharts() {
  var unit = (globalConfig && globalConfig.temp_unit === 'F') ? 'F°' : 'C°';
  for (var id in data) {
    if (document.getElementById('cv-' + id))
      drawChart('cv-' + id, data[id], prevData[id], furnaces[id] ? furnaces[id].profile : [], furnaces[id] ? furnaces[id].total_duration : 0, unit);
    if (document.getElementById('ov-cv-' + id))
      drawChart('ov-cv-' + id, data[id], prevData[id], furnaces[id] ? furnaces[id].profile : [], furnaces[id] ? furnaces[id].total_duration : 0, unit);
  }
  if (document.getElementById('tab-compare').classList.contains('active')) drawCompareChart(unit);
}
initTheme();

// ================== تنظیمات، کاربران، ساعت ==================
function saveSystemConfig() {
  var cfg = {
    temp_unit: document.getElementById('cfg-temp-unit').value,
    temp_offset: parseFloat(document.getElementById('cfg-temp-offset').value) || 0,
    safety_limit: parseFloat(document.getElementById('cfg-safety-limit').value) || 1200,
    tg_token: document.getElementById('cfg-tg-token').value,
    tg_chat_id: document.getElementById('cfg-tg-chat').value
  };
  fetch('/save_system_config', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(cfg)
  }).then(r => r.json()).then(res => {
    if (res.status === 'ok') {
      globalConfig = cfg;
      alert(currentLang === 'fa' ? 'تنظیمات ذخیره شد و به تمام کوره‌ها ارسال شد.' : 'Settings saved and sent to all furnaces.');
    } else alert('Error: ' + (res.msg || 'Unknown'));
  }).catch(e => alert('Error: ' + e.message));
}

function manageUsers() {
  var user = document.getElementById('new-username').value;
  var pass = document.getElementById('new-password').value;
  if (!user || !pass) return alert(currentLang === 'fa' ? 'نام کاربری و رمز را وارد کنید.' : 'Enter username and password.');
  fetch('/manage_users', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({username: user, password: pass})
  }).then(r => r.json()).then(res => {
    if (res.status === 'ok') { alert('Saved.'); location.reload(); }
    else alert('Error: ' + (res.msg || 'Unknown'));
  }).catch(e => alert('Error: ' + e.message));
}

function cleanupOldLogs() {
  var isFa = currentLang === 'fa';
  var days = parseInt(document.getElementById('cleanup-days').value) || 30;
  if (days < 1) days = 1;
  if (!confirm(isFa ? 'آیا لاگ‌های قدیمی‌تر از ' + days + ' روز حذف شوند؟ این عمل قابل بازگشت نیست.' : 'Delete logs older than ' + days + ' days? This cannot be undone.')) return;
  fetch('/cleanup_old_logs', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({days: days})
  }).then(r => r.json()).then(res => {
    if (res.status === 'ok') {
      alert(isFa ? (res.deleted + ' رکورد لاگ قدیمی حذف شد. (قدیمی‌تر از ' + res.cutoff + ')') : (res.deleted + ' old log records deleted. (Older than ' + res.cutoff + ')'));
    } else alert('Error: ' + (res.msg || 'Unknown'));
  }).catch(e => alert('Error: ' + e.message));
}

function updateTopClock() {
  var now = new Date();
  var locale = currentLang === 'fa' ? 'fa-IR' : 'en-US';
  var tc = document.getElementById('top-clock');
  if (tc) tc.innerText = now.toLocaleDateString(locale) + " - " + now.toLocaleTimeString(locale);
}
setInterval(updateTopClock, 1000);
updateTopClock();

function saveTime() {
  var dt = document.getElementById('time-input').value;
  if (!dt) return alert(currentLang === 'fa' ? "لطفاً تاریخ و ساعت را انتخاب کنید." : "Please select date and time.");
  fetch('/set_time', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({datetime: dt})
  }).then(r => r.json()).then(res => {
    if (res.status === 'ok') { alert('Saved.'); location.reload(); }
    else alert('Error: ' + (res.msg || 'Unknown'));
  }).catch(e => alert('Error: ' + e.message));
}

# ============================================================
# PART 10-B: JS — Alarm, Furnace Elements, Steps, Profiles
# ============================================================

HTML_PAGE += r"""

// ================== آلارم صوتی ==================
document.addEventListener('click', function() {
  if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
  if (audioCtx.state === 'suspended') audioCtx.resume();
});

function playBeep() {
  if (!audioCtx) return;
  try {
    var osc = audioCtx.createOscillator();
    var gain = audioCtx.createGain();
    osc.type = 'square'; osc.frequency.value = 800; gain.gain.value = 0.1;
    osc.connect(gain); gain.connect(audioCtx.destination);
    osc.start(); osc.stop(audioCtx.currentTime + 0.2);
  } catch(e) {}
}
function startAlarm() { if (!alarmInterval) { playBeep(); alarmInterval = setInterval(playBeep, 1000); } }
function stopAlarm() { if (alarmInterval) { clearInterval(alarmInterval); alarmInterval = null; } }

function formatTime(seconds) {
  if (isNaN(seconds) || seconds < 0) return "00:00";
  var m = Math.floor(seconds / 60);
  var s = Math.floor(seconds % 60);
  return (m < 10 ? "0" : "") + m + ":" + (s < 10 ? "0" : "") + s;
}

// ================== ساخت المان‌های کوره ==================
function createFurnaceElements(i) {
  var isFa = currentLang === 'fa';
  var t = translations[currentLang];

  var tabsContainer = document.getElementById('tabs-container');
  tabsContainer.innerHTML += '<button class="tab-btn" id="btn-' + i + '" data-tab="' + i + '" onclick="showTab(' + i + ')">' + (isFa ? 'کوره ' + i : 'Furnace ' + i) + '</button>';

  var content = document.getElementById('content-container');
  content.innerHTML += `
    <div id="tab-${i}" class="tab-content">
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px; flex-wrap:wrap; gap:10px;">
          <h3 style="margin:0; display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
            ${isFa ? 'وضعیت کوره ' + i : 'Furnace ' + i + ' Status'}
            <span id="st-${i}" class="badge bg-secondary">MANUAL</span>
            <span id="temp-live-${i}" class="badge-temp">-- C°</span>
            <span id="current-step-${i}" class="badge-step">${t.step_running}: -</span>
            <span id="conn-${i}" class="conn-dot conn-fail"></span>
          </h3>
          <span id="relay-${i}" class="relay-off">${t.heater_off}</span>
        </div>
        <div style="display:flex; gap:10px; margin-bottom:15px; flex-wrap:wrap;">
          <button class="green" onclick="startF(${i})">${t.start}</button>
          <button class="blue" id="resume-btn-${i}" onclick="resumeF(${i})">${t.pause}</button>
          <button class="red" onclick="stopF(${i})">${t.stop}</button>
        </div>
        <div id="steps-${i}" style="max-height:300px; overflow-y:auto;"></div>
        <div style="display:flex; gap:10px; margin-top:15px; flex-wrap:wrap;">
          <button class="blue" onclick="addStep(${i})">${t.add_step}</button>
          <button class="yellow" onclick="saveProf(${i})">${t.save_profile}</button>
          <button class="gray" onclick="loadProf(${i})">${t.load_profile}</button>
        </div>
      </div>
      <div class="card">
        <h4 style="text-align:center;">
          ${isFa ? 'نمودار دما (سایه قرمز = المنت روشن، خط چین زرد = پروفایل)' : 'Temperature Chart (Red = Heater ON, Yellow = Profile)'}
        </h4>
        <div class="chart-container"><canvas id="cv-${i}"></canvas></div>
        <div style="display:flex; justify-content:space-between; margin-top:10px; flex-wrap:wrap; gap:10px;">
          <span><span id="lbl-elapsed-${i}">${t.past}</span>: <span id="elapsed-${i}">00:00</span></span>
          <span><span id="lbl-remaining-${i}">${t.remaining}</span>: <span id="remaining-${i}">00:00</span></span>
        </div>
        <button class="gray" id="dl-chart-${i}" style="width:100%; margin-top:15px;" onclick="downloadChart(${i})">${t.download_chart}</button>
      </div>
    </div>`;

  document.getElementById('overview-grid').innerHTML += `
    <div class="ov-card" id="ov-card-${i}" onclick="showTab(${i})">
      <div style="display:flex; justify-content:space-between;">
        <span><span id="ov-conn-${i}" class="conn-dot conn-fail"></span><span id="ov-label-${i}">${isFa ? 'کوره ' + i : 'Furnace ' + i}</span></span>
        <span id="ov-relay-${i}" class="relay-off">OFF</span>
      </div>
      <div style="display:flex; justify-content:space-between; margin-top:5px;">
        <span id="ov-st-${i}" class="badge bg-secondary">MANUAL</span>
        <span id="ov-temp-${i}" style="color:var(--primary); font-weight:bold;">--</span>
      </div>
      <div class="chart-container"><canvas id="ov-cv-${i}"></canvas></div>
    </div>`;

  prevData[i] = [];
  if (!data[i]) data[i] = [];
  loadChartHistory(i);
  setTimeout(function() {
    resizeCanvas('cv-' + i);
    resizeCanvas('ov-cv-' + i);
  }, 50);
}

// ================== مراحل ==================
function buildStepRow(id, index, temp, time, ramp) {
  var isFa = currentLang === 'fa';
  var t = translations[currentLang];
  var div = document.createElement('div');
  div.className = 'step-row';
  div.innerHTML = `
    <span style="color:var(--primary); font-weight:bold;">${t.step} ${index + 1}</span>
    <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
      <input type="number" value="${temp}" oninput="debounceUpdateSteps(${id})" placeholder="${t.temp}">
      <span style="color:var(--text-muted)">C°</span>
      <input type="number" value="${time}" oninput="debounceUpdateSteps(${id})" placeholder="${t.minute}">
      <span style="color:var(--text-muted)">${t.minute}</span>
      <label class="ramp-toggle ${ramp ? 'on' : ''}" title="${ramp ? t.ramp_on : t.ramp_off}">
        <input type="checkbox" ${ramp ? 'checked' : ''} onchange="onRampChange(this, ${id})">
        <span>${t.ramp_label}</span>
      </label>
      <button class="red" style="padding:5px 10px;" onclick="this.closest('.step-row').remove(); updateSteps(${id})">✖</button>
    </div>`;
  return div;
}

// --- FIX #33: debounce برای جلوگیری از flood ---
function debounceUpdateSteps(id) {
  if (stepDebounceTimers[id]) clearTimeout(stepDebounceTimers[id]);
  stepDebounceTimers[id] = setTimeout(function() { updateSteps(id); }, 400);
}

function onRampChange(checkbox, id) {
  var label = checkbox.closest('.ramp-toggle');
  if (checkbox.checked) label.classList.add('on'); else label.classList.remove('on');
  var t = translations[currentLang];
  label.title = checkbox.checked ? t.ramp_on : t.ramp_off;
  updateSteps(id);
}

function addStep(id) {
  var container = document.getElementById('steps-' + id);
  var count = container.children.length;
  var div = buildStepRow(id, count, 100 * (count + 1), 10, true);
  container.appendChild(div);
  updateSteps(id);
}

function updateSteps(id) {
  var container = document.getElementById('steps-' + id);
  var rows = container.querySelectorAll('.step-row');
  var stepsArr = [];
  rows.forEach(function(row) {
    var inputs = row.querySelectorAll('input');
    var temp = parseInt(inputs[0].value) || 0;
    var time = parseInt(inputs[1].value) || 0;
    var ramp = inputs[2].checked;
    stepsArr.push({temp: temp, time: time, ramp: ramp});
  });
  // --- FIX #32/#41: catch + نمایش خطای سرور ---
  fetch('/update_steps', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({id: id, steps: stepsArr})
  }).then(r => r.json()).then(res => {
    if (res.status !== 'ok') console.warn('update_steps:', res.msg);
  }).catch(e => console.warn('update_steps failed:', e));
}

// ================== پروفایل ==================
function saveProf(id) {
  var isFa = currentLang === 'fa';
  var name = prompt(isFa ? "نام برنامه پخت (مثلاً: پخت آجر):" : "Profile Name (e.g. Brick Bake):");
  if (!name) return;
  var container = document.getElementById('steps-' + id);
  var rows = container.querySelectorAll('.step-row');
  var stepsArr = [];
  rows.forEach(function(row) {
    var inputs = row.querySelectorAll('input');
    stepsArr.push({
      temp: parseInt(inputs[0].value) || 0,
      time: parseInt(inputs[1].value) || 0,
      ramp: inputs[2].checked
    });
  });
  localStorage.setItem("prof_" + name, JSON.stringify(stepsArr));
  alert(isFa ? "برنامه ذخیره شد!" : "Profile saved!");
}

function loadProf(id) {
  var isFa = currentLang === 'fa';
  var list = [];
  for (var i = 0; i < localStorage.length; i++) {
    var k = localStorage.key(i);
    if (k.indexOf("prof_") === 0) list.push(k.replace("prof_", ""));
  }
  if (list.length === 0) return alert(isFa ? "هیچ برنامه‌ای وجود ندارد!" : "No profiles found!");
  var name = prompt((isFa ? "نام برنامه را وارد کنید:" : "Enter profile name:") + "\n" + list.join("\n"));
  if (name && localStorage.getItem("prof_" + name)) {
    var d = JSON.parse(localStorage.getItem("prof_" + name));
    d = d.map(function(s) { return {temp: s.temp, time: s.time, ramp: (s.ramp !== undefined ? s.ramp : true)}; });
    var container = document.getElementById('steps-' + id);
    container.innerHTML = '';
    d.forEach(function(s, idx) {
      container.appendChild(buildStepRow(id, idx, s.temp, s.time, s.ramp));
    });
    updateSteps(id);
  }
}

# ============================================================
# PART 10-C: JS — Control, History, Download, Polling, Charts + app.run
# ============================================================

HTML_PAGE += r"""

// ================== کنترل کوره ==================
function startF(id) {
  var isFa = currentLang === 'fa';
  if (!confirm(isFa ? "شروع از اول؟ (مراحل پاک می‌شوند)" : "Start over? (Steps will be cleared)")) return;
  var container = document.getElementById('steps-' + id);
  var rows = container.querySelectorAll('.step-row');
  var stepsArr = [];
  rows.forEach(function(row) {
    var inputs = row.querySelectorAll('input');
    stepsArr.push({
      temp: parseInt(inputs[0].value) || 0,
      time: parseInt(inputs[1].value) || 0,
      ramp: inputs[2].checked
    });
  });
  if (stepsArr.length === 0) {
    alert(isFa ? "هیچ مرحله‌ای وجود ندارد!" : "No steps found!");
    return;
  }
  data[id] = [];
  fetch('/start', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({id: id, profile: stepsArr})
  }).then(r => r.json()).then(res => {
    if (res.status !== 'ok') alert((isFa ? 'خطا: ' : 'Error: ') + (res.msg || ''));
  }).catch(e => alert('Error: ' + e.message));
}

function resumeF(id) {
  var stEl = document.getElementById('st-' + id);
  var currentMode = stEl ? stEl.innerText : 'MANUAL';
  if (currentMode === 'RUNNING') {
    fetch('/pause', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({id: id})})
      .catch(e => console.warn(e));
  } else {
    fetch('/resume', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({id: id})})
      .then(r => r.json()).then(res => {
        if (res.status !== 'ok') alert((currentLang === 'fa' ? 'خطا: ' : 'Error: ') + (res.msg || ''));
      }).catch(e => alert('Error: ' + e.message));
  }
}

function stopF(id) {
  var stEl = document.getElementById('st-' + id);
  var isError = stEl && stEl.innerText === 'ERROR';
  var isFa = currentLang === 'fa';
  var msg = isError ? (isFa ? "آیا خطای کوره ریست شود؟" : "Reset furnace error?") : (isFa ? "توقف کامل؟" : "Full stop?");
  if (!confirm(msg)) return;
  fetch('/stop', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({id: id})})
    .then(() => { if (isError) setTimeout(() => location.reload(), 500); })
    .catch(e => alert('Error: ' + e.message));
}

function downloadChart(id) {
  var canvas = document.getElementById('cv-' + id);
  if (!canvas) return;
  var link = document.createElement('a');
  link.download = 'Furnace_' + id + '.png';
  link.href = canvas.toDataURL('image/png');
  link.click();
}

// ================== کوره‌ها و مدیریت ==================
function requestNewFurnace() {
  if (activeFurnaces >= 22) return alert(currentLang === 'fa' ? "حداکثر ۲۲ کوره!" : "Max 22 furnaces!");
  if (!confirm(currentLang === 'fa' ? "کوره جدید اضافه شود؟" : "Add new furnace?")) return;
  fetch('/add_furnace', { method: 'POST' }).catch(e => alert('Error: ' + e.message));
}

function requestRemoveFurnace() {
  if (activeFurnaces <= 0) return;
  if (!confirm(currentLang === 'fa' ? "آخرین کوره حذف شود؟" : "Remove last furnace?")) return;
  fetch('/remove_furnace', { method: 'POST' }).catch(e => alert('Error: ' + e.message));
}

// ================== تاریخچه ==================
function loadHistory() {
  fetch('/history_all').then(r => r.json()).then(res => {
    var tbody = document.getElementById('history-body');
    var isFa = currentLang === 'fa';
    if (!res || res.length === 0) {
      tbody.innerHTML = '<tr><td colspan="3" style="color:var(--text-muted); text-align:center;">' + (isFa ? 'هیچ گزارشی یافت نشد.' : 'No records found.') + '</td></tr>';
      return;
    }
    var html = '';
    res.forEach(function(h) {
      var color = h.mode === 'RUNNING' ? 'orange' : (h.mode === 'FINISHED' ? 'var(--success)' : 'gray');
      var modeText = h.mode === 'RUNNING'
        ? (isFa ? 'در حال اجرا' : 'Running')
        : (h.mode === 'FINISHED' ? (isFa ? 'پایان یافته' : 'Finished') : h.mode);
      var fName = (isFa ? 'کوره ' : 'Furnace ') + h.furnace_id;
      html += '<tr><td style="font-weight:bold;">' + fName + '</td><td>' + h.start_time + '</td><td style="color:' + color + '; font-weight:bold;">' + modeText + '</td></tr>';
    });
    tbody.innerHTML = html;
  }).catch(function() {
    document.getElementById('history-body').innerHTML = '<tr><td colspan="3" style="color:var(--danger);">Error</td></tr>';
  });
}

// ================== loadChartHistory ==================
function loadChartHistory(id) {
  fetch('/chart_history/' + id).then(r => r.json()).then(res => {
    if (res.points && res.points.length > 0) {
      if (!data[id]) data[id] = [];
      var existingTimes = {};
      data[id].forEach(function(p) { existingTimes[Math.round(p.t)] = true; });
      var newPoints = res.points.filter(function(p) { return !existingTimes[Math.round(p.t)]; });
      data[id] = newPoints.concat(data[id]).sort(function(a, b) { return a.t - b.t; });
      if (furnaces[id]) {
        furnaces[id].profile = res.profile || [];
        furnaces[id].total_duration = res.total_duration || 0;
      }
      var unit = (globalConfig && globalConfig.temp_unit === 'F') ? 'F°' : 'C°';
      drawChart('cv-' + id, data[id], prevData[id], res.profile || [], res.total_duration || 0, unit);
      drawChart('ov-cv-' + id, data[id], prevData[id], res.profile || [], res.total_duration || 0, unit);
    }
  }).catch(e => console.log('Error loading chart history', e));
}

// ================== resize و tab ==================
function resizeCanvas(id) {
  var c = document.getElementById(id);
  if (c) {
    var dpr = window.devicePixelRatio || 1;
    var rect = c.parentElement.getBoundingClientRect();
    c.width = rect.width * dpr;
    c.height = rect.height * dpr;
    c.style.width = rect.width + 'px';
    c.style.height = rect.height + 'px';
    var ctx = c.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
}

// --- FIX #30/#31: showTab با id و data-tab ---
function showTab(id) {
  document.querySelectorAll('.tab-content').forEach(function(e) { e.className = 'tab-content'; });
  document.querySelectorAll('.tab-btn').forEach(function(e) { e.className = 'tab-btn'; });
  var tabId = (typeof id === 'number') ? ('tab-' + id) : ('tab-' + id);
  var btnId = (typeof id === 'number') ? ('btn-' + id) : ('tab-btn-' + id);
  var tabEl = document.getElementById(tabId);
  var btnEl = document.getElementById(btnId);
  if (tabEl) tabEl.className = 'tab-content active';
  if (btnEl) btnEl.className = 'tab-btn active';
  setTimeout(function() {
    if (typeof id === 'number') {
      resizeCanvas('cv-' + id);
    } else if (id === 'compare') {
      resizeCanvas('compare-cv');
    } else if (id === 'overview') {
      for (var k = 1; k <= activeFurnaces; k++) resizeCanvas('ov-cv-' + k);
    } else if (id === 'settings') {
      loadHistory();
    }
  }, 50);
}

// ================== Polling ==================
setInterval(function() {
  fetch('/status').then(r => r.json()).then(resp => {
    var d = resp.furnaces;
    var cfg = resp.config;
    globalConfig = cfg;
    furnaces = d;

    var isConnected = resp.serial_connected;
    var isFa = currentLang === 'fa';
    var gcs = document.getElementById('global-com-status');
    gcs.innerText = isConnected
      ? (isFa ? "متصل (" + resp.com_port + ")" : "Connected (" + resp.com_port + ")")
      : (isFa ? "قطع (آفلاین)" : "Disconnected (Offline)");
    gcs.style.color = isConnected ? "var(--success)" : "var(--danger)";
    document.getElementById('alarm-banner').style.display = isConnected ? 'none' : 'block';

    if (!isConnected && Notification.permission === "granted") {
      if (!window.lastNotifTime || Date.now() - window.lastNotifTime > 30000) {
        new Notification(isFa ? "⚠️ هشدار کوره" : "⚠️ Furnace Alert", {
          body: isFa ? "ارتباط شبکه قطع است" : "Arduino connection lost!"
        });
        window.lastNotifTime = Date.now();
      }
    }

    var safetyTriggered = false;

    while (activeFurnaces < resp.active_count) {
      activeFurnaces++;
      createFurnaceElements(activeFurnaces);
    }
    while (activeFurnaces > resp.active_count) {
      var btnEl = document.getElementById('btn-' + activeFurnaces); if (btnEl) btnEl.remove();
      var tabEl = document.getElementById('tab-' + activeFurnaces); if (tabEl) tabEl.remove();
      var ovEl = document.getElementById('ov-card-' + activeFurnaces); if (ovEl) ovEl.remove();
      delete data[activeFurnaces];
      delete prevData[activeFurnaces];
      activeFurnaces--;
    }

    var tempUnitSym = cfg.temp_unit === 'F' ? 'F°' : 'C°';

    for (var id in d) {
      var numId = parseInt(id);
      if (numId > activeFurnaces) continue;
      var stEl = document.getElementById('st-' + id);
      if (!stEl) continue;

      var displayTemp = d[id].temp + cfg.temp_offset;
      if (cfg.temp_unit === 'F') displayTemp = (displayTemp * 9 / 5) + 32;
      if (d[id].temp > cfg.safety_limit) safetyTriggered = true;

      var tempLiveEl = document.getElementById('temp-live-' + id);
      if (tempLiveEl) {
        if (d[id].sensor_err) {
          tempLiveEl.innerText = "ERR";
          tempLiveEl.style.backgroundColor = "var(--danger)";
        } else {
          tempLiveEl.innerText = displayTemp.toFixed(1) + ' ' + tempUnitSym;
          tempLiveEl.style.backgroundColor = "var(--primary)";
        }
      }

      stEl.innerText = d[id].mode;
      var ovSt = document.getElementById('ov-st-' + id);
      if (ovSt) ovSt.innerText = d[id].mode;

      var stepEl = document.getElementById('current-step-' + id);
      if (stepEl) {
        if (d[id].mode === 'RUNNING' || d[id].mode === 'PAUSED') {
          stepEl.style.display = 'inline-block';
          stepEl.innerText = (isFa ? 'مرحله ' : 'Step ') + (d[id].step + 1) + (isFa ? ' در حال اجرا' : ' running');
        } else {
          stepEl.style.display = 'none';
        }
      }

      var btn = document.getElementById('resume-btn-' + id);
      if (btn) {
        if (d[id].mode === 'RUNNING') { btn.innerHTML = isFa ? '⏸ مکث (Pause)' : '⏸ Pause'; btn.className = 'orange'; }
        else if (d[id].mode === 'ERROR') { btn.innerHTML = isFa ? '⛔ خطا' : '⛔ Error'; btn.className = 'red'; }
        else { btn.innerHTML = isFa ? '▶ ادامه (Resume)' : '▶ Resume'; btn.className = 'blue'; }
      }

      var modeClass = 'badge bg-secondary';
      if (d[id].mode === 'RUNNING') modeClass = 'badge bg-warning';
      else if (d[id].mode === 'FINISHED') modeClass = 'badge bg-success';
      else if (d[id].mode === 'PAUSED') modeClass = 'badge bg-secondary';
      else if (d[id].mode === 'ERROR') modeClass = 'badge bg-danger';
      if (d[id].sensor_err) modeClass = 'badge bg-danger';
      stEl.className = modeClass;
      if (ovSt) ovSt.className = modeClass;

      var relayEl = document.getElementById('relay-' + id);
      if (relayEl) {
        relayEl.innerText = d[id].relay === 'ON' ? (isFa ? 'المنت روشن' : 'Heater ON') : (isFa ? 'المنت خاموش' : 'Heater OFF');
        relayEl.className = d[id].relay === 'ON' ? 'relay-on' : 'relay-off';
      }
      var ovRelay = document.getElementById('ov-relay-' + id);
      if (ovRelay) { ovRelay.innerText = d[id].relay; ovRelay.className = d[id].relay === 'ON' ? 'relay-on' : 'relay-off'; }

      var connEl = document.getElementById('conn-' + id);
      if (connEl) connEl.className = d[id].is_online ? 'conn-dot conn-ok' : 'conn-dot conn-fail';
      var ovConn = document.getElementById('ov-conn-' + id);
      if (ovConn) ovConn.className = d[id].is_online ? 'conn-dot conn-ok' : 'conn-dot conn-fail';

      var ovTemp = document.getElementById('ov-temp-' + id);
      if (ovTemp) ovTemp.innerText = d[id].sensor_err ? "ERR" : displayTemp.toFixed(1) + ' ' + tempUnitSym;

      var elapsedEl = document.getElementById('elapsed-' + id);
      if (elapsedEl) elapsedEl.innerText = formatTime(d[id].elapsed);
      var remainingEl = document.getElementById('remaining-' + id);
      if (remainingEl) remainingEl.innerText = formatTime(d[id].remaining_time);

      if (d[id].mode === 'RUNNING' || (data[id] && data[id].length === 0)) {
        if (!data[id]) data[id] = [];
        var lastPoint = data[id][data[id].length - 1];
        if (!lastPoint || Math.abs(lastPoint.t - d[id].elapsed) >= 1) {
          data[id].push({
            t: d[id].elapsed,
            v: d[id].sensor_err ? 0 : displayTemp,
            r: d[id].relay,
            err: d[id].sensor_err,
            target: d[id].live_target || 0
          });
          if (data[id].length > 2000) data[id].shift();
        }
      }

      drawChart('cv-' + id, data[id], prevData[id], d[id].profile, d[id].total_duration, tempUnitSym);
      drawChart('ov-cv-' + id, data[id], prevData[id], d[id].profile, d[id].total_duration, tempUnitSym);
    }
    document.getElementById('safety-banner').style.display = safetyTriggered ? 'block' : 'none';
    if (safetyTriggered) startAlarm(); else stopAlarm();

    if (document.getElementById('tab-compare').classList.contains('active')) {
      drawCompareChart(tempUnitSym);
    }
  }).catch(err => { console.log('Status error:', err); });
}, 500);

// ================== رسم نمودار ==================
function drawChart(cid, actual, previous, profile, totalDuration, unitSym) {
  var c = document.getElementById(cid);
  if (!c) return;
  var ctx = c.getContext('2d');
  var w = c.clientWidth, h = c.clientHeight;
  if (w === 0 || h === 0) return;
  ctx.clearRect(0, 0, w, h);
  var padL = 50, padB = 30, padT = 20, padR = 20, gw = w - padL - padR, gh = h - padT - padB;
  var chartBg = getComputedStyle(document.documentElement).getPropertyValue('--chart-bg').trim();
  ctx.fillStyle = chartBg; ctx.fillRect(0, 0, w, h);

  // --- FIX #28: reduce جایگزین Math.max(...array) ---
  var maxT = 100;
  if (profile && profile.length) maxT = Math.max(maxT, profile.reduce(function(m, p) { return Math.max(m, p.temp); }, 0));
  if (actual && actual.length) maxT = Math.max(maxT, actual.reduce(function(m, a) { return Math.max(m, a.v); }, 0));
  if (previous && previous.length) maxT = Math.max(maxT, previous.reduce(function(m, a) { return Math.max(m, a.v); }, 0));
  maxT = Math.ceil((maxT + 50) / 100) * 100;

  var maxTime = totalDuration > 0 ? totalDuration : 600;
  if (actual && actual.length) maxTime = Math.max(maxTime, actual[actual.length - 1].t);
  if (profile && profile.length) maxTime = Math.max(maxTime, profile.reduce(function(a, b) { return a + b.time * 60; }, 0));

  if (actual && actual.length > 0) {
    for (var i = 0; i < actual.length; i++) {
      var startX = padL + (actual[i].t / maxTime) * gw;
      var endX = (i < actual.length - 1) ? padL + (actual[i + 1].t / maxTime) * gw : startX + 5;
      if (actual[i].r === 'ON') { ctx.fillStyle = 'rgba(239, 68, 68, 0.2)'; ctx.fillRect(startX, padT, endX - startX, gh); }
      if (actual[i].err) { ctx.fillStyle = 'rgba(100, 116, 139, 0.4)'; ctx.fillRect(startX, padT, endX - startX, gh); }
    }
  }

  var isLight = document.body.classList.contains('light-mode');
  ctx.strokeStyle = isLight ? '#cbd5e1' : '#334155';
  ctx.fillStyle = isLight ? '#475569' : '#94a3b8';
  ctx.font = '11px Tahoma';
  for (var i = 0; i <= 5; i++) {
    var val = (maxT / 5) * i, y = h - padB - (val / maxT) * gh;
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke();
    ctx.textAlign = 'right'; ctx.fillText(val.toFixed(0) + unitSym, padL - 10, y + 4);
  }
  for (var i = 0; i <= 5; i++) {
    var val2 = (maxTime / 5) * i, x = padL + (val2 / maxTime) * gw;
    ctx.beginPath(); ctx.moveTo(x, padT); ctx.lineTo(x, h - padB); ctx.stroke();
    ctx.textAlign = 'center'; ctx.fillText(Math.floor(val2 / 60) + "m", x, h - 10);
  }

  if (previous && previous.length > 0) {
    ctx.strokeStyle = 'rgba(100, 116, 139, 0.4)'; ctx.lineWidth = 2;
    ctx.beginPath();
    for (var i = 0; i < previous.length; i++) {
      var x = padL + (previous[i].t / maxTime) * gw, y = h - padB - (previous[i].v / maxT) * gh;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }

  if (profile && profile.length > 0) {
    ctx.strokeStyle = '#f59e0b';
    ctx.setLineDash([5, 5]);
    ctx.lineWidth = 2;
    ctx.beginPath();
    var acc = 0, lastTemp = 25;
    ctx.moveTo(padL, h - padB - (lastTemp / maxT) * gh);
    for (var pi = 0; pi < profile.length; pi++) {
      var p = profile[pi];
      var ramp = (p.ramp !== undefined) ? p.ramp : true;
      if (ramp) {
        acc += p.time * 60;
        ctx.lineTo(padL + (acc / maxTime) * gw, h - padB - (p.temp / maxT) * gh);
        lastTemp = p.temp;
      } else {
        acc += 120;
        ctx.lineTo(padL + (acc / maxTime) * gw, h - padB - (p.temp / maxT) * gh);
        acc += p.time * 60;
        ctx.lineTo(padL + (acc / maxTime) * gw, h - padB - (p.temp / maxT) * gh);
        lastTemp = p.temp;
      }
    }
    ctx.stroke();
    ctx.setLineDash([]);
  }

  if (actual && actual.length > 0) {
    ctx.strokeStyle = '#0ea5e9';
    ctx.lineWidth = 2.5;
    ctx.lineCap = 'round'; ctx.lineJoin = 'round';
    ctx.beginPath();
    var started = false;
    for (var ai = 0; ai < actual.length; ai++) {
      if (actual[ai].err) { started = false; continue; }
      var ax = padL + (actual[ai].t / maxTime) * gw, ay = h - padB - (actual[ai].v / maxT) * gh;
      if (!started) { ctx.moveTo(ax, ay); started = true; }
      else ctx.lineTo(ax, ay);
    }
    ctx.stroke();
  }
}

function drawCompareChart(unitSym) {
  var c = document.getElementById('compare-cv');
  if (!c) return;
  if (c.width === 0 || c.height === 0) resizeCanvas('compare-cv');
  var ctx = c.getContext('2d');
  var w = c.clientWidth, h = c.clientHeight;
  if (w === 0 || h === 0) return;
  ctx.clearRect(0, 0, w, h);
  var padL = 50, padB = 30, padT = 20, padR = 20, gw = w - padL - padR, gh = h - padT - padB;
  var chartBg = getComputedStyle(document.documentElement).getPropertyValue('--chart-bg').trim();
  ctx.fillStyle = chartBg; ctx.fillRect(0, 0, w, h);

  var maxT = 100, maxTime = 600, hasData = false;
  var colors = ['#0ea5e9','#ef4444','#22c55e','#eab308','#a855f7','#f97316','#06b6d4','#ec4899','#8b5cf6','#14b8a6','#f43f5e','#84cc16','#6366f1','#d946ef','#10b981','#f59e0b','#3b82f6','#ef4444','#22c55e','#eab308','#a855f7','#f97316'];

  for (var id in data) {
    if (parseInt(id) <= activeFurnaces && data[id] && data[id].length > 0) {
      hasData = true;
      for (var i = 0; i < data[id].length; i++) {
        if (data[id][i].v > maxT) maxT = data[id][i].v;
        if (data[id][i].t > maxTime) maxTime = data[id][i].t;
      }
    }
  }
  maxT = Math.ceil((maxT + 50) / 100) * 100;
  maxTime = Math.ceil(maxTime / 60) * 60;

  var isLight = document.body.classList.contains('light-mode');
  ctx.strokeStyle = isLight ? '#cbd5e1' : '#334155';
  ctx.fillStyle = isLight ? '#475569' : '#94a3b8';
  ctx.font = '12px Tahoma';
  for (var i = 0; i <= 5; i++) {
    var val = (maxT / 5) * i, y = h - padB - (val / maxT) * gh;
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke();
    ctx.textAlign = 'right'; ctx.fillText(val.toFixed(0) + unitSym, padL - 10, y + 4);
  }
  for (var i = 0; i <= 5; i++) {
    var val2 = (maxTime / 5) * i, x = padL + (val2 / maxTime) * gw;
    ctx.beginPath(); ctx.moveTo(x, padT); ctx.lineTo(x, h - padB); ctx.stroke();
    ctx.textAlign = 'center'; ctx.fillText(Math.floor(val2 / 60) + "m", x, h - 10);
  }

  if (!hasData) {
    var t = translations[currentLang];
    ctx.fillStyle = isLight ? '#1e293b' : '#f8fafc';
    ctx.font = '20px Tahoma';
    ctx.textAlign = 'center';
    ctx.fillText(t.no_furnace_started, w / 2, h / 2 - 10);
    ctx.font = '14px Tahoma';
    ctx.fillStyle = '#64748b';
    ctx.fillText(t.only_running, w / 2, h / 2 + 20);
    return;
  }

  var legendX = padL + 10, legendY = padT + 10;
  for (var id in data) {
    if (parseInt(id) <= activeFurnaces && data[id] && data[id].length > 0) {
      ctx.strokeStyle = colors[(parseInt(id) - 1) % colors.length];
      ctx.lineWidth = 2;
      ctx.lineCap = 'round'; ctx.lineJoin = 'round';
      ctx.beginPath();
      var started = false;
      for (var i = 0; i < data[id].length; i++) {
        if (data[id][i].err) { started = false; continue; }
        var x = padL + (data[id][i].t / maxTime) * gw;
        var y = h - padB - (data[id][i].v / maxT) * gh;
        if (!started) { ctx.moveTo(x, y); started = true; }
        else ctx.lineTo(x, y);
      }
      ctx.stroke();
      ctx.fillStyle = colors[(parseInt(id) - 1) % colors.length];
      ctx.fillRect(legendX, legendY, 15, 15);
      ctx.fillStyle = isLight ? '#1e293b' : '#f8fafc';
      ctx.textAlign = 'right';
      ctx.fillText((currentLang === 'fa' ? 'کوره ' : 'Furnace ') + id, legendX + 75, legendY + 12);
      legendY += 25;
    }
  }
}
</script>
</body>
</html>
"""

# ============================================================
# Routes و اجرای نهایی
# ============================================================

@app.route('/')
@login_required
def index():
    return render_template_string(HTML_PAGE)


if __name__ == '__main__':
    threading.Thread(target=sim, daemon=True).start()
    threading.Thread(target=db_logger, daemon=True).start()
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)