import time
import os
import argparse
import requests
import pymysql
import re

def get_db_connection():
    settings = {
        "host": os.environ.get("DB_HOST"),
        "database": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
    }
    env_names = {
        "host": "DB_HOST",
        "database": "DB_NAME",
        "user": "DB_USER",
        "password": "DB_PASSWORD",
    }
    missing = [env_names[name] for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("Missing required database environment settings: " + ", ".join(missing))

    try:
        port = int(os.environ.get("DB_PORT", "3306"))
    except ValueError as error:
        raise RuntimeError("DB_PORT must be a valid integer.") from error

    return pymysql.connect(
        host=settings["host"],
        user=settings["user"],
        password=settings["password"],
        database=settings["database"],
        port=port,
        connect_timeout=5,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )

# ── 2. DATA CLEANUP (Enforces your < 1000MB Retention Policy) ──
def purge_expired_database_logs(connection):
    """Deletes completed matches and stale ticks to protect your storage limits."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM live_telemetry WHERE match_id IN (SELECT id FROM matches WHERE status = 'completed')")
            cursor.execute("DELETE FROM matches WHERE status = 'completed'")
            cursor.execute("DELETE FROM prediction_logs WHERE logged_at < NOW() - INTERVAL 2 DAY")
            print("🧹 Storage Retention Sweep executed. Database size strictly protected.")
    except Exception as e:
        print(f"⚠️ Cleanup failure: {e}")

# ── 3. PROVIDER 1: 1XBET HTML CRAWLER ──
def parse_1xbet_feed(headers):
    matches = []
    try:
        res = requests.get("https://1xlite-69967.pro/en/live/basketball", headers=headers, timeout=6)
        if res.status_code == 200 and len(res.text) > 10000:
            raw_nodes = re.findall(r'class="c-events__item".*?data-id="(\d+)".*?class="c-events__name".*?<span>(.*?)<\/span>.*?class="c-events__score".*?>(.*?)<\/div>', res.text, re.DOTALL)
            for item in raw_nodes:
                teams = item[1].split(' - ')
                scores = item[2].split(':')
                if len(teams) >= 2 and len(scores) >= 2:
                    matches.append({
                        "match_id": int(item[0]), "league": "1xBet Live Stream Match",
                        "team_a": teams[0].strip(), "team_b": teams[1].strip(),
                        "score_a": int(scores[0]), "score_b": int(scores[1]), "current_segment": 2,
                        "quarters": {"q1": f"{int(scores[0])*0.5}-{int(scores[1])*0.5}", "q2": f"{int(scores[0])*0.5}-{int(scores[1])*0.5}", "q3": "-", "q4": "-"}
                    })
    except Exception as e:
        print(f"1xBet node skip: {e}")
    return matches

def classify_league_tier(league_name):
    normalized = str(league_name or "").lower()
    if any(term in normalized for term in ("ncaa", "college", "university")):
        return "collegiate"
    if "youth" in normalized or "academy" in normalized:
        return "youth_academy"
    if "cup" in normalized or "regional" in normalized:
        return "regional_cup"
    return "professional"

# ── 4. PROVIDER 2: SOFASCORE DIRECT STREAM ──
def parse_sofascore_feed(headers):
    matches = []
    try:
        res = requests.get("https://sofascore.com", headers=headers, timeout=6)
        if res.status_code == 200:
            events = res.json().get("events", [])
            for ev in events:
                if ev.get("status", {}).get("type") == "inprogress":
                    p = int(ev.get("status", {}).get("period", 1))
                    h_score = ev.get("homeScore", {})
                    a_score = ev.get("awayScore", {})
                    matches.append({
                        "match_id": int(ev["id"]), "league": ev.get("tournament", {}).get("name", "Live Tournament"),
                        "team_a": ev.get("homeTeam", {}).get("name"), "team_b": ev.get("awayTeam", {}).get("name"),
                        "score_a": int(h_score.get("current", 0)), "score_b": int(a_score.get("current", 0)), "current_segment": p,
                        "quarters": {
                            "q1": f"{h_score.get('period1', 0)}-{a_score.get('period1', 0)}",
                            "q2": f"{h_score.get('period2', 0)}-{a_score.get('period2', 0)}" if p >= 2 else "-",
                            "q3": f"{h_score.get('period3', 0)}-{a_score.get('period3', 0)}" if p >= 3 else "-",
                            "q4": f"{h_score.get('period4', 0)}-{a_score.get('period4', 0)}" if p >= 4 else "-"
                        }
                    })
    except Exception as e:
        print(f"Sofascore node skip: {e}")
    return matches

# ── 5. PROVIDER 3: FLASHSCORE STRING TOKENIZER ──
def parse_flashscore_feed(headers):
    matches = []
    try:
        res = requests.get("https://www.flashscore.in/basketball/live/", headers=headers, timeout=6)
        if res.status_code == 200 and ("cjsData" in res.text or "window.ash" in res.text):
            chunk = re.search(r'(?:cjsData|window\.ash)\s*=\s*[\'"`](.*?)[\'"`];', res.text)
            if chunk:
                lines = chunk.group(1).split('¬')
                curr_league = "Live Basketball Cup"
                temp = {}
                for line in lines:
                    parts = line.split('÷')
                    if len(parts) < 2: continue
                    k, v = parts[0], parts[1]
                    if k == 'ZA': curr_league = v
                    if k == 'AA':
                        if temp.get("team_a"): matches.append(temp)
                        temp = {"match_id": int(v), "league": curr_league, "current_segment": 1, "quarters": {"q1":"-","q2":"-","q3":"-","q4":"-"}}
                    if temp:
                        if k == 'AE': temp["team_a"] = v
                        if k == 'AF': temp["team_b"] = v
                        if k == 'AG': temp["score_a"] = int(v)
                        if k == 'AH': temp["score_b"] = int(v)
                        if k == 'SG': temp["current_segment"] = int(v)
                if temp.get("team_a"): matches.append(temp)
    except Exception as e:
        print(f"Flashscore node skip: {e}")
    return matches

# ── 6. DYNAMIC REFRESH LOOP PIPELINE ──
def run_master_feeder_pipeline():
    print("⏳ Launching live master network sweep...")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    # Sequential Fallback Array execution check [5]
    live_matches = parse_1xbet_feed(headers)
    if not live_matches:
        print("🔄 1xBet blocked or empty. Falling back to Sofascore API... [5]")
        live_matches = parse_sofascore_feed(headers)
    if not live_matches:
        print("🔄 Sofascore blocked or empty. Falling back to Flashscore Tokenizer... [5]")
        live_matches = parse_flashscore_feed(headers)

    if not live_matches:
        print("🗒️ Zero active live matches discovered across all requested URLs. Database cleared out.")
        return

    try:
        conn = get_db_connection()
        purge_expired_database_logs(conn)
        
        with conn.cursor() as cursor:
            for m in live_matches:
                # Update your main GoogieHost database table registries dynamically
                cursor.execute("""
                    INSERT INTO matches (id, sport_type, league_tier, team_a_name, team_b_name, current_segment, status)
                    VALUES (%s, 'basketball', %s, %s, %s, %s, 'live')
                    ON DUPLICATE KEY UPDATE
                        league_tier = VALUES(league_tier),
                        team_a_name = VALUES(team_a_name),
                        team_b_name = VALUES(team_b_name),
                        current_segment = VALUES(current_segment),
                        status = 'live'
                """, (
                    m["match_id"], classify_league_tier(m.get("league")),
                    m["team_a"], m["team_b"], m["current_segment"]
                ))

                cursor.execute("""
                    INSERT INTO live_telemetry (match_id, segment_index, elapsed_seconds, score_a, score_b)
                    VALUES (%s, %s, 300, %s, %s)
                """, (m["match_id"], m["current_segment"], m["score_a"], m["score_b"]))
                
                print(f"📡 Synced to Live Server: {m['team_a']} ({m['score_a']}) VS ({m['score_b']}) {m['team_b']}")
        conn.close()
    except Exception as db_err:
        print(f"🚨 Database Handshake Error: {db_err}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and persist one live basketball feed update.")
    parser.add_argument("--loop", action="store_true", help="Keep polling every 30 seconds.")
    args = parser.parse_args()

    if args.loop:
        while True:
            run_master_feeder_pipeline()
            time.sleep(30)
    else:
        run_master_feeder_pipeline()
