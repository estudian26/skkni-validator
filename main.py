# main.py
import os, time, re, json, tempfile, requests, pandas as pd, gspread, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from gspread.utils import rowcol_to_a1

# -------- Config via env vars --------
INPUT_GID  = int(os.getenv("INPUT_GID", "372282629"))
OUTPUT_GID = int(os.getenv("OUTPUT_GID", "372282629"))
SHEET_KEY  = os.getenv("SHEET_KEY")  # required
SERP_KEY   = os.getenv("SERPAPI_API_KEY")  # required

SMTP_HOST  = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT  = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER  = os.getenv("SMTP_USER", "")
SMTP_PASS  = os.getenv("SMTP_PASS", "")
RECIPIENTS = [e.strip() for e in os.getenv("RECIPIENTS", "").split(",") if e.strip()]

if not SHEET_KEY or not SERP_KEY:
    raise SystemExit("Missing SHEET_KEY or SERPAPI_API_KEY env var.")

# -------- Write service account JSON from secret to a temp file --------
GSHEETS_JSON = os.getenv("GSHEETS_JSON")  # required: full JSON as a single secret
if not GSHEETS_JSON:
    raise SystemExit("Missing GSHEETS_JSON env var (service account JSON).")

with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
    tf.write(GSHEETS_JSON)
    CREDS_FILE = tf.name

# -------- HTTP session --------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (SKKNI-Checker)"})

# -------- SerpAPI helper --------
def serp_search(query: str, api_key: str, retries: int = 3, timeout: int = 30):
    url = "https://serpapi.com/search.json"
    last_err = None
    for i in range(retries):
        try:
            resp = SESSION.get(
                url,
                params={"q": query, "api_key": api_key, "hl": "id", "num": 10},
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (i + 1))
    raise last_err

# -------- Stricter status detection --------
def _status_from_blob(blob_up: str) -> str:
    if re.search(r"\bTIDAK\s*BERLAKU\b", blob_up):
        return "Dicabut"
    if re.search(r"\bDICABUT\b", blob_up):
        return "Dicabut"
    if re.search(r"\bBERLAKU\b", blob_up):
        return "Berlaku"
    return "Unknown"

def _looks_like_same_skkni(text_up: str, nomor: int, tahun: int) -> bool:
    num_pat = rf"(NOMOR|NO\.)\s*{nomor}\b"
    thn_pat = rf"(TAHUN)\s*{tahun}\b"
    m_num = re.search(num_pat, text_up)
    m_thn = re.search(thn_pat, text_up)
    if not m_num or not m_thn:
        return False
    return abs(m_num.start() - m_thn.start()) <= 120

def _is_listing_or_search_url(url: str) -> bool:
    u = (url or "").lower()
    return any(p in u for p in ["/search", "/?s=", "/kategori/", "/category/", "/tag/"])

def _verify_from_page(url: str, timeout: int = 25) -> str:
    try:
        html = SESSION.get(url, timeout=timeout).text.upper()
        if re.search(r"STATUS\s*[:\-]?\s*BERLAKU\b", html):
            return "Berlaku"
        if re.search(r"STATUS\s*[:\-]?\s*TIDAK\s*BERLAKU\b", html) or re.search(r"\bDICABUT\b", html):
            return "Dicabut"
    except Exception:
        pass
    return "Unknown"

def check_status_snippet(nomor: int, tahun: int) -> str:
    query = f'"Nomor {nomor} Tahun {tahun}" "SKKNI" site:skkni.kemnaker.go.id'
    data = serp_search(query, SERP_KEY)

    qualified = []
    for res in data.get("organic_results", []) or []:
        title   = (res.get("title") or "")
        snippet = (res.get("snippet") or "")
        url     = res.get("link") or res.get("displayed_link") or ""
        blob_up = f"{title} {snippet}".upper()

        if _is_listing_or_search_url(url):
            continue
        if "SKKNI" not in blob_up:
            continue
        if not _looks_like_same_skkni(blob_up, nomor, tahun):
            continue

        guess = _status_from_blob(blob_up)
        qualified.append((url, guess))

    if not qualified:
        return "Tidak ditemukan"

    # Verify only when guess is Dicabut to save quota
    for url, guess in qualified:
        if guess == "Dicabut":
            verified = _verify_from_page(url)
            if verified in ("Berlaku", "Dicabut"):
                return verified

    guesses = [g for _, g in qualified if g in ("Berlaku", "Dicabut")]
    if not guesses:
        return "Tidak ditemukan"
    if "Berlaku" in guesses and "Dicabut" in guesses:
        return "Berlaku"
    return guesses[0]

# -------- Email builder --------
def build_dicabut_alert(df: pd.DataFrame) -> pd.DataFrame:
    df_alert = (
        df[df["Status"].astype(str).str.upper() == "DICABUT"]
        [["Nama Skema", "Nomor SKKNI", "Tahun SKKNI"]]
        .rename(columns={"Nomor SKKNI": "Nomor", "Tahun SKKNI": "Tahun"})
        .copy()
    )
    if not df_alert.empty:
        df_alert["Nomor"] = pd.to_numeric(df_alert["Nomor"], errors="coerce").astype("Int64")
        df_alert["Tahun"] = pd.to_numeric(df_alert["Tahun"], errors="coerce").astype("Int64")
        df_alert = df_alert.sort_values(["Nama Skema", "Tahun", "Nomor"])
    return df_alert

def send_weekly_alert(df_alert: pd.DataFrame) -> bool:
    if not SMTP_USER or not SMTP_PASS or not RECIPIENTS:
        print("SMTP not configured, skip email.")
        return False
    if df_alert.empty:
        print("No Dicabut in sheet. No email sent.")
        return False
    count = len(df_alert)
    subject = f"[Pemberitahuan Monitor SKKNI] {count} SKKNI baru terdeteksi DICABUT"
    lines = ["Berikut daftar SKKNI berstatus DICABUT yang masih terbuka:", ""]
    for _, r in df_alert.iterrows():
        lines.append(f"- {r['Nama Skema']}, Nomor {r['Nomor']} Tahun {r['Tahun']}")
    lines += ["", "Tindakan yang diharapkan:",
              "1. Verifikasi status di skkni.kemnaker.go.id.",
              "2. Sesuaikan dokumen/skema bila terdampak.",
              "", "Email ini otomatis terkirim."]
    body = "\n".join(lines)
    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(RECIPIENTS)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(SMTP_USER, RECIPIENTS, msg.as_string())
    print(f"✓ Email sent to: {', '.join(RECIPIENTS)}")
    return True

# -------- Sheet helpers --------
def _ensure_and_get_col(ws, col_name: str) -> int:
    header = ws.row_values(1)
    if col_name in header:
        return header.index(col_name) + 1
    ws.update_cell(1, len(header) + 1, col_name)
    return len(header) + 1

def _get_col(ws, col_name: str) -> int:
    header = ws.row_values(1)
    if col_name not in header:
        raise ValueError(f"Header '{col_name}' not found in the sheet.")
    return header.index(col_name) + 1

# -------- Main --------
def main():
    gc = gspread.service_account(filename=CREDS_FILE)
    ss = gc.open_by_key(SHEET_KEY)
    ws_in  = ss.get_worksheet_by_id(INPUT_GID)
    ws_out = ss.get_worksheet_by_id(OUTPUT_GID)
    print("✓ Connected to Google Sheets")

    records = ws_in.get_all_records()
    df_raw = pd.DataFrame(records)

    if "Nama Skema" in df_raw.columns:
        df_raw["Nama Skema"] = (
            df_raw["Nama Skema"].astype("string").replace("", pd.NA).ffill().fillna("")
        )

    required_cols = {"Nomor SKKNI", "Tahun SKKNI"}
    missing = required_cols - set(df_raw.columns)
    if missing:
        raise ValueError(f"Missing required columns in Sheet: {missing}")

    for col in ["Nomor SKKNI", "Tahun SKKNI"]:
        df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce").astype("Int64")

    df_pairs = (
        df_raw[["Nomor SKKNI", "Tahun SKKNI"]]
        .rename(columns={"Nomor SKKNI": "Nomor", "Tahun SKKNI": "Tahun"})
        .dropna(how="any")
        .drop_duplicates(subset=["Nomor", "Tahun"])
        .reset_index(drop=True)
    )

    status_rows = []
    for _, r in df_pairs.iterrows():
        nomor = int(r["Nomor"]) if pd.notna(r["Nomor"]) else None
        tahun = int(r["Tahun"]) if pd.notna(r["Tahun"]) else None
        status = "Tidak ditemukan" if (nomor is None or tahun is None) else check_status_snippet(nomor, tahun)
        status_rows.append({"Nomor": nomor, "Tahun": tahun, "Status": status})
        time.sleep(1.2)
    df_status = pd.DataFrame(status_rows)

    status_map = {(int(r.Nomor), int(r.Tahun)): r.Status for _, r in df_status.iterrows()
                  if pd.notna(r.Nomor) and pd.notna(r.Tahun)}

    def _map_status(row):
        try:
            key = (int(row["Nomor SKKNI"]), int(row["Tahun SKKNI"]))
        except Exception:
            return "Tidak ditemukan"
        return status_map.get(key, "Tidak ditemukan")

    if "Status" not in df_raw.columns:
        df_raw.insert(df_raw.columns.get_loc("Tahun SKKNI") + 1, "Status", None)

    df_raw["Status"] = df_raw.apply(_map_status, axis=1)

    # Resolve actual header positions
    status_idx = _ensure_and_get_col(ws_out, "Status")
    col_nomor_idx  = _get_col(ws_out, "Nomor SKKNI")
    col_tahun_idx  = _get_col(ws_out, "Tahun SKKNI")

    # Ensure enough columns
    current_cols = len(ws_out.row_values(1))
    if status_idx > current_cols:
        ws_out.add_cols(status_idx - current_cols)

    # Write Status column
    start = rowcol_to_a1(2, status_idx)
    end   = rowcol_to_a1(len(df_raw) + 1, status_idx)
    ws_out.update(f"{start}:{end}", [[v] for v in df_raw["Status"].tolist()])
    print(f"✓ Wrote 'Status' to column index {status_idx}")

    # Highlight Dicabut rows
    fmt = {"backgroundColor": {"red": 0.95, "green": 0.80, "blue": 0.80}}
    mask_dicabut = df_raw["Status"].astype(str).str.upper() == "DICABUT"
    highlighted = []
    for i in df_raw.index[mask_dicabut]:
        rownum = i + 2
        for c in (col_nomor_idx, col_tahun_idx, status_idx):
            a1 = rowcol_to_a1(rownum, c)
            ws_out.format(a1, fmt)
            highlighted.append(a1)

    if highlighted:
        print("✓ Highlighted cells:", ", ".join(highlighted))
    else:
        print("No Dicabut rows to highlight.")

    # Email alert
    df_alert_any = build_dicabut_alert(df_raw)
    send_weekly_alert(df_alert_any)

if __name__ == "__main__":
    main()
