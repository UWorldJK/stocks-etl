# src/send_report_email.py
"""
Send the ETL report email with a small, curated set of inline charts (no image attachments)
and the CSV attached. Uses AWS SES raw email so inline images render across clients.
"""

import os
import sys
import re
from datetime import datetime
from typing import Dict, List, Tuple

# Optional: if you still have an email_handler for legacy flows, we won't use it here.
# from email_handler import send_email  # not used with raw MIME path

import boto3
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders


# ---- Config -----------------------------------------------------------------

# Priority tickers to feature (ordered by broad market/tech/macro signal value)
FEATURE_PRIORITY = ["SPY", "QQQ", "TLT", "AAPL", "NVDA"]

# If fewer than this exist, we'll fall back to any other tickers present
NUM_INLINE_CHARTS = 5  # target 4–5; will cap at this number

# Chart directory produced by your pipeline
CHART_DIR = os.environ.get("CHART_DIR", "artifacts/charts")

# File preference within each ticker (first match wins)
PREFERRED_PATTERN_ORDER = [
    r"_ma(_\d+)?_timeseries\.png$",   # moving averages first
    r"_rsi(_\d+)?_timeseries\.png$",  # RSI next
    r"_vol(_.*)?\.png$",              # volatility
    r"_returns(_.*)?\.png$",          # returns
    r"\.png$",                        # anything else as a last resort
]


# ---- Helpers ----------------------------------------------------------------

def _validate_env() -> Tuple[str, str, str, str]:
    sender = os.environ.get("SENDER_EMAIL")
    recipient = os.environ.get("RECIPIENT_EMAIL")
    csv_path = os.environ.get("ATTACHMENT_PATH")
    aws_region = os.environ.get("AWS_REGION", "us-east-1")

    missing = []
    if not sender: missing.append("SENDER_EMAIL")
    if not recipient: missing.append("RECIPIENT_EMAIL")
    if not csv_path: missing.append("ATTACHMENT_PATH")

    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    return sender, recipient, csv_path, aws_region


def _scan_charts(chart_dir: str) -> List[str]:
    if not os.path.isdir(chart_dir):
        print(f"[info] Charts directory not found: {chart_dir}")
        return []
    files = [os.path.join(chart_dir, f) for f in os.listdir(chart_dir) if f.lower().endswith(".png")]
    files.sort()
    return files


def _group_by_ticker(chart_paths: List[str]) -> Dict[str, List[str]]:
    # Expect names like AAPL_ma_7_timeseries.png
    groups: Dict[str, List[str]] = {}
    for p in chart_paths:
        name = os.path.basename(p)
        # ticker is prefix until first underscore; fallback: first token before dot
        ticker = name.split("_")[0].upper() if "_" in name else name.split(".")[0].upper()
        groups.setdefault(ticker, []).append(p)
    # Sort each group's files deterministically
    for k in groups:
        groups[k].sort()
    return groups


def _pick_best_for_ticker(files: List[str]) -> str:
    for pat in PREFERRED_PATTERN_ORDER:
        rx = re.compile(pat, re.IGNORECASE)
        for f in files:
            if rx.search(os.path.basename(f)):
                return f
    return files[0]


def _select_inline_charts(groups: Dict[str, List[str]]) -> List[Tuple[str, str]]:
    """
    Returns list of (ticker, filepath) for charts to embed.
    """
    selected: List[Tuple[str, str]] = []

    # 1) Try priority tickers in order
    for t in FEATURE_PRIORITY:
        if t in groups and groups[t]:
            selected.append((t, _pick_best_for_ticker(groups[t])))
        if len(selected) >= NUM_INLINE_CHARTS:
            return selected

    # 2) Fill with any remaining tickers we haven't used yet
    for t, files in groups.items():
        if t in {x[0] for x in selected}:
            continue
        selected.append((t, _pick_best_for_ticker(files)))
        if len(selected) >= NUM_INLINE_CHARTS:
            break

    return selected


def _build_html(current_date: str, csv_name: str, csv_size_mb: float,
                img_cids: List[Tuple[str, str]]) -> str:
    """
    img_cids: list of (ticker, cid)
    """
    # Simple responsive grid of images with captions
    grid_items = "\n".join(
        f"""
        <div class="card">
            <div class="cap">{ticker}</div>
            <img src="cid:{cid}" alt="{ticker} chart" />
        </div>
        """.strip()
        for ticker, cid in img_cids
    )

    return f"""\
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
      background:#f8f9fa; margin:0; padding:24px;
    }}
    .container {{ max-width: 760px; margin: 0 auto; background:#fff; border-radius:12px; box-shadow:0 6px 20px rgba(0,0,0,0.08); overflow:hidden; }}
    .header {{ background: linear-gradient(135deg,#667eea 0%,#764ba2 100%); color:#fff; padding:24px; text-align:center; }}
    .header h2 {{ margin:0; font-size:26px; }}
    .content {{ padding:24px; }}
    .summary {{ background:#e3f2fd; border-left:4px solid #2196f3; border-radius:8px; padding:16px; margin:16px 0; }}
    .kpis {{ display:flex; gap:12px; flex-wrap:wrap; }}
    .kpi {{ flex:1 1 120px; text-align:center; }}
    .kpi .num {{ font-weight:700; font-size:22px; color:#1976d2; }}
    .grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:16px; margin-top:8px; }}
    .card {{ border:1px solid #eee; border-radius:10px; padding:12px; background:#fff; }}
    .card img {{ width:100%; height:auto; border-radius:8px; display:block; }}
    .cap {{ font-weight:600; margin-bottom:8px; color:#7b1fa2; }}
    .footer {{ background:#f5f5f5; color:#666; text-align:center; padding:16px; font-size:14px; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h2>📊 Financial ETL Report</h2>
      <div style="opacity:.9">{current_date}</div>
    </div>
    <div class="content">
      <p>Hello! 👋 Your latest market update is ready.</p>

      <div class="summary">
        <div class="kpis">
          <div class="kpi"><div class="num">{csv_size_mb:.1f}</div><div>MB CSV Data</div></div>
          <div class="kpi"><div class="num">{len(img_cids)}</div><div>Inline Charts</div></div>
        </div>
        <p><b>Data File:</b> {csv_name}</p>
        <p><b>Analysis:</b> Moving averages, RSI, volatility, returns.</p>
      </div>

      <h3 style="margin:0 0 8px 0; color:#7b1fa2;">Featured Charts</h3>
      <div class="grid">{grid_items}</div>

      <p style="margin-top:16px;">The attached CSV includes daily metrics for all tracked tickers.</p>
    </div>
    <div class="footer">
      <em>Generated automatically by your Financial ETL Pipeline.</em>
    </div>
  </div>
</body>
</html>
""".strip()


def _build_plain_text(date_str: str, csv_name: str, csv_size_mb: float,
                      selected: List[Tuple[str, str]]) -> str:
    tickers = ", ".join(t for t, _ in selected) if selected else "None"
    return (
        f"Financial ETL Pipeline Report - {date_str}\n\n"
        f"CSV: {csv_name} ({csv_size_mb:.2f} MB)\n"
        f"Featured charts: {tickers}\n\n"
        f"The CSV contains daily metrics (MAs, RSI, volatility, returns)."
    )


def _attach_csv(msg_mixed: MIMEMultipart, csv_path: str):
    with open(csv_path, "rb") as f:
        part = MIMEBase("text", "csv")
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", "attachment", filename=os.path.basename(csv_path))
    msg_mixed.attach(part)


def _attach_inline_images(msg_related: MIMEMultipart,
                          selections: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    """
    Attach images as inline and return list of (ticker, cid) pairs for HTML.
    """
    cid_pairs: List[Tuple[str, str]] = []
    for idx, (ticker, path) in enumerate(selections, start=1):
        cid = f"chart-{idx}-{ticker.lower()}"
        with open(path, "rb") as f:
            img = MIMEImage(f.read(), _subtype="png")
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=os.path.basename(path))
        msg_related.attach(img)
        cid_pairs.append((ticker, cid))
    return cid_pairs


def _send_via_ses_raw(sender: str, recipient: str, region: str, msg_root: MIMEMultipart) -> str:
    ses = boto3.client("ses", region_name=region)
    resp = ses.send_raw_email(
        Source=sender,
        Destinations=[recipient],
        RawMessage={"Data": msg_root.as_string()},
    )
    return resp.get("MessageId", "unknown")


# ---- Main -------------------------------------------------------------------

def main() -> int:
    try:
        sender_email, recipient_email, csv_path, aws_region = _validate_env()

        # Scan and choose charts
        chart_paths = _scan_charts(CHART_DIR)
        groups = _group_by_ticker(chart_paths)
        selections = _select_inline_charts(groups)  # [(ticker, file), ...]

        date_str = datetime.now().strftime("%Y-%m-%d")
        subject = f"📊 Financial ETL Report - {date_str}"

        csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)

        # Build MIME structure: mixed (csv) -> related (inline) -> alternative (text/html)
        msg_mixed = MIMEMultipart("mixed")
        msg_mixed["Subject"] = subject
        msg_mixed["From"] = sender_email
        msg_mixed["To"] = recipient_email

        msg_related = MIMEMultipart("related")
        msg_alt = MIMEMultipart("alternative")

        # Inline images (CID)
        cid_pairs = _attach_inline_images(msg_related, selections)

        # Bodies
        body_text = _build_plain_text(date_str, os.path.basename(csv_path), csv_size_mb, selections)
        body_html = _build_html(date_str, os.path.basename(csv_path), csv_size_mb, cid_pairs)

        msg_alt.attach(MIMEText(body_text, "plain"))
        msg_alt.attach(MIMEText(body_html, "html"))

        # Nest multiparts
        msg_related.attach(msg_alt)
        msg_mixed.attach(msg_related)

        # Attach CSV only (images are inline)
        _attach_csv(msg_mixed, csv_path)

        # Send
        message_id = _send_via_ses_raw(sender_email, recipient_email, aws_region, msg_mixed)

        # Logging
        print(f"Sending email from {sender_email} to {recipient_email}")
        print(f"Attached CSV: {csv_path}")
        if selections:
            print("Embedded charts:")
            for t, p in selections:
                print(f"  - {t}: {p}")
        else:
            print("No charts found to embed.")
        print(f"✅ Email sent (SES MessageId: {message_id})")
        return 0

    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
