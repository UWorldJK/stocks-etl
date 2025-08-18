# src/send_report_email.py
"""
Send the ETL pipeline report via email with a few inline charts (with captions)
while staying under SES's 10MB raw limit.

Key tactics:
- Embed only a small number of compressed charts (JPEG if Pillow available)
- Zip the CSV if it's big
- Dynamically trim embeds until the size estimate fits under a safe budget
"""

import os
import sys
import math
import zipfile
import mimetypes
from datetime import datetime
from email_handler import send_email

# ------------------------------
# Tunables via env (safe defaults)
# ------------------------------
MAX_RAW_BYTES = int(os.environ.get("SES_MAX_RAW_BYTES", "9000000"))  # < 10MB hard limit
MAX_EMBED_IMAGES = int(os.environ.get("MAX_EMBED_IMAGES", "4"))
EMBED_WIDTH_PX = int(os.environ.get("EMBED_WIDTH_PX", "900"))
EMBED_JPEG_QUALITY = int(os.environ.get("EMBED_JPEG_QUALITY", "60"))

# Your tickers (order = priority for choosing embeds)
TICKERS = [
    "SPY","QQQ","VTI","AAPL","MSFT","GOOGL","NVDA","AMZN",
    "TSLA","META","XLK","SOXX","ARKK","IGV","TLT","PLTR"
]

# Try to enable optional image shrinking
try:
    from PIL import Image  # type: ignore
except Exception:
    Image = None


# ---------- Helpers ----------
def base64_overhead(n_bytes: int) -> int:
    """Size after base64 encoding (approx)."""
    return int(math.ceil(n_bytes / 3.0) * 4)

def estimate_raw_size_bytes(body_text: str, body_html: str, attachment_paths: list[str]) -> int:
    """
    Rough SES raw message size estimate.
    Includes headers/body cushion + base64 attachments.
    Note: Inline <img src="data:..."> lives inside body_html bytes.
    """
    size = 50_000  # headers/boundaries cushion
    size += len(body_text.encode("utf-8"))
    size += len(body_html.encode("utf-8"))
    for p in attachment_paths:
        try:
            fs = os.path.getsize(p)
            size += base64_overhead(fs) + 2048
        except OSError:
            pass
    return size

def compress_csv_if_needed(csv_path: str) -> tuple[str, bool]:
    """Zip CSV if > 3MB. Returns (path, zipped?)."""
    if not os.path.exists(csv_path):
        return csv_path, False
    fs = os.path.getsize(csv_path)
    if fs <= 3_000_000:
        return csv_path, False
    out_dir = os.path.join("artifacts", "tmp")
    os.makedirs(out_dir, exist_ok=True)
    zip_path = os.path.join(out_dir, os.path.basename(csv_path) + ".zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv_path, arcname=os.path.basename(csv_path))
    return zip_path, True

def image_to_inline_base64(path: str, max_w: int, jpeg_quality: int) -> tuple[str, str, int]:
    """
    Return (data_uri_mime, base64_str, decoded_bytes_len).
    If Pillow available: convert to JPEG + downscale.
    Otherwise: read original and mark mime via mimetypes.
    """
    import base64

    if Image is not None:
        try:
            img = Image.open(path)
            img = img.convert("RGB")
            w, h = img.size
            if w > max_w:
                h = int(h * (max_w / float(w)))
                img = img.resize((max_w, h))
            # Write to temp JPEG in memory
            from io import BytesIO
            buf = BytesIO()
            img.save(buf, "JPEG", quality=jpeg_quality, optimize=True, progressive=True)
            data = buf.getvalue()
            return "image/jpeg", base64.b64encode(data).decode("utf-8"), len(data)
        except Exception:
            pass  # fall back to raw

    # Fallback: read original file (PNG likely)
    try:
        with open(path, "rb") as f:
            raw = f.read()
        mime, _ = mimetypes.guess_type(path)
        if not mime:
            mime = "application/octet-stream"
        return mime, base64.b64encode(raw).decode("utf-8"), len(raw)
    except Exception:
        # Return empty image on error
        return "application/octet-stream", "", 0

def discover_charts(chart_dir: str) -> list[str]:
    if not os.path.exists(chart_dir):
        return []
    files = [os.path.join(chart_dir, f) for f in os.listdir(chart_dir) if f.lower().endswith(".png")]
    files.sort()
    return files

def priority_embed_list(chart_files: list[str]) -> list[str]:
    """
    Build a priority list for embedding based on:
    1) Summary/overview charts
    2) MA/RSI for your key tickers (in TICKERS order)
    """
    # 1) high-level summaries
    summary = [
        "financial_dashboard.png",
        "moving_averages_comparison.png",
        "rsi_analysis.png",
        "volatility_analysis.png",
        "all_tickers_ma_7_comparison.png",
        "all_tickers_rsi_comparison.png",
    ]
    chosen = []
    for name in summary:
        for p in chart_files:
            if os.path.basename(p) == name:
                chosen.append(p)
                break

    # 2) per-ticker key charts
    per_ticker_suffixes = ["_ma_7_timeseries.png", "_rsi_timeseries.png"]
    for t in TICKERS:
        for suff in per_ticker_suffixes:
            target = f"{t}{suff}"
            for p in chart_files:
                if os.path.basename(p) == target:
                    chosen.append(p)
                    break

    # Deduplicate while preserving order
    seen = set()
    out = []
    for p in chosen:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out

def caption_for_chart(filename: str) -> str:
    name = filename.lower()
    if "financial_dashboard" in name:
        return "Portfolio overview dashboard: recent performance & key indicators."
    if "moving_averages_comparison" in name:
        return "Moving averages comparison across selected tickers (short-term trend signal)."
    if "rsi_analysis" in name:
        return "RSI (14) across instruments (overbought ≈70+, oversold ≈30−)."
    if "volatility_analysis" in name:
        return "Volatility snapshot (e.g., rolling 30-day) by instrument."
    if "all_tickers_ma_7_comparison" in name:
        return "7-day moving average breadth across all tracked tickers."
    if "all_tickers_rsi_comparison" in name:
        return "RSI breadth across all tracked tickers."
    # Per-ticker
    for t in TICKERS:
        if filename.startswith(f"{t}_"):
            if filename.endswith("_ma_7_timeseries.png"):
                return f"{t}: 7-day MA vs. price (short-term trend)."
            if filename.endswith("_rsi_timeseries.png"):
                return f"{t}: RSI(14) momentum gauge (70=OB, 30=OS)."
    return filename

def render_html(current_date: str, csv_name: str, csv_zipped: bool, embeds: list[dict], est_mb: float) -> str:
    cards = []
    for e in embeds:
        # Avoid massive inline HTML/CSS; keep it small for size budget
        cards.append(
            f"""<div style="margin:16px 0;">
  <div style="font-weight:600;margin-bottom:6px;">{e['title']}</div>
  <img alt="{e['title']}" src="data:{e['mime']};base64,{e['b64']}" style="max-width:100%;height:auto;border:1px solid #ddd;border-radius:6px;">
  <div style="font-size:12px;color:#555;margin-top:6px;">{e['caption']}</div>
</div>"""
        )
    cards_html = "\n".join(cards)

    return f"""<html><body style="font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;">
<div style="max-width:800px;margin:0 auto;padding:16px;">
  <h2>📊 Financial ETL Report <span style="opacity:.7;font-size:14px;">{current_date}</span></h2>
  <div style="padding:10px;border-left:4px solid #2196f3;background:#eef6ff;border-radius:6px;margin:12px 0;">
    <div><b>CSV:</b> {csv_name}{' (zipped)' if csv_zipped else ''}</div>
    <div><b>Embedded charts:</b> {len(embeds)}</div>
    <div style="opacity:.7;">Estimated raw email size: ~{est_mb:.2f} MB</div>
  </div>
  {cards_html if cards_html else '<p>No charts embedded.</p>'}
  <p style="color:#666;font-size:13px;margin-top:16px;">
    Indicators include Moving Averages, RSI, rolling volatility, and returns. A small set of charts is embedded to meet SES size limits.
  </p>
</div>
</body></html>"""

# ---------- Main ----------
def main() -> int:
    sender_email = os.environ.get("SENDER_EMAIL")
    recipient_email = os.environ.get("RECIPIENT_EMAIL")
    attachment_path = os.environ.get("ATTACHMENT_PATH")  # CSV

    missing = [k for k, v in {
        "SENDER_EMAIL": sender_email,
        "RECIPIENT_EMAIL": recipient_email,
        "ATTACHMENT_PATH": attachment_path,
    }.items() if not v]
    if missing:
        print(f"Error: Missing required environment variables: {', '.join(missing)}")
        return 1

    if not os.path.exists(attachment_path):
        print(f"Error: CSV file not found: {attachment_path}")
        return 1

    # Charts
    chart_dir = "artifacts/charts"
    chart_files = discover_charts(chart_dir)
    print(f"Found {len(chart_files)} chart files in {chart_dir}")

    # Pick priority list for embedding (based on your tickers)
    priority = priority_embed_list(chart_files)
    print(f"Priority charts available for embedding: {len(priority)}")

    # Zip CSV if large
    csv_path, csv_zipped = compress_csv_if_needed(attachment_path)

    current_date = datetime.now().strftime("%Y-%m-%d")
    subject = f"📊 Financial ETL Report - {current_date}"

    csv_size_mb = (os.path.getsize(csv_path) / (1024 * 1024)) if os.path.exists(csv_path) else 0.0
    body_text = f"""Financial ETL Pipeline Report - {current_date}

Hello,

Your financial pipeline completed successfully.

Summary:
- CSV: {os.path.basename(csv_path)} ({csv_size_mb:.2f} MB){' (zipped)' if csv_zipped else ''}
- Embedded charts: limited for SES 10MB raw cap

Notes:
- We embed a few compressed charts with short explanations.
- Full chart set is available in your run workspace or artifacts.

— Financial ETL Bot
""".strip()

    # Prepare embeds (start with MAX_EMBED_IMAGES; lower if Pillow missing)
    max_embeds = MAX_EMBED_IMAGES if Image is not None else min(MAX_EMBED_IMAGES, 2)

    # Convert first N priority charts to inline base64 (compressed if possible)
    embeds = []
    candidates = priority[:max_embeds]
    for p in candidates:
        mime, b64, decoded_len = image_to_inline_base64(p, EMBED_WIDTH_PX, EMBED_JPEG_QUALITY)
        if not b64:
            continue
        embeds.append({
            "path": p,
            "mime": mime,
            "b64": b64,
            "decoded_len": decoded_len,
            "title": os.path.basename(p).replace(".png", "").replace("_", " ").upper(),
            "caption": caption_for_chart(os.path.basename(p)),
        })

    # Build HTML and ensure total size stays under budget; trim embeds if needed
    attachments = [csv_path]  # attachments (CSV only)
    def html_with_est(embeds_list):
        html = render_html(current_date, os.path.basename(csv_path), csv_zipped, embeds_list, 0.0)
        est_bytes = estimate_raw_size_bytes(body_text, html, attachments)
        return html, est_bytes

    body_html, est_bytes = html_with_est(embeds)
    while est_bytes > MAX_RAW_BYTES and embeds:
        print(f"Estimated raw size {est_bytes/1024/1024:.2f} MB exceeds limit; trimming one embed...")
        embeds.pop()  # drop the last (lowest priority)
        body_html, est_bytes = html_with_est(embeds)

    est_mb_final = est_bytes / (1024 * 1024)
    print(f"Final embedded charts: {len(embeds)}; Estimated raw size: ~{est_mb_final:.2f} MB")

    # Log chosen embeds
    for e in embeds:
        kb = (e["decoded_len"] / 1024.0)
        print(f"Embed: {os.path.basename(e['path'])} -> ~{kb:.0f} KB ({e['mime']})")

    try:
        message_id = send_email(
            sender=sender_email,
            recipient=recipient_email,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachment_paths=attachments  # Only CSV to keep size small
        )
        print(f"✅ Email sent! Message ID: {message_id}")
        return 0
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
