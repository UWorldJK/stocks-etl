# src/send_report_email.py
"""
Script to send the ETL pipeline report via email with embedded charts (JPEG),
laid out in a grid. Called from GitHub Actions workflow.
"""
import os
import sys
import inspect
from datetime import datetime
from email_handler import send_email
from chart_generator import generate_email_charts
import pandas as pd
from typing import Dict, List

def main():
    """Main function to send the ETL report email with embedded charts."""
    # Get required environment variables
    sender_email = os.environ.get("SENDER_EMAIL", "jacobkurry1@gmail.com")
    recipient_email = os.environ.get("RECIPIENT_EMAIL", "jacobkurry1@gmail.com")
    attachment_path = os.environ.get("ATTACHMENT_PATH", "data/daily_metrics.csv")  # CSV file

    # Validate required environment variables
    missing_vars = []
    if not sender_email:
        missing_vars.append("SENDER_EMAIL")
    if not recipient_email:
        missing_vars.append("RECIPIENT_EMAIL")
    if not attachment_path:
        missing_vars.append("ATTACHMENT_PATH")

    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        return 1

    # Verify CSV attachment exists
    if not os.path.exists(attachment_path):
        print(f"Error: CSV file not found: {attachment_path}")
        return 1

    # Get basic CSV info for summary
    try:
        df = pd.read_csv(attachment_path)
        num_tickers = len(df['ticker'].unique()) if 'ticker' in df.columns else 0
        num_records = len(df)
        date_range = ""
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            start_date = df['date'].min().strftime('%Y-%m-%d')
            end_date = df['date'].max().strftime('%Y-%m-%d')
            date_range = f"{start_date} to {end_date}"
    except Exception as e:
        print(f"Warning: Could not read CSV for summary: {e}")
        num_tickers = 0
        num_records = 0
        date_range = "Unknown"

    # Generate charts (JPEG on disk)
    try:
        print("🎨 Generating charts for email embedding...")
        embedded_charts: Dict[str, Dict[str, str]] = generate_email_charts(attachment_path)
        print(f"✅ Generated {len(embedded_charts)} charts for embedding")
    except Exception as e:
        print(f"⚠️  Warning: Could not generate charts: {e}")
        embedded_charts = {}

    # Prep inline CID map
    inline_images: List[Dict[str, str]] = []
    gallery_items: List[Dict[str, str]] = []

    for i, (ckey, cdata) in enumerate(embedded_charts.items(), start=1):
        img_path = cdata.get("image_path")
        if not img_path or not os.path.exists(img_path):
            continue
        cid = f"chart{i}@etl"
        inline_images.append({"cid": cid, "path": img_path})
        gallery_items.append({
            "cid": cid,
            "title": cdata.get("title", f"Chart {i}"),
            "description": cdata.get("description", "")
        })

    # File info for email content
    csv_size = os.path.getsize(attachment_path)
    csv_size_mb = csv_size / (1024 * 1024)

    # Email meta
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_time = datetime.now().strftime("%H:%M")
    subject = f"📊 Financial ETL Report - {current_date}"

    # Build HTML/text bodies (grid if inline supported, otherwise fallback)
    supports_inline = _send_email_supports_inline_images()

    if gallery_items and supports_inline:
        charts_html = generate_charts_grid_html(gallery_items)
        charts_text = generate_charts_text(embedded_charts)
    elif gallery_items and not supports_inline:
        charts_html = generate_charts_fallback_html([p["path"] for p in inline_images])
        charts_text = generate_charts_text(embedded_charts)
    else:
        charts_html = '''
        <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 8px; padding: 20px; margin: 20px 0;">
            <h3 style="color: #856404; margin-top: 0;">⚠️ Charts Not Available</h3>
            <p style="color: #856404; margin-bottom: 0;">Charts could not be generated for this report. Please check the data format and try again.</p>
        </div>
        '''
        charts_text = "⚠️ Charts could not be generated for this report."

    # Text version
    body_text = f"""
Financial ETL Pipeline Report - {current_date}

Hello,

Your financial data pipeline has completed successfully! 

📋 REPORT SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Date: {current_date} at {current_time}
• Tickers Analyzed: {num_tickers} assets
• Total Records: {num_records:,} data points
• Date Range: {date_range}
• CSV File: {os.path.basename(attachment_path)} ({csv_size_mb:.2f} MB)
• Charts: {len(gallery_items)} visualizations

📈 ANALYSIS OVERVIEW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{charts_text}

Best,
Financial ETL Bot
    """.strip()

    # HTML version (theme + grid)
    body_html = build_full_html(
        current_date=current_date,
        current_time=current_time,
        num_tickers=num_tickers,
        num_records=num_records,
        csv_size_mb=csv_size_mb,
        embedded_count=len(gallery_items),
        attachment_path=attachment_path,
        date_range=date_range,
        charts_html=charts_html
    )

    # Build attachments list
    attachment_paths = [attachment_path]

    # Send
    try:
        print(f"📧 Sending email from {sender_email} to {recipient_email}")
        print(f"📄 CSV attachment: {attachment_path} ({csv_size_mb:.2f} MB)")
        print(f"🖼  Inline images: {len(inline_images)} (supported={supports_inline})")
        print(f"📈 Tracking {num_tickers} tickers with {num_records:,} data points")

        if supports_inline and inline_images:
            message_id = send_email(
                sender=sender_email,
                recipient=recipient_email,
                subject=subject,
                body_text=body_text,
                body_html=body_html,
                attachment_paths=attachment_paths,   # CSV
                inline_images=inline_images          # CID-embedded JPEGs
            )
        else:
            # Attach images if inline not supported
            attachment_paths += [img["path"] for img in inline_images]
            message_id = send_email(
                sender=sender_email,
                recipient=recipient_email,
                subject=subject,
                body_text=body_text,
                body_html=body_html,
                attachment_paths=attachment_paths
            )

        print(f"✅ Email sent! Message ID: {message_id}")
        return 0

    except TypeError as e:
        # If your email_handler doesn't support inline_images for some reason
        print(f"ℹ️  send_email() does not support inline_images; retrying without inlines. Error: {e}")
        try:
            attachment_paths += [img["path"] for img in inline_images]
            body_html_fallback = generate_charts_fallback_html([p["path"] for p in inline_images])
            body_html_final = build_full_html(
                current_date=current_date,
                current_time=current_time,
                num_tickers=num_tickers,
                num_records=num_records,
                csv_size_mb=csv_size_mb,
                embedded_count=len(gallery_items),
                attachment_path=attachment_path,
                date_range=date_range,
                charts_html=body_html_fallback
            )
            message_id = send_email(
                sender=sender_email,
                recipient=recipient_email,
                subject=subject,
                body_text=body_text,
                body_html=body_html_final,
                attachment_paths=attachment_paths
            )
            print(f"✅ Email sent (fallback mode). Message ID: {message_id}")
            return 0
        except Exception as e2:
            print(f"❌ Failed to send email (fallback): {e2}")
            import traceback
            traceback.print_exc()
            return 1
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        import traceback
        traceback.print_exc()
        return 1


def _send_email_supports_inline_images() -> bool:
    """Detect whether send_email(sender, ..., inline_images=...) is supported."""
    try:
        sig = inspect.signature(send_email)
        return 'inline_images' in sig.parameters
    except Exception:
        return False


def build_full_html(
    current_date: str,
    current_time: str,
    num_tickers: int,
    num_records: int,
    csv_size_mb: float,
    embedded_count: int,
    attachment_path: str,
    date_range: str,
    charts_html: str
) -> str:
    """Assemble the complete HTML using your existing theme + charts_html injected."""
    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Financial ETL Report - {current_date}</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
body {{
  margin:0; padding:0; background:#f8fafc; color:#2c3e50;
  font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;
}}
.email-container {{ max-width:800px; margin:0 auto; background:#ffffff; }}
.header {{ background:#667eea; color:#fff; padding:24px 18px; text-align:center; }}
.header h1 {{ margin:0 0 6px 0; font-size:24px; }}
.header .subtitle {{ opacity:0.9; font-size:14px; }}
.content {{ padding:24px 18px; }}
.summary-card {{ background:#f5f7fa; border:1px solid #e2e8f0; border-radius:12px; padding:18px; margin:18px 0; }}
.metrics-grid {{ width:100%; border-collapse:separate; border-spacing:12px; }}
.metric-item {{ background:#fff; border:1px solid #e2e8f0; border-radius:8px; text-align:center; padding:16px; }}
.metric-number {{ font-size:22px; font-weight:700; color:#667eea; display:block; }}
.metric-label {{ font-size:11px; color:#64748b; text-transform:uppercase; letter-spacing:0.5px; }}
.chart-grid {{ width:100%; border-collapse:separate; border-spacing:12px; }}
.chart-cell {{ width:50%; vertical-align:top; }}
.chart-card {{ background:#fff; border:1px solid #e2e8f0; border-radius:10px; padding:12px; }}
.chart-title {{ font-size:16px; font-weight:600; margin:0 0 6px 0; color:#1a365d; }}
.chart-desc {{ font-size:12px; color:#64748b; margin:0 0 8px 0; }}
.chart-img {{ display:block; width:100%; height:auto; border:1px solid #e2e8f0; border-radius:6px; }}
.footer {{ background:#263238; color:#eceff1; text-align:center; padding:16px; font-size:12px; }}
@media (max-width:620px) {{
  .chart-cell {{ display:block; width:100%; }}
}}
</style>
</head>
<body>
  <div class="email-container">
    <div class="header">
      <h1>📊 Financial ETL Report</h1>
      <div class="subtitle">{current_date} • {current_time}</div>
    </div>
    <div class="content">
      <div class="summary-card">
        <h2 style="margin:0 0 10px 0; font-size:18px;">Report Summary</h2>
        <table class="metrics-grid" role="presentation" cellpadding="0" cellspacing="0" width="100%">
          <tr>
            <td class="metric-item">
              <span class="metric-number">{num_tickers}</span>
              <span class="metric-label">Assets Tracked</span>
            </td>
            <td class="metric-item">
              <span class="metric-number">{num_records:,}</span>
              <span class="metric-label">Data Points</span>
            </td>
            <td class="metric-item">
              <span class="metric-number">{csv_size_mb:.1f}</span>
              <span class="metric-label">MB CSV Data</span>
            </td>
            <td class="metric-item">
              <span class="metric-number">{embedded_count}</span>
              <span class="metric-label">Charts</span>
            </td>
          </tr>
        </table>
        <div style="font-size:13px; color:#374151; margin-top:10px;">
          <div><strong>📄 Data File:</strong> {os.path.basename(attachment_path)}</div>
          <div><strong>📅 Date Range:</strong> {date_range}</div>
          <div><strong>📈 Analysis:</strong> Moving averages, RSI, volatility, daily returns</div>
        </div>
      </div>

      {charts_html}

      <div style="background:#f0f9ff; border-left:4px solid #0ea5e9; border-radius:8px; padding:12px 14px; margin:16px 0; font-size:13px; color:#0c4a6e;">
        <strong>📎 Data Access:</strong> The CSV attachment contains full detail for custom analysis.
      </div>
    </div>
    <div class="footer">
      <div>Generated automatically by your Financial ETL Pipeline</div>
      <div style="opacity:0.8; margin-top:6px;">© {current_date}</div>
    </div>
  </div>
</body>
</html>
    """.strip()


def generate_charts_grid_html(items):
    rows = []
    for i in range(0, len(items), 2):
        left = items[i]
        right = items[i+1] if i+1 < len(items) else None
        rows.append(f"""
<tr>
  <td class="chart-cell">
    <div class="chart-card">
      <div class="chart-title">{_escape_html(left['title'])}</div>
      <div class="chart-desc">{_escape_html(left.get('description',''))}</div>
      <img class="chart-img" src="cid:{left['cid']}" alt="{_escape_html(left['title'])}">
    </div>
  </td>
  {(
    f'''<td class="chart-cell">
      <div class="chart-card">
        <div class="chart-title">{_escape_html(right['title'])}</div>
        <div class="chart-desc">{_escape_html(right.get('description',''))}</div>
        <img class="chart-img" src="cid:{right['cid']}" alt="{_escape_html(right['title'])}">
      </div>
    </td>''' if right else '<td class="chart-cell"></td>'
  )}
</tr>
""")
    return f'<table class="chart-grid" role="presentation" cellpadding="0" cellspacing="0" width="100%">{"".join(rows)}</table>'


def generate_charts_fallback_html(paths: List[str]) -> str:
    if not paths:
        return ""
    lis = "\n".join(
        f'<li style="margin:6px 0; font-size:13px; color:#374151;">{_escape_html(os.path.basename(p))}</li>'
        for p in paths
    )
    return f"""
<div style="background:#fff3cd; border:1px solid #ffeaa7; border-radius:8px; padding:12px; margin:16px 0;">
  <div style="color:#7c5200; font-weight:600; margin-bottom:6px;">Inline charts not supported by mailer; attached instead:</div>
  <ul style="margin:0; padding-left:18px;">{lis}</ul>
</div>
""".strip()


def generate_charts_text(charts_dict: Dict[str, Dict[str, str]]) -> str:
    if not charts_dict:
        return "• Charts could not be generated for this report"
    lines = []
    for _, cdata in charts_dict.items():
        title = cdata.get('title', 'Chart')
        desc = cdata.get('description', '')
        lines.append(f"• {title}: {desc}" if desc else f"• {title}")
    return "\n".join(lines)


def _escape_html(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
