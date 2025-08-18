# scripts/send_report_email.py
import os
from email_handler import send_email
from datetime import datetime, timedelta, timezone

attachment = "data/daily_metrics.csv"
sender = "jacobkurry1@gmail.com"
recipient = "jacobkurry1@gmail.com"
subject = f"Daily Stock Report for, {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
body_text = f"""Hello,
sender: 
Attached is the latest CSV from the ETL.

Workflow run: 
"""
body_html = f"""<html><body>
  <h2>Daily Stock Report</h2>
  <p>Attached is the latest CSV from the ETL.</p>
</body></html>"""

send_email(sender, recipient, subject, body_text, body_html, attachment)
