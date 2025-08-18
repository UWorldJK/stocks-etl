# scripts/send_report_email.py
import os
from src.email_handler import send_email

attachment = os.environ["ATTACHMENT_PATH"]
sender = os.environ["SENDER_EMAIL"]
recipient = os.environ.get("RECIPIENT_EMAIL", sender)

sha = os.environ.get("GITHUB_SHA", "")[:7]
branch = os.environ.get("GITHUB_REF_NAME", "")
repo = os.environ.get("GITHUB_REPOSITORY", "")
run_id = os.environ.get("GITHUB_RUN_ID", "")
run_url = f"https://github.com/{repo}/actions/runs/{run_id}"

subject = f"Daily Stock Report • {branch}@{sha}"
body_text = f"""Hello,

Attached is the latest CSV from the ETL.

Workflow run: {run_url}
"""
body_html = f"""<html><body>
  <h2>Daily Stock Report</h2>
  <p>Attached is the latest CSV from the ETL.</p>
  <p><a href="{run_url}">View workflow run</a></p>
</body></html>"""

send_email(sender, recipient, subject, body_text, body_html, attachment)
