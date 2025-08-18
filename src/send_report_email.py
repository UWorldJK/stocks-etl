# src/send_report_email.py
"""
Script to send the ETL pipeline report via email.
Called from GitHub Actions workflow.
"""
import os
import sys
from datetime import datetime
from email_handler import send_email

def main():
    """Main function to send the ETL report email."""
    
    # Get required environment variables
    sender_email = os.environ.get("SENDER_EMAIL")
    recipient_email = os.environ.get("RECIPIENT_EMAIL")
    attachment_path = os.environ.get("ATTACHMENT_PATH")
    
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
    
    # Verify attachment exists
    if not os.path.exists(attachment_path):
        print(f"Error: Attachment file not found: {attachment_path}")
        return 1
    
    # Get file info for email content
    file_size = os.path.getsize(attachment_path)
    file_size_mb = file_size / (1024 * 1024)
    
    # Create email content
    current_date = datetime.now().strftime("%Y-%m-%d")
    subject = f"ETL Pipeline Report - {current_date}"
    
    body_text = f"""
ETL Pipeline Report - {current_date}

Hello,

The ETL pipeline has completed successfully. Please find the daily metrics report attached.

Report Details:
- Date: {current_date}
- File: {os.path.basename(attachment_path)}
- Size: {file_size_mb:.2f} MB ({file_size:,} bytes)

This email was sent automatically from the GitHub Actions workflow.

Best regards,
ETL Pipeline Bot
    """.strip()
    
    body_html = f"""
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
        .content {{ margin: 20px 0; }}
        .details {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 15px 0; }}
        .footer {{ color: #7f8c8d; font-style: italic; margin-top: 30px; }}
    </style>
</head>
<body>
    <div class="header">
        <h2>ETL Pipeline Report - {current_date}</h2>
    </div>
    
    <div class="content">
        <p>Hello,</p>
        <p>The ETL pipeline has completed successfully. Please find the daily metrics report attached.</p>
        
        <div class="details">
            <h3>Report Details:</h3>
            <ul>
                <li><strong>Date:</strong> {current_date}</li>
                <li><strong>File:</strong> {os.path.basename(attachment_path)}</li>
                <li><strong>Size:</strong> {file_size_mb:.2f} MB ({file_size:,} bytes)</li>
            </ul>
        </div>
    </div>
    
    <div class="footer">
        <p>This email was sent automatically from the GitHub Actions workflow.</p>
        <p><strong>ETL Pipeline Bot</strong></p>
    </div>
</body>
</html>
    """.strip()
    
    try:
        print(f"Sending email from {sender_email} to {recipient_email}")
        print(f"Attachment: {attachment_path}")
        
        message_id = send_email(
            sender=sender_email,
            recipient=recipient_email,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachment_path=attachment_path
        )
        
        print(f"✅ Email sent successfully! Message ID: {message_id}")
        return 0
        
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)