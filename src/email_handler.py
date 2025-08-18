# src/email_handler.py
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import boto3
import os
from botocore.exceptions import ClientError

def send_email(sender, recipient, subject, body_text, body_html=None, attachment_path=None):
    """
    Send an email via AWS SES with optional HTML body and attachment.
    
    Args:
        sender (str): Email address of sender (must be verified in SES)
        recipient (str): Email address of recipient
        subject (str): Email subject line
        body_text (str): Plain text body content
        body_html (str, optional): HTML body content
        attachment_path (str, optional): Path to file to attach
    """
    CHARSET = "utf-8"
    AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")
    
    # Initialize SES client
    try:
        client = boto3.client("ses", region_name=AWS_REGION)
    except Exception as e:
        print(f"Failed to create SES client: {e}")
        raise
    
    # Create message container
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    
    # Create body container
    if body_html:
        # Both plain text and HTML
        msg_body = MIMEMultipart("alternative")
        msg_body.attach(MIMEText(body_text, "plain", CHARSET))
        msg_body.attach(MIMEText(body_html, "html", CHARSET))
        msg.attach(msg_body)
    else:
        # Plain text only
        msg.attach(MIMEText(body_text, "plain", CHARSET))
    
    # Add attachment if provided
    if attachment_path and os.path.exists(attachment_path):
        try:
            with open(attachment_path, "rb") as f:
                att = MIMEApplication(f.read())
            att.add_header(
                "Content-Disposition",
                "attachment",
                filename=os.path.basename(attachment_path)
            )
            msg.attach(att)
            print(f"Attachment added: {attachment_path}")
        except Exception as e:
            print(f"Failed to attach file {attachment_path}: {e}")
            raise
    elif attachment_path:
        print(f"Warning: Attachment path {attachment_path} does not exist")
    
    # Send the email
    try:
        response = client.send_raw_email(
            Source=sender,
            Destinations=[recipient],
            RawMessage={"Data": msg.as_bytes()}  # Changed from as_string() to as_bytes()
        )
        print(f"Email sent successfully! Message ID: {response['MessageId']}")
        return response["MessageId"]
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        error_message = e.response["Error"]["Message"]
        print(f"SES ClientError [{error_code}]: {error_message}")
        raise
    except Exception as e:
        print(f"Unexpected error sending email: {e}")
        raise


if __name__ == "__main__":
    # Example usage / testing
    import sys
    
    # Get environment variables
    sender_email = os.environ.get("SENDER_EMAIL")
    recipient_email = os.environ.get("RECIPIENT_EMAIL")
    attachment_path = os.environ.get("ATTACHMENT_PATH")
    
    if not sender_email or not recipient_email:
        print("Error: SENDER_EMAIL and RECIPIENT_EMAIL environment variables are required")
        sys.exit(1)
    
    # Email content
    subject = "ETL Pipeline Report"
    body_text = """
Hello,

Please find attached the daily metrics report from the ETL pipeline.

This email was sent automatically from the GitHub Actions workflow.

Best regards,
ETL Pipeline Bot
    """.strip()
    
    body_html = """
<html>
<head></head>
<body>
    <h2>ETL Pipeline Report</h2>
    <p>Hello,</p>
    <p>Please find attached the daily metrics report from the ETL pipeline.</p>
    <p><em>This email was sent automatically from the GitHub Actions workflow.</em></p>
    <p>Best regards,<br>
    <strong>ETL Pipeline Bot</strong></p>
</body>
</html>
    """.strip()
    
    try:
        send_email(
            sender=sender_email,
            recipient=recipient_email,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachment_path=attachment_path
        )
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")
        sys.exit(1)