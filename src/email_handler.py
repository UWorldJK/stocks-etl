# src/email_handler.py
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import boto3
import os
from botocore.exceptions import ClientError

def send_email(sender, recipient, subject, body_text, body_html=None, attachment_paths=None):
    """
    Send an email via AWS SES with optional HTML body and multiple attachments.
    
    Args:
        sender (str): Email address of sender (must be verified in SES)
        recipient (str): Email address of recipient
        subject (str): Email subject line
        body_text (str): Plain text body content
        body_html (str, optional): HTML body content
        attachment_paths (str or list, optional): Path(s) to file(s) to attach
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
    
    # Add attachments if provided
    if attachment_paths:
        # Convert single path to list for uniform handling
        if isinstance(attachment_paths, str):
            attachment_paths = [attachment_paths]
        
        attachment_count = 0
        for attachment_path in attachment_paths:
            if os.path.exists(attachment_path):
                try:
                    with open(attachment_path, "rb") as f:
                        att = MIMEApplication(f.read())
                    
                    # Determine content type based on file extension
                    filename = os.path.basename(attachment_path)
                    if filename.lower().endswith('.png'):
                        att.add_header("Content-Type", "image/png")
                    elif filename.lower().endswith('.jpg') or filename.lower().endswith('.jpeg'):
                        att.add_header("Content-Type", "image/jpeg")
                    elif filename.lower().endswith('.csv'):
                        att.add_header("Content-Type", "text/csv")
                    
                    att.add_header(
                        "Content-Disposition",
                        "attachment",
                        filename=filename
                    )
                    msg.attach(att)
                    attachment_count += 1
                    print(f"✅ Attachment added: {attachment_path} ({os.path.getsize(attachment_path)} bytes)")
                except Exception as e:
                    print(f"❌ Failed to attach file {attachment_path}: {e}")
                    raise
            else:
                print(f"⚠️  Warning: Attachment path {attachment_path} does not exist")
        
        print(f"📎 Total attachments: {attachment_count}")
    
    # Send the email
    try:
        response = client.send_raw_email(
            Source=sender,
            Destinations=[recipient],
            RawMessage={"Data": msg.as_bytes()}  # Use as_bytes() instead of as_string()
        )
        print(f"📧 Email sent successfully! Message ID: {response['MessageId']}")
        return response["MessageId"]
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        error_message = e.response["Error"]["Message"]
        print(f"❌ SES ClientError [{error_code}]: {error_message}")
        
        # Provide helpful error messages for common issues
        if error_code == "MessageRejected":
            if "Email address not verified" in error_message:
                print("💡 Tip: Make sure both sender and recipient emails are verified in AWS SES")
                print("💡 If you're in SES sandbox mode, both emails must be verified")
        elif error_code == "SendingQuotaExceeded":
            print("💡 Tip: You've exceeded your SES sending quota. Check your SES console")
        elif error_code == "InvalidParameterValue":
            print("💡 Tip: Check your email addresses and message content")
        
        raise
    except Exception as e:
        print(f"❌ Unexpected error sending email: {e}")
        raise


# Legacy function for backward compatibility
def send_email_single(sender, recipient, subject, body_text, body_html=None, attachment_path=None):
    """
    Legacy function for backward compatibility.
    Use send_email() instead - it supports both single and multiple attachments.
    """
    return send_email(sender, recipient, subject, body_text, body_html, attachment_path)


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
    subject = "📊 Test ETL Pipeline Report"
    body_text = """
Hello,

This is a test email from the ETL pipeline email handler.

The handler now supports:
- Multiple attachments (CSV + charts)
- Better error handling
- Content-type detection
- Improved logging

Best regards,
ETL Pipeline Bot
    """.strip()
    
    body_html = """
<html>
<head></head>
<body>
    <h2>📊 Test ETL Pipeline Report</h2>
    <p>Hello,</p>
    <p>This is a test email from the ETL pipeline email handler.</p>
    
    <h3>New Features:</h3>
    <ul>
        <li>✅ Multiple attachments (CSV + charts)</li>
        <li>✅ Better error handling</li>
        <li>✅ Content-type detection</li>
        <li>✅ Improved logging</li>
    </ul>
    
    <p><em>This email was sent automatically for testing purposes.</em></p>
    <p><strong>ETL Pipeline Bot</strong></p>
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
            attachment_paths=attachment_path  # Can be single path or list
        )
        print("✅ Test email sent successfully!")
    except Exception as e:
        print(f"❌ Failed to send test email: {e}")
        sys.exit(1)