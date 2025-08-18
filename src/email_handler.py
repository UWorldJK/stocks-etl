# src/email_handler.py
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import boto3
import os
from botocore.exceptions import ClientError

def send_email(sender, recipient, subject, body_text, body_html, attachment_path):
    CHARSET = "utf-8"
    AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-west-2")
    client = boto3.client("ses", region_name=AWS_REGION)

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    # Body (plain + html)
    msg_body = MIMEMultipart("alternative")
    msg_body.attach(MIMEText(body_text, "plain", CHARSET))
    msg_body.attach(MIMEText(body_html, "html", CHARSET))
    msg.attach(msg_body)

    # Attachment
    with open(attachment_path, "rb") as f:
        att = MIMEApplication(f.read())
    att.add_header("Content-Disposition", "attachment", filename=os.path.basename(attachment_path))
    msg.attach(att)

    try:
        resp = client.send_raw_email(
            Source=sender,
            Destinations=[recipient],
            RawMessage={"Data": msg.as_string()},
        )
        print("Email sent! Message ID:", resp["MessageId"])
    except ClientError as e:
        print("SES error:", e.response["Error"]["Message"])
        raise
