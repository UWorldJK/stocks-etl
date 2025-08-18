# src/email_handler.py
import os
import mimetypes
import boto3
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders


def send_email(
    sender,
    recipient,
    subject,
    body_text,
    body_html=None,
    attachment_paths=None,
    inline_images=None,  # [{"cid": "chart1@etl", "path": "/abs/path.jpg"}, ...]
):
    """
    Send an email via AWS SES with optional HTML, file attachments,
    and inline CID images (for Gmail/Outlook/Apple Mail).
    """
    CHARSET = "utf-8"
    AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")
    client = boto3.client("ses", region_name=AWS_REGION)

    # OUTER: mixed (attachments live here)
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    has_html_or_inline = bool(body_html or inline_images)

    if has_html_or_inline:
        # alternative (text/plain + related)
        alt = MIMEMultipart("alternative")
        msg.attach(alt)

        # plain text body
        alt.attach(MIMEText(body_text or "", "plain", CHARSET))

        # related (html + inline images)
        related = MIMEMultipart("related")
        alt.attach(related)

        # html body
        related.attach(MIMEText(body_html or "", "html", CHARSET))

        # inline images with matching Content-ID
        for item in (inline_images or []):
            cid = item["cid"]                    # e.g. "chart1@etl"
            path = item["path"]

            with open(path, "rb") as f:
                data = f.read()

            # choose subtype
            subtype = "jpeg" if path.lower().endswith((".jpg", ".jpeg")) else None
            if subtype is None:
                ctype, _ = mimetypes.guess_type(path)
                subtype = (ctype.split("/", 1)[1] if ctype and ctype.startswith("image/") else "octet-stream")

            img = MIMEImage(data, _subtype=subtype, name=os.path.basename(path))
            img.add_header("Content-ID", f"<{cid}>")                       # MUST be bracketed
            img.add_header("Content-Disposition", "inline", filename=os.path.basename(path))
            img.add_header("X-Attachment-Id", cid)                         # Gmail hint
            related.attach(img)
    else:
        # text-only mail
        msg.attach(MIMEText(body_text or "", "plain", CHARSET))

    # Regular attachments (CSV, etc.)
    paths = attachment_paths if isinstance(attachment_paths, list) else ([attachment_paths] if attachment_paths else [])
    for p in paths:
        if not p or not os.path.exists(p):
            print(f"⚠️ Attachment not found: {p}")
            continue
        ctype, enc = mimetypes.guess_type(p)
        if ctype is None or enc is not None:
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        with open(p, "rb") as f:
            part = MIMEBase(maintype, subtype)
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename=os.path.basename(p))
        msg.attach(part)
        print(f"✅ Attachment added: {p} ({os.path.getsize(p)} bytes)")

    # Send as RAW bytes
    try:
        resp = client.send_raw_email(
            Source=sender,
            Destinations=[recipient],
            RawMessage={"Data": msg.as_bytes()},
        )
        print(f"📧 Email sent successfully! Message ID: {resp['MessageId']}")
        return resp["MessageId"]
    except ClientError as e:
        code = e.response["Error"]["Code"]
        msg_err = e.response["Error"]["Message"]
        print(f"❌ SES ClientError [{code}]: {msg_err}")
        if code == "MessageRejected" and "Email address not verified" in msg_err:
            print("💡 Verify sender/recipient in SES or move account out of sandbox.")
        raise


# Legacy wrapper
def send_email_single(sender, recipient, subject, body_text, body_html=None, attachment_path=None):
    return send_email(sender, recipient, subject, body_text, body_html, attachment_path)
