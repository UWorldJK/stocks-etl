# src/send_report_email.py
"""
Script to send the ETL pipeline report via email.
Called from GitHub Actions workflow.
"""

import os
import sys
import base64
from datetime import datetime
from email_handler import send_email


def embed_image_base64(file_path):
    """Read an image file and return a base64-encoded data URI."""
    with open(file_path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"


def main():
    """Main function to send the ETL report email."""

    # Get required environment variables
    sender_email = os.environ.get("SENDER_EMAIL")
    recipient_email = os.environ.get("RECIPIENT_EMAIL")
    attachment_path = os.environ.get("ATTACHMENT_PATH")  # CSV file

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

    # Find chart files
    chart_dir = "artifacts/charts"
    chart_files = []
    if os.path.exists(chart_dir):
        for file in os.listdir(chart_dir):
            if file.endswith(".png"):
                chart_files.append(os.path.join(chart_dir, file))
        chart_files.sort()
        print(f"Found {len(chart_files)} chart files")
    else:
        print(f"Charts directory not found: {chart_dir}")

    # Only attach the CSV — charts will be inline
    all_attachments = [attachment_path]

    # Get file info for email content
    csv_size = os.path.getsize(attachment_path)
    csv_size_mb = csv_size / (1024 * 1024)

    total_size = sum(os.path.getsize(f) for f in all_attachments if os.path.exists(f))
    total_size_mb = total_size / (1024 * 1024)

    # Create email content
    current_date = datetime.now().strftime("%Y-%m-%d")
    subject = f"📊 Financial ETL Report - {current_date}"

    # Generate chart HTML inline
    charts_html = ""
    if chart_files:
        charts_html = """
        <div class="charts-box">
            <h3 style="margin-top: 0; color: #7b1fa2;">📈 Visualization Charts</h3>
        """
        for chart in chart_files:
            img_b64 = embed_image_base64(chart)
            charts_html += f"""
            <div style="margin-bottom:20px;">
                <p>📊 {os.path.basename(chart)}</p>
                <img src="{img_b64}" alt="{os.path.basename(chart)}" 
                     style="max-width:100%; height:auto; border:1px solid #ccc; border-radius:6px;" />
            </div>
            """
        charts_html += "</div>"
    else:
        charts_html = "<p>⚠️ No charts generated</p>"

    body_text = f"""
Financial ETL Pipeline Report - {current_date}

Hello,

Your financial data pipeline has completed successfully! 

📋 Report Summary:
- Date: {current_date}
- CSV Data: {os.path.basename(attachment_path)} ({csv_size_mb:.2f} MB)
- Visualization Charts: {len(chart_files)} inline

📦 Total Package: {total_size_mb:.2f} MB across {len(all_attachments)} file(s)

The CSV contains daily metrics for your tracked tickers including moving averages, RSI, volatility, and returns data.
Charts are embedded inline in this email.

This report was generated automatically by your ETL pipeline.

Best regards,
📈 Financial ETL Bot
    """.strip()

    body_html = f"""
<html>
<head>
    <style>
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; 
            margin: 0; 
            padding: 20px;
            background-color: #f8f9fa;
        }}
        .container {{
            max-width: 600px;
            margin: 0 auto;
            background-color: white;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }}
        .header {{ 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; 
            padding: 25px;
            border-radius: 10px 10px 0 0;
            text-align: center;
        }}
        .header h2 {{ margin: 0; font-size: 24px; }}
        .content {{ padding: 25px; }}
        .summary-box {{ 
            background-color: #e3f2fd; 
            padding: 20px; 
            border-radius: 8px; 
            margin: 20px 0;
            border-left: 4px solid #2196f3;
        }}
        .stats {{ 
            display: flex; 
            justify-content: space-between; 
            margin: 15px 0;
            flex-wrap: wrap;
        }}
        .stat-item {{ 
            text-align: center; 
            flex: 1; 
            min-width: 120px;
            margin: 5px;
        }}
        .stat-number {{ 
            font-size: 24px; 
            font-weight: bold; 
            color: #2196f3; 
            display: block;
        }}
        .stat-label {{ 
            font-size: 12px; 
            color: #666; 
            text-transform: uppercase;
        }}
        .footer {{ 
            background-color: #f5f5f5; 
            color: #666; 
            text-align: center; 
            padding: 20px;
            border-radius: 0 0 10px 10px;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>📊 Financial ETL Report</h2>
            <p style="margin: 5px 0 0 0; opacity: 0.9;">{current_date}</p>
        </div>
        
        <div class="content">
            <p>Hello! 👋</p>
            <p>Your financial data pipeline has completed successfully and your latest market analysis is ready!</p>
            
            <div class="summary-box">
                <h3 style="margin-top: 0; color: #1976d2;">📋 Report Summary</h3>
                <div class="stats">
                    <div class="stat-item">
                        <span class="stat-number">{csv_size_mb:.1f}</span>
                        <span class="stat-label">MB CSV Data</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-number">{len(chart_files)}</span>
                        <span class="stat-label">Charts Inline</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-number">{total_size_mb:.1f}</span>
                        <span class="stat-label">MB Total</span>
                    </div>
                </div>
                
                <p><strong>📄 Data File:</strong> {os.path.basename(attachment_path)}</p>
                <p><strong>📈 Analysis:</strong> Moving averages, RSI, volatility, and returns data</p>
            </div>
            
            {charts_html}
            
            <p>The attached CSV contains comprehensive analysis of your tracked financial instruments. Charts are embedded inline above.</p>
        </div>
        
        <div class="footer">
            <p><em>This report was generated automatically by your Financial ETL Pipeline</em></p>
            <p><strong>📈 Financial ETL Bot</strong></p>
        </div>
    </div>
</body>
</html>
    """.strip()

    try:
        print(f"Sending email from {sender_email} to {recipient_email}")
        print(f"CSV attachment: {attachment_path}")
        print(f"Chart inline count: {len(chart_files)}")

        message_id = send_email(
            sender=sender_email,
            recipient=recipient_email,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachment_paths=all_attachments,  # only CSV
        )

        print(f"✅ Email sent successfully! Message ID: {message_id}")
        print(f"📊 Sent {len(all_attachments)} attachment(s) ({total_size_mb:.2f} MB)")
        return 0

    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
