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
            if file.endswith('.png'):
                chart_files.append(os.path.join(chart_dir, file))
        chart_files.sort()
        print(f"Found {len(chart_files)} chart files")
    else:
        print(f"Charts directory not found: {chart_dir}")
    
    # Prepare all attachments
    all_attachments = [attachment_path] + chart_files
    
    # Get file info for email content
    csv_size = os.path.getsize(attachment_path)
    csv_size_mb = csv_size / (1024 * 1024)
    
    total_size = sum(os.path.getsize(f) for f in all_attachments if os.path.exists(f))
    total_size_mb = total_size / (1024 * 1024)
    
    # Create email content
    current_date = datetime.now().strftime("%Y-%m-%d")
    subject = f"📊 Financial ETL Report - {current_date}"
    
    # Generate chart list for email
    chart_list = ""
    if chart_files:
        chart_list = "\n".join([f"  📈 {os.path.basename(f)}" for f in chart_files])
    else:
        chart_list = "  ⚠️  No charts generated"
    
    body_text = f"""
Financial ETL Pipeline Report - {current_date}

Hello,

Your financial data pipeline has completed successfully! 

📋 Report Summary:
- Date: {current_date}
- CSV Data: {os.path.basename(attachment_path)} ({csv_size_mb:.2f} MB)
- Visualization Charts: {len(chart_files)} files

📈 Charts Included:
{chart_list}

📦 Total Package: {total_size_mb:.2f} MB across {len(all_attachments)} files

The CSV contains daily metrics for your tracked tickers including moving averages, RSI, volatility, and returns data. The charts provide visual analysis of price trends, technical indicators, and market volatility.

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
        .charts-box {{ 
            background-color: #f3e5f5; 
            padding: 20px; 
            border-radius: 8px; 
            margin: 20px 0;
            border-left: 4px solid #9c27b0;
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
        .chart-list {{ 
            list-style: none; 
            padding: 0; 
        }}
        .chart-list li {{ 
            padding: 8px 0; 
            border-bottom: 1px solid #eee;
            font-size: 14px;
        }}
        .chart-list li:last-child {{ border-bottom: none; }}
        .footer {{ 
            background-color: #f5f5f5; 
            color: #666; 
            text-align: center; 
            padding: 20px;
            border-radius: 0 0 10px 10px;
            font-size: 14px;
        }}
        .emoji {{ font-size: 18px; }}
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
                        <span class="stat-label">Charts</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-number">{total_size_mb:.1f}</span>
                        <span class="stat-label">MB Total</span>
                    </div>
                </div>
                
                <p><strong>📄 Data File:</strong> {os.path.basename(attachment_path)}</p>
                <p><strong>📈 Analysis:</strong> Moving averages, RSI, volatility, and returns data</p>
            </div>
            
            {"" if not chart_files else f'''
            <div class="charts-box">
                <h3 style="margin-top: 0; color: #7b1fa2;">📈 Visualization Charts</h3>
                <ul class="chart-list">
                    {"".join([f"<li>📊 {os.path.basename(f)}</li>" for f in chart_files])}
                </ul>
            </div>
            '''}
            
            <p>The attached files contain comprehensive analysis of your tracked financial instruments with technical indicators and market trends.</p>
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
        print(f"Chart attachments: {len(chart_files)}")
        for chart in chart_files:
            print(f"  - {chart}")
        
        message_id = send_email(
            sender=sender_email,
            recipient=recipient_email,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachment_paths=all_attachments  # Updated to use multiple attachments
        )
        
        print(f"✅ Email sent successfully! Message ID: {message_id}")
        print(f"📊 Sent {len(all_attachments)} total attachments ({total_size_mb:.2f} MB)")
        return 0
        
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)