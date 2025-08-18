# src/send_report_email.py
"""
Script to send the ETL pipeline report via email with embedded charts.
Called from GitHub Actions workflow.
"""
import os
import sys
from datetime import datetime
from email_handler import send_email
from chart_generator import generate_email_charts

def main():
    """Main function to send the ETL report email with embedded charts."""
    
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
    
    # Generate embedded charts
    try:
        print("🎨 Generating charts for email embedding...")
        embedded_charts = generate_email_charts(attachment_path)
        print(f"✅ Generated {len(embedded_charts)} charts for embedding")
    except Exception as e:
        print(f"⚠️  Warning: Could not generate charts: {e}")
        embedded_charts = {}
    
    # Get file info for email content
    csv_size = os.path.getsize(attachment_path)
    csv_size_mb = csv_size / (1024 * 1024)
    
    # Create email content
    current_date = datetime.now().strftime("%Y-%m-%d")
    subject = f"📊 Financial ETL Report - {current_date}"
    
    # Generate chart sections for email
    charts_html = ""
    charts_text = ""
    
    if embedded_charts:
        charts_html = generate_charts_html(embedded_charts)
        charts_text = generate_charts_text(embedded_charts)
    else:
        charts_html = '<p style="color: #ff9800;">⚠️ Charts could not be generated for this report.</p>'
        charts_text = "⚠️ Charts could not be generated for this report."
    
    # Text version of email
    body_text = f"""
Financial ETL Pipeline Report - {current_date}

Hello,

Your financial data pipeline has completed successfully! 

📋 Report Summary:
- Date: {current_date}
- CSV Data: {os.path.basename(attachment_path)} ({csv_size_mb:.2f} MB)
- Embedded Charts: {len(embedded_charts)} visualizations

📈 Analysis Included:
{charts_text}

The attached CSV contains comprehensive daily metrics for your tracked tickers including moving averages, RSI, volatility, and returns data. The embedded charts above provide visual analysis of price trends, technical indicators, and market performance.

Key Metrics Explained:
• Moving Averages (7d/30d): Smoothed price trends over time periods
• RSI: Relative Strength Index - measures momentum (0-100 scale)
  - Above 70: Potentially overbought
  - Below 30: Potentially oversold
• Volatility: Standard deviation of returns (measure of price stability)
• Daily Returns: Percentage change in price day-over-day

This report was generated automatically by your ETL pipeline.

Best regards,
📈 Financial ETL Bot
    """.strip()
    
    # HTML version with embedded charts
    body_html = f"""
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; 
            margin: 0; 
            padding: 20px;
            background-color: #f8f9fa;
            line-height: 1.6;
        }}
        .container {{
            max-width: 800px;
            margin: 0 auto;
            background-color: white;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }}
        .header {{ 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; 
            padding: 25px;
            text-align: center;
        }}
        .header h2 {{ margin: 0; font-size: 24px; }}
        .header p {{ margin: 5px 0 0 0; opacity: 0.9; }}
        .content {{ padding: 25px; }}
        .summary-box {{ 
            background-color: #e3f2fd; 
            padding: 20px; 
            border-radius: 8px; 
            margin: 20px 0;
            border-left: 4px solid #2196f3;
        }}
        .chart-section {{
            margin: 30px 0;
            padding: 20px;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            background-color: #fafafa;
        }}
        .chart-title {{
            font-size: 18px;
            font-weight: bold;
            color: #2c3e50;
            margin: 0 0 10px 0;
        }}
        .chart-description {{
            font-size: 14px;
            color: #666;
            margin-bottom: 15px;
            line-height: 1.5;
        }}
        .chart-image {{
            width: 100%;
            max-width: 100%;
            height: auto;
            border-radius: 4px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
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
        .metrics-explanation {{
            background-color: #f0f4f8;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            border-left: 4px solid #3498db;
        }}
        .metrics-explanation h3 {{
            color: #2c3e50;
            margin-top: 0;
        }}
        .metrics-explanation ul {{
            padding-left: 20px;
        }}
        .metrics-explanation li {{
            margin: 8px 0;
        }}
        .footer {{ 
            background-color: #f5f5f5; 
            color: #666; 
            text-align: center; 
            padding: 20px;
            font-size: 14px;
        }}
        .emoji {{ font-size: 18px; }}
        
        /* Responsive design */
        @media (max-width: 600px) {{
            .container {{
                margin: 10px;
                border-radius: 5px;
            }}
            .content {{
                padding: 15px;
            }}
            .stats {{
                flex-direction: column;
            }}
            .stat-item {{
                min-width: auto;
                margin: 10px 0;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>📊 Financial ETL Report</h2>
            <p>{current_date}</p>
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
                        <span class="stat-number">{len(embedded_charts)}</span>
                        <span class="stat-label">Embedded Charts</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-number">1</span>
                        <span class="stat-label">CSV Attachment</span>
                    </div>
                </div>
                
                <p><strong>📄 Data File:</strong> {os.path.basename(attachment_path)}</p>
                <p><strong>📈 Analysis:</strong> Moving averages, RSI, volatility, and returns data</p>
            </div>
            
            {charts_html}
            
            <div class="metrics-explanation">
                <h3>📚 Key Metrics Explained</h3>
                <ul>
                    <li><strong>Moving Averages (7d/30d):</strong> Smoothed price trends over different time periods. Short-term vs long-term crossovers may signal trend changes.</li>
                    <li><strong>RSI (Relative Strength Index):</strong> Momentum indicator on a 0-100 scale. Values above 70 suggest overbought conditions, below 30 suggest oversold.</li>
                    <li><strong>Volatility:</strong> Standard deviation of returns, measuring price stability. Higher values indicate more price fluctuation.</li>
                    <li><strong>Daily Returns:</strong> Percentage change in price from day to day, showing short-term performance.</li>
                </ul>
            </div>
            
            <p>The attached CSV file contains comprehensive data for detailed analysis. Charts above provide visual insights into current market conditions and trends.</p>
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
        print(f"📧 Sending email from {sender_email} to {recipient_email}")
        print(f"📄 CSV attachment: {attachment_path} ({csv_size_mb:.2f} MB)")
        print(f"📊 Embedded charts: {len(embedded_charts)}")
        
        message_id = send_email(
            sender=sender_email,
            recipient=recipient_email,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachment_paths=[attachment_path]  # Only CSV attachment now
        )
        
        print(f"✅ Email sent successfully! Message ID: {message_id}")
        print(f"📊 Sent email with {len(embedded_charts)} embedded charts and 1 CSV attachment")
        return 0
        
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        import traceback
        traceback.print_exc()
        return 1


def generate_charts_html(charts_dict: dict) -> str:
    """Generate HTML for embedded charts."""
    html_sections = []
    
    for chart_key, chart_data in charts_dict.items():
        section_html = f'''
        <div class="chart-section">
            <h3 class="chart-title">{chart_data['title']}</h3>
            <p class="chart-description">{chart_data['description']}</p>
            <img src="{chart_data['image']}" alt="{chart_data['title']}" class="chart-image">
        </div>
        '''
        html_sections.append(section_html)
    
    return '\n'.join(html_sections)


def generate_charts_text(charts_dict: dict) -> str:
    """Generate text description of charts for plain text email."""
    text_sections = []
    
    for i, (chart_key, chart_data) in enumerate(charts_dict.items(), 1):
        text_sections.append(f"{i}. {chart_data['title']}: {chart_data['description']}")
    
    return '\n'.join(text_sections)


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)