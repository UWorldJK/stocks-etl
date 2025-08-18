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
import pandas as pd

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
    
    # Get basic CSV info for summary
    try:
        df = pd.read_csv(attachment_path)
        num_tickers = len(df['ticker'].unique()) if 'ticker' in df.columns else 0
        num_records = len(df)
        date_range = ""
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            start_date = df['date'].min().strftime('%Y-%m-%d')
            end_date = df['date'].max().strftime('%Y-%m-%d')
            date_range = f"{start_date} to {end_date}"
    except Exception as e:
        print(f"Warning: Could not read CSV for summary: {e}")
        num_tickers = 0
        num_records = 0
        date_range = "Unknown"
    
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
    current_time = datetime.now().strftime("%H:%M UTC")
    subject = f"📊 Financial ETL Report - {current_date}"
    
    # Generate chart sections for email
    charts_html = ""
    charts_text = ""
    
    if embedded_charts:
        charts_html = generate_charts_html(embedded_charts)
        charts_text = generate_charts_text(embedded_charts)
    else:
        charts_html = '''
        <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 8px; padding: 20px; margin: 20px 0;">
            <h3 style="color: #856404; margin-top: 0;">⚠️ Charts Not Available</h3>
            <p style="color: #856404; margin-bottom: 0;">Charts could not be generated for this report. Please check the data format and try again.</p>
        </div>
        '''
        charts_text = "⚠️ Charts could not be generated for this report."
    
    # Text version of email
    body_text = f"""
Financial ETL Pipeline Report - {current_date}

Hello,

Your financial data pipeline has completed successfully! 

📋 REPORT SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Date: {current_date} at {current_time}
• Tickers Analyzed: {num_tickers} assets
• Total Records: {num_records:,} data points
• Date Range: {date_range}
• CSV File: {os.path.basename(attachment_path)} ({csv_size_mb:.2f} MB)
• Embedded Charts: {len(embedded_charts)} visualizations

📈 ANALYSIS OVERVIEW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{charts_text}

📚 KEY METRICS EXPLAINED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Moving Averages (7d/30d): Smoothed price trends over time periods
  - Golden Cross: 7d MA crosses above 30d MA (bullish signal)
  - Death Cross: 7d MA crosses below 30d MA (bearish signal)

• RSI (Relative Strength Index): Momentum indicator (0-100 scale)
  - Above 70: Potentially overbought (consider selling)
  - Below 30: Potentially oversold (consider buying)
  - Around 50: Neutral momentum

• Volatility: Standard deviation of returns (price stability measure)
  - Higher values: More price fluctuation and risk
  - Lower values: More stable price movement

• Daily Returns: Day-over-day percentage price changes
  - Positive: Price increased
  - Negative: Price decreased

The attached CSV contains comprehensive data for detailed analysis. 
Charts in the email provide visual insights into current market conditions.

This report was generated automatically by your Financial ETL Pipeline.

Best regards,
📈 Financial ETL Bot

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Generated on {current_date} at {current_time}
    """.strip()
    
    # Professional HTML version with embedded charts
    body_html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Financial ETL Report - {current_date}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; 
            line-height: 1.6;
            color: #2c3e50;
            background-color: #f8fafc;
            margin: 0;
            padding: 0;
        }}
        
        .email-container {{
            max-width: 800px;
            margin: 0 auto;
            background-color: #ffffff;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
        }}
        
        .header {{ 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; 
            padding: 40px 30px;
            text-align: center;
            position: relative;
        }}
        
        .header::after {{
            content: '';
            position: absolute;
            bottom: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, #f093fb 0%, #f5576c 50%, #4facfe 100%);
        }}
        
        .header h1 {{ 
            font-size: 32px; 
            font-weight: 700;
            margin-bottom: 10px;
            text-shadow: 0 2px 4px rgba(0,0,0,0.3);
        }}
        
        .header .subtitle {{ 
            font-size: 18px; 
            opacity: 0.9;
            font-weight: 300;
        }}
        
        .content {{ 
            padding: 40px 30px; 
        }}
        
        .greeting {{
            font-size: 18px;
            color: #2c3e50;
            margin-bottom: 30px;
            line-height: 1.7;
        }}
        
        .summary-card {{ 
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            border-radius: 12px;
            padding: 30px;
            margin: 30px 0;
            border: 1px solid #e2e8f0;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
        }}
        
        .summary-card h2 {{
            color: #1a365d;
            font-size: 24px;
            margin-bottom: 20px;
            font-weight: 600;
            display: flex;
            align-items: center;
        }}
        
        .summary-card h2::before {{
            content: '📋';
            margin-right: 10px;
            font-size: 28px;
        }}
        
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        
        .metric-item {{
            text-align: center;
            background: white;
            padding: 20px 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border: 2px solid transparent;
            transition: all 0.3s ease;
        }}
        
        .metric-item:hover {{
            border-color: #667eea;
            transform: translateY(-2px);
        }}
        
        .metric-number {{ 
            font-size: 28px; 
            font-weight: 700; 
            color: #667eea;
            display: block;
            margin-bottom: 5px;
        }}
        
        .metric-label {{ 
            font-size: 12px; 
            color: #64748b; 
            text-transform: uppercase;
            font-weight: 600;
            letter-spacing: 0.5px;
        }}
        
        .chart-section {{
            margin: 40px 0;
            background: white;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
            border: 1px solid #e2e8f0;
        }}
        
        .chart-header {{
            background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
            padding: 25px 30px;
            border-bottom: 1px solid #e2e8f0;
        }}
        
        .chart-title {{
            font-size: 22px;
            font-weight: 600;
            color: #1a365d;
            margin: 0 0 10px 0;
            display: flex;
            align-items: center;
        }}
        
        .chart-title::before {{
            content: '📊';
            margin-right: 12px;
            font-size: 24px;
        }}
        
        .chart-description {{
            font-size: 15px;
            color: #64748b;
            line-height: 1.6;
            margin: 0;
        }}
        
        .chart-content {{
            padding: 30px;
            text-align: center;
        }}
        
        .chart-image {{
            max-width: 100%;
            height: auto;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
            border: 1px solid #e2e8f0;
        }}
        
        .info-section {{
            background: linear-gradient(135deg, #e0f2fe 0%, #b3e5fc 100%);
            border-radius: 12px;
            padding: 30px;
            margin: 30px 0;
            border-left: 5px solid #0288d1;
        }}
        
        .info-section h3 {{
            color: #01579b;
            font-size: 20px;
            margin-bottom: 20px;
            font-weight: 600;
            display: flex;
            align-items: center;
        }}
        
        .info-section h3::before {{
            content: '📚';
            margin-right: 10px;
            font-size: 24px;
        }}
        
        .metrics-list {{
            list-style: none;
            padding: 0;
        }}
        
        .metrics-list li {{
            margin: 15px 0;
            padding: 15px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }}
        
        .metrics-list strong {{
            color: #01579b;
            font-weight: 600;
        }}
        
        .footer {{ 
            background: linear-gradient(135deg, #263238 0%, #37474f 100%);
            color: #eceff1;
            text-align: center; 
            padding: 30px;
            font-size: 14px;
        }}
        
        .footer p {{
            margin: 5px 0;
        }}
        
        .footer .signature {{
            font-weight: 600;
            font-size: 16px;
            margin-top: 15px;
        }}
        
        .data-details {{
            background: #f8fafc;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            border: 1px solid #e2e8f0;
        }}
        
        .data-details strong {{
            color: #1a365d;
        }}
        
        /* Responsive Design */
        @media (max-width: 600px) {{
            .email-container {{
                margin: 0;
                box-shadow: none;
            }}
            
            .header, .content {{
                padding: 20px;
            }}
            
            .header h1 {{
                font-size: 24px;
            }}
            
            .summary-card, .chart-content, .info-section {{
                padding: 20px;
            }}
            
            .metrics-grid {{
                grid-template-columns: repeat(2, 1fr);
                gap: 15px;
            }}
            
            .metric-number {{
                font-size: 20px;
            }}
        }}
        
        /* Dark mode support */
        @media (prefers-color-scheme: dark) {{
            .chart-image {{
                filter: brightness(0.9);
            }}
        }}
    </style>
</head>
<body>
    <div class="email-container">
        <div class="header">
            <h1>📊 Financial ETL Report</h1>
            <div class="subtitle">{current_date} • {current_time}</div>
        </div>
        
        <div class="content">
            <div class="greeting">
                <strong>Hello! 👋</strong><br>
                Your financial data pipeline has completed successfully and your comprehensive market analysis is ready for review.
            </div>
            
            <div class="summary-card">
                <h2>Report Summary</h2>
                
                <div class="metrics-grid">
                    <div class="metric-item">
                        <span class="metric-number">{num_tickers}</span>
                        <span class="metric-label">Assets Tracked</span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-number">{num_records:,}</span>
                        <span class="metric-label">Data Points</span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-number">{csv_size_mb:.1f}</span>
                        <span class="metric-label">MB CSV Data</span>
                    </div>
                    <div class="metric-item">
                        <span class="metric-number">{len(embedded_charts)}</span>
                        <span class="metric-label">Visual Charts</span>
                    </div>
                </div>
                
                <div class="data-details">
                    <p><strong>📄 Data File:</strong> {os.path.basename(attachment_path)}</p>
                    <p><strong>📅 Date Range:</strong> {date_range}</p>
                    <p><strong>📈 Analysis:</strong> Moving averages, RSI momentum, volatility metrics, and returns data</p>
                </div>
            </div>
            
            {charts_html}
            
            <div class="info-section">
                <h3>Key Metrics Explained</h3>
                <ul class="metrics-list">
                    <li>
                        <strong>Moving Averages (7d/30d):</strong> Smoothed price trends over different time periods. Golden Cross (7d above 30d) suggests bullish momentum, Death Cross (7d below 30d) suggests bearish momentum.
                    </li>
                    <li>
                        <strong>RSI (Relative Strength Index):</strong> Momentum oscillator (0-100). Values above 70 indicate overbought conditions, below 30 indicate oversold conditions.
                    </li>
                    <li>
                        <strong>Volatility:</strong> Standard deviation of returns measuring price stability. Higher values indicate greater price fluctuation and risk.
                    </li>
                    <li>
                        <strong>Daily Returns:</strong> Day-over-day percentage price changes showing short-term performance and trend direction.
                    </li>
                </ul>
            </div>
            
            <div style="background: #f0f9ff; border-radius: 12px; padding: 25px; margin: 30px 0; border-left: 5px solid #0ea5e9;">
                <p style="margin: 0; color: #0c4a6e; font-size: 16px;">
                    <strong>📎 Data Access:</strong> The attached CSV file contains comprehensive raw data for detailed analysis and custom reporting. All charts above provide visual insights into current market conditions and trends.
                </p>
            </div>
        </div>
        
        <div class="footer">
            <p><em>This report was generated automatically by your Financial ETL Pipeline</em></p>
            <div class="signature">📈 Financial ETL Bot</div>
            <p style="margin-top: 15px; font-size: 12px; opacity: 0.7;">Generated on {current_date} at {current_time}</p>
        </div>
    </div>
</body>
</html>
    """.strip()
    
    try:
        print(f"📧 Sending professional email from {sender_email} to {recipient_email}")
        print(f"📄 CSV attachment: {attachment_path} ({csv_size_mb:.2f} MB)")
        print(f"📊 Embedded charts: {len(embedded_charts)}")
        print(f"📈 Tracking {num_tickers} tickers with {num_records:,} data points")
        
        message_id = send_email(
            sender=sender_email,
            recipient=recipient_email,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachment_paths=[attachment_path]  # Only CSV attachment
        )
        
        print(f"✅ Professional email sent successfully! Message ID: {message_id}")
        print(f"📊 Email includes {len(embedded_charts)} embedded charts with comprehensive analysis")
        return 0
        
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        import traceback
        traceback.print_exc()
        return 1


def generate_charts_html(charts_dict: dict) -> str:
    """Generate professional HTML for embedded charts."""
    if not charts_dict:
        return '''
        <div class="chart-section">
            <div class="chart-header">
                <h3 class="chart-title">Charts Not Available</h3>
                <p class="chart-description">Charts could not be generated for this report. Please check the data format and try again.</p>
            </div>
        </div>
        '''
    
    html_sections = []
    
    for chart_key, chart_data in charts_dict.items():
        section_html = f'''
        <div class="chart-section">
            <div class="chart-header">
                <h3 class="chart-title">{chart_data['title']}</h3>
                <p class="chart-description">{chart_data['description']}</p>
            </div>
            <div class="chart-content">
                <img src="{chart_data['image']}" alt="{chart_data['title']}" class="chart-image" />
            </div>
        </div>
        '''
        html_sections.append(section_html)
    
    return '\n'.join(html_sections)


def generate_charts_text(charts_dict: dict) -> str:
    """Generate text description of charts for plain text email."""
    if not charts_dict:
        return "• Charts could not be generated for this report"
    
    text_sections = []
    
    for i, (chart_key, chart_data) in enumerate(charts_dict.items(), 1):
        text_sections.append(f"• {chart_data['title']}: {chart_data['description']}")
    
    return '\n'.join(text_sections)


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)