# src/chart_generator.py
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import seaborn as sns
import numpy as np
from datetime import datetime
import os
import base64
from io import BytesIO
from typing import List, Optional, Dict

# Set style
plt.style.use('default')
sns.set_palette("husl")

class ChartGenerator:
    def __init__(self, output_dir: str = "data/charts"):
        """Initialize chart generator with output directory."""
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Set matplotlib parameters for professional quality
        plt.rcParams['figure.dpi'] = 150
        plt.rcParams['savefig.dpi'] = 150
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.linewidth'] = 0.8
        plt.rcParams['grid.alpha'] = 0.3
        plt.rcParams['axes.spines.top'] = False
        plt.rcParams['axes.spines.right'] = False
    
    def _save_chart_as_base64(self, fig, format='png') -> str:
        """Convert matplotlib figure to base64 string for embedding in email."""
        buffer = BytesIO()
        fig.savefig(buffer, format=format, bbox_inches='tight', 
                   facecolor='white', edgecolor='none', dpi=150,
                   pad_inches=0.1, transparent=False)
        buffer.seek(0)
        
        # Convert to base64
        image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        buffer.close()
        
        return f"data:image/{format};base64,{image_base64}"
    
    def create_summary_dashboard(self, df: pd.DataFrame, date_col: str = 'date') -> Dict[str, str]:
        """
        Create a comprehensive but compact dashboard optimized for email embedding.
        Returns base64 encoded images and descriptions.
        """
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        charts = {}
        
        # 1. Market Overview Chart - Professional styling
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.patch.set_facecolor('white')
        
        # Color palette for professional look
        colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E', '#577590']
        
        # Moving Averages (top-left) - Professional styling
        ax1 = axes[0, 0]
        if 'ma_7' in df.columns:
            for i, ticker in enumerate(sorted(df['ticker'].unique())):
                ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
                color = colors[i % len(colors)]
                ax1.plot(ticker_data[date_col], ticker_data['ma_7'], 
                        linewidth=2.5, alpha=0.9, label=ticker, color=color)
            ax1.set_title('7-Day Moving Averages', fontweight='bold', fontsize=14, pad=15)
            ax1.legend(fontsize=9, framealpha=0.9)
            ax1.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, fontsize=9)
            ax1.set_ylabel('Price ($)', fontsize=10, fontweight='bold')
            
            # Style improvements
            ax1.spines['top'].set_visible(False)
            ax1.spines['right'].set_visible(False)
            ax1.set_facecolor('#fafafa')
        
        # RSI (top-right) - Professional styling
        ax2 = axes[0, 1]
        if 'rsi' in df.columns:
            for i, ticker in enumerate(sorted(df['ticker'].unique())):
                ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
                color = colors[i % len(colors)]
                ax2.plot(ticker_data[date_col], ticker_data['rsi'], 
                        linewidth=2.5, alpha=0.9, label=ticker, color=color)
            
            # RSI threshold lines
            ax2.axhline(y=70, color='#DC2626', linestyle='--', alpha=0.7, linewidth=2, label='Overbought')
            ax2.axhline(y=30, color='#16A34A', linestyle='--', alpha=0.7, linewidth=2, label='Oversold')
            ax2.axhline(y=50, color='#6B7280', linestyle='-', alpha=0.4, linewidth=1)
            
            ax2.set_title('RSI - Momentum Indicator', fontweight='bold', fontsize=14, pad=15)
            ax2.set_ylim(0, 100)
            ax2.legend(fontsize=9, framealpha=0.9)
            ax2.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, fontsize=9)
            ax2.set_ylabel('RSI Value', fontsize=10, fontweight='bold')
            
            # Style improvements
            ax2.spines['top'].set_visible(False)
            ax2.spines['right'].set_visible(False)
            ax2.set_facecolor('#fafafa')
        ax2 = axes[0, 1]
        if 'rsi' in df.columns:
            for ticker in sorted(df['ticker'].unique()):
                ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
                ax2.plot(ticker_data[date_col], ticker_data['rsi'], 
                        linewidth=2, alpha=0.8, label=ticker)
            ax2.axhline(y=70, color='red', linestyle='--', alpha=0.5, label='Overbought')
            ax2.axhline(y=30, color='green', linestyle='--', alpha=0.5, label='Oversold')
            ax2.set_title('RSI - Momentum Indicator', fontweight='bold')
            ax2.set_ylim(0, 100)
            ax2.legend(fontsize=8)
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
        
        # Volatility (bottom-left)
        ax3 = axes[1, 0]
        if 'vol_30' in df.columns:
            for ticker in sorted(df['ticker'].unique()):
                ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
                ax3.plot(ticker_data[date_col], ticker_data['vol_30'] * 100, 
                        linewidth=2, alpha=0.8, label=ticker)
            ax3.set_title('30-Day Volatility (%)', fontweight='bold')
            ax3.legend(fontsize=8)
            ax3.grid(True, alpha=0.3)
            ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
        
        # Returns Distribution (bottom-right)
        ax4 = axes[1, 1]
        if 'return_1d' in df.columns:
            for ticker in sorted(df['ticker'].unique()):
                ticker_data = df[df['ticker'] == ticker]['return_1d'].dropna()
                if len(ticker_data) > 0:
                    ax4.hist(ticker_data * 100, alpha=0.6, bins=20, label=ticker)
            ax4.set_title('Daily Returns Distribution', fontweight='bold')
            ax4.set_xlabel('Return (%)')
            ax4.legend(fontsize=8)
            ax4.grid(True, alpha=0.3)
        
        plt.suptitle(f'Market Overview - {datetime.now().strftime("%Y-%m-%d")}', 
                    fontsize=14, fontweight='bold')
        plt.tight_layout()
        
        charts['market_overview'] = {
            'image': self._save_chart_as_base64(fig),
            'title': 'Market Overview Dashboard',
            'description': 'Comprehensive view showing moving averages, RSI momentum indicator, volatility trends, and daily returns distribution across all tracked assets.'
        }
        plt.close(fig)
        
        # 2. Performance Summary Chart
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
        
        # Latest metrics comparison
        latest_data = df.loc[df.groupby('ticker')[date_col].idxmax()]
        
        if 'ma_7' in df.columns and len(latest_data) > 0:
            tickers = latest_data['ticker'].tolist()
            ma_values = latest_data['ma_7'].tolist()
            
            bars = ax1.bar(tickers, ma_values, alpha=0.7, color=plt.cm.Set3(np.linspace(0, 1, len(tickers))))
            ax1.set_title('Current 7-Day Moving Averages', fontweight='bold')
            ax1.set_ylabel('Price ($)')
            ax1.tick_params(axis='x', rotation=45)
            
            # Add value labels on bars
            for bar, value in zip(bars, ma_values):
                ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(ma_values)*0.01,
                        f'${value:.2f}', ha='center', va='bottom', fontsize=8)
        
        # RSI status
        if 'rsi' in df.columns and len(latest_data) > 0:
            rsi_values = latest_data['rsi'].tolist()
            colors = ['red' if x > 70 else 'green' if x < 30 else 'gray' for x in rsi_values]
            
            bars = ax2.bar(tickers, rsi_values, alpha=0.7, color=colors)
            ax2.set_title('Current RSI Status', fontweight='bold')
            ax2.set_ylabel('RSI Value')
            ax2.tick_params(axis='x', rotation=45)
            ax2.axhline(y=70, color='red', linestyle='--', alpha=0.5)
            ax2.axhline(y=30, color='green', linestyle='--', alpha=0.5)
            ax2.set_ylim(0, 100)
            
            # Add value labels
            for bar, value in zip(bars, rsi_values):
                ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
                        f'{value:.1f}', ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        
        charts['performance_summary'] = {
            'image': self._save_chart_as_base64(fig),
            'title': 'Current Performance Snapshot',
            'description': 'Latest moving averages and RSI values. Red bars indicate overbought conditions (RSI > 70), green bars indicate oversold conditions (RSI < 30).'
        }
        plt.close(fig)
        
        # 3. Trend Analysis Chart (only if we have enough data points)
        if len(df) > 10:
            fig, ax = plt.subplots(1, 1, figsize=(12, 6))
            
            if 'ma_7' in df.columns and 'ma_30' in df.columns:
                for ticker in sorted(df['ticker'].unique()):
                    ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
                    if len(ticker_data) > 1:
                        ax.plot(ticker_data[date_col], ticker_data['ma_7'], 
                               linewidth=2, alpha=0.8, label=f'{ticker} (7d)', linestyle='-')
                        ax.plot(ticker_data[date_col], ticker_data['ma_30'], 
                               linewidth=1.5, alpha=0.6, label=f'{ticker} (30d)', linestyle='--')
                
                ax.set_title('Moving Average Trends Comparison', fontweight='bold')
                ax.set_ylabel('Price ($)')
                ax.set_xlabel('Date')
                ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
                ax.grid(True, alpha=0.3)
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
                
                plt.tight_layout()
                
                charts['trend_analysis'] = {
                    'image': self._save_chart_as_base64(fig),
                    'title': 'Moving Average Trends',
                    'description': 'Comparison of 7-day (solid) and 30-day (dashed) moving averages. Crossovers between short and long-term averages may indicate trend changes.'
                }
            plt.close(fig)
        
        return charts
    
    def create_metrics_table_chart(self, df: pd.DataFrame, date_col: str = 'date') -> Dict[str, str]:
        """Create a visual metrics table as a chart."""
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        # Get latest data for each ticker
        latest_data = df.loc[df.groupby('ticker')[date_col].idxmax()]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.axis('off')
        
        # Prepare table data
        table_data = []
        headers = ['Ticker', '7d MA', '30d MA', 'RSI', '1d Return', '30d Vol']
        
        for _, row in latest_data.iterrows():
            table_row = [
                row['ticker'],
                f"${row.get('ma_7', 0):.2f}" if pd.notna(row.get('ma_7')) else 'N/A',
                f"${row.get('ma_30', 0):.2f}" if pd.notna(row.get('ma_30')) else 'N/A',
                f"{row.get('rsi', 0):.1f}" if pd.notna(row.get('rsi')) else 'N/A',
                f"{row.get('return_1d', 0)*100:.2f}%" if pd.notna(row.get('return_1d')) else 'N/A',
                f"{row.get('vol_30', 0)*100:.2f}%" if pd.notna(row.get('vol_30')) else 'N/A'
            ]
            table_data.append(table_row)
        
        # Create table
        table = ax.table(cellText=table_data,
                        colLabels=headers,
                        cellLoc='center',
                        loc='center',
                        bbox=[0, 0, 1, 1])
        
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2.5)
        
        # Style the table
        for i in range(len(headers)):
            table[(0, i)].set_facecolor('#4CAF50')
            table[(0, i)].set_text_props(weight='bold', color='white')
        
        # Color code RSI values
        for i, row in enumerate(table_data):
            if row[3] != 'N/A':
                rsi_val = float(row[3])
                if rsi_val > 70:
                    table[(i+1, 3)].set_facecolor('#ffcdd2')  # Light red
                elif rsi_val < 30:
                    table[(i+1, 3)].set_facecolor('#c8e6c9')  # Light green
        
        ax.set_title(f'Latest Metrics Summary - {datetime.now().strftime("%Y-%m-%d")}', 
                    fontsize=14, fontweight='bold', pad=20)
        
        return {
            'metrics_table': {
                'image': self._save_chart_as_base64(fig),
                'title': 'Current Metrics Table',
                'description': 'Summary of latest values for all tracked metrics. RSI values are color-coded: red background indicates overbought (>70), green indicates oversold (<30).'
            }
        }


def generate_email_charts(csv_path: str, max_charts: int = 3) -> Dict[str, Dict[str, str]]:
    """
    Generate optimized charts for email embedding.
    
    Args:
        csv_path: Path to the CSV file
        max_charts: Maximum number of charts to generate
    
    Returns:
        Dictionary with chart data including base64 images and descriptions
    """
    # Read the CSV
    df = pd.read_csv(csv_path)
    
    # Initialize chart generator
    chart_gen = ChartGenerator()
    
    # Validate expected columns
    expected_cols = ['date', 'ticker']
    if not all(col in df.columns for col in expected_cols):
        raise ValueError(f"CSV must contain columns: {expected_cols}")
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    print(f"Generating email-optimized charts for {len(df['ticker'].unique())} tickers...")
    
    all_charts = {}
    
    # Generate summary dashboard (most important charts)
    try:
        dashboard_charts = chart_gen.create_summary_dashboard(df)
        all_charts.update(dashboard_charts)
        print(f"✅ Created {len(dashboard_charts)} dashboard charts")
    except Exception as e:
        print(f"Warning: Could not create dashboard charts: {e}")
    
    # Generate metrics table
    try:
        table_chart = chart_gen.create_metrics_table_chart(df)
        all_charts.update(table_chart)
        print(f"✅ Created metrics table chart")
    except Exception as e:
        print(f"Warning: Could not create metrics table: {e}")
    
    print(f"📊 Generated {len(all_charts)} total charts for email embedding")
    return all_charts


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python chart_generator.py <csv_file_path>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    
    if not os.path.exists(csv_file):
        print(f"Error: CSV file not found: {csv_file}")
        sys.exit(1)
    
    try:
        charts = generate_email_charts(csv_file)
        print(f"\n✅ Successfully generated {len(charts)} charts for email:")
        for chart_name, chart_data in charts.items():
            print(f"  - {chart_data['title']}")
    except Exception as e:
        print(f"❌ Error generating charts: {e}")
        sys.exit(1)