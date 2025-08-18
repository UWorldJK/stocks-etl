# src/chart_generator.py
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import seaborn as sns
import numpy as np
from datetime import datetime
import os
from typing import Dict

# Set style
plt.style.use('default')
sns.set_palette("husl")

class ChartGenerator:
    def __init__(self, output_dir: str = "data/charts"):
        """Initialize chart generator with output directory."""
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

        # Matplotlib params
        plt.rcParams['figure.dpi'] = 150
        plt.rcParams['savefig.dpi'] = 150
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.linewidth'] = 0.8
        plt.rcParams['grid.alpha'] = 0.3
        plt.rcParams['axes.spines.top'] = False
        plt.rcParams['axes.spines.right'] = False

    def _save_chart(self, fig, name: str, fmt: str = "jpeg") -> str:
        """
        Save figure to disk as an image and return its absolute path.
        Files are timestamped to avoid collisions.
        """
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        ext = "jpg" if fmt.lower() in ("jpg", "jpeg") else fmt.lower()
        filename = f"{name}_{ts}.{ext}"
        path = os.path.join(self.output_dir, filename)

        save_kwargs = dict(
            bbox_inches="tight",
            facecolor="white",
            edgecolor="none",
            dpi=150,
            pad_inches=0.1,
            transparent=False,
        )

        # Use 'jpeg' for the format string when ext is jpg
        fig.savefig(
            path,
            format=("jpeg" if ext == "jpg" else ext),
            **save_kwargs
        )
        plt.close(fig)
        return os.path.abspath(path)

    def create_summary_dashboard(self, df: pd.DataFrame, date_col: str = "date") -> Dict[str, Dict[str, str]]:
        """
        Create a dashboard of charts and save each as JPEG.
        Returns dict with file paths, titles, and descriptions.
        """
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])

        charts: Dict[str, Dict[str, str]] = {}

        # --- MARKET OVERVIEW (2x2) ---
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.patch.set_facecolor('white')
        colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E', '#577590']

        # Moving Averages (top-left)
        ax1 = axes[0, 0]
        if 'ma_7' in df.columns:
            for i, ticker in enumerate(sorted(df['ticker'].unique())):
                tdf = df[df['ticker'] == ticker].sort_values(date_col)
                ax1.plot(tdf[date_col], tdf['ma_7'], linewidth=2.5, alpha=0.9,
                         label=ticker, color=colors[i % len(colors)])
            ax1.set_title('7-Day Moving Averages', fontweight='bold', fontsize=14, pad=15)
            ax1.legend(fontsize=9, framealpha=0.9)
            ax1.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, fontsize=9)
            ax1.set_ylabel('Price ($)', fontsize=10, fontweight='bold')
            ax1.set_facecolor('#fafafa')

        # RSI (top-right)
        ax2 = axes[0, 1]
        if 'rsi' in df.columns:
            for i, ticker in enumerate(sorted(df['ticker'].unique())):
                tdf = df[df['ticker'] == ticker].sort_values(date_col)
                ax2.plot(tdf[date_col], tdf['rsi'], linewidth=2.5, alpha=0.9,
                         label=ticker, color=colors[i % len(colors)])
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
            ax2.set_facecolor('#fafafa')

        # Volatility (bottom-left)
        ax3 = axes[1, 0]
        if 'vol_30' in df.columns:
            for ticker in sorted(df['ticker'].unique()):
                tdf = df[df['ticker'] == ticker].sort_values(date_col)
                ax3.plot(tdf[date_col], tdf['vol_30'] * 100, linewidth=2, alpha=0.8, label=ticker)
            ax3.set_title('30-Day Volatility (%)', fontweight='bold')
            ax3.legend(fontsize=8)
            ax3.grid(True, alpha=0.3)
            ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)

        # Returns Distribution (bottom-right)
        ax4 = axes[1, 1]
        if 'return_1d' in df.columns:
            for ticker in sorted(df['ticker'].unique()):
                series = df[df['ticker'] == ticker]['return_1d'].dropna()
                if len(series) > 0:
                    ax4.hist(series * 100, alpha=0.6, bins=20, label=ticker)
            ax4.set_title('Daily Returns Distribution', fontweight='bold')
            ax4.set_xlabel('Return (%)')
            ax4.legend(fontsize=8)
            ax4.grid(True, alpha=0.3)

        plt.suptitle(f'Market Overview - {datetime.now().strftime("%Y-%m-%d")}',
                     fontsize=14, fontweight='bold')
        plt.tight_layout()

        mo_path = self._save_chart(fig, "market_overview", fmt="jpeg")
        charts['market_overview'] = {
            'image_path': mo_path,
            'title': 'Market Overview Dashboard',
            'description': 'Moving averages, RSI, volatility trends, and returns distribution across tracked assets.'
        }

        # --- PERFORMANCE SUMMARY (MA + RSI bars) ---
        fig, (ax1b, ax2b) = plt.subplots(1, 2, figsize=(12, 5))
        latest_data = df.loc[df.groupby('ticker')[date_col].idxmax()]

        # Current 7d MA
        if 'ma_7' in df.columns and len(latest_data) > 0:
            tickers = latest_data['ticker'].tolist()
            ma_values = latest_data['ma_7'].tolist()
            bars = ax1b.bar(tickers, ma_values, alpha=0.7,
                            color=plt.cm.Set3(np.linspace(0, 1, len(tickers))))
            ax1b.set_title('Current 7-Day Moving Averages', fontweight='bold')
            ax1b.set_ylabel('Price ($)')
            ax1b.tick_params(axis='x', rotation=45)
            for bar, value in zip(bars, ma_values):
                ax1b.text(bar.get_x() + bar.get_width()/2,
                          bar.get_height() + max(ma_values)*0.01,
                          f'${value:.2f}', ha='center', va='bottom', fontsize=8)

        # RSI status
        if 'rsi' in df.columns and len(latest_data) > 0:
            rsi_values = latest_data['rsi'].tolist()
            colors_rsi = ['red' if x > 70 else 'green' if x < 30 else 'gray' for x in rsi_values]
            bars = ax2b.bar(tickers, rsi_values, alpha=0.7, color=colors_rsi)
            ax2b.set_title('Current RSI Status', fontweight='bold')
            ax2b.set_ylabel('RSI Value')
            ax2b.tick_params(axis='x', rotation=45)
            ax2b.axhline(y=70, color='red', linestyle='--', alpha=0.5)
            ax2b.axhline(y=30, color='green', linestyle='--', alpha=0.5)
            ax2b.set_ylim(0, 100)
            for bar, value in zip(bars, rsi_values):
                ax2b.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
                          f'{value:.1f}', ha='center', va='bottom', fontsize=8)

        plt.tight_layout()
        ps_path = self._save_chart(fig, "performance_summary", fmt="jpeg")
        charts['performance_summary'] = {
            'image_path': ps_path,
            'title': 'Current Performance Snapshot',
            'description': 'Latest 7d moving averages and RSI. Red = overbought (>70), green = oversold (<30).'
        }

        # --- TREND ANALYSIS (7d vs 30d) ---
        if len(df) > 10 and {'ma_7', 'ma_30'}.issubset(df.columns):
            fig, ax = plt.subplots(1, 1, figsize=(12, 6))
            for ticker in sorted(df['ticker'].unique()):
                tdf = df[df['ticker'] == ticker].sort_values(date_col)
                if len(tdf) > 1:
                    ax.plot(tdf[date_col], tdf['ma_7'], linewidth=2, alpha=0.8,
                            label=f'{ticker} (7d)', linestyle='-')
                    ax.plot(tdf[date_col], tdf['ma_30'], linewidth=1.5, alpha=0.6,
                            label=f'{ticker} (30d)', linestyle='--')
            ax.set_title('Moving Average Trends Comparison', fontweight='bold')
            ax.set_ylabel('Price ($)'); ax.set_xlabel('Date')
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
            ax.grid(True, alpha=0.3)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            plt.tight_layout()

            ta_path = self._save_chart(fig, "trend_analysis", fmt="jpeg")
            charts['trend_analysis'] = {
                'image_path': ta_path,
                'title': 'Moving Average Trends',
                'description': '7d (solid) vs 30d (dashed) averages. Crossovers may indicate trend changes.'
            }

        return charts

    def create_metrics_table_chart(self, df: pd.DataFrame, date_col: str = 'date') -> Dict[str, Dict[str, str]]:
        """Create a visual metrics table as a chart and save as JPEG."""
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        latest_data = df.loc[df.groupby('ticker')[date_col].idxmax()]

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.axis('off')

        # Table data
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

        table = ax.table(cellText=table_data, colLabels=headers, cellLoc='center',
                         loc='center', bbox=[0, 0, 1, 1])
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2.5)

        # Header styling
        for i in range(len(headers)):
            table[(0, i)].set_facecolor('#4CAF50')
            table[(0, i)].set_text_props(weight='bold', color='white')

        # RSI color coding
        for i, row in enumerate(table_data):
            if row[3] != 'N/A':
                rsi_val = float(row[3])
                if rsi_val > 70:
                    table[(i+1, 3)].set_facecolor('#ffcdd2')  # Light red
                elif rsi_val < 30:
                    table[(i+1, 3)].set_facecolor('#c8e6c9')  # Light green

        ax.set_title(f'Latest Metrics Summary - {datetime.now().strftime("%Y-%m-%d")}',
                     fontsize=14, fontweight='bold', pad=20)

        mt_path = self._save_chart(fig, "metrics_table", fmt="jpeg")
        return {
            'metrics_table': {
                'image_path': mt_path,
                'title': 'Current Metrics Table',
                'description': 'Latest values for all tracked metrics. RSI cells color-coded.'
            }
        }


def generate_email_charts(csv_path: str, max_charts: int = 3) -> Dict[str, Dict[str, str]]:
    """
    Generate charts and save them as JPEG files.
    Returns a dict where each entry has an 'image_path' you can attach/embed in email.
    """
    df = pd.read_csv(csv_path)

    chart_gen = ChartGenerator()

    expected_cols = ['date', 'ticker']
    if not all(col in df.columns for col in expected_cols):
        raise ValueError(f"CSV must contain columns: {expected_cols}")

    df['date'] = pd.to_datetime(df['date'])

    print(f"Generating JPEG charts for {len(df['ticker'].unique())} tickers...")

    all_charts: Dict[str, Dict[str, str]] = {}

    try:
        dashboard_charts = chart_gen.create_summary_dashboard(df)
        all_charts.update(dashboard_charts)
        print(f"✅ Created {len(dashboard_charts)} dashboard charts")
    except Exception as e:
        print(f"Warning: Could not create dashboard charts: {e}")

    try:
        table_chart = chart_gen.create_metrics_table_chart(df)
        all_charts.update(table_chart)
        print(f"✅ Created metrics table chart")
    except Exception as e:
        print(f"Warning: Could not create metrics table: {e}")

    # Truncate to max_charts if requested
    if isinstance(max_charts, int) and max_charts > 0:
        all_charts = dict(list(all_charts.items())[:max_charts])

    print(f"📊 Generated {len(all_charts)} total JPEG charts")
    return all_charts


if __name__ == "__main__":
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
        print(f"\n✅ Successfully generated {len(charts)} charts:")
        for _, chart_data in charts.items():
            print(f"  - {chart_data['title']} -> {chart_data['image_path']}")
    except Exception as e:
        print(f"❌ Error generating charts: {e}")
        sys.exit(1)
