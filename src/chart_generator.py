# src/chart_generator.py
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import seaborn as sns
import numpy as np
from datetime import datetime
import os
from typing import List, Optional

# Set style
plt.style.use('default')
sns.set_palette("husl")

class ChartGenerator:
    def __init__(self, output_dir: str = "data/charts"):
        """Initialize chart generator with output directory."""
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Set matplotlib parameters for better quality
        plt.rcParams['figure.dpi'] = 300
        plt.rcParams['savefig.dpi'] = 300
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 10
    
    def create_ticker_time_series(self, df: pd.DataFrame, ticker: str, 
                                metric_col: str, date_col: str = 'date',
                                title_suffix: str = "") -> str:
        """
        Create a time series chart for a single ticker.
        
        Args:
            df: DataFrame with ticker data
            ticker: Ticker symbol to plot
            metric_col: Column name for the metric to plot
            date_col: Column name for dates
            title_suffix: Additional text for chart title
        
        Returns:
            Path to saved chart file
        """
        # Filter data for this ticker
        ticker_data = df[df['ticker'] == ticker].copy()
        
        if ticker_data.empty:
            raise ValueError(f"No data found for ticker: {ticker}")
        
        # Ensure date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(ticker_data[date_col]):
            ticker_data[date_col] = pd.to_datetime(ticker_data[date_col])
        
        # Sort by date
        ticker_data = ticker_data.sort_values(date_col)
        
        # Create the plot
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Plot the main line
        ax.plot(ticker_data[date_col], ticker_data[metric_col], 
               linewidth=2.5, marker='o', markersize=4, alpha=0.8,
               label=f'{ticker} {metric_col}')
        
        # Add trend line
        if len(ticker_data) > 1:
            z = np.polyfit(mdates.date2num(ticker_data[date_col]), 
                          ticker_data[metric_col], 1)
            p = np.poly1d(z)
            ax.plot(ticker_data[date_col], 
                   p(mdates.date2num(ticker_data[date_col])), 
                   "--", alpha=0.8, color='red', linewidth=1.5,
                   label='Trend')
        
        # Formatting
        ax.set_title(f'{ticker} - {metric_col.replace("_", " ").title()} Over Time{title_suffix}', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel(metric_col.replace("_", " ").title(), fontsize=12, fontweight='bold')
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        plt.xticks(rotation=45)
        
        # Add grid
        ax.grid(True, alpha=0.3)
        
        # Add legend
        ax.legend(loc='best')
        
        # Add stats box
        stats_text = f'''Statistics:
Latest: {ticker_data[metric_col].iloc[-1]:.2f}
Min: {ticker_data[metric_col].min():.2f}
Max: {ticker_data[metric_col].max():.2f}
Avg: {ticker_data[metric_col].mean():.2f}'''
        
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
               verticalalignment='top', bbox=dict(boxstyle='round', 
               facecolor='wheat', alpha=0.8), fontsize=9)
        
        # Tight layout
        plt.tight_layout()
        
        # Save the chart
        filename = f"{ticker}_{metric_col}_timeseries.png"
        filepath = os.path.join(self.output_dir, filename)
        plt.savefig(filepath, bbox_inches='tight', facecolor='white')
        plt.close()
        
        print(f"Created chart: {filepath}")
        return filepath
    
    def create_multi_ticker_comparison(self, df: pd.DataFrame, 
                                     metric_col: str, date_col: str = 'date',
                                     title_suffix: str = "") -> str:
        """
        Create a comparison chart for multiple tickers.
        
        Args:
            df: DataFrame with ticker data
            metric_col: Column name for the metric to plot
            date_col: Column name for dates
            title_suffix: Additional text for chart title
        
        Returns:
            Path to saved chart file
        """
        # Ensure date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            df[date_col] = pd.to_datetime(df[date_col])
        
        # Get unique tickers
        tickers = df['ticker'].unique()
        
        if len(tickers) == 0:
            raise ValueError("No tickers found in data")
        
        # Create the plot
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Plot each ticker
        for ticker in tickers:
            ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
            ax.plot(ticker_data[date_col], ticker_data[metric_col], 
                   linewidth=2, marker='o', markersize=3, alpha=0.8,
                   label=ticker)
        
        # Formatting
        ax.set_title(f'All Tickers - {metric_col.replace("_", " ").title()} Comparison{title_suffix}', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel(metric_col.replace("_", " ").title(), fontsize=12, fontweight='bold')
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        plt.xticks(rotation=45)
        
        # Add grid
        ax.grid(True, alpha=0.3)
        
        # Add legend
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # Tight layout
        plt.tight_layout()
        
        # Save the chart
        filename = f"all_tickers_{metric_col}_comparison.png"
        filepath = os.path.join(self.output_dir, filename)
        plt.savefig(filepath, bbox_inches='tight', facecolor='white')
        plt.close()
        
        print(f"Created comparison chart: {filepath}")
        return filepath
    
    def create_summary_dashboard(self, df: pd.DataFrame, 
                               metrics: List[str], date_col: str = 'date') -> str:
        """
        Create a dashboard with multiple subplots showing different metrics.
        
        Args:
            df: DataFrame with ticker data
            metrics: List of metric columns to plot
            date_col: Column name for dates
        
        Returns:
            Path to saved chart file
        """
        # Ensure date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            df[date_col] = pd.to_datetime(df[date_col])
        
        # Determine subplot layout
        n_metrics = len(metrics)
        n_cols = 2
        n_rows = (n_metrics + n_cols - 1) // n_cols
        
        # Create subplots
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 6*n_rows))
        if n_metrics == 1:
            axes = [axes]
        elif n_rows == 1:
            axes = axes.reshape(1, -1)
        
        # Plot each metric
        for i, metric in enumerate(metrics):
            row = i // n_cols
            col = i % n_cols
            ax = axes[row, col] if n_rows > 1 else axes[col]
            
            # Plot each ticker for this metric
            for ticker in df['ticker'].unique():
                ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
                ax.plot(ticker_data[date_col], ticker_data[metric], 
                       linewidth=2, marker='o', markersize=2, alpha=0.8,
                       label=ticker)
            
            # Formatting
            ax.set_title(f'{metric.replace("_", " ").title()}', 
                        fontsize=14, fontweight='bold')
            ax.set_xlabel('Date', fontsize=10)
            ax.set_ylabel(metric.replace("_", " ").title(), fontsize=10)
            ax.grid(True, alpha=0.3)
            ax.legend(fontsize=8)
            
            # Format dates
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        # Hide empty subplots
        for i in range(n_metrics, n_rows * n_cols):
            row = i // n_cols
            col = i % n_cols
            if n_rows > 1:
                axes[row, col].set_visible(False)
            else:
                axes[col].set_visible(False)
        
        # Add main title
        fig.suptitle('ETL Pipeline - Metrics Dashboard', fontsize=18, fontweight='bold')
        
        # Tight layout
        plt.tight_layout()
        
        # Save the chart
        filename = "dashboard_summary.png"
        filepath = os.path.join(self.output_dir, filename)
        plt.savefig(filepath, bbox_inches='tight', facecolor='white')
        plt.close()
        
        print(f"Created dashboard: {filepath}")
        return filepath
    
    def create_price_ma_chart(self, df: pd.DataFrame, date_col: str = 'date') -> str:
        """Create a chart showing moving averages for all tickers."""
        if 'ma_7' not in df.columns or 'ma_30' not in df.columns:
            raise ValueError("DataFrame must contain ma_7 and ma_30 columns")
        
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()
        
        tickers = sorted(df['ticker'].unique())
        
        for i, ticker in enumerate(tickers):
            if i >= len(axes):
                break
                
            ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
            ax = axes[i]
            
            # Plot moving averages
            ax.plot(ticker_data[date_col], ticker_data['ma_7'], 
                   linewidth=2, label='7-day MA', alpha=0.8)
            ax.plot(ticker_data[date_col], ticker_data['ma_30'], 
                   linewidth=2, label='30-day MA', alpha=0.8)
            
            ax.set_title(f'{ticker} - Moving Averages', fontsize=12, fontweight='bold')
            ax.set_ylabel('Price ($)')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # Format dates
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        # Hide unused subplots
        for i in range(len(tickers), len(axes)):
            axes[i].set_visible(False)
        
        fig.suptitle('Moving Averages Comparison', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        filename = "moving_averages_comparison.png"
        filepath = os.path.join(self.output_dir, filename)
        plt.savefig(filepath, bbox_inches='tight', facecolor='white')
        plt.close()
        
        print(f"Created moving averages chart: {filepath}")
        return filepath
    
    def create_rsi_chart(self, df: pd.DataFrame, date_col: str = 'date') -> str:
        """Create RSI chart with overbought/oversold levels."""
        if 'rsi' not in df.columns:
            raise ValueError("DataFrame must contain rsi column")
        
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()
        
        tickers = sorted(df['ticker'].unique())
        
        for i, ticker in enumerate(tickers):
            if i >= len(axes):
                break
                
            ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
            ax = axes[i]
            
            # Plot RSI
            ax.plot(ticker_data[date_col], ticker_data['rsi'], 
                   linewidth=2, color='purple', alpha=0.8)
            
            # Add overbought/oversold lines
            ax.axhline(y=70, color='red', linestyle='--', alpha=0.7, label='Overbought (70)')
            ax.axhline(y=30, color='green', linestyle='--', alpha=0.7, label='Oversold (30)')
            ax.axhline(y=50, color='gray', linestyle='-', alpha=0.5, label='Neutral (50)')
            
            ax.set_title(f'{ticker} - RSI', fontsize=12, fontweight='bold')
            ax.set_ylabel('RSI')
            ax.set_ylim(0, 100)
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # Format dates
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        # Hide unused subplots
        for i in range(len(tickers), len(axes)):
            axes[i].set_visible(False)
        
        fig.suptitle('RSI (Relative Strength Index)', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        filename = "rsi_analysis.png"
        filepath = os.path.join(self.output_dir, filename)
        plt.savefig(filepath, bbox_inches='tight', facecolor='white')
        plt.close()
        
        print(f"Created RSI chart: {filepath}")
        return filepath
    
    def create_volatility_chart(self, df: pd.DataFrame, date_col: str = 'date') -> str:
        """Create volatility comparison chart."""
        if 'vol_7' not in df.columns or 'vol_30' not in df.columns:
            raise ValueError("DataFrame must contain vol_7 and vol_30 columns")
        
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        
        # 7-day volatility
        for ticker in sorted(df['ticker'].unique()):
            ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
            ax1.plot(ticker_data[date_col], ticker_data['vol_7'], 
                    linewidth=2, marker='o', markersize=2, alpha=0.8, label=ticker)
        
        ax1.set_title('7-Day Volatility', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Volatility (Standard Deviation)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        
        # 30-day volatility
        for ticker in sorted(df['ticker'].unique()):
            ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
            ax2.plot(ticker_data[date_col], ticker_data['vol_30'], 
                    linewidth=2, marker='o', markersize=2, alpha=0.8, label=ticker)
        
        ax2.set_title('30-Day Volatility', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Volatility (Standard Deviation)')
        ax2.set_xlabel('Date')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
        
        fig.suptitle('Volatility Analysis', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        filename = "volatility_analysis.png"
        filepath = os.path.join(self.output_dir, filename)
        plt.savefig(filepath, bbox_inches='tight', facecolor='white')
        plt.close()
        
        print(f"Created volatility chart: {filepath}")
        return filepath
    
    def create_financial_dashboard(self, df: pd.DataFrame, date_col: str = 'date') -> str:
        """Create a comprehensive financial dashboard."""
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        fig = plt.figure(figsize=(20, 12))
        
        # Create a 3x2 grid
        gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.2)
        
        # 1. Moving Averages Comparison
        ax1 = fig.add_subplot(gs[0, 0])
        for ticker in sorted(df['ticker'].unique()):
            ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
            if 'ma_7' in df.columns:
                ax1.plot(ticker_data[date_col], ticker_data['ma_7'], 
                        linewidth=1.5, alpha=0.8, label=f'{ticker} 7d')
        ax1.set_title('7-Day Moving Averages', fontweight='bold')
        ax1.legend(fontsize=8)
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        
        # 2. RSI Overview
        ax2 = fig.add_subplot(gs[0, 1])
        for ticker in sorted(df['ticker'].unique()):
            ticker_data = df[df['ticker'] == ticker].sort_values(date_col)
            if 'rsi' in df.columns:
                ax2.plot(ticker_data[date_col], ticker_data['rsi'], 
                        linewidth=1.5, alpha=0.8, label=ticker)
        ax2.axhline(y=70, color='red', linestyle='--', alpha=0.5)
        ax2.axhline(y=30, color='green', linestyle='--', alpha=0.5)
        ax2.set_title('RSI Comparison', fontweight='bold')
        ax2.set_ylim(0, 100)
        ax2.legend(fontsize=8)
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        
        # 3. Volatility Heatmap
        ax3 = fig.add_subplot(gs[1, :])
        if 'vol_30' in df.columns:
            # Create pivot table for heatmap
            pivot_data = df.pivot_table(values='vol_30', index='ticker', 
                                      columns=df[date_col].dt.strftime('%m-%d'), 
                                      aggfunc='mean')
            # Only show last 20 days to keep readable
            pivot_data = pivot_data.iloc[:, -20:] if pivot_data.shape[1] > 20 else pivot_data
            
            sns.heatmap(pivot_data, ax=ax3, cmap='YlOrRd', cbar_kws={'label': '30d Volatility'})
            ax3.set_title('30-Day Volatility Heatmap (Last 20 Days)', fontweight='bold')
            ax3.set_xlabel('Date')
            ax3.set_ylabel('Ticker')
        
        # 4. Returns Distribution
        ax4 = fig.add_subplot(gs[2, 0])
        if 'return_1d' in df.columns:
            for ticker in sorted(df['ticker'].unique()):
                ticker_data = df[df['ticker'] == ticker]['return_1d'].dropna()
                ax4.hist(ticker_data * 100, alpha=0.6, bins=30, label=ticker)
            ax4.set_title('Daily Returns Distribution (%)', fontweight='bold')
            ax4.set_xlabel('Daily Return (%)')
            ax4.set_ylabel('Frequency')
            ax4.legend(fontsize=8)
            ax4.grid(True, alpha=0.3)
        
        # 5. Current Metrics Table
        ax5 = fig.add_subplot(gs[2, 1])
        ax5.axis('off')  # Hide axes for table
        
        # Get latest metrics for each ticker
        latest_data = df.loc[df.groupby('ticker')[date_col].idxmax()]
        table_data = []
        for _, row in latest_data.iterrows():
            table_data.append([
                row['ticker'],
                f"{row.get('ma_7', 0):.2f}" if pd.notna(row.get('ma_7')) else 'N/A',
                f"{row.get('rsi', 0):.1f}" if pd.notna(row.get('rsi')) else 'N/A',
                f"{row.get('vol_30', 0)*100:.2f}%" if pd.notna(row.get('vol_30')) else 'N/A'
            ])
        
        table = ax5.table(cellText=table_data,
                         colLabels=['Ticker', '7d MA', 'RSI', '30d Vol'],
                         cellLoc='center',
                         loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        ax5.set_title('Latest Metrics', fontweight='bold', pad=20)
        
        fig.suptitle(f'Financial Dashboard - {datetime.now().strftime("%Y-%m-%d")}', 
                    fontsize=18, fontweight='bold')
        
        filename = "financial_dashboard.png"
        filepath = os.path.join(self.output_dir, filename)
        plt.savefig(filepath, bbox_inches='tight', facecolor='white')
        plt.close()
        
        print(f"Created financial dashboard: {filepath}")
        return filepath
        """Get list of all chart files in the output directory."""
        chart_files = []
        for file in os.listdir(self.output_dir):
            if file.endswith('.png'):
                chart_files.append(os.path.join(self.output_dir, file))
        return sorted(chart_files)


def generate_charts_from_csv(csv_path: str, output_dir: str = "data/charts") -> List[str]:
    """
    Generate all charts from your ETL pipeline CSV file.
    Expected columns: date, ticker, return_1d, ma_7, ma_30, vol_7, vol_30, rsi
    
    Args:
        csv_path: Path to the CSV file
        output_dir: Directory to save charts
    
    Returns:
        List of paths to created chart files
    """
    # Read the CSV
    df = pd.read_csv(csv_path)
    
    # Initialize chart generator
    chart_gen = ChartGenerator(output_dir)
    
    # Validate expected columns from your pipeline
    expected_cols = ['date', 'ticker']
    if not all(col in df.columns for col in expected_cols):
        raise ValueError(f"CSV must contain columns: {expected_cols}")
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Define the metrics we want to chart (from your pipeline)
    metrics = ['return_1d', 'ma_7', 'ma_30', 'vol_7', 'vol_30', 'rsi']
    available_metrics = [col for col in metrics if col in df.columns]
    
    if not available_metrics:
        raise ValueError("No expected metric columns found in CSV")
    
    chart_paths = []
    
    # Create individual ticker charts for key metrics
    key_metrics = ['ma_7', 'rsi']  # Focus on most important ones
    for ticker in df['ticker'].unique():
        for metric in key_metrics:
            if metric in df.columns:
                try:
                    chart_path = chart_gen.create_ticker_time_series(
                        df, ticker, metric, date_col='date',
                        title_suffix=f" ({datetime.now().strftime('%Y-%m-%d')})"
                    )
                    chart_paths.append(chart_path)
                except Exception as e:
                    print(f"Warning: Could not create chart for {ticker} - {metric}: {e}")
    
    # Create comparison charts for key metrics
    comparison_metrics = ['ma_7', 'rsi', 'vol_30']
    for metric in comparison_metrics:
        if metric in df.columns:
            try:
                chart_path = chart_gen.create_multi_ticker_comparison(
                    df, metric, date_col='date',
                    title_suffix=f" ({datetime.now().strftime('%Y-%m-%d')})"
                )
                chart_paths.append(chart_path)
            except Exception as e:
                print(f"Warning: Could not create comparison chart for {metric}: {e}")
    
    # Create financial-specific charts
    try:
        # Price vs Moving Average chart
        chart_path = chart_gen.create_price_ma_chart(df)
        chart_paths.append(chart_path)
    except Exception as e:
        print(f"Warning: Could not create price/MA chart: {e}")
    
    try:
        # RSI chart with overbought/oversold levels
        chart_path = chart_gen.create_rsi_chart(df)
        chart_paths.append(chart_path)
    except Exception as e:
        print(f"Warning: Could not create RSI chart: {e}")
    
    try:
        # Volatility comparison chart
        chart_path = chart_gen.create_volatility_chart(df)
        chart_paths.append(chart_path)
    except Exception as e:
        print(f"Warning: Could not create volatility chart: {e}")
    
    # Create dashboard with key metrics
    try:
        chart_path = chart_gen.create_financial_dashboard(df)
        chart_paths.append(chart_path)
    except Exception as e:
        print(f"Warning: Could not create dashboard: {e}")
    
    return chart_paths


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
        charts = generate_charts_from_csv(csv_file)
        print(f"\n✅ Successfully generated {len(charts)} charts:")
        for chart in charts:
            print(f"  - {chart}")
    except Exception as e:
        print(f"❌ Error generating charts: {e}")
        sys.exit(1)