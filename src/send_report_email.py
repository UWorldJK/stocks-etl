# In your send_report_email.py, modify the chart selection logic:

def get_priority_charts(chart_files):
    """Select only the most important charts to embed."""
    priority_patterns = [
        'financial_dashboard.png',
        'moving_averages_comparison.png', 
        'rsi_analysis.png',
        'volatility_analysis.png',
        'all_tickers_ma_7_comparison.png',
        'all_tickers_rsi_comparison.png'
    ]
    
    priority_charts = []
    for pattern in priority_patterns:
        for chart_file in chart_files:
            if pattern in os.path.basename(chart_file):
                priority_charts.append(chart_file)
                break
    
    return priority_charts

# Then in your main() function, replace the embedding logic:

# Find chart files (keep all for attachments)
chart_dir = "artifacts/charts"
chart_files = []
if os.path.exists(chart_dir):
    for file in os.listdir(chart_dir):
        if file.endswith('.png'):
            chart_files.append(os.path.join(chart_dir, file))
    chart_files.sort()
    print(f"Found {len(chart_files)} chart files")

# Get priority charts for embedding (limit to key summary charts)
priority_charts = get_priority_charts(chart_files)
print(f"Selected {len(priority_charts)} priority charts for embedding")

# Encode only priority images to base64 for embedding
embedded_images = []
for i, chart_file in enumerate(priority_charts):
    base64_data = encode_image_to_base64(chart_file)
    if base64_data:
        embedded_images.append({
            'filename': os.path.basename(chart_file),
            'base64': base64_data,
            'cid': f'chart_{i}'
        })