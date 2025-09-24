import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from pathlib import Path

# Base path configuration
BASE_DIR = Path(__file__).parent.parent.parent
REPORTS_DIR = BASE_DIR / "reports"

class QualityDashboard:
    def __init__(self, quality_results):
        self.quality_results = quality_results
        self.reports_dir = REPORTS_DIR
        self.reports_dir.mkdir(exist_ok=True)
    
    def generate_quality_report(self):
        """Generate comprehensive quality report"""
        report_data = []
        for result in self.quality_results:
            report_data.append({
                'Table': result['table_name'],
                'Timestamp': result['timestamp'],
                'Total Records': result['total_records'],
                'Quality Score': result['quality_score'],
                'Status': result['overall_status'],
                'Passed Checks': sum(1 for check in result['checks'].values() if check['passed']),
                'Total Checks': len(result['checks'])
            })
        return pd.DataFrame(report_data)
    
    def create_quality_visualization(self, output_filename="quality_dashboard.html"):
        """Create HTML dashboard dengan path yang benar"""
        output_path = self.reports_dir / output_filename
        image_path = self.reports_dir / "quality_metrics.png"

        df_report = self.generate_quality_report()
        
        # --- Visualization ---
        plt.figure(figsize=(12, 8))
        
        # Quality scores by table
        plt.subplot(2, 2, 1)
        sns.barplot(x='Table', y='Quality Score', data=df_report)
        plt.title('Data Quality Scores by Table')
        plt.xticks(rotation=45)
        
        # Records distribution
        plt.subplot(2, 2, 2)
        plt.pie(df_report['Total Records'], labels=df_report['Table'], autopct='%1.1f%%')
        plt.title('Record Distribution')
        
        plt.tight_layout()
        plt.savefig(image_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        # --- HTML Report ---
        html_content = f"""
        <html>
        <head>
            <title>Data Quality Dashboard - {datetime.now().strftime('%Y-%m-%d')}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .metric {{ margin: 10px 0; padding: 10px; border-left: 4px solid #007acc; }}
                .pass {{ border-color: green; }}
                .warning {{ border-color: orange; }}
                .fail {{ border-color: red; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: center; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Data Quality Dashboard</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <h2>Summary</h2>
            {df_report.to_html(classes='table table-striped', index=False)}
            
            <h2>Quality Metrics Visualization</h2>
            <img src="{image_path.name}" alt="Quality Metrics" style="max-width: 100%;">
            
            <h2>Detailed Checks</h2>
        """
        
        for result in self.quality_results:
            status_class = result['overall_status'].lower()
            html_content += f"""
            <div class="metric {status_class}">
                <h3>{result['table_name']} - {result['overall_status']}</h3>
                <p>Quality Score: {result['quality_score']:.2f}%</p>
                <ul>
            """
            for check_name, check_result in result['checks'].items():
                status_icon = "✅" if check_result['passed'] else "❌"
                html_content += f"<li>{status_icon} {check_name}: {check_result['passed']}</li>"
            html_content += "</ul></div>"
        
        html_content += "</body></html>"
        
        with open(output_path, 'w', encoding="utf-8") as f:
            f.write(html_content)
        
        print(f"Quality dashboard generated: {output_path}")
