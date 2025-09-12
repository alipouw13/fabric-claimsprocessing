# Python Visual Script for Power BI - Insurance Claims Analysis
# This script demonstrates advanced analytics and visualizations for insurance claims data
# Use this in Power BI's Python visual with the insurance claims dataset
# make sure you pip install necessary packages in your Python environment in your command prompt: 
# pip install pandas numpy matplotlib seaborn scikit-learn scipy
# Reference: https://learn.microsoft.com/en-us/power-bi/connect-data/service-python-packages-support

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Handle warnings - not available in Power BI Python visuals
try:
    import warnings
    warnings.filterwarnings('ignore')
except ImportError:
    # warnings package not available in Power BI Python visuals
    pass

# Set style for better visualizations
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Dataset structure: claim_amount_total, severity_category, risk_category, processing_flag, 
# claim_date, claim_amount_vehicle, claim_amount_property, claim_amount_injury

def create_claims_analysis_dashboard():
    """
    Creates a comprehensive claims analysis dashboard with multiple visualizations
    """
    
    # Ensure dataset is available - in Power BI this should be the connected dataset
    try:
        df = dataset.copy()
    except NameError:
        print("Error: 'dataset' variable not found. This script should be run in Power BI with connected data.")
        return
    
    # Data preparation
    df = df.drop_duplicates()
    
    # Calculate derived metrics
    df['claim_month'] = pd.to_datetime(df['claim_date']).dt.month if 'claim_date' in df.columns else np.random.randint(1, 13, len(df))
    df['claim_year'] = pd.to_datetime(df['claim_date']).dt.year if 'claim_date' in df.columns else 2024
    df['log_claim_amount'] = np.log1p(df['claim_amount_total'])
    
    # Calculate percentage breakdowns of claim components
    df['vehicle_pct'] = (df['claim_amount_vehicle'] / df['claim_amount_total'] * 100).fillna(0)
    df['property_pct'] = (df['claim_amount_property'] / df['claim_amount_total'] * 100).fillna(0)
    df['injury_pct'] = (df['claim_amount_injury'] / df['claim_amount_total'] * 100).fillna(0)
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    fig.suptitle('Insurance Claims Advanced Analytics Dashboard', fontsize=16, fontweight='bold')
    
    # 1. Claim Amount Distribution with Severity Overlay
    ax1 = axes[0, 0]
    df_filtered = df[df['claim_amount_total'] <= df['claim_amount_total'].quantile(0.95)]
    
    # Histogram with KDE
    ax1.hist(df_filtered['claim_amount_total'], bins=50, alpha=0.7, color='skyblue', density=True)
    df_filtered['claim_amount_total'].plot.kde(ax=ax1, color='red', linewidth=2)
    ax1.set_xlabel('Claim Amount ($)')
    ax1.set_ylabel('Density')
    ax1.set_title('Claim Amount Distribution\n(95th Percentile View)')
    ax1.ticklabel_format(style='plain', axis='x')
    
    # Add severity markers
    severity_means = df.groupby('severity_category')['claim_amount_total'].mean()
    for severity, mean_amount in severity_means.items():
        if mean_amount <= df_filtered['claim_amount_total'].max():
            ax1.axvline(mean_amount, linestyle='--', alpha=0.8, 
                       label=f'{severity}: ${mean_amount:,.0f}')
    ax1.legend()
    
    # 2. Risk Segmentation using K-Means Clustering
    ax2 = axes[0, 1]
    
    # Prepare features for clustering using available columns
    features = ['claim_amount_total', 'vehicle_pct', 'property_pct', 'injury_pct']
    
    # Handle missing values and standardize
    df_cluster = df[features].fillna(df[features].median())
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(df_cluster)
    
    # Perform K-means clustering
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    df['risk_cluster'] = kmeans.fit_predict(features_scaled)
    
    # Create scatter plot using claim amount vs injury percentage
    scatter = ax2.scatter(df['claim_amount_total'], df['injury_pct'], 
                         c=df['risk_cluster'], cmap='viridis', alpha=0.6, s=30)
    ax2.set_xlabel('Claim Amount ($)')
    ax2.set_ylabel('Injury Component (%)')
    ax2.set_title('Risk Segmentation\n(ML-based Clustering)')
    plt.colorbar(scatter, ax=ax2, label='Risk Cluster')
    
    # 3. Geographic Risk Heatmap
    ax3 = axes[0, 2]
    
    # Calculate risk metrics by location
    location_risk = df.groupby('borough').agg({
        'claim_amount_total': 'mean',
        'severity': 'mean',
        'claim_no': 'count'
    }).round(2)
    
    # Create heatmap data
    risk_matrix = location_risk.values.T
    
    im = ax3.imshow(risk_matrix, cmap='Reds', aspect='auto')
    ax3.set_xticks(range(len(location_risk.index)))
    ax3.set_xticklabels(location_risk.index, rotation=45, ha='right')
    ax3.set_yticks(range(len(location_risk.columns)))
    ax3.set_yticklabels(['Avg Claim Amount', 'Avg Severity', 'Claim Count'])
    ax3.set_title('Geographic Risk Heatmap')
    
    # Add text annotations
    for i in range(len(location_risk.columns)):
        for j in range(len(location_risk.index)):
            text = ax3.text(j, i, f'{risk_matrix[i, j]:.0f}',
                           ha="center", va="center", color="black", fontsize=8)
    
    plt.colorbar(im, ax=ax3)
    
    # 4. Predictive Risk Scoring
    ax4 = axes[1, 0]
    
    # Calculate composite risk score
    df['risk_score'] = (
        (df['severity'] / df['severity'].max()) * 0.4 +
        (df['claim_amount_total'] / df['claim_amount_total'].max()) * 0.3 +
        (df['vehicle_age'] / df['vehicle_age'].max()) * 0.3
    ) * 100
    
    # Create risk score distribution
    risk_bins = [0, 20, 40, 60, 80, 100]
    risk_labels = ['Very Low', 'Low', 'Medium', 'High', 'Very High']
    df['risk_category'] = pd.cut(df['risk_score'], bins=risk_bins, labels=risk_labels)
    
    risk_counts = df['risk_category'].value_counts()
    colors = ['green', 'lightgreen', 'yellow', 'orange', 'red']
    
    wedges, texts, autotexts = ax4.pie(risk_counts.values, labels=risk_counts.index, 
                                      colors=colors, autopct='%1.1f%%', startangle=90)
    ax4.set_title('Risk Score Distribution\n(Composite Risk Model)')
    
    # 5. Temporal Claims Pattern
    ax5 = axes[1, 1]
    
    # Monthly claims analysis
    monthly_claims = df.groupby('claim_month').agg({
        'claim_amount_total': ['sum', 'mean', 'count']
    }).round(2)
    monthly_claims.columns = ['Total_Amount', 'Avg_Amount', 'Count']
    
    # Dual axis plot
    ax5_twin = ax5.twinx()
    
    line1 = ax5.plot(monthly_claims.index, monthly_claims['Total_Amount'], 
                     'b-o', linewidth=2, label='Total Amount')
    line2 = ax5_twin.plot(monthly_claims.index, monthly_claims['Count'], 
                         'r-s', linewidth=2, label='Claim Count')
    
    ax5.set_xlabel('Month')
    ax5.set_ylabel('Total Claim Amount ($)', color='blue')
    ax5_twin.set_ylabel('Number of Claims', color='red')
    ax5.set_title('Monthly Claims Trend Analysis')
    ax5.tick_params(axis='y', labelcolor='blue')
    ax5_twin.tick_params(axis='y', labelcolor='red')
    
    # Add trend lines
    z1 = np.polyfit(monthly_claims.index, monthly_claims['Total_Amount'], 1)
    p1 = np.poly1d(z1)
    ax5.plot(monthly_claims.index, p1(monthly_claims.index), "b--", alpha=0.8)
    
    # 6. Processing Flag Analysis
    ax6 = axes[1, 2]
    
    # Processing efficiency analysis
    processing_stats = df.groupby('processing_flag').agg({
        'claim_amount_total': ['mean', 'count'],
        'severity': 'mean'
    }).round(2)
    
    processing_stats.columns = ['Avg_Amount', 'Count', 'Avg_Severity']
    
    # Stacked bar chart
    x_pos = np.arange(len(processing_stats.index))
    
    bars1 = ax6.bar(x_pos, processing_stats['Avg_Amount'], 
                   label='Avg Claim Amount', alpha=0.8, color='lightblue')
    bars2 = ax6.bar(x_pos, processing_stats['Avg_Severity'] * 10000,  # Scale for visibility
                   bottom=processing_stats['Avg_Amount'], 
                   label='Avg Severity (×10k)', alpha=0.8, color='lightcoral')
    
    ax6.set_xlabel('Processing Flag')
    ax6.set_ylabel('Amount ($)')
    ax6.set_title('Processing Flag Analysis\n(Amount vs Severity)')
    ax6.set_xticks(x_pos)
    ax6.set_xticklabels(processing_stats.index, rotation=45, ha='right')
    ax6.legend()
    
    # Add value labels on bars
    for i, bar in enumerate(bars1):
        height = bar.get_height()
        ax6.text(bar.get_x() + bar.get_width()/2., height/2,
                f'${height:,.0f}', ha='center', va='center', fontweight='bold')
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.93)
    
    # Additional insights text
    fig.text(0.02, 0.02, 
             f'Analytics Summary: {len(df)} claims analyzed | Avg Amount: ${df["claim_amount_total"].mean():,.0f} | '
             f'High Risk Claims: {(df["risk_score"] > 60).sum()} ({(df["risk_score"] > 60).mean()*100:.1f}%)',
             fontsize=10, style='italic')
    
    plt.show()

# Advanced Statistical Analysis Function
def create_statistical_analysis():
    """
    Creates statistical analysis visualizations for deeper insights
    """
    df = dataset.copy()
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Statistical Analysis Dashboard', fontsize=14, fontweight='bold')
    
    # 1. Correlation Matrix
    ax1 = axes[0, 0]
    numeric_cols = ['claim_amount_total', 'severity', 'driver_age', 'premium', 'sum_insured']
    available_cols = [col for col in numeric_cols if col in df.columns]
    
    if len(available_cols) >= 2:
        corr_matrix = df[available_cols].corr()
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, 
                   square=True, ax=ax1, fmt='.2f')
        ax1.set_title('Correlation Matrix')
    
    # 2. Box Plot Analysis
    ax2 = axes[0, 1]
    if 'severity_category' in df.columns:
        df_filtered = df[df['claim_amount_total'] <= df['claim_amount_total'].quantile(0.95)]
        box_data = [group['claim_amount_total'].values for name, group in df_filtered.groupby('severity_category')]
        ax2.boxplot(box_data, labels=df_filtered['severity_category'].unique())
        ax2.set_ylabel('Claim Amount ($)')
        ax2.set_xlabel('Severity Category')
        ax2.set_title('Claim Amount by Severity\n(Box Plot Analysis)')
        ax2.tick_params(axis='x', rotation=45)
    
    # 3. Severity Analysis by Risk Category
    ax3 = axes[1, 0]
    
    # Create severity analysis based on available columns
    if 'severity_category' in df.columns and 'risk_category' in df.columns:
        # Create cross-tabulation for heatmap
        severity_risk = pd.crosstab(df['severity_category'], df['risk_category'], 
                                   values=df['claim_amount_total'], aggfunc='mean')
        
        # Create heatmap if enough data
        if severity_risk.shape[0] > 1 and severity_risk.shape[1] > 1:
            sns.heatmap(severity_risk, annot=True, fmt='.0f', cmap='Reds', ax=ax3)
            ax3.set_title('Average Claim Amount\nby Severity and Risk Category')
            ax3.set_xlabel('Risk Category')
            ax3.set_ylabel('Severity Category')
        else:
            # Fallback to simple bar chart
            severity_avg = df.groupby('severity_category')['claim_amount_total'].mean()
            severity_avg.plot(kind='bar', ax=ax3, color='lightcoral')
            ax3.set_title('Average Claim Amount by Severity')
            ax3.set_xlabel('Severity Category')
            ax3.set_ylabel('Average Claim Amount ($)')
            plt.setp(ax3.get_xticklabels(), rotation=45)
    else:
        # Outlier detection if no categorical data available
        from scipy import stats
        z_scores = np.abs(stats.zscore(df['claim_amount_total']))
        threshold = 3
        outliers = df[z_scores > threshold]
        
        ax3.scatter(df.index, df['claim_amount_total'], alpha=0.6, s=20, label='Normal Claims')
        ax3.scatter(outliers.index, outliers['claim_amount_total'], 
                   color='red', s=30, label=f'Outliers (n={len(outliers)})')
        ax3.set_xlabel('Claim Index')
        ax3.set_ylabel('Claim Amount ($)')
        ax3.set_title('Outlier Detection\n(Z-score > 3)')
        ax3.legend()
    
    # 4. Distribution Comparison
    ax4 = axes[1, 1]
    if 'processing_flag' in df.columns:
        for flag in df['processing_flag'].unique()[:3]:  # Limit to 3 categories
            subset = df[df['processing_flag'] == flag]['claim_amount_total']
            subset_filtered = subset[subset <= subset.quantile(0.95)]
            ax4.hist(subset_filtered, alpha=0.5, label=f'{flag} (n={len(subset)})', bins=20)
        
        ax4.set_xlabel('Claim Amount ($)')
        ax4.set_ylabel('Frequency')
        ax4.set_title('Claim Amount Distribution\nby Processing Flag')
        ax4.legend()
    
    plt.tight_layout()
    plt.show()

# Execute the main analysis
try:
    create_claims_analysis_dashboard()
    print("Dashboard created successfully!")
except Exception as e:
    print(f"Error creating dashboard: {str(e)}")
    # Fallback simple visualization using dataset variable
    try:
        plt.figure(figsize=(10, 6))
        dataset['claim_amount_total'].hist(bins=30, alpha=0.7)
        plt.title('Claims Amount Distribution')
        plt.xlabel('Claim Amount ($)')
        plt.ylabel('Frequency')
        plt.show()
    except:
        # If dataset is not defined, show message
        print("Please ensure the dataset is properly loaded in Power BI")

# Optional: Create statistical analysis
create_statistical_analysis()
