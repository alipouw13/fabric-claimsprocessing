# Azure AI Foundry Integration Guide for Smart Claims

## Overview

This guide explains how to integrate Azure AI Foundry models into your Smart Claims severity prediction pipeline. Azure AI Foundry provides access to 1900+ state-of-the-art AI models for enhanced damage assessment accuracy.

## Azure AI Foundry Benefits

### **Model Catalog Access**
- **11,000+ Models**: Access to Microsoft's curated model catalog including:
  - Vision models (GPT-4o Vision, Computer Vision APIs)
  - Language models (GPT-5, Llama, Mistral...)
  - Specialized insurance/damage assessment models
  - Custom fine-tuned models

### **Deployment Options**
- **Serverless**: Pay-per-use, auto-scaling, no infrastructure management
- **Managed Endpoints**: Dedicated compute with guaranteed throughput
- **Standard Deployment**: Traditional Azure ML deployment with full control

### **Enterprise Features**
- **Unified API**: Single endpoint for multiple models
- **Model Evaluation**: Built-in comparison and benchmarking tools
- **Safety & Compliance**: Content filtering and responsible AI features
- **Cost Management**: Usage tracking and budget controls

### **MLflow Integration**
- **Native Integration**: Azure AI Foundry natively supports MLflow
- **Model Registry**: Centralized model versioning and lifecycle management
- **Experiment Tracking**: Log and compare model performance
- **Deployment Pipeline**: Seamless model promotion from dev to production

## Integration Options

### Option 1: Azure AI Foundry (Recommended)

**Best for**: Production scenarios requiring high accuracy and enterprise features

```python
# Configuration in 05b_severity_prediction_silver_fabric.py
ml_config = {
    "use_simulated_predictions": False,
    "use_foundry_model": True,
    "foundry_endpoint": "https://your-endpoint.inference.ml.azure.com",
    "foundry_key": "your-api-key",
    "model_name": "gpt-4o-vision"  # or your deployed model
}
```

**Advantages**:
- Latest open-source models: chat, vision, embedding, etc.
- No infrastructure management
- Enterprise-grade security and compliance

**Implementation Steps**:
1. Deploy vision model in Azure AI Foundry
2. Configure endpoint and authentication
3. Update `ml_config` in severity prediction script
4. Test with sample images

### Option 2: MLflow Registry

**Best for**: Teams already using MLflow with custom models

```python
ml_config = {
    "use_simulated_predictions": False,
    "use_foundry_model": False,
    "mlflow_model_name": "severity-prediction-v2",
    "mlflow_stage": "Production"
}
```

**Advantages**:
- ✅ Custom model deployment
- ✅ Full control over model architecture
- ✅ Integrated with existing MLflow workflows
- ✅ Version control and A/B testing

### Option 3: Hybrid Approach

**Best for**: Gradual migration and comparison testing

```python
ml_config = {
    "use_simulated_predictions": False,
    "use_foundry_model": True,
    "fallback_to_mlflow": True,  # Use MLflow if Foundry fails
    "compare_models": True       # Log predictions from both models
}
```

## Setup Instructions

### 1. Azure AI Foundry Setup

```bash
# Install Azure AI Foundry SDK
pip install azure-ai-inference azure-identity

# Login to Azure
az login

# Create AI Foundry project (if not exists)
az ml workspace create --name smart-claims-ai --resource-group your-rg
```

### 2. Deploy Vision Model

1. **Navigate to Azure AI Foundry Studio**
2. **Browse Model Catalog**
   - Search for "vision"
   - Select appropriate model (e.g., GPT-4o Vision)
3. **Deploy Model**
   - Choose deployment type (Serverless recommended)
   - Configure endpoint settings
   - Note endpoint URL and API key

### 3. Configure Fabric Notebook

Update the configuration section in `05b_severity_prediction_silver_fabric.py`:

```python
# Azure AI Foundry Configuration
ml_config = {
    "use_simulated_predictions": False,
    "use_foundry_model": True,
    
    # Azure AI Foundry Settings
    "foundry_endpoint": "https://your-endpoint.inference.ml.azure.com",
    "foundry_key": "your-api-key",  # Store in Key Vault in production
    "model_name": "gpt-4o-vision",
    
    # Prediction Settings
    "batch_size": 10,
    "max_retries": 3,
    "timeout_seconds": 30,
    
    # Quality Settings
    "confidence_threshold": 0.8,
    "fallback_enabled": True
}
```

### 4. Security Best Practices

```python
# Use Azure Key Vault for secrets
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://your-keyvault.vault.azure.net/", credential=credential)

ml_config = {
    "foundry_endpoint": client.get_secret("foundry-endpoint").value,
    "foundry_key": client.get_secret("foundry-api-key").value
}
```

## Model Performance Comparison

### Accuracy Expectations

| Model Type | Accuracy | Cost | Latency | Best Use Case |
|------------|----------|------|---------|---------------|
| **Azure AI Foundry Vision** | 95%+ | Medium | Low | Production, high accuracy required |
| **Custom MLflow Model** | 80-90% | Low | Medium | Custom domains, cost-sensitive |
| **Simulated** | 60-70% | None | Instant | Development, testing |

### Cost Optimization

```python
# Implement smart batching and caching
ml_config = {
    "use_foundry_model": True,
    "enable_caching": True,           # Cache results for duplicate images
    "batch_processing": True,         # Process multiple images per API call
    "quality_filtering": True,        # Only process high-quality images
    "cost_threshold": 100.0          # Daily cost limit in USD
}
```

## Monitoring and Observability

### 1. Performance Metrics

```python
# Add to your pipeline monitoring
metrics = {
    "prediction_accuracy": 0.95,
    "api_latency_ms": 250,
    "cost_per_prediction": 0.02,
    "error_rate": 0.001,
    "throughput_per_hour": 1000
}
```

### 2. Model Drift Detection

```python
# Monitor prediction distribution changes
from scipy import stats

def detect_model_drift(current_predictions, baseline_predictions):
    """Detect if model predictions have drifted from baseline"""
    statistic, p_value = stats.ks_2samp(current_predictions, baseline_predictions)
    drift_detected = p_value < 0.05
    return drift_detected, p_value
```

### 3. Error Handling and Resilience

```python
# Implement robust error handling
class ModelInferenceError(Exception):
    pass

def predict_with_resilience(image_data):
    for attempt in range(ml_config["max_retries"]):
        try:
            return foundry_client.predict(image_data)
        except Exception as e:
            if attempt == ml_config["max_retries"] - 1:
                logger.error(f"All retry attempts failed: {e}")
                # Fallback to simulated prediction
                return simulate_severity_prediction(image_data)
            time.sleep(2 ** attempt)  # Exponential backoff
```

## Migration Strategy

### Phase 1: Parallel Testing (Weeks 1-2)
- Run both simulated and AI Foundry models
- Compare predictions and accuracy
- Monitor costs and performance

### Phase 2: Gradual Rollout (Weeks 3-4)
- Start with 10% of traffic to AI Foundry
- Increase to 50% based on performance
- Full migration when confidence is high

### Phase 3: Optimization (Weeks 5-6)
- Fine-tune model parameters
- Implement cost optimizations
- Add advanced monitoring

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   ```bash
   # Verify Azure credentials
   az account show
   az ml workspace show --name smart-claims-ai
   ```

2. **Model Endpoint Not Found**
   ```python
   # Test endpoint availability
   import requests
   response = requests.get(f"{foundry_endpoint}/health")
   print(f"Status: {response.status_code}")
   ```

3. **Rate Limiting**
   ```python
   # Implement exponential backoff
   import time
   from random import uniform
   
   def backoff_strategy(attempt):
       return min(300, (2 ** attempt) + uniform(0, 1))
   ```

### Performance Optimization

1. **Batch Processing**
   ```python
   # Process multiple images in single API call
   def batch_predict(image_batch):
       return foundry_client.batch_predict(image_batch, batch_size=10)
   ```

2. **Caching Strategy**
   ```python
   # Implement Redis caching for repeated images
   import redis
   cache = redis.Redis(host='your-redis-endpoint')
   
   def cached_predict(image_hash):
       cached_result = cache.get(image_hash)
       if cached_result:
           return json.loads(cached_result)
       # Make prediction and cache result
   ```

## Next Steps

1. **Choose Integration Option** based on your requirements
2. **Set up Azure AI Foundry** workspace and deploy models
3. **Update Configuration** in Fabric notebooks
4. **Test Pipeline** with sample data
5. **Monitor Performance** and optimize as needed

## Support Resources

- **Azure AI Foundry Documentation**: [docs.microsoft.com/azure/ai-foundry](https://docs.microsoft.com/azure/ai-foundry)
- **MLflow Integration Guide**: [mlflow.org/docs/latest/models.html](https://mlflow.org/docs/latest/models.html)
- **Computer Vision Best Practices**: [docs.microsoft.com/azure/cognitive-services/computer-vision](https://docs.microsoft.com/azure/cognitive-services/computer-vision)

---

*This guide provides a comprehensive approach to integrating Azure AI Foundry into your Smart Claims pipeline. Choose the integration option that best fits your technical requirements, budget, and timeline.*
