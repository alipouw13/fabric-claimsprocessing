# Fabric Workspace Variable Set Configuration Guide

## Setting up Azure AI Foundry Credentials in Microsoft Fabric

This guide walks you through configuring the `FY26INS_env` variable set in your Microsoft Fabric workspace to securely store Azure AI Foundry credentials.

## Step 1: Create the Variable Set

### Using Fabric Portal
1. **Navigate to your Fabric workspace**
2. **Go to Settings** → **Workspace settings**
3. **Select "Environment variables"** or **"Variable sets"**
4. **Click "New variable set"**
5. **Name the variable set:** `FY26INS_env`
6. **Add description:** "Azure AI Foundry credentials for Smart Claims processing"

### Using Fabric API (PowerShell)
```powershell
# Install Fabric PowerShell module if not already installed
Install-Module -Name Microsoft.PowerBI.DataPipelines

# Connect to Fabric
Connect-FabricServiceAccount

# Create variable set
$variableSet = @{
    name = "FY26INS_env"
    description = "Azure AI Foundry credentials for Smart Claims processing"
    variables = @()
}

New-FabricVariableSet -WorkspaceId "your-workspace-id" -VariableSet $variableSet
```

## Step 2: Add Required Variables

Add the following variables to your `FY26INS_env` variable set:

### Variable 1: AZURE_AI_FOUNDRY_ENDPOINT
- **Name:** `AZURE_AI_FOUNDRY_ENDPOINT`
- **Type:** String
- **Value:** Your Azure AI Foundry endpoint URL
- **Example:** `https://your-resource.services.ai.azure.com`
- **Security:** Can be visible (endpoint URLs are not sensitive)

### Variable 2: AZURE_AI_FOUNDRY_KEY
- **Name:** `AZURE_AI_FOUNDRY_KEY`
- **Type:** Secret/String
- **Value:** Your Azure AI Foundry API key
- **Security:** ⚠️ **Mark as Secret** - This should be encrypted

## Step 3: Get Your Azure AI Foundry Credentials

### From Azure AI Foundry Studio
1. **Go to Azure AI Foundry Studio** (https://ai.azure.com)
2. **Select your project**
3. **Navigate to "Deployments"**
4. **Click on your deployed model**
5. **Copy the endpoint URL and API key**

### From Azure Portal
1. **Open Azure Portal** (https://portal.azure.com)
2. **Navigate to your Azure AI service resource**
3. **Go to "Keys and Endpoint"**
4. **Copy Key 1 or Key 2 and the Endpoint**

## Step 4: Configure Variable Set Access

### Grant Notebook Access
1. **In your variable set settings**
2. **Go to "Access permissions"**
3. **Add your notebook/pipeline** to allowed resources
4. **Set permission level** to "Read" (minimum required)

### Using Fabric APIs
```python
# Grant access to specific notebooks or pipelines
fabric_client.grant_variable_set_access(
    workspace_id="your-workspace-id",
    variable_set_name="FY26INS_env",
    principal_id="notebook-id",
    permission="Read"
)
```

## Step 5: Test the Configuration

### Test in Fabric Notebook
```python
# Test credential access
try:
    endpoint = notebookutils.credentials.getSecret("FY26INS_env", "AZURE_AI_FOUNDRY_ENDPOINT")
    key = notebookutils.credentials.getSecret("FY26INS_env", "AZURE_AI_FOUNDRY_KEY")
    
    print("✅ Credentials successfully retrieved")
    print(f"Endpoint: {endpoint[:50]}...")  # Show partial for security
    print(f"Key length: {len(key)} characters")
    
except Exception as e:
    print(f"❌ Error accessing credentials: {e}")
```

## Step 6: Update ML Configuration

Once the variable set is configured, your Smart Claims notebook will automatically use these credentials:

```python
# Configuration will be automatically loaded
ml_config = {
    "use_simulated_predictions": False,  # Enable real AI model
    "use_foundry_model": True,          # Use Azure AI Foundry
    "model_name": "gpt-4o-vision",      # Your deployed model
    "batch_size": 10,
    "max_retries": 3
}
```

## Security Best Practices

### 1. Secure Secret Management
- ✅ Always mark API keys as **Secret** type
- ✅ Use least-privilege access (Read-only for notebooks)
- ✅ Regularly rotate API keys
- ✅ Monitor variable set access logs

### 2. Environment Separation
```python
# Create separate variable sets for different environments
# Development: FY26INS_env_dev
# Testing: FY26INS_env_test  
# Production: FY26INS_env_prod
```

### 3. Access Control
- ✅ Limit variable set access to specific notebooks
- ✅ Use Azure AD groups for team access
- ✅ Regular access reviews
- ✅ Audit trail monitoring

## Troubleshooting

### Common Issues

#### 1. "Variable set not found"
```python
# Check if variable set exists
available_sets = notebookutils.credentials.listVariableSets()
print("Available variable sets:", available_sets)
```

**Solution:**
- Verify variable set name spelling: `FY26INS_env`
- Check workspace permissions
- Ensure variable set is created in correct workspace

#### 2. "Access denied"
```python
# Check current notebook permissions
try:
    test_access = notebookutils.credentials.getSecret("FY26INS_env", "test")
except Exception as e:
    print(f"Access error: {e}")
```

**Solution:**
- Grant notebook access to variable set
- Check Azure AD permissions
- Verify workspace role assignments

#### 3. "Secret not found"
```python
# List available variables in set
try:
    variables = notebookutils.credentials.listSecrets("FY26INS_env")
    print("Available variables:", variables)
except Exception as e:
    print(f"Cannot list variables: {e}")
```

**Solution:**
- Check variable names: `AZURE_AI_FOUNDRY_ENDPOINT`, `AZURE_AI_FOUNDRY_KEY`
- Verify variables are created in correct variable set
- Check variable type (String vs Secret)

## Alternative Credential Methods

### Method 1: Azure Key Vault Integration
```python
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://your-keyvault.vault.azure.net/", credential=credential)

foundry_endpoint = client.get_secret("foundry-endpoint").value
foundry_key = client.get_secret("foundry-key").value
```

### Method 2: Environment Variables (Development)
```python
import os

# For development/testing only
foundry_endpoint = os.environ.get("AZURE_AI_FOUNDRY_ENDPOINT")
foundry_key = os.environ.get("AZURE_AI_FOUNDRY_KEY")
```

### Method 3: Azure Managed Identity
```python
from azure.identity import ManagedIdentityCredential
from azure.ai.inference import ChatCompletionsClient

# Use managed identity for authentication
credential = ManagedIdentityCredential()
client = ChatCompletionsClient(
    endpoint="https://your-resource.services.ai.azure.com",
    credential=credential
)
```

## Monitoring and Maintenance

### 1. Regular Health Checks
```python
def test_ai_foundry_connection():
    """Test Azure AI Foundry connectivity"""
    try:
        endpoint = notebookutils.credentials.getSecret("FY26INS_env", "AZURE_AI_FOUNDRY_ENDPOINT")
        key = notebookutils.credentials.getSecret("FY26INS_env", "AZURE_AI_FOUNDRY_KEY")
        
        client = ChatCompletionsClient(endpoint=endpoint, credential=AzureKeyCredential(key))
        
        # Test with simple request
        response = client.complete(
            model="gpt-4o",
            messages=[{"role": "user", "content": "test"}],
            max_tokens=1
        )
        
        return True, "Connection successful"
    except Exception as e:
        return False, str(e)

# Run health check
success, message = test_ai_foundry_connection()
print(f"Health check: {'✅' if success else '❌'} {message}")
```

### 2. Cost Monitoring
```python
# Add cost tracking to your ML configuration
ml_config = {
    "cost_tracking": True,
    "daily_budget_limit": 100.0,  # USD
    "alert_threshold": 80.0       # 80% of budget
}
```

### 3. Usage Analytics
```python
# Log API usage for monitoring
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ai_foundry_usage")

def log_api_call(model, tokens_used, cost_estimate):
    logger.info(f"AI Foundry API call - Model: {model}, Tokens: {tokens_used}, Cost: ${cost_estimate:.4f}")
```

## Next Steps

1. ✅ Create `FY26INS_env` variable set in your Fabric workspace
2. ✅ Add `AZURE_AI_FOUNDRY_ENDPOINT` and `AZURE_AI_FOUNDRY_KEY` variables
3. ✅ Grant access to your Smart Claims notebook
4. ✅ Test credential access using the test script
5. ✅ Update `ml_config` to enable Azure AI Foundry integration
6. ✅ Run the enhanced severity prediction pipeline

Your Smart Claims solution will now use Azure AI Foundry's advanced computer vision models for accurate damage assessment while maintaining secure credential management through Fabric's variable set system.
