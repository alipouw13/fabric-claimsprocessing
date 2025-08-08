# Damage Severity Model Artifacts Guide (Fabric Lakehouse)

Place model artifacts under the Lakehouse Files area so the import script (`setup/import_model.py`) can register them with MLflow.

## Directory Structure (Recommended)
```
Files/
  model_artifacts/
    damage_severity_model/
      MLmodel
      model.pkl              (or model.pt / model.joblib)
      conda.yaml             (or python_env.yaml)
      requirements.txt       (optional – pip fallback)
      model.json             (optional – lightweight heuristic config)
      README.md              (optional documentation)
```

## Option A: Full MLflow Model (Preferred)
A minimal `MLmodel` file example:
```
artifact_path: model
flavors:
  python_function:
    env: conda.yaml
    loader_module: mlflow.pyfunc.model
    python_version: 3.10
    data: model.pkl
run_id: placeholder-run-id
model_uuid: placeholder-uuid
mlflow_version: 2.x.x
```

Minimal `conda.yaml` example:
```yaml
name: damage-severity-env
dependencies:
  - python=3.10
  - pip
  - pip:
    - mlflow
    - pandas
    - scikit-learn   # if needed
```

If you use `python_env.yaml` instead of `conda.yaml`, MLflow will honor that.

Include the actual serialized model file (`model.pkl`, `model.pt`, etc.).

## Option B: Lightweight Wrapper (JSON Only)
If you do not have a serialized model yet, supply only:
```
Files/model_artifacts/damage_severity_model/model.json
```
Example `model.json`:
```json
{
  "version": "1.0",
  "description": "Heuristic damage severity scoring based on image name tokens",
  "base_scores": {"High": 0.85, "Medium": 0.55, "Low": 0.25},
  "jitter": 0.05
}
```
The import script detects absence of `MLmodel` and wraps this JSON inside a custom `PythonModel` that produces severity scores.

## Large Model Extras (Optional)
Add any preprocessing assets:
```
preprocessing/
  label_encoder.pkl
  image_normalization.json
```
These will be logged as extra artifacts if you extend the import script.

## Steps to Upload in Fabric
1. Open Lakehouse → Files tab.
2. Create folders: `model_artifacts/damage_severity_model`.
3. Upload the chosen artifacts (Option A or B).
4. Run notebook: `setup/import_model.py`.
5. Confirm registration in MLflow Models pane.

## Verifying Registration
In a Fabric notebook:
```python
import mlflow
model = mlflow.pyfunc.load_model("models:/damage_severity_model/Production")
print(model.predict({"image_name": ["test_High.jpg"]}))
```

## Integration With Processing (05b Script)
The severity prediction script attempts to load the registered model if available:
1. Tries `models:/damage_severity_model/Production`.
2. Falls back to simulated predictions if load fails.

Adjust stage or name via environment variables:
```
DAMAGE_SEVERITY_MODEL_NAME=damage_severity_model
DAMAGE_SEVERITY_MODEL_STAGE=Production
```

## Frequently Asked Questions
| Question | Answer |
|----------|--------|
| Do I need both `MLmodel` and `model.json`? | No. Use full MLflow artifacts OR the simple JSON wrapper approach. |
| Can I add extra Python deps later? | Yes – update `conda.yaml` (or `python_env.yaml`) and re-import. |
| How do I update the model? | Replace artifacts, re-run import script – new version registered automatically. |
| What if the stage promotion fails? | Model remains registered; you can manually promote in the UI. |

## Next Steps
- Replace heuristic wrapper with real trained model artifacts.
- Add monitoring (log metrics per batch) in silver layer pipeline.
- Version control model artifact folder in a separate storage or repo.

---
This guide ensures consistent, reproducible model registration in Microsoft Fabric.
