from prefect import flow, task
import requests
from google.cloud import secretmanager

def get_secret(secret_id, project_id="ba882-team9"):
    """Helper function to access secrets from Secret Manager"""
    sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = sm.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def send_slack_alert(message: str):
    """Helper function to send Slack notifications"""
    slack_webhook_url = get_secret(secret_id="SLACK_WEBHOOK_URL")
    payload = {"text": message}
    try:
        response = requests.post(slack_webhook_url, json=payload)
        response.raise_for_status()
        print("Slack alert sent successfully")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Slack alert: {e}")

def invoke_service(url: str, method: str = "POST", payload: dict = None):
    """Helper function to invoke Cloud Run services"""
    try:
        if method == "POST":
            response = requests.post(url, json=payload or {})
        else:
            response = requests.get(url, params=payload or {})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Service invocation failed: {e}")
        raise

@task(retries=3)
def train_model():
    """Train ML models"""
    url = "https://ml-sf-train-1076180164120.us-central1.run.app/train"
    response = invoke_service(url, method="POST")
    send_slack_alert("Model training completed successfully.")
    return response

@task(retries=3)
def make_predictions():
    """Generate predictions"""
    url = "https://ml-sf-predict-1076180164120.us-central1.run.app/predict"
    response = invoke_service(url, method="POST")
    send_slack_alert("Predictions generated successfully.")
    return response

@flow(name="ml-training-prediction-flow", log_prints=True)
def ml_pipeline():
    """Main ML pipeline flow"""
    try:
        send_slack_alert("🚀 Starting ML pipeline run")

        # Train models
        training_result = train_model()
        print("Model training completed")
        print(f"Training result: {training_result}")

        # Generate predictions
        prediction_result = make_predictions()
        print("Predictions generated")
        print(f"Prediction result: {prediction_result}")

        send_slack_alert("✅ ML pipeline completed successfully!")

    except Exception as e:
        send_slack_alert(f"❌ ML pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    ml_pipeline()