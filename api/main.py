"""FastAPI application for the VitalSync recommendation API."""

from dotenv import load_dotenv

from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from api.recommender import get_recommendation
from api.s3_reader import get_available_users, get_user_data

VALID_METRICS = ["heart_rate", "steps", "spo2"]

app = FastAPI(title="VitalSync Recommendation API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health_check():
    return {"status": "ok"}


@app.get("/api/metrics")
def list_metrics():
    return VALID_METRICS


@app.get("/api/users")
def list_users():
    try:
        users = get_available_users()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return users


@app.get("/api/recommendations")
def recommendations(
    user_id: str = Query(..., description="User identifier"),
    metric_name: str = Query(..., description="Metric name: heart_rate, steps, or spo2"),
):
    if metric_name not in VALID_METRICS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric_name '{metric_name}'. Must be one of {VALID_METRICS}.",
        )

    try:
        data_summary = get_user_data(user_id, metric_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    try:
        recommendation = get_recommendation(user_id, metric_name, data_summary)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Recommendation generation failed: {exc}",
        )

    return {
        "user_id": user_id,
        "metric_name": metric_name,
        "data_summary": data_summary,
        "recommendation": recommendation,
    }
