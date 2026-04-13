"""Generate health recommendations via the Anthropic API."""

import os

import anthropic
from dotenv import load_dotenv

load_dotenv(dotenv_path="/home/bashr/projects/VitalSync/.env")

ANTHROPIC_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 500

HEALTH_BASELINES = {
    "heart_rate": "Normal resting heart rate: 60-100 bpm. Below 60 may indicate bradycardia; above 100 may indicate tachycardia.",
    "spo2": "Normal SpO2: 95-100%. Below 95% may indicate hypoxemia and warrants medical attention.",
    "steps": "Recommended daily steps: 8,000-10,000. Fewer than 5,000 is considered sedentary.",
}


def get_recommendation(user_id: str, metric_name: str, data_summary: dict) -> str:
    """Call the Anthropic API to produce a personalized health improvement plan.

    Args:
        user_id: Identifier for the user.
        metric_name: One of heart_rate, steps, spo2.
        data_summary: Dict with min, max, avg, latest, trend, window_count.

    Returns:
        The recommendation text from Claude.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    baseline = HEALTH_BASELINES.get(metric_name, "No specific baseline available.")

    prompt = (
        f"You are a health-data assistant for VitalSync. A user (ID: {user_id}) has "
        f"the following aggregated data for '{metric_name}' over the last 24 hours:\n\n"
        f"  - Minimum: {data_summary['min']}\n"
        f"  - Maximum: {data_summary['max']}\n"
        f"  - Average: {data_summary['avg']}\n"
        f"  - Latest value: {data_summary['latest']}\n"
        f"  - Trend: {data_summary['trend']}\n"
        f"  - Data points (windows): {data_summary['window_count']}\n\n"
        f"General health baseline for this metric:\n{baseline}\n\n"
        f"Based on the data above, provide a realistic and personalized improvement "
        f"plan with 3-5 actionable tips. Be concise and practical. Do not provide "
        f"medical diagnoses — frame advice as general wellness suggestions."
    )

    message = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=MAX_TOKENS,
        messages=[{"role": "user", "content": prompt}],
    )

    return message.content[0].text
