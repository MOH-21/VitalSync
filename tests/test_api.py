"""Integration tests for the VitalSync recommendation API."""

import pytest
import requests


class TestHealthEndpoint:
    def test_health(self, api_base_url):
        resp = requests.get(f"{api_base_url}/api/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestMetricsEndpoint:
    def test_returns_three_metrics(self, api_base_url):
        resp = requests.get(f"{api_base_url}/api/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert set(data) == {"heart_rate", "steps", "spo2"}


class TestUsersEndpoint:
    def test_returns_list(self, api_base_url):
        resp = requests.get(f"{api_base_url}/api/users")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) >= 1


class TestRecommendations:
    def test_valid_request(self, api_base_url):
        resp = requests.get(
            f"{api_base_url}/api/recommendations",
            params={"user_id": "user_001", "metric_name": "heart_rate"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "recommendation" in data
        assert "data_summary" in data
        assert data["user_id"] == "user_001"
        assert data["metric_name"] == "heart_rate"

    def test_response_shape(self, api_base_url):
        resp = requests.get(
            f"{api_base_url}/api/recommendations",
            params={"user_id": "user_001", "metric_name": "spo2"},
        )
        assert resp.status_code == 200
        summary = resp.json()["data_summary"]
        for key in ["min", "max", "avg", "latest", "trend"]:
            assert key in summary

    def test_invalid_metric(self, api_base_url):
        resp = requests.get(
            f"{api_base_url}/api/recommendations",
            params={"user_id": "user_001", "metric_name": "invalid"},
        )
        assert resp.status_code == 400

    def test_invalid_user(self, api_base_url):
        resp = requests.get(
            f"{api_base_url}/api/recommendations",
            params={"user_id": "nonexistent", "metric_name": "heart_rate"},
        )
        assert resp.status_code == 404
