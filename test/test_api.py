import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from fastapi.testclient import TestClient
from src.main import app
import pytest

from src.main import app

client = TestClient(app)

def test_upload_csv():
    csv_path = os.path.join(os.path.dirname(__file__), "..", "upload-csv", "hired_employees.csv")
    with open(csv_path, "rb") as f:
        response = client.post(
            "/upload-csv/",
            files={"file": ("hired_employees.csv", f, "text/csv")},
            params={"table": "hired_employees"}
        )
    assert response.status_code == 200


def test_hired_by_quarter():
    response = client.get("/hired-by-quarter/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0

def test_above_average_hired():
    response = client.get("/above-average-hired/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0