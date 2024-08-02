from fastapi.testclient import TestClient
from schemathesis.specs.openapi.loaders import from_asgi
from syte_pipeline.app import app

app.openapi_version = "3.0.2"  # Required since schemathesis thinks it can't support 3.1


# Use property base testing of the entire API using schemathesis
schema = from_asgi("/openapi.json", app)
client = TestClient(app)


@schema.parametrize()
def test_property_base(case) -> None:
    response = case.call_asgi()
    case.validate_response(response)


def test_get_aircraft_endpoint():
    response = client.get("/api/s1/aircraft/?num_results=2&page=0")
    assert response.status_code == 200


def test_get_aircraft_data():
    response = client.get("/api/s1/aircraft/?num_results=2&page=0")
    assert len(response.json()) == 2
