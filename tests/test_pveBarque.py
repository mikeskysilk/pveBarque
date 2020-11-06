import pytest
import pveBarque.pveBarque as pveBarque


@pytest.fixture
def app():
    app = pveBarque.pveBarque.app.test_client()
    return app


def test_example(app):
    response = app.get("/barque/info")
    assert response.status_code == 401
