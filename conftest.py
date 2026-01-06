import logging

import pytest
from opensearchpy import OpenSearch


def pytest_addoption(parser):
    parser.addoption("--host", action="store", default="vpc-prod-data-search-aj3e6jdscdw5qqcoitbop3f64m.ap-northeast-2.es.amazonaws.com", help="OpenSearch host")
    parser.addoption("--port", action="store", default=443, type=int, help="OpenSearch port")


@pytest.fixture
def opensearch_client(request):
    host = request.config.getoption("--host")
    port = request.config.getoption("--port")
    logging.info(f"Connecting to OpenSearch at {host}:{port}")
    return OpenSearch(hosts=[{"host": host, "port": port}], use_ssl=True, verify_certs=True, http_compress=True)
