# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Exercise public hosts through the ordinary native ADBC driver manager."""

import os
from pathlib import Path

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as adbc
import pytest
from test_service import TestWorker
from test_tcp import certificates, tls_config  # noqa: F401 -- Share the module-scoped fixture.

from grainlift import Service, TcpLimits, TcpServer


def test_native_reuse_partial_results_and_server_restart(certificates: Path) -> None:  # noqa: F811
    """Reuse completed streams, discard partial streams and invalidate handles on restart."""
    driver = os.environ.get("GRAINLIFT_NATIVE_DRIVER")
    if not driver:
        pytest.skip("Set GRAINLIFT_NATIVE_DRIVER to the compiled Grainlift shared library")
    limits = TcpLimits(drain_seconds=0.01)
    first = TcpServer(Service(TestWorker()), tls=tls_config(certificates), limits=limits).start()
    port = first.address[1]
    options = {
        "grainlift.uri": f"tls+tcp://127.0.0.1:{port}",
        "grainlift.target": "default",
        "grainlift.tls.ca": str(certificates / "ca.pem"),
        "grainlift.tls.cert": str(certificates / "client.pem"),
        "grainlift.tls.key": str(certificates / "client-key.pem"),
        "grainlift.tls.server_name": "localhost",
    }
    try:
        with (
            adbc.connect(
                driver=driver, entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True
            ) as connection,
            connection.cursor() as cursor,
        ):
            for _ in range(10):
                cursor.execute("query")
                assert cursor.fetch_arrow_table().column(0).to_pylist() == [1, 2, 3]
            assert first.statistics["opened"] == 2
            cursor.execute("query")
            with cursor.fetch_record_batch() as reader:
                assert reader.read_next_batch().num_rows == 2
            cursor.execute("query")
            assert cursor.fetch_arrow_table().num_rows == 3
            assert first.statistics["opened"] == 3
            first.close()
            with TcpServer(Service(TestWorker()), port=port, tls=tls_config(certificates), limits=limits) as second:
                with pytest.raises(manager.Error):
                    cursor.execute("query")
                with (
                    adbc.connect(
                        driver=driver, entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True
                    ) as fresh,
                    fresh.cursor() as query,
                ):
                    query.execute("query")
                    assert query.fetch_arrow_table().num_rows == 3
                assert second.statistics["opened"] == 2
    finally:
        first.close()
    assert not first.service._sessions
    assert first.statistics["active"] == 0
