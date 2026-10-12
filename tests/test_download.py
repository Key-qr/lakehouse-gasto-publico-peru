"""La descarga se prueba contra un servidor HTTP local que soporta Range, como el del MEF."""

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from gasto_lakehouse.ingestion.download import download

PAYLOAD = b"ANO_EJE,MES_EJE\n" + b"2024,1\n" * 50_000


class RangeHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.end_headers()

    def do_GET(self):
        start = 0
        rng = self.headers.get("Range")
        if rng:
            start = int(rng.split("=")[1].split("-")[0])
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{len(PAYLOAD) - 1}/{len(PAYLOAD)}")
        else:
            self.send_response(200)
        body = PAYLOAD[start:]
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture(scope="module")
def server():
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), RangeHandler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def test_descarga_completa(server, tmp_path):
    p = download(f"{server}/2024-Gasto.csv", tmp_path)
    assert p.read_bytes() == PAYLOAD
    assert not (tmp_path / "2024-Gasto.csv.part").exists()


def test_reanuda_desde_archivo_parcial(server, tmp_path):
    (tmp_path / "2024-Gasto.csv.part").write_bytes(PAYLOAD[:1000])
    p = download(f"{server}/2024-Gasto.csv", tmp_path)
    assert p.read_bytes() == PAYLOAD


def test_no_rebaja_si_ya_esta_completo(server, tmp_path):
    p = download(f"{server}/2024-Gasto.csv", tmp_path)
    mtime = p.stat().st_mtime_ns
    download(f"{server}/2024-Gasto.csv", tmp_path)
    assert p.stat().st_mtime_ns == mtime
