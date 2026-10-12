"""Descarga de los CSV del MEF a la zona landing.

Los archivos anuales pesan varios GB y el servidor del MEF corta conexiones largas, así que la
descarga es reanudable (HTTP Range), escribe a ``.part`` y solo renombra al terminar. Si el
archivo local ya tiene el mismo tamaño que anuncia el servidor, no se vuelve a bajar.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)

CHUNK = 4 << 20
HEADERS = {"User-Agent": "gasto-lakehouse/0.1 (+https://github.com/Key-qr)"}


def remote_size(url: str, session: requests.Session, timeout: int = 30) -> int | None:
    try:
        r = session.head(url, allow_redirects=True, timeout=timeout)
        r.raise_for_status()
        size = r.headers.get("Content-Length")
        return int(size) if size else None
    except requests.RequestException as exc:
        log.warning("HEAD %s falló: %s", url, exc)
        return None


def download(
    url: str,
    dest_dir: Path,
    filename: str | None = None,
    retries: int = 5,
    timeout: int = 60,
    session: requests.Session | None = None,
) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    final = dest_dir / (filename or url.rsplit("/", 1)[-1])
    part = final.with_name(final.name + ".part")
    session = session or requests.Session()
    session.headers.update(HEADERS)

    expected = remote_size(url, session)
    if final.exists() and expected is not None and final.stat().st_size == expected:
        log.info("%s ya está completo (%s bytes), se omite", final.name, expected)
        return final

    for attempt in range(1, retries + 1):
        offset = part.stat().st_size if part.exists() else 0
        headers = {"Range": f"bytes={offset}-"} if offset else {}
        try:
            with session.get(url, headers=headers, stream=True, timeout=timeout) as r:
                if r.status_code == 416:  # ya estaba todo descargado
                    break
                r.raise_for_status()
                resumed = r.status_code == 206
                mode = "ab" if resumed else "wb"
                if offset and not resumed:
                    log.info("El servidor no soporta Range: se reinicia la descarga")
                with open(part, mode) as fh:
                    for chunk in r.iter_content(CHUNK):
                        fh.write(chunk)
            if expected is None or part.stat().st_size >= expected:
                break
            log.warning("Descarga incompleta (%s/%s), reintentando", part.stat().st_size, expected)
        except requests.RequestException as exc:
            wait = min(2**attempt, 60)
            log.warning("Intento %s/%s falló (%s); reintento en %ss", attempt, retries, exc, wait)
            time.sleep(wait)
    else:
        raise RuntimeError(f"No se pudo descargar {url} tras {retries} intentos")

    if expected is not None and part.stat().st_size != expected:
        raise RuntimeError(
            f"Tamaño inesperado para {final.name}: {part.stat().st_size} != {expected}"
        )
    part.replace(final)
    log.info("Descargado %s (%.1f MB)", final.name, final.stat().st_size / 1e6)
    return final
