from __future__ import annotations

from pathlib import Path


def export_ttl(data_dir: Path, out_ttl: Path) -> None:
    """Application-layer facade (wire to infra exporter)."""
    raise NotImplementedError


def validate_shacl(shapes_ttl: Path, data_ttl: Path) -> None:
    """Run SHACL validation (pyshacl)."""
    raise NotImplementedError
