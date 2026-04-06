"""Tests for ReductionService ZIP validation."""
import pytest
from backend.services.reduction_service import ReductionService
from backend.services.job_store import ReductionProgressStore


@pytest.fixture
def service():
    return ReductionService(progress_store=ReductionProgressStore())


def test_reject_non_zip_bytes(service):
    """Should reject files with invalid ZIP magic signature."""
    invalid_data = b"\x82\x00\x01\x02"  # Invalid magic bytes
    with pytest.raises(Exception) as exc_info:
        service.reduce_uploaded_zip(zip_bytes=invalid_data, compact=False)

    assert exc_info.value.status_code == 422
    assert "not a valid ZIP archive" in str(exc_info.value.detail)


def test_reject_empty_bytes(service):
    """Should reject empty byte arrays."""
    with pytest.raises(Exception) as exc_info:
        service.reduce_uploaded_zip(zip_bytes=b"", compact=False)

    assert exc_info.value.status_code == 422
    assert "empty" in str(exc_info.value.detail)


def test_accept_zip_magic_signature(service):
    """Should accept files with valid ZIP magic signature (PK)."""
    # Minimal ZIP-like structure with PK signature
    # This will fail later in processing, but should pass magic check
    zip_bytes = b"PK\x05\x06" + b"\x00" * 18  # Empty ZIP end of central dir

    # Empty ZIP with valid signature should process without error
    result = service.reduce_uploaded_zip(zip_bytes=zip_bytes, compact=False)
    assert "summary" in result
    assert "reduced_report" in result
