from app.models import BatchStatus
from app.repositories.jobs import derive_batch_status


def test_batch_is_partial_when_indexed_and_failed_jobs_remain() -> None:
    assert derive_batch_status(total=3, indexed=2, failed=1) is BatchStatus.PARTIAL


def test_batch_is_pending_when_no_job_is_terminal() -> None:
    assert derive_batch_status(total=2, indexed=0, failed=0) is BatchStatus.PENDING


def test_batch_is_completed_when_every_job_is_indexed() -> None:
    assert derive_batch_status(total=2, indexed=2, failed=0) is BatchStatus.COMPLETED


def test_empty_batch_is_completed() -> None:
    assert derive_batch_status(total=0, indexed=0, failed=0) is BatchStatus.COMPLETED


def test_batch_is_failed_when_every_job_failed() -> None:
    assert derive_batch_status(total=2, indexed=0, failed=2) is BatchStatus.FAILED
