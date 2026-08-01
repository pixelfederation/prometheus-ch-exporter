import kopf

from promch import leadership


def _reset_active() -> None:
    """Normalize the module-global flag before each test (default is active)."""
    leadership.mark_active()


def test_active_by_default() -> None:
    _reset_active()
    assert leadership.is_active() is True


def test_mark_standby_forces_standby() -> None:
    _reset_active()
    leadership.mark_standby()
    assert leadership.is_active() is False


def test_never_active_pod_stays_standby() -> None:
    """A pod that boots standby (peering) and never runs a daemon must stay 0:
    mark_standby at startup, then no mark_active is ever called."""
    _reset_active()
    leadership.mark_standby()  # startup under peering
    # no daemon ever starts (frozen watch) -> nothing flips us active
    assert leadership.is_active() is False


def test_operator_pausing_flips_to_standby() -> None:
    _reset_active()
    leadership.note_daemon_stopped(kopf.DaemonStoppingReason.OPERATOR_PAUSING)
    assert leadership.is_active() is False


def test_resource_deleted_does_not_change_leadership() -> None:
    _reset_active()
    leadership.note_daemon_stopped(kopf.DaemonStoppingReason.RESOURCE_DELETED)
    assert leadership.is_active() is True


def test_operator_exiting_does_not_change_leadership() -> None:
    _reset_active()
    leadership.note_daemon_stopped(kopf.DaemonStoppingReason.OPERATOR_EXITING)
    assert leadership.is_active() is True


def test_none_reason_does_not_change_leadership() -> None:
    _reset_active()
    leadership.note_daemon_stopped(None)
    assert leadership.is_active() is True


def test_mark_active_resumes_after_pause() -> None:
    _reset_active()
    leadership.note_daemon_stopped(kopf.DaemonStoppingReason.OPERATOR_PAUSING)
    assert leadership.is_active() is False
    leadership.mark_active()  # kopf respawns the daemon on resume
    assert leadership.is_active() is True


def test_combined_reason_flag_containing_pausing_flips() -> None:
    """kopf reasons are a Flag and can be OR-combined; OPERATOR_PAUSING counts."""
    _reset_active()
    reason = kopf.DaemonStoppingReason.OPERATOR_PAUSING | kopf.DaemonStoppingReason.DAEMON_CANCELLED
    leadership.note_daemon_stopped(reason)
    assert leadership.is_active() is False
