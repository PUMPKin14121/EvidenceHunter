"""Bounded local control tests: no live network and no formal Dataset."""
import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
import uuid

import pytest
import EvidenceHunter_collector as collector
import EvidenceHunter_collector_runtime_control as control
import EvidenceHunter_collector_supervisor as supervisor

pytestmark = pytest.mark.skipif(os.name != 'nt', reason='Windows runtime lease')


def read(path):
    return json.loads(path.read_text('utf-8'))


def finish(session, cause='NATURAL_DURATION_COMPLETE', checks=None):
    checkpoint = session.root / 'continuity_checkpoint.json'
    summary = session.root / 'soak_summary.json'
    control.persist(checkpoint, {'checkpoint': 123})
    # Historical quality defects are deliberately not cleanliness predicates.
    control.persist(summary, {'dropped_count': 2, 'gap_count': 1})
    session.finalize(cause, checks or {'drained': True}, checkpoint, summary)


def test_t1_i2_running_precedes_build_and_start_failure_releases(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['collector', '--dataset-id', 'synthetic', '--runtime-dir', str(tmp_path)])
    called = []
    def build(*args):
        records = list((tmp_path/'operations/sessions').glob('*/session.json'))
        assert len(records) == 1
        assert read(records[0])['status'] == 'RUNNING'
        assert 'session_end_cleanliness' not in read(records[0])
        called.append(True)
        raise RuntimeError('synthetic startup failure')
    monkeypatch.setattr(collector, 'build_components', build)
    with pytest.raises(RuntimeError, match='synthetic'):
        collector.main()
    assert called == [True]
    assert not (tmp_path/'raw').exists()
    lease = control.Lease('synthetic', tmp_path)
    lease.close()


@pytest.mark.parametrize('failed_name', ['owner_identity.json', 'session.json'])
def test_i2_initial_write_failure_never_builds(tmp_path, monkeypatch, failed_name):
    original = control.persist
    def fail(path, value):
        if Path(path).name == failed_name:
            raise OSError('initial evidence unavailable')
        return original(path, value)
    monkeypatch.setattr(control, 'persist', fail)
    monkeypatch.setattr(sys, 'argv', ['collector', '--dataset-id', 'synthetic', '--runtime-dir', str(tmp_path)])
    monkeypatch.setattr(collector, 'build_components', lambda *a: pytest.fail('writers reachable'))
    with pytest.raises(OSError):
        collector.main()
    assert not (tmp_path/'raw').exists()
    control.Lease('synthetic', tmp_path).close()


def test_t2_existing_closes_contender_handle_no_raw(tmp_path):
    first = control.Session('synthetic', tmp_path)
    owner = (tmp_path/'operations/owner_identity.json').read_bytes()
    try:
        with pytest.raises(RuntimeError, match='ALREADY_RUNNING'):
            control.Session('synthetic', tmp_path)
        assert (tmp_path/'operations/owner_identity.json').read_bytes() == owner
        assert not (tmp_path/'raw').exists()
    finally:
        first.close()
    # A leaked contender handle would keep the named object alive and fail here.
    control.Lease('synthetic', tmp_path).close()


def test_t4_stale_owner_is_not_lock(tmp_path):
    control.persist(tmp_path/'operations/owner_identity.json', {'worker_pid': os.getpid(), 'creation_filetime': 0, 'image_path': 'wrong'})
    s = control.Session('synthetic', tmp_path)
    try:
        assert s.record['owner_identity']['creation_filetime'] != 0
    finally:
        s.close()


def test_t6_t7_t10_t11_t12_session_stop_and_history(tmp_path):
    first = control.Session('synthetic', tmp_path)
    request = str(uuid.uuid4())
    try:
        a = control.request_graceful_stop(tmp_path, first.session_id, request)
        assert control.request_graceful_stop(tmp_path, first.session_id, request) == a
        assert first.wait(2) == 'GRACEFUL_STOP_CONFIRMED'
        assert first.poll_stop() is True
        assert 'session_end_cleanliness' not in read(first.directory/'session.json')
        finish(first, 'GRACEFUL_STOP_CONFIRMED')
        historical = (first.directory/'session.json').read_bytes()
    finally:
        first.close()
    second = control.Session('synthetic', tmp_path)
    try:
        assert second.session_id != first.session_id
        assert second.wait(0) == 'NATURAL_DURATION_COMPLETE'
        finish(second)
        assert (first.directory/'session.json').read_bytes() == historical
        assert read(second.directory/'session.json')['session_end_cleanliness'] == 'CLEAN'
    finally:
        second.close()
    third = control.Session('synthetic', tmp_path)
    incomplete = (third.directory/'session.json').read_bytes()
    third.close()
    fourth = control.Session('synthetic', tmp_path)
    try:
        assert (third.directory/'session.json').read_bytes() == incomplete
        assert 'session_end_cleanliness' not in read(third.directory/'session.json')
        assert 'reconciliation_pass' not in fourth.record
    finally:
        fourth.close()


@pytest.mark.parametrize('identifier', ['', '..', '../x', 'x/y', 'x\\y', 'NOT-UUID', str(uuid.uuid4()).upper()])
def test_i3_rejects_unsafe_ids_before_paths(tmp_path, identifier):
    with pytest.raises((ValueError, AttributeError)):
        control.request_graceful_stop(tmp_path, identifier, str(uuid.uuid4()))
    with pytest.raises((ValueError, AttributeError)):
        control.request_graceful_stop(tmp_path, str(uuid.uuid4()), identifier)
    assert list(tmp_path.iterdir()) == []


def test_t13_canonical_pair_names(tmp_path):
    assert control.lease_name('a', tmp_path) != control.lease_name('b', tmp_path)
    assert control.lease_name('a', tmp_path) != control.lease_name('a', tmp_path/'other')
    assert control.lease_name('a', tmp_path) == control.lease_name('a', tmp_path/'x'/'..')


@pytest.mark.parametrize('check', ['threads_stopped', 'writers_stopped', 'backlog_zero', 'final_sink_ok'])
def test_t14_failed_check_withholds_clean(tmp_path, check):
    s = control.Session('synthetic', tmp_path)
    try:
        with pytest.raises(RuntimeError, match='FINALIZATION_CHECK'):
            finish(s, checks={check: False})
        assert 'session_end_cleanliness' not in read(s.directory/'session.json')
    finally:
        s.close()


@pytest.mark.parametrize('name', ['final_checkpoint.json', 'final_summary.json', 'session.json'])
def test_t14_failed_final_write_withholds_clean(tmp_path, monkeypatch, name):
    s = control.Session('synthetic', tmp_path)
    original = control.persist
    def fail(path, value):
        if Path(path).parent == s.directory and Path(path).name == name:
            raise OSError('finalization write failure')
        return original(path, value)
    monkeypatch.setattr(control, 'persist', fail)
    try:
        with pytest.raises(OSError):
            finish(s)
        assert 'session_end_cleanliness' not in read(s.directory/'session.json')
    finally:
        s.close()


def test_t15_request_identity_mismatch_ignored(tmp_path):
    s = control.Session('synthetic', tmp_path)
    try:
        rid = str(uuid.uuid4())
        p = tmp_path/'operations/stop_requests'/s.session_id/(rid+'.json')
        control.persist(p, dict(target_collection_session_id=str(uuid.uuid4()), stop_request_id=rid, requested_at_utc=control.utc()))
        assert not s.poll_stop()
        assert not (tmp_path/'operations/stop_acks').exists()
    finally:
        s.close()


def test_i1_and_t16_last_error_and_null_handle(tmp_path, monkeypatch):
    calls = []
    class Fn:
        def __init__(self, fn): self.fn = fn
        def __call__(self, *a): return self.fn(*a)
    def create(*args):
        assert ctypes.get_last_error() == 0
        assert args[1] is False
        calls.append(args)
        ctypes.set_last_error(5)
        return None
    kernel = SimpleNamespace(CreateMutexW=Fn(create), CloseHandle=Fn(lambda h: True))
    def dll(name, **kwargs):
        assert kwargs['use_last_error'] is True
        return kernel
    monkeypatch.setattr(ctypes, 'WinDLL', dll)
    ctypes.set_last_error(183)
    with pytest.raises(OSError): control.Lease('synthetic', tmp_path)
    assert len(calls) == 1
    assert not (tmp_path/'raw').exists()


def test_t8_backoff_interruptible(monkeypatch):
    event = threading.Event()
    entered = threading.Event()
    original_wait = event.wait
    def wait(seconds):
        entered.set()
        return original_wait(seconds)
    event.wait = wait
    class Conn:
        state = 'DISCONNECTED'
        close_reason = 'synthetic'
        def __init__(self, *a, **kw): pass
        def start(self): return self
        def wait_until_ready_or_failed(self): pass
        def stop(self): pass
    monkeypatch.setattr(supervisor, 'WSConnection', Conn)
    sup = SimpleNamespace(transition=lambda *a, **kw: None)
    t = threading.Thread(target=supervisor._run_component_lifecycle_body, kwargs=dict(
        name='aggTrade', url_fn=lambda: '', route_class='market', stream_name='test',
        on_message=lambda x: None, supervisor=sup, stop_event=event,
        silent_stall_seconds=10, stall_check_interval_seconds=.01,
        reconnect_backoff_seconds=30, max_reconnect_backoff_seconds=30,
        connect_fn=None, on_lifecycle_event=None))
    t.start()
    try:
        assert entered.wait(2)
        event.set()
        t.join(2)
        assert not t.is_alive()
    finally:
        event.set(); t.join(35)


def test_dataset_id_required_before_session(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['collector', '--runtime-dir', str(tmp_path)])
    with pytest.raises(SystemExit): collector.main()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('operator_stop', [False, True])
def test_real_shutdown_path_offline(tmp_path, monkeypatch, operator_stop):
    original_build = collector.build_components
    def build(*args):
        parts = original_build(*args)
        parts['depth_collector'].ensure_initial_sync = lambda: None
        parts['clock_observer'].run = lambda stop_event, **kw: stop_event.wait(5)
        # Historical drop is accounted, not unresolved shutdown failure.
        parts['writers'][0].dropped_count = 1
        return parts
    monkeypatch.setattr(collector, 'build_components', build)
    monkeypatch.setattr(collector, 'run_component_lifecycle', lambda **kw: kw['stop_event'].wait(5))
    monkeypatch.setattr(sys, 'argv', ['collector','--dataset-id','synthetic','--runtime-dir',str(tmp_path),'--duration-seconds','0'])
    original_wait = control.Session.wait
    def wait(session, duration):
        if operator_stop:
            control.request_graceful_stop(tmp_path, session.session_id, str(uuid.uuid4()))
        return original_wait(session, duration)
    monkeypatch.setattr(control.Session, 'wait', wait)
    collector.main()
    record = read(next((tmp_path/'operations/sessions').glob('*/session.json')))
    assert record['session_end_cleanliness'] == 'CLEAN'
    assert record['machine_observed_end_cause'] == ('GRACEFUL_STOP_CONFIRMED' if operator_stop else 'NATURAL_DURATION_COMPLETE')
    assert (tmp_path/'continuity_checkpoint.json').exists()
    assert read(tmp_path/'soak_summary.json')['writers']['aggTrade']['dropped_count'] == 1


CHILD = r'''
import sys,time
from pathlib import Path
import EvidenceHunter_collector_runtime_control as c
root=Path(sys.argv[1]); slot=sys.argv[2]
while not (root/'go').exists(): time.sleep(.01)
try:
 s=c.Session('synthetic', root/'data')
except RuntimeError:
 (root/slot).write_text('LOSER'); raise SystemExit(0)
(root/slot).write_text('WINNER')
while not (root/'release').exists(): time.sleep(.01)
s.close()
'''


def test_t3_t5_t9_process_race_and_crash(tmp_path):
    children=[]
    try:
        for slot in ('one','two'):
            children.append(subprocess.Popen([sys.executable,'-B','-c',CHILD,str(tmp_path),slot], creationflags=subprocess.CREATE_NO_WINDOW))
        (tmp_path/'go').touch()
        deadline=time.monotonic()+15
        while not all((tmp_path/s).exists() for s in ('one','two')):
            assert time.monotonic()<deadline
            time.sleep(.02)
        results=[(tmp_path/s).read_text() for s in ('one','two')]
        assert sorted(results)==['LOSER','WINNER']
        winner=children[results.index('WINNER')]
        # Actual session owner PID, never blindly terminate the venv launcher.
        owner=read(tmp_path/'data/operations/owner_identity.json')
        from EvidenceHunter_resource_sampling import WindowsProcess
        handle=WindowsProcess(owner['worker_pid'])
        try:
            assert handle.identity()['creation_filetime']==owner['creation_filetime']
            handle.check(handle.k.TerminateProcess(handle.handle, 137))
            assert handle.done(5000)
        finally: handle.close()
        winner.wait(10)
        children[results.index('LOSER')].wait(10)
        records=list((tmp_path/'data/operations/sessions').glob('*/session.json'))
        assert len(records)==1 and 'session_end_cleanliness' not in read(records[0])
        c=control.Session('synthetic', tmp_path/'data'); c.close()
    finally:
        (tmp_path/'release').touch()
        for child in children:
            try: child.wait(10)
            except subprocess.TimeoutExpired: child.kill(); child.wait(10)

