"""Windows single-instance lease and session-bound operational evidence.

Does not authorize collection, reconcile previous sessions, or alter raw schemas.
"""
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import time
import uuid

from EvidenceHunter_resource_sampling import WindowsProcess


def utc():
    return datetime.now(timezone.utc).isoformat()


def safe_id(value):
    if not isinstance(value, str) or str(uuid.UUID(value)) != value:
        raise ValueError('NON_CANONICAL_UUID')
    return value


def validate_dataset_id(value):
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError('EXPLICIT_DATASET_ID_REQUIRED')
    return value


def canonical_root(root):
    return Path(os.path.normcase(str(Path(root).resolve())))


def lease_name(dataset_id, root):
    pair = [validate_dataset_id(dataset_id), str(canonical_root(root))]
    digest = hashlib.sha256(json.dumps(pair, ensure_ascii=True).encode()).hexdigest()
    return 'Global\\EvidenceHunter_COLLECTOR_OWNER_' + digest


def persist(path, value):
    """Unique temporary file, flush/fsync, replace, and exact readback.

    No claim of surviving storage-device failure or power-loss directory metadata loss.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode('utf-8')
    pending = path.with_name(path.name + '.' + str(uuid.uuid4()) + '.pending')
    try:
        with pending.open('xb') as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        os.replace(pending, path)
        if path.read_bytes() != data:
            raise RuntimeError('EVIDENCE_READBACK_MISMATCH')
    finally:
        if pending.exists():
            pending.unlink()


class Lease:
    def __init__(self, dataset_id, root):
        self.name = lease_name(dataset_id, root)
        self.handle = None
        self.kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        self.kernel.CreateMutexW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR]
        self.kernel.CreateMutexW.restype = wintypes.HANDLE
        self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        self.kernel.CloseHandle.restype = wintypes.BOOL
        ctypes.set_last_error(0)
        handle = self.kernel.CreateMutexW(None, False, self.name)
        error = ctypes.get_last_error()
        if not handle:
            raise ctypes.WinError(error)
        if error:
            self.kernel.CloseHandle(handle)
            if error == 183:
                raise RuntimeError('COLLECTOR_ALREADY_RUNNING')
            raise ctypes.WinError(error)
        self.handle = handle

    def close(self):
        if self.handle is not None:
            if not self.kernel.CloseHandle(self.handle):
                raise ctypes.WinError(ctypes.get_last_error())
            self.handle = None


def request_graceful_stop(runtime_root, target_session_id, request_id):
    session_id, request_id = safe_id(target_session_id), safe_id(request_id)
    root = canonical_root(runtime_root)
    record = json.loads((root / 'operations' / 'sessions' / session_id / 'session.json').read_text('utf-8'))
    if record.get('collection_session_id') != session_id or record.get('status') != 'RUNNING':
        raise RuntimeError('TARGET_SESSION_NOT_RUNNING')
    path = root / 'operations' / 'stop_requests' / session_id / (request_id + '.json')
    if path.exists():
        value = json.loads(path.read_text('utf-8'))
        if value.get('target_collection_session_id') != session_id or value.get('stop_request_id') != request_id:
            raise RuntimeError('STOP_REQUEST_IDENTITY_MISMATCH')
        return value
    value = dict(target_collection_session_id=session_id, stop_request_id=request_id, requested_at_utc=utc())
    persist(path, value)
    return value


class Session:
    def __init__(self, dataset_id, runtime_root):
        self.dataset_id = validate_dataset_id(dataset_id)
        self.root = canonical_root(runtime_root)
        self.lease = Lease(self.dataset_id, self.root)
        self.record = None
        self.accepted_request = None
        try:
            self.session_id = safe_id(str(uuid.uuid4()))
            self.directory = self.root / 'operations' / 'sessions' / self.session_id
            self.directory.mkdir(parents=True, exist_ok=False)
            handle = WindowsProcess(os.getpid())
            try:
                identity = handle.identity()
            finally:
                handle.close()
            owner = dict(identity, dataset_id=self.dataset_id, runtime_root=str(self.root),
                         collection_session_id=self.session_id, acquired_at_utc=utc(),
                         identity_method='self_os_pid_and_kernel_process_identity')
            persist(self.root / 'operations' / 'owner_identity.json', owner)
            self.record = dict(status='RUNNING', dataset_id=self.dataset_id,
                               runtime_root=str(self.root), collection_session_id=self.session_id,
                               session_start_utc=utc(), owner_identity=owner)
            persist(self.directory / 'session.json', self.record)
        except BaseException:
            self.lease.close()
            raise

    def poll_stop(self):
        if self.accepted_request is not None:
            return True
        folder = self.root / 'operations' / 'stop_requests' / self.session_id
        for path in sorted(folder.glob('*.json')):
            try:
                request_id = safe_id(path.stem)
                value = json.loads(path.read_text('utf-8'))
                if value.get('target_collection_session_id') != self.session_id or value.get('stop_request_id') != request_id:
                    continue
                stamp = datetime.fromisoformat(value['requested_at_utc'])
                if stamp.tzinfo is None:
                    continue
            except (ValueError, KeyError, TypeError):
                continue
            ack = dict(target_collection_session_id=self.session_id, stop_request_id=request_id,
                       acknowledged_at_utc=utc())
            persist(self.root / 'operations' / 'stop_acks' / self.session_id / (request_id + '.json'), ack)
            self.accepted_request = ack
            return True
        return False

    def wait(self, duration):
        if not math.isfinite(duration) or duration < 0:
            raise ValueError('INVALID_DURATION')
        deadline = time.monotonic() + duration
        while True:
            if self.poll_stop():
                return 'GRACEFUL_STOP_CONFIRMED'
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return 'NATURAL_DURATION_COMPLETE'
            time.sleep(min(1.0, remaining))

    def finalize(self, cause, checks, checkpoint_path, summary_path):
        if self.record['status'] != 'RUNNING':
            raise RuntimeError('SESSION_ALREADY_FINALIZED')
        if cause not in ('GRACEFUL_STOP_CONFIRMED', 'NATURAL_DURATION_COMPLETE'):
            raise ValueError('UNPROVEN_END_CAUSE')
        if not checks or not all(value is True for value in checks.values()):
            raise RuntimeError('CLEAN_FINALIZATION_CHECK_FAILED')
        if cause == 'GRACEFUL_STOP_CONFIRMED' and self.accepted_request is None:
            raise RuntimeError('STOP_ACK_MISSING')
        copies = {}
        for name, source in [('final_checkpoint.json', checkpoint_path), ('final_summary.json', summary_path)]:
            source = Path(source)
            value = json.loads(source.read_text('utf-8'))
            if not isinstance(value, dict):
                raise RuntimeError('FINAL_EVIDENCE_NOT_OBJECT')
            target = self.directory / name
            persist(target, value)
            copies[name] = dict(sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
                                source_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
            if json.loads(target.read_text('utf-8')) != value:
                raise RuntimeError('SESSION_COPY_MISMATCH')
        final = dict(self.record, status='FINALIZED', session_end_utc=utc(),
                     session_end_cleanliness='CLEAN', machine_observed_end_cause=cause,
                     finalization_checks=checks, stop_ack=self.accepted_request, evidence=copies)
        persist(self.directory / 'session.json', final)
        self.record = final

    def fail(self, error):
        if self.record is not None and self.record['status'] == 'RUNNING':
            final = dict(self.record, status='FINALIZATION_FAILED', session_end_utc=utc(),
                         session_end_cleanliness='UNCLEAN', machine_observed_end_cause='UNKNOWN',
                         finalization_error=type(error).__name__ + ': ' + str(error))
            persist(self.directory / 'session.json', final)
            self.record = final

    def close(self):
        self.lease.close()

