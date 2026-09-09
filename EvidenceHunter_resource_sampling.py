"""COLLECTOR_RESOURCE_SAMPLING_FIX: Windows worker measurement harness.

Owner scope: actual-worker identity, OS exit code, RSS/private-memory/CPU only.
Primary builder: Claude (cloud session), per
CHANGE_ROLES_COLLECTOR_RESOURCE_SAMPLING_FIX_20260908.json. No independent_reviewer
role this changeset; PROJECT_OWNER and GPT act as scope/governance approvers via
chat relay (see that file's verification_mechanism field).
Initial draft provenance: Codex authored the original draft of this module and of
tests/test_resource_sampling.py before this changeset's formal scope approval; per
governance, Codex is not this changeset's primary builder.
No existing collector/readiness code or historical report changes. Two new
implementation/test files (this module + targeted tests); governance artifacts
and the versioned V2 readiness report are tracked separately under this changeset.
No new repo directories, no full pytest or restart experiment. All run evidence
goes to a NEW OS-temp directory; no formal Dataset.
--collect is explicit; --probe-run runs only a synthetic CPU/memory workload to
verify the measurement chain.
"""
import argparse
import ctypes
from ctypes import wintypes as w
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import time
import uuid


def write_json(path, value):
    # Each evidence path is single-writer, and handshake readers see whole JSON.
    pending = path.with_suffix(path.suffix + '.pending')
    pending.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')
    os.replace(pending, path)


class Memory(ctypes.Structure):
    _fields_ = [('cb', w.DWORD), ('PageFaultCount', w.DWORD)] + [
        (name, ctypes.c_size_t) for name in (
            'PeakWorkingSetSize', 'WorkingSetSize', 'QuotaPeakPagedPoolUsage',
            'QuotaPagedPoolUsage', 'QuotaPeakNonPagedPoolUsage',
            'QuotaNonPagedPoolUsage', 'PagefileUsage', 'PeakPagefileUsage', 'PrivateUsage')]


class WindowsProcess:
    def __init__(self, pid):
        self.k = ctypes.WinDLL('kernel32', use_last_error=True)
        self.ps = ctypes.WinDLL('psapi', use_last_error=True)
        signatures = {
            'OpenProcess': ([w.DWORD, w.BOOL, w.DWORD], w.HANDLE),
            'GetProcessId': ([w.HANDLE], w.DWORD),
            'GetProcessTimes': ([w.HANDLE] + [ctypes.POINTER(w.FILETIME)] * 4, w.BOOL),
            'QueryFullProcessImageNameW': ([w.HANDLE, w.DWORD, w.LPWSTR, ctypes.POINTER(w.DWORD)], w.BOOL),
            'WaitForSingleObject': ([w.HANDLE, w.DWORD], w.DWORD),
            'GetExitCodeProcess': ([w.HANDLE, ctypes.POINTER(w.DWORD)], w.BOOL),
            'TerminateProcess': ([w.HANDLE, w.UINT], w.BOOL),
            'CloseHandle': ([w.HANDLE], w.BOOL),
        }
        for name, (args, result) in signatures.items():
            fn = getattr(self.k, name); fn.argtypes = args; fn.restype = result
        self.ps.GetProcessMemoryInfo.argtypes = [w.HANDLE, ctypes.POINTER(Memory), w.DWORD]
        self.ps.GetProcessMemoryInfo.restype = w.BOOL
        self.handle = self.k.OpenProcess(0x100000 | 0x0400 | 0x0010 | 0x0001, False, pid)
        self.check(self.handle)
        self.pid = self.k.GetProcessId(self.handle)
        if self.pid != pid:
            self.close(); raise RuntimeError('OS_HANDLE_PID_MISMATCH')

    @staticmethod
    def check(ok):
        if not ok:
            raise ctypes.WinError(ctypes.get_last_error())

    def times(self):
        values = [w.FILETIME() for _ in range(4)]
        self.check(self.k.GetProcessTimes(self.handle, *[ctypes.byref(v) for v in values]))
        return [(v.dwHighDateTime << 32) | v.dwLowDateTime for v in values]

    def identity(self):
        buf = ctypes.create_unicode_buffer(32768); size = w.DWORD(len(buf))
        self.check(self.k.QueryFullProcessImageNameW(self.handle, 0, buf, ctypes.byref(size)))
        return {'worker_pid': self.pid, 'creation_filetime': self.times()[0], 'image_path': buf.value}

    def sample(self, start):
        memory = Memory(); memory.cb = ctypes.sizeof(memory)
        self.check(self.ps.GetProcessMemoryInfo(self.handle, ctypes.byref(memory), memory.cb))
        created, exited, kernel, user = self.times()
        return {'sampled_pid': self.pid, 'creation_filetime': created,
                'elapsed_seconds': time.monotonic() - start, 't_iso': time.time(),
                'working_set_bytes': memory.WorkingSetSize, 'private_memory_bytes': memory.PrivateUsage,
                'cpu_total_seconds': (kernel + user) / 10_000_000}

    def done(self, milliseconds=0):
        result = self.k.WaitForSingleObject(self.handle, milliseconds)
        if result not in (0, 258):
            raise RuntimeError(f'WAIT_FAILED:{result}')
        return result == 0

    def exit_code(self):
        if not self.done():
            raise RuntimeError('WORKER_NOT_EXITED')
        code = w.DWORD(); self.check(self.k.GetExitCodeProcess(self.handle, ctypes.byref(code)))
        return code.value

    def close(self):
        if self.handle:
            self.k.CloseHandle(self.handle); self.handle = None


def verify_identity(claim, observed, nonce):
    if claim.get('nonce') != nonce:
        raise RuntimeError('WORKER_NONCE_MISMATCH')
    for field in ('worker_pid', 'creation_filetime', 'image_path'):
        if claim.get(field) != observed.get(field):
            raise RuntimeError('WORKER_IDENTITY_MISMATCH:' + field)
    if claim.get('identity_method') != 'self_os_pid_and_kernel_process_identity':
        raise RuntimeError('WORKER_IDENTITY_METHOD_MISSING')


def worker(args):
    if not args.probe and not getattr(args, "dataset_id", None):
        raise ValueError("EXPLICIT_DATASET_ID_REQUIRED")
    directory = Path(args.output)
    handle = WindowsProcess(os.getpid())
    claim = handle.identity(); handle.close()
    claim.update(nonce=args.nonce, identity_method='self_os_pid_and_kernel_process_identity')
    write_json(directory / 'worker_identity.json', claim)
    deadline = time.monotonic() + 30
    while not (directory / 'identity_ack.json').exists():
        if time.monotonic() > deadline:
            raise RuntimeError('WORKER_IDENTITY_ACK_TIMEOUT')
        time.sleep(.02)
    ack = json.loads((directory / 'identity_ack.json').read_text())
    verify_identity(claim, ack, args.nonce)
    if args.probe:
        memory = bytearray(8 * 1024 * 1024)
        end = time.monotonic() + args.duration
        while time.monotonic() < end:
            for i in range(0, len(memory), 4096):
                memory[i] = (memory[i] + 1) % 256
        raise SystemExit(args.probe_exit)
    # The SAME process that provided its identity now runs the unmodified collector.
    target = Path(__file__).with_name('EvidenceHunter_collector.py')
    sys.argv = [str(target), '--dataset-id', args.dataset_id, '--runtime-dir', str(directory), '--duration-seconds', str(args.duration)]
    runpy.run_path(str(target), run_name='__main__')


def measure(directory, *, duration=3600, interval=60, probe=False, probe_exit=0, dataset_id=None):
    if not probe and (not isinstance(dataset_id, str) or not dataset_id.strip()):
        raise ValueError('EXPLICIT_DATASET_ID_REQUIRED')
    if os.name != 'nt':
        raise RuntimeError('WINDOWS_ONLY')
    if not (0 < duration <= 3600 and 0 < interval <= 60):
        raise ValueError('Bounded duration <=3600 and interval <=60 required')
    directory = Path(directory).resolve()
    if not directory.is_relative_to(Path(tempfile.gettempdir()).resolve()):
        raise ValueError('Evidence must use a new OS temp directory')
    directory.mkdir(exist_ok=False)
    nonce = uuid.uuid4().hex
    command = [sys.executable, '-B', str(Path(__file__).resolve()), '--worker', '--output', str(directory),
               '--nonce', nonce, '--duration', str(duration), '--probe-exit', str(probe_exit)]
    if probe:
        command.append('--probe')
    else:
        command.extend(['--dataset-id', dataset_id])
    handle = None; samples = []; identity = None; failure = None; exit_evidence = None
    with (directory/'soak_stdout.log').open('wb') as out, (directory/'soak_stderr.log').open('wb') as err:
        launcher = subprocess.Popen(command, stdout=out, stderr=err, creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            deadline = time.monotonic() + 30
            while not (directory/'worker_identity.json').exists():
                if launcher.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('NO_WORKER_HANDSHAKE')
                time.sleep(.02)
            claim = json.loads((directory/'worker_identity.json').read_text())
            handle = WindowsProcess(claim['worker_pid'])
            observed = handle.identity(); verify_identity(claim, observed, nonce)
            identity = {**observed, 'launcher_pid': launcher.pid, 'nonce': nonce, 'verified': True,
                        'method': 'worker self-handshake + parent OS-handle PID/creation-time/image verification; same-process runpy',
                        'command': command}
            start = time.monotonic()
            samples.append(handle.sample(start))
            write_json(directory/'identity_ack.json', {**observed, 'nonce': nonce})
            # Hold this exact OS handle through exit: no process-name search or PID reuse.
            while not handle.done(max(1, int(interval*1000))):
                samples.append(handle.sample(start))
                write_json(directory/'process_samples.json', {'schema':'COLLECTOR_PROCESS_SAMPLES_V1',
                           'identity_verification':identity, 'samples':samples})
                if time.monotonic()-start > duration+120:
                    raise RuntimeError('BOUNDED_WORKER_TIMEOUT')
            exit_evidence = {'schema':'COLLECTOR_PROCESS_EXIT_EVIDENCE_V1', 'worker_pid':handle.pid,
                             'creation_filetime':observed['creation_filetime'], 'nonce':nonce,
                             'verified':True, 'exit_code':handle.exit_code(),
                             'method':'GetExitCodeProcess on verified actual-worker handle after signaled wait',
                             'cpu_total_seconds_at_exit':sum(handle.times()[2:])/10_000_000}
        except Exception as exc:
            failure = f'{type(exc).__name__}: {exc}'
        finally:
            if handle:
                if not handle.done():
                    # Only this run's identity-verified worker; never kill unrelated processes.
                    if identity is not None:
                        handle.check(handle.k.TerminateProcess(handle.handle, 124)); handle.done(10000)
                handle.close()
            try:
                launcher.wait(timeout=35)
            except subprocess.TimeoutExpired:
                failure = (failure or '') + '; LAUNCHER_EXIT_NOT_OBSERVED'
    write_json(directory/'process_samples.json', {'schema':'COLLECTOR_PROCESS_SAMPLES_V1',
               'identity_verification':identity, 'samples':samples, 'measurement_failure':failure})
    if exit_evidence is not None:
        write_json(directory/'process_exit_evidence.json', exit_evidence)
    result = {'measurement_status':'FAIL' if failure else 'PASS', 'failure':failure,
              'mode':'PROBE_NOT_READINESS' if probe else 'G4_G5_ONLY', 'directory':str(directory),
              'identity':identity, 'exit_evidence':exit_evidence, 'sample_count':len(samples),
              'sampler_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    write_json(directory/'measurement_result.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--worker', action='store_true'); mode.add_argument('--collect', action='store_true')
    mode.add_argument('--probe-run', action='store_true')
    parser.add_argument('--probe', action='store_true'); parser.add_argument('--probe-exit',type=int,default=0)
    parser.add_argument('--nonce'); parser.add_argument('--output',required=True)
    parser.add_argument('--duration',type=float,default=3600); parser.add_argument('--interval',type=float,default=60)
    parser.add_argument('--dataset-id')
    args=parser.parse_args()
    if not (args.probe_run or args.probe) and not args.dataset_id:
        parser.error('--dataset-id is required for collection')
    if args.worker:
        worker(args); return 0
    result=measure(args.output,duration=args.duration,interval=args.interval,
                   probe=args.probe_run,probe_exit=args.probe_exit,dataset_id=args.dataset_id)
    print(json.dumps(result,indent=2))
    return 0 if result['measurement_status']=='PASS' and result['exit_evidence']['exit_code']==0 else 1


if __name__=='__main__':
    raise SystemExit(main())

