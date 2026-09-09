"""Only tests the new measurement chain. Does not run collector/restart/soak."""
import json
import os
import tempfile
from pathlib import Path
import uuid

import pytest
import EvidenceHunter_resource_sampling as sampling


@pytest.mark.parametrize('field,value', [('worker_pid',0),('creation_filetime',0),('image_path','wrong')])
def test_identity_mismatch_rejected(field,value):
    claim={'worker_pid':42,'creation_filetime':123,'image_path':'python.exe',
           'nonce':'token','identity_method':'self_os_pid_and_kernel_process_identity'}
    observed={**claim,field:value}
    with pytest.raises(RuntimeError,match='IDENTITY_MISMATCH'):
        sampling.verify_identity(claim,observed,'token')


def test_nonce_mismatch_rejected():
    with pytest.raises(RuntimeError,match='NONCE_MISMATCH'):
        sampling.verify_identity({'nonce':'old'},{},'new')


@pytest.mark.skipif(os.name!='nt',reason='Windows OS APIs')
@pytest.mark.parametrize('exit_code',[0,7])
def test_actual_worker_resources_and_exit_code(exit_code):
    directory=Path(tempfile.gettempdir())/('btc_sampling_probe_'+uuid.uuid4().hex)
    result=sampling.measure(directory,duration=1.2,interval=.15,probe=True,probe_exit=exit_code)
    assert result['measurement_status']=='PASS',result
    samples=json.loads((directory/'process_samples.json').read_text())
    identity=samples['identity_verification']; data=samples['samples']
    assert identity['verified'] is True
    assert len(data)>=3
    assert all(s['sampled_pid']==identity['worker_pid'] for s in data)
    assert all(s['creation_filetime']==identity['creation_filetime'] for s in data)
    assert all(s['working_set_bytes']>0 and s['private_memory_bytes']>0 for s in data)
    assert data[-1]['cpu_total_seconds']>data[0]['cpu_total_seconds']
    assert result['exit_evidence']['exit_code']==exit_code
    assert result['exit_evidence']['worker_pid']==identity['worker_pid']
    assert not (directory/'soak_summary.json').exists()


def test_existing_directory_is_never_reused(tmp_path):
    with pytest.raises((FileExistsError,ValueError)):
        sampling.measure(tmp_path,probe=True)


def test_collect_requires_explicit_id_before_directory(tmp_path):
    target=tmp_path/'not_created'
    with pytest.raises(ValueError,match='EXPLICIT_DATASET_ID'):
        sampling.measure(target)
    assert not target.exists()


def test_collect_forwards_dataset_to_worker(tmp_path,monkeypatch):
    import subprocess
    captured=[]
    def popen(command,**kwargs):
        captured.append(command)
        raise RuntimeError('intercepted before process launch')
    monkeypatch.setattr(subprocess,'Popen',popen)
    with pytest.raises(RuntimeError,match='intercepted'):
        sampling.measure(tmp_path/'new',dataset_id='explicit-test-id')
    command=captured[0]
    assert command[command.index('--dataset-id')+1]=='explicit-test-id'


def test_worker_forwards_dataset_to_collector(tmp_path,monkeypatch):
    from types import SimpleNamespace
    import sys
    claim={'worker_pid':42,'creation_filetime':123,'image_path':'python.exe'}
    class Handle:
        def __init__(self,pid): pass
        def identity(self): return dict(claim)
        def close(self): pass
    monkeypatch.setattr(sampling,'WindowsProcess',Handle)
    sampling.write_json(tmp_path/'identity_ack.json',{**claim,'nonce':'test'})
    seen=[]
    monkeypatch.setattr(sampling.runpy,'run_path',lambda *a,**kw:seen.append(list(sys.argv)))
    monkeypatch.setattr(sys,'argv',['original'])
    sampling.worker(SimpleNamespace(output=str(tmp_path),nonce='test',probe=False,
                                    dataset_id='explicit-test-id',duration=1))
    assert seen[0][seen[0].index('--dataset-id')+1]=='explicit-test-id'

