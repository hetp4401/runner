"""Check Mac service configuration and generated startup jobs in an isolated home."""
import importlib.util
import json
from pathlib import Path
import plistlib
import tempfile
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('mac_docker', Path(__file__).resolve().parents[1] / 'mac-docker.py')
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)

with tempfile.TemporaryDirectory(prefix='runner-mac-service-') as directory:
    root = Path(directory)
    source = root / 'source'
    source.mkdir()
    (source / 'compose.yaml').write_text('services: {}\n')
    environment = root / 'controller.env'
    environment.write_text('DOMAIN=example.test\n')
    environment.chmod(0o600)
    filename = root / 'service.json'
    filename.write_text(json.dumps({'source': str(source), 'environment_file': str(environment),
                                   'logs': str(root / 'logs'), 'profile': 'runners', 'project': 'runners'}))
    filename.chmod(0o600)
    with patch.object(helper.shutil, 'which', return_value='/test/bin/tool'):
        config, env = helper.configuration(filename)
    assert env['DOCKER_CONTEXT'] == 'colima-runners'
    assert env['CONTROL_ENV_FILE'] == str(environment)
    print('ok private service configuration needs only runtime paths')

    with patch.object(Path, 'home', return_value=root), patch.object(helper.sys, 'platform', 'darwin'):
        helper.install(config, 'io.runners.test')
        jobs = list((root / 'Library/LaunchAgents').glob('*.plist'))
        assert len(jobs) == 1
        job = plistlib.loads(jobs[0].read_bytes())
        assert job['Label'] == 'io.runners.test'
        assert job['ProgramArguments'][2:] == ['run', '--config', config['filename']]
        assert job['RunAtLoad'] and job['KeepAlive']
        assert 'StartCalendarInterval' not in job and 'StartInterval' not in job
        assert jobs[0].stat().st_mode & 0o077 == 0
        print('ok installation creates one private controller startup job')
        original = jobs[0].read_bytes()
        try:
            helper.install(config, 'io.runners.test')
        except ValueError:
            pass
        else:
            raise AssertionError('installation must refuse an existing job')
        assert jobs[0].read_bytes() == original
        print('ok installation preserves an existing startup job')

print('3 Mac Docker service checks passed')
