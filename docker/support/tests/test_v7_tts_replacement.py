import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import asset_gen_v7_lite_tts_install as install
from asset_gen_v7_lite_tts import TTSResidency


class ReplacementTest(unittest.TestCase):
    def test_rejects_wrong_worker_before_any_install(self):
        with patch.dict(os.environ, {'SERVER_TYPE':'asset_gen_v7_lite','DM_INSTANCE_ID':'123'}):
            with self.assertRaisesRegex(ValueError, 'identity'):
                install.install(types.SimpleNamespace(instance='456'))

    def test_new_worker_identity_reaches_install_not_retired_canary_gate(self):
        with tempfile.TemporaryDirectory() as root, patch.dict(os.environ, {'SERVER_TYPE':'asset_gen_v7_lite','DM_INSTANCE_ID':'50203390'}):
            with patch.object(install, 'run', side_effect=RuntimeError('reached clone')):
                with self.assertRaisesRegex(RuntimeError, 'reached clone'):
                    install.install(types.SimpleNamespace(instance='50203390', root=root))

    def test_missing_models_defer_warmup_without_failure(self):
        tts = object.__new__(TTSResidency)
        with tempfile.TemporaryDirectory() as root:
            tts.config = {'deferMissingCheckpoint':True,'checkpointDir':root,'checkpointHashes':{'model':'hash'}}
            self.assertFalse(tts._checkpoint_files_present())
            Path(root,'model').write_bytes(b'installed')
            self.assertTrue(tts._checkpoint_files_present())

    def test_miner_identity_varies_but_compute_options_do_not(self):
        expected = {'commandPolicy':'srbminer_pearlhash_v1'}
        parts = [b'/workspace/.fcs/prl/prl_gpu_miner',b'--disable-cpu',b'--algorithm',b'pearlhash',b'--pool',b'pool:123',b'--wallet',b'account.worker1']
        self.assertTrue(TTSResidency._mining_command_matches(parts, expected))
        parts[-1] = b'account.worker2'
        self.assertTrue(TTSResidency._mining_command_matches(parts, expected))
        self.assertFalse(TTSResidency._mining_command_matches(parts+[b'--gpu-intensity',b'99'], expected))
        parts[3] = b'other'
        self.assertFalse(TTSResidency._mining_command_matches(parts, expected))

    def test_production_rejects_unmeasured_policy(self):
        with tempfile.TemporaryDirectory() as root:
            policy=Path(root,'policy.json');policy.write_text('{}')
            with self.assertRaisesRegex(RuntimeError,'does not match'):
                install.activate_production({'sourceRevision':install.REVISION},policy,Path(root))

    def test_bootstrap_requires_installation_before_readiness(self):
        shell=Path(ROOT,'asset_gen_v7_lite.sh').read_text()
        self.assertLess(shell.index('bash "${WORKSPACE}/asset_gen_v7_lite_tts_provision.sh"'),shell.index('> "${READINESS_PATH}"'))
        self.assertIn('refusing silent fallback provisioning',shell)
        self.assertIn('TTS_FAST_ALL_REQUIRED=true',shell)
        installer=Path(ROOT,'asset_gen_v7_lite_tts_provision.sh').read_text()
        self.assertIn('--production-policy',installer)
        self.assertIn('--allow-missing-checkpoint',installer)


if __name__ == '__main__':
    unittest.main()

class ActivationTest(unittest.TestCase):
    def setUp(self):
        self.policy = json.loads((ROOT / 'breeze_tts2_production_policy.json').read_text())
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = {**self.policy, 'python':'unused','canaryInstanceIds':['987654'],'stateDir':str(self.root)}
        self.probe = json.dumps({'fingerprint':self.policy['measuredRuntimeFingerprint'],'deviceCount':1,'vram':32*1024**3})

    def test_fresh_worker_gets_its_own_enabled_policy(self):
        with patch.object(install.subprocess,'check_output',return_value=self.probe):
            install.activate_production(self.config,ROOT/'breeze_tts2_production_policy.json',self.root)
        saved=json.loads((self.root/'config.json').read_text())
        self.assertEqual(saved['canaryInstanceIds'],['987654'])
        self.assertTrue(saved['enabled'])
        self.assertFalse(saved['diagnosticsEnabled'])
        self.assertTrue(saved['deferMissingCheckpoint'])

    def test_changed_hardware_fails_before_activation(self):
        with patch.object(install.subprocess,'check_output',return_value=json.dumps({'fingerprint':'changed','deviceCount':1,'vram':32*1024**3})):
            with self.assertRaisesRegex(RuntimeError,'Unvalidated'):
                install.activate_production(self.config,ROOT/'breeze_tts2_production_policy.json',self.root)
        self.assertFalse((self.root/'config.json').exists())

    def test_source_hash_change_cannot_activate(self):
        self.policy['supportHashes']['tts_profiles.py']='0'*64
        changed=self.root/'policy.json';changed.write_text(json.dumps(self.policy))
        with self.assertRaisesRegex(RuntimeError,'hash mismatch'):
            install.activate_production(self.config,changed,self.root)
        self.assertFalse((self.root/'config.json').exists())

    def test_missing_sampling_controls_cannot_activate(self):
        self.policy.pop('validatedSamplingControls')
        changed=self.root/'policy.json';changed.write_text(json.dumps(self.policy))
        with self.assertRaisesRegex(RuntimeError,'sampling controls'):
            install.activate_production(self.config,changed,self.root)
        self.assertFalse((self.root/'config.json').exists())
