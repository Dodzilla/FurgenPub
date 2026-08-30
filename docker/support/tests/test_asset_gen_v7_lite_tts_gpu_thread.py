"""CPU regression for CUDA handle ownership across independent socket requests."""
import tempfile
import threading
import unittest

from test_asset_gen_v7_lite_tts_runtime import (
    FakeGpuBackend, FakePermits, TtsRuntimeApp, _params, _permit,
    _write_config, load_profile_spec,
)


class GpuThreadTest(unittest.TestCase):
    def test_warmup_and_requests_reuse_one_gpu_thread(self):
        with tempfile.TemporaryDirectory() as directory:
            _, config = _write_config(directory)
            backend = FakeGpuBackend(load_profile_spec("compact"))
            seen = []
            warmup, generate = backend.warmup, backend.generate
            def record(call, *args, **kwargs):
                seen.append(threading.current_thread())
                return call(*args, **kwargs)
            backend.warmup = lambda **kwargs: record(warmup, **kwargs)
            backend.generate = lambda *args, **kwargs: record(generate, *args, **kwargs)
            app = TtsRuntimeApp(config, "unused", backend=backend, permit_client=FakePermits(True))
            try:
                responses = []
                requests = [{"method": "warmup", "permit": _permit("warmup")}]
                requests += [{"method": "generate", "requestId": f"r-{i}",
                              "permit": _permit(), "params": _params()} for i in range(2)]
                callers = []
                for request in requests:
                    thread = threading.Thread(target=lambda req=request: responses.append(app.handle(req)))
                    callers.append(thread)
                    thread.start()
                    thread.join(timeout=2)
                    self.assertFalse(thread.is_alive())
                self.assertTrue(all(r["ok"] for r in responses), responses)
                self.assertEqual(len(seen), 3)
                self.assertIs(seen[0], seen[1])
                self.assertIs(seen[1], seen[2])
                self.assertNotIn(seen[0], callers)
            finally:
                app.gpu_executor.shutdown()
