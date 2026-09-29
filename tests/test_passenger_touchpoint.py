"""Behavioral tests for touchpoint queues, reproducibility and dataset writing."""

import contextlib
import io
import json
import unittest
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from pydantic import ValidationError

from airport_eye.cli import main
from airport_eye.generators.passenger_touchpoint import QueueState, generate, write_dataset
from airport_eye.models import EventEnvelope
from airport_eye.touchpoint_config import TouchpointInventory, TouchpointRunConfig, load_run

ROOT = Path(__file__).resolve().parents[1]


@contextlib.contextmanager
def test_directory():
    # Normal inherited directory permissions work in restricted Windows runners;
    # tempfile's owner-only directory ACL can exclude the sandbox identity.
    root = ROOT / f'.touchpoint-test-{uuid4()}'
    root.mkdir()
    try:
        yield root
    finally:
        assert root.resolve().parent == ROOT.resolve()
        output = root / 'generated_data'
        if output.exists():
            for file in output.iterdir():
                file.unlink()
            output.rmdir()
        for file in root.iterdir():
            file.unlink()
        root.rmdir()


class TouchpointTests(unittest.TestCase):
    def setUp(self):
        self.config, self.inventory, _ = load_run(ROOT / "config/passenger_touchpoint.json")
        self.tp = self.inventory.touchpoints[0].model_copy(deep=True)

    def test_fifo_metrics_at_window_boundaries(self):
        self.tp.simulation.arrival_rate_per_hour = 0
        self.tp.simulation.service_channels = 1
        self.tp.simulation.service_time_min_seconds = 30
        self.tp.simulation.service_time_max_seconds = 30
        self.tp.simulation.initial_queue_length = 3
        state = QueueState(self.tp, 42)
        self.assertEqual(state.advance(60), (2, 15.0, 30.0))
        self.assertEqual(len(state.waiting), 0)
        self.assertEqual(len(state.serving), 1)
        self.assertEqual(state.advance(120), (1, 60.0, 30.0))
        self.assertEqual(state.advance(180), (0, None, None))

    def test_queue_conservation_and_channel_capacity(self):
        state = QueueState(self.tp, 42)
        for cutoff in range(60, 3601, 60):
            state.advance(cutoff)
            self.assertEqual(state.profile.initial_queue_length + state.arrivals,
                             state.completed + len(state.waiting) + len(state.serving))
            self.assertLessEqual(len(state.serving), state.profile.service_channels)

    def test_count_cadence_keys_and_reproducibility(self):
        events = list(generate(self.config, self.inventory))
        enabled = [tp for tp in self.inventory.touchpoints if tp.enabled]
        self.assertEqual(len(events), 60 * len(enabled))
        self.assertEqual(len({e.event_id for e in events}), len(events))
        self.assertEqual([e.model_dump_json() for e in events],
                         [e.model_dump_json() for e in generate(self.config, self.inventory)])
        for tp in enabled:
            series = [e for e in events if e.entity_id == tp.touchpoint_id]
            self.assertEqual([e.sequence_no for e in series], list(range(60)))
            self.assertEqual(series[0].event_time, self.config.start_time + timedelta(seconds=60))
            self.assertEqual(series[-1].event_time, self.config.end_time)
            for event in series:
                EventEnvelope.model_validate_json(event.model_dump_json())
                self.assertEqual(event.payload['touchpoint_id'], event.entity_id)
        self.assertEqual({e.entity_id for e in events}, {tp.touchpoint_id for tp in enabled})

    def test_editable_inventory_and_interval(self):
        config = self.config.model_copy(update={'interval_seconds': 120})
        inventory = self.inventory.model_copy(deep=True)
        original = list(generate(config, inventory))
        inventory.touchpoints.reverse()
        self.assertEqual(original, list(generate(config, inventory)))
        added = self.tp.model_copy(update={'touchpoint_id': 'SYN-NEW-CUSTOM', 'touchpoint_type': 'custom_process'})
        inventory.touchpoints = [self.tp, added]
        events = list(generate(config, inventory))
        self.assertEqual(len(events), 60)
        self.assertEqual([e for e in original if e.entity_id == self.tp.touchpoint_id],
                         [e for e in events if e.entity_id == self.tp.touchpoint_id])
        inventory.touchpoints = [added]
        self.assertEqual(len(list(generate(config, inventory))), 30)

    def test_invalid_configuration_and_catalog(self):
        for changes in [{'interval_seconds': 0}, {'interval_seconds': 61},
                        {'start_time': '2026-09-29T08:00:00'},
                        {'end_time': self.config.start_time}]:
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                TouchpointRunConfig.model_validate({**self.config.model_dump(), **changes})
        raw = self.inventory.model_dump()
        raw['touchpoints'].append(raw['touchpoints'][0])
        with self.assertRaises(ValidationError):
            TouchpointInventory.model_validate(raw)
        raw = self.inventory.model_dump()
        raw['touchpoints'][0]['simulation']['service_time_min_seconds'] = -1
        with self.assertRaises(ValidationError):
            TouchpointInventory.model_validate(raw)

    def test_cli_output_and_replacement(self):
        with test_directory() as folder:
            root = Path(folder)
            self.assertTrue(root.resolve().is_relative_to(ROOT.resolve()))
            inventory = self.inventory.model_copy(update={'touchpoints': [self.tp]})
            (root / 'inventory.json').write_text(inventory.model_dump_json(), encoding='utf-8')
            config = self.config.model_copy(update={'inventory_file': 'inventory.json', 'output_directory': 'generated_data'})
            path = root / 'run.json'
            path.write_text(config.model_dump_json(), encoding='utf-8')
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(['generate', 'passenger_touchpoint', '--config', str(path)]), 0)
            output = root / 'generated_data/passenger_touchpoint.jsonl'
            content = output.read_bytes()
            self.assertEqual(len(content.splitlines()), 60)
            for line in content.splitlines():
                EventEnvelope.model_validate_json(line)
            write_dataset(path)
            self.assertEqual(output.read_bytes(), content)
            self.assertEqual(list(output.parent.iterdir()), [output])
            # Bad reruns must leave the previous complete dataset intact.
            raw = config.model_dump(mode='json')
            raw['interval_seconds'] = -1
            path.write_text(json.dumps(raw), encoding='utf-8')
            with self.assertRaises(ValidationError):
                write_dataset(path)
            self.assertEqual(output.read_bytes(), content)


if __name__ == '__main__':
    unittest.main()
