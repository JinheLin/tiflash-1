import unittest

from monitor_apply_progress import classify, ProgressTracker


def peer(commit, applied, last=None, diagnostics=None):
    return dict(raft_status=dict(hard_state=dict(commit=commit), applied=applied,
                                last_index=last or commit, persisted_index=last or commit),
                apply_diagnostics=diagnostics)


class ApplyProgressTests(unittest.TestCase):
    def test_distinguishes_transport_and_local_pipeline_stalls(self):
        leader = peer(100, 100)
        self.assertEqual(classify(leader, peer(90, 90), 90)['stage'], 'receiving')
        self.assertEqual(classify(leader, peer(100, 90, last=95), 90)['stage'], 'persisting')
        scheduled = peer(100, 90, diagnostics=dict(last_applying_index=100))
        self.assertEqual(classify(leader, scheduled, 90)['stage'], 'scheduled_not_completed')
        gated = peer(100, 90, diagnostics=dict(last_applying_index=90, raw_node_has_ready=True,
                                             fsm_has_ready=False))
        self.assertEqual(classify(leader, gated, 90)['stage'], 'raw_ready_not_polled')
        self.assertEqual(classify(leader, peer(100, 100), 90)['stage'], 'cpp_behind_proxy')
        self.assertEqual(classify(leader, peer(100, 100), 100)['stage'], 'caught_up')

    def test_commit_growth_does_not_hide_stalled_apply(self):
        tracker = ProgressTracker(20)
        stalled = lambda commit: classify(peer(commit, commit), peer(commit, 50,
            diagnostics=dict(last_applying_index=50)), 50)
        self.assertFalse(tracker.update(143, stalled(100), 1, 0)['stalled'])
        self.assertTrue(tracker.update(143, stalled(200), 1, 21)['stalled'])
        self.assertFalse(tracker.update(143, stalled(200), 2, 22)['stalled'])

    def test_unavailable_endpoint_breaks_continuous_stall_evidence(self):
        tracker = ProgressTracker(20)
        lagging = classify(peer(100, 100), peer(100, 50), 50)
        tracker.update(143, lagging, 1, 0)
        unavailable = tracker.update(143, classify({}, {}, None), 1, 30)
        self.assertFalse(unavailable['stalled'])
        self.assertFalse(tracker.update(143, lagging, 1, 40)['stalled'])

    def test_advancing_apply_resets_stagnation_timer(self):
        tracker = ProgressTracker(20)
        at = lambda applied: classify(peer(100, 100), peer(100, applied), applied)
        tracker.update(143, at(50), 1, 0)
        self.assertFalse(tracker.update(143, at(80), 1, 19)['stalled'])
        self.assertFalse(tracker.update(143, at(80), 1, 21)['stalled'])

    def test_ready_flag_changes_do_not_hide_stalled_apply(self):
        tracker = ProgressTracker(20)
        at = lambda ready: classify(peer(100, 100), peer(100, 50,
            diagnostics=dict(last_applying_index=50, raw_node_has_ready=ready,
                             fsm_has_ready=False)), 50)
        tracker.update(143, at(False), 1, 0)
        self.assertTrue(tracker.update(143, at(True), 1, 21)['stalled'])


if __name__ == '__main__':
    unittest.main()
