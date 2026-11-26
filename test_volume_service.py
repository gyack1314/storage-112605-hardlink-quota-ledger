"""共享卷命名空间/配额服务的语义回归测试。"""

import json
import tempfile
import unittest
from pathlib import Path

from volume_service import (
    AppendNotFound,
    DirectoryNotEmpty,
    QuotaExceeded,
    VolumeService,
    iter_scenario_rows,
    run_scenario,
)

ROOT = Path(__file__).resolve().parent
EXAMPLES = ROOT / "examples"


class HardLinkQuotaTests(unittest.TestCase):
    def test_same_identity_counts_once_per_domain_and_full_in_each_domain(self):
        volume = VolumeService()
        volume.add_domain("/press", 100)
        volume.add_domain("/lab", 100)
        volume.mkdir("/press/jobs")
        created = volume.create_file("/press/jobs/sample", 9, "coupon")
        volume.link("/press/jobs/sample", "/press/keep")
        volume.link("/press/jobs/sample", "/lab/sample")

        self.assertEqual(created["inode"], 1)
        press = volume.inspect("/press")
        lab = volume.inspect("/lab")
        self.assertEqual(press["confirmed_bytes"], 9)
        self.assertEqual(lab["confirmed_bytes"], 9)
        self.assertEqual(press["file_charges"][0]["paths_in_domain"], [
            "/press/jobs/sample",
            "/press/keep",
        ])
        self.assertEqual(lab["file_charges"][0]["paths_in_domain"], ["/lab/sample"])

        via_lab = volume.open_file("/lab/sample", "lab-writer")
        self.assertEqual(via_lab["file_id"], 1)
        volume.append(id="grow", handle="lab-writer", max=4)
        self.assertEqual(volume.inspect("/lab")["pending_upper_bound_bytes"], 4)
        self.assertEqual(volume.inspect("/press")["pending_upper_bound_bytes"], 4)

    def test_same_domain_second_alias_adds_no_quota(self):
        volume = VolumeService()
        volume.add_domain("/d", 10)
        volume.create_file("/d/a", 5)
        result = volume.link("/d/a", "/d/b")
        self.assertTrue(result["already_covered_by_domain"])
        self.assertEqual(result["added_quota_bytes"], 0)
        self.assertEqual(volume.inspect("/d")["used_bytes"], 5)

    def test_target_domain_existing_identity_adds_no_second_share_on_rename(self):
        volume = VolumeService()
        volume.add_domain("/a", 100)
        volume.add_domain("/b", 100)
        volume.create_file("/a/f", 10)
        volume.link("/a/f", "/b/f")

        result = volume.rename("/a/f", "/b/new-name", replace=True)
        self.assertTrue(result["ok"])
        b = volume.inspect("/b")
        self.assertEqual(b["used_bytes"], 10)
        self.assertEqual(b["file_charges"][0]["paths_in_domain"], [
            "/b/f",
            "/b/new-name",
        ])
        self.assertEqual(volume.inspect("/a")["used_bytes"], 0)


class AppendReservationTests(unittest.TestCase):
    def test_growth_rejection_explains_confirmed_length_and_request(self):
        volume = VolumeService()
        volume.add_domain("/lab", 15)
        volume.create_file("/lab/sample", 9)
        volume.open_file("/lab/sample", "writer")

        with self.assertRaises(QuotaExceeded) as context:
            volume.append(id="extra", handle="writer", max=8)

        details = context.exception.details
        self.assertEqual(details["blocking_domain"], "/lab")
        self.assertEqual(details["confirmed_length"], 9)
        self.assertEqual(details["requested_max"], 8)
        self.assertEqual(details["limit"], 15)
        self.assertEqual(details["projected_committed_bytes"], 17)
        self.assertIn("当前受确认长度为 9", context.exception.message)
        self.assertIn("申请上限 8", context.exception.message)

    def test_duplicate_append_does_not_create_second_reservation(self):
        volume = VolumeService()
        volume.add_domain("/d", 30)
        volume.create_file("/d/f", 9)
        volume.open_file("/d/f", "h")

        first = volume.append(id="extra", handle="h", max=8)
        second = volume.append(id="extra", handle="h", max=8)
        self.assertFalse(first.get("duplicate", False))
        self.assertTrue(second["duplicate"])
        self.assertEqual(volume.inspect("/d")["pending_upper_bound_bytes"], 8)

        confirmed = volume.confirm(id="extra", appended=4)
        self.assertEqual(confirmed["released_difference_bytes"], 4)
        report = volume.inspect("/d")
        self.assertEqual(report["confirmed_bytes"], 13)
        self.assertEqual(report["pending_upper_bound_bytes"], 0)

        repeated_confirm = volume.confirm(id="extra", appended=4)
        self.assertTrue(repeated_confirm["duplicate"])

    def test_pending_append_permits_lookup_link_unlink_and_rename_when_quota_allows(self):
        volume = VolumeService()
        volume.add_domain("/d", 100)
        volume.create_file("/d/f", 5)
        volume.open_file("/d/f", "h")
        volume.append(id="p", handle="h", max=3)

        self.assertEqual(volume.inspect("/d/f")["type"], "file")
        volume.link("/d/f", "/d/g")
        volume.rename("/d/g", "/d/hardlink-renamed")
        volume.unlink("/d/hardlink-renamed")
        # 原始入口和句柄仍在，身份与预留仍可核对。
        self.assertEqual(volume.inspect("/d")["pending_upper_bound_bytes"], 3)
        self.assertEqual(volume.inspect("h")["domain"], "/d")


class ExampleScenarioTests(unittest.TestCase):
    def test_cross_domain_pending_rename_then_handle_keeps_press_charge(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            service = VolumeService(state)
            rows = list(iter_scenario_rows(
                EXAMPLES / "cross-domain-rename-with-growth.jsonl"
            ))

            responses_before_failure = [
                service.handle_message(row) for row in rows[:8]
            ]
            failed_rename = responses_before_failure[7]
            self.assertFalse(failed_rename["ok"])
            self.assertEqual(failed_rename["blocking_domain"], "/lab")
            self.assertEqual(failed_rename["confirmed_bytes"], 9)
            self.assertEqual(failed_rename["pending_upper_bound_bytes"], 8)

            # 失败重命名必须保持命名空间不变。
            self.assertTrue(service.path_exists("/press/jobs/sample"))
            self.assertFalse(service.path_exists("/lab/jobs/sample"))

            later_responses = []
            for row in rows[8:]:
                if row["kind"] == "close":
                    break
                later_responses.append(service.handle_message(row))

            self.assertTrue(later_responses[1]["ok"])  # 第二次 rename
            press = service.inspect("/press")
            self.assertEqual(press["confirmed_bytes"], 13)
            self.assertTrue(any("writer" in line for line in press["explanations"]))

            # 场景中的 restart 已从同一状态文件恢复；再显式从磁盘开一个进程。
            restarted = VolumeService.load(state)
            self.assertIn("writer", restarted.handles)
            self.assertEqual(restarted.appends["extra"]["status"], "confirmed")

            close_response = service.close("writer")
            self.assertTrue(close_response["ok"])
            self.assertEqual(service.inspect("/press")["used_bytes"], 0)

    def test_replacement_release_waits_for_open_handle_close(self):
        volume = VolumeService()
        volume.add_domain("/main", 30)
        volume.add_domain("/aux", 12)
        volume.create_file("/main/run", 7)
        volume.create_file("/aux/run", 8)
        volume.open_file("/aux/run", "old-read")

        with self.assertRaises(QuotaExceeded) as context:
            volume.rename("/main/run", "/aux/run", replace=True)
        self.assertEqual(context.exception.blocking_domain, "/aux")
        self.assertTrue(volume.path_exists("/main/run"))
        self.assertTrue(volume.path_exists("/aux/run"))
        self.assertEqual(volume.inspect("/aux")["used_bytes"], 8)

        volume.close("old-read")
        retry = volume.rename("/main/run", "/aux/run", replace=True)
        self.assertTrue(retry["ok"])
        self.assertEqual(retry["replaced"]["released_confirmed_bytes"], 8)
        self.assertEqual(volume.inspect("/aux")["used_bytes"], 7)


class RenameAtomicityTests(unittest.TestCase):
    def test_failed_rename_leaves_entire_tree_and_persisted_state_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            volume = VolumeService(state)
            volume.add_domain("/a", 20)
            volume.mkdir("/a/src")
            volume.mkdir("/a/src/inner")
            volume.create_file("/a/src/inner/f", 10)
            volume.mkdir("/a/dst")
            volume.mkdir("/a/dst/nonempty")
            volume.create_file("/a/dst/nonempty/g", 1)

            with self.assertRaises(DirectoryNotEmpty):
                volume.rename("/a/src", "/a/dst")

            reloaded = VolumeService.load(state)
            self.assertTrue(reloaded.path_exists("/a/src/inner/f"))
            self.assertTrue(reloaded.path_exists("/a/dst/nonempty/g"))
            self.assertFalse(reloaded.path_exists("/a/dst/inner/f"))

    def test_quota_failure_in_cross_domain_tree_is_atomic(self):
        volume = VolumeService()
        volume.add_domain("/a", 100)
        volume.add_domain("/b", 5)
        volume.mkdir("/a/tree")
        volume.create_file("/a/tree/f1", 4)
        volume.create_file("/a/tree/f2", 4)

        with self.assertRaises(QuotaExceeded) as context:
            volume.rename("/a/tree", "/b/tree")
        self.assertEqual(context.exception.details["blocking_domain"], "/b")
        self.assertTrue(volume.path_exists("/a/tree/f1"))
        self.assertTrue(volume.path_exists("/a/tree/f2"))
        self.assertFalse(volume.path_exists("/b/tree/f1"))

    def test_nonempty_directory_cannot_be_replaced_even_with_replace(self):
        volume = VolumeService()
        volume.add_domain("/d", 100)
        volume.mkdir("/d/src")
        volume.mkdir("/d/target")
        volume.create_file("/d/target/child", 1)
        with self.assertRaises(DirectoryNotEmpty):
            volume.rename("/d/src", "/d/target", replace=True)


class RestartAndQuarantineTests(unittest.TestCase):
    def test_restart_preserves_open_handles_and_pending_allocations(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            volume = VolumeService(state)
            volume.add_domain("/d", 50)
            volume.create_file("/d/f", 9)
            volume.open_file("/d/f", "writer")
            volume.append(id="extra", handle="writer", max=8)

            volume.restart()
            self.assertEqual(volume.restart_count, 1)
            self.assertIn("writer", volume.handles)
            self.assertEqual(volume.appends["extra"]["status"], "pending")
            self.assertEqual(volume.inspect("/d")["pending_upper_bound_bytes"], 8)

            new_process = VolumeService.load(state)
            self.assertIn("writer", new_process.handles)
            self.assertEqual(
                new_process.inspect("/d")["pending_upper_bound_bytes"], 8
            )
            result = new_process.confirm(id="extra", appended=2)
            self.assertEqual(result["confirmed_size"], 11)

    def test_orphan_with_pending_write_is_quarantined_then_cleaned(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            volume = VolumeService(state)
            volume.add_domain("/d", 100)
            volume.create_file("/d/sample", 5)
            fid = volume.path_index["/d/sample"]
            volume.open_file("/d/sample", "writer")
            volume.append(id="write", handle="writer", max=7)

            volume.unlink("/d/sample")
            self.assertIn(fid, volume.files)  # 句柄仍在。
            closed = volume.close("writer")
            self.assertTrue(closed["quarantined"])
            self.assertTrue(volume.files[fid]["quarantined"])
            self.assertEqual(volume.inspect("/d")["used_bytes"], 0)
            self.assertEqual(volume.inspect("/d")["pending_upper_bound_bytes"], 7)

            restarted = VolumeService.load(state)
            self.assertTrue(restarted.files[fid]["quarantined"])
            self.assertEqual(
                restarted.inspect_volume()["pending_appends"][0]["id"], "write"
            )

            confirmed = restarted.confirm(id="write", appended=3)
            self.assertTrue(confirmed["cleaned_up"])
            self.assertNotIn(fid, restarted.files)
            self.assertEqual(restarted.inspect("/d")["pending_upper_bound_bytes"], 0)

            new_file = restarted.create_file("/d/new", 1)
            self.assertNotEqual(new_file["inode"], fid)
            self.assertEqual(new_file["inode"], fid + 1)

    def test_confirm_unknown_append_is_reported(self):
        volume = VolumeService()
        volume.add_domain("/d", 10)
        with self.assertRaises(AppendNotFound):
            volume.confirm(id="missing", appended=0)


class JsonScenarioEntryTests(unittest.TestCase):
    def test_all_examples_dispatch_and_inspect_are_serializable(self):
        for example in EXAMPLES.glob("*.jsonl"):
            with self.subTest(example=example.name):
                responses = run_scenario(example)
                json.dumps(responses, ensure_ascii=False)
                self.assertTrue(responses)
                self.assertTrue(all("ok" in response for response in responses))


if __name__ == "__main__":
    unittest.main()
