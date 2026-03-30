from unittest.mock import MagicMock
from zodb_convert.manifest import upload_from_manifest


class TestUploadFromManifest:
    def test_uploads_all_entries(self, tmp_path):
        s3 = MagicMock()
        manifest = tmp_path / "manifest.tsv"

        entries = []
        for i in range(3):
            blob = tmp_path / f"blob{i}"
            blob.write_bytes(f"data{i}".encode())
            entries.append(f"{blob}\tblobs/{i}.blob\t{i}\t5")
        manifest.write_text("\n".join(entries) + "\n")

        stats = upload_from_manifest(str(manifest), s3, workers=2)

        assert s3.upload_file.call_count == 3
        assert stats["uploaded"] == 3
        assert stats["failed"] == 0

    def test_cleans_temp_files_after_upload(self, tmp_path):
        s3 = MagicMock()
        manifest = tmp_path / "manifest.tsv"
        blob = tmp_path / "blob0"
        blob.write_bytes(b"data")
        manifest.write_text(f"{blob}\tblobs/0.blob\t0\t4\n")

        upload_from_manifest(str(manifest), s3, workers=1, cleanup=True)

        assert not blob.exists()

    def test_retries_and_reports_failures(self, tmp_path):
        s3 = MagicMock()
        s3.upload_file.side_effect = Exception("permanent")
        manifest = tmp_path / "manifest.tsv"
        blob = tmp_path / "blob0"
        blob.write_bytes(b"data")
        manifest.write_text(f"{blob}\tblobs/0.blob\t0\t4\n")

        stats = upload_from_manifest(
            str(manifest),
            s3,
            workers=1,
            max_retries=1,
            retry_base_delay=0,
        )

        assert stats["failed"] == 1
        assert stats["uploaded"] == 0

    def test_skips_missing_files(self, tmp_path):
        s3 = MagicMock()
        manifest = tmp_path / "manifest.tsv"
        manifest.write_text("/nonexistent/blob\tblobs/0.blob\t0\t4\n")

        stats = upload_from_manifest(str(manifest), s3, workers=1)

        assert stats["skipped"] == 1
        assert s3.upload_file.call_count == 0

    def test_skips_malformed_lines(self, tmp_path):
        s3 = MagicMock()
        manifest = tmp_path / "manifest.tsv"
        blob = tmp_path / "blob0"
        blob.write_bytes(b"data")
        # Mix valid and malformed lines
        manifest.write_text(f"too\tfew\nfields\n{blob}\tblobs/0.blob\t0\t4\n\n")

        stats = upload_from_manifest(str(manifest), s3, workers=1)

        assert stats["uploaded"] == 1
        assert s3.upload_file.call_count == 1

    def test_empty_manifest(self, tmp_path):
        s3 = MagicMock()
        manifest = tmp_path / "manifest.tsv"
        manifest.write_text("")

        stats = upload_from_manifest(str(manifest), s3, workers=1)

        assert stats["uploaded"] == 0
        assert stats["failed"] == 0
        assert stats["skipped"] == 0

    def test_retry_exhaustion(self, tmp_path):
        """All retries fail, triggering the retry-exhaustion logging path."""
        s3 = MagicMock()
        s3.upload_file.side_effect = Exception("transient")
        manifest = tmp_path / "manifest.tsv"
        blob = tmp_path / "blob0"
        blob.write_bytes(b"data")
        manifest.write_text(f"{blob}\tblobs/0.blob\t0\t4\n")

        stats = upload_from_manifest(
            str(manifest),
            s3,
            workers=1,
            max_retries=3,
            retry_base_delay=0,
        )

        assert stats["failed"] == 1
        assert stats["uploaded"] == 0
        # All 3 attempts were made
        assert s3.upload_file.call_count == 3
