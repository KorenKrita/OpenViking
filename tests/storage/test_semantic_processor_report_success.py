# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Tests for SemanticProcessor success/error reporting on memory messages."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from openviking.storage.queuefs.semantic_processor import SemanticProcessor


class TestSemanticProcessorMemorySuccessReporting:
    """Test memory-path success/error reporting behavior."""

    def _create_processor(self, monkeypatch, entries, ls_side_effect=None):
        processor = SemanticProcessor(max_concurrent_llm=1)
        processor.report_success = MagicMock()
        processor.report_error = MagicMock()
        processor._ctx_from_semantic_msg = MagicMock(return_value=None)
        processor._merge_request_stats = MagicMock()
        processor._generate_single_file_summary = AsyncMock(
            return_value={"name": "file1.txt", "summary": "test summary"}
        )
        processor._generate_overview = AsyncMock(return_value="# Overview\n\nTest overview")
        processor._extract_abstract_from_overview = MagicMock(return_value="abstract")
        processor._enforce_size_limits = MagicMock(return_value=("overview", "abstract"))
        processor._vectorize_directory = AsyncMock()

        mock_fs = MagicMock()
        if ls_side_effect:
            mock_fs.ls = AsyncMock(side_effect=ls_side_effect)
        else:
            mock_fs.ls = AsyncMock(return_value=entries)
        mock_fs.write_file = AsyncMock()

        monkeypatch.setattr(
            "openviking.storage.queuefs.semantic_processor.get_viking_fs",
            lambda: mock_fs,
        )

        return processor

    @pytest.mark.asyncio
    async def test_on_dequeue_memory_reports_success_once(self, monkeypatch):
        """Memory messages should report success exactly once."""
        processor = self._create_processor(
            monkeypatch,
            [{"name": "file1.txt", "isDir": False}],
        )

        await processor.on_dequeue(
            {
                "uri": "viking://test/memory",
                "context_type": "memory",
                "recursive": True,
            }
        )

        processor.report_success.assert_called_once()
        processor.report_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_dequeue_memory_empty_directory_reports_success_once(self, monkeypatch):
        """Empty memory directories should still complete successfully once."""
        processor = self._create_processor(monkeypatch, [])

        await processor.on_dequeue(
            {
                "uri": "viking://test/memory",
                "context_type": "memory",
                "recursive": True,
            }
        )

        processor.report_success.assert_called_once()
        processor.report_error.assert_not_called()
        processor._vectorize_directory.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_on_dequeue_memory_ls_failure_reports_error(self, monkeypatch):
        """Memory directory ls failure should report error, not success."""
        processor = self._create_processor(
            monkeypatch,
            entries=None,
            ls_side_effect=Exception("Permission denied"),
        )

        await processor.on_dequeue(
            {
                "uri": "viking://test/memory",
                "context_type": "memory",
                "recursive": True,
            }
        )

        processor.report_success.assert_not_called()
        processor.report_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_dequeue_memory_write_failure_reports_error(self, monkeypatch):
        """Memory directory write failure should report error, not success."""
        processor = self._create_processor(
            monkeypatch,
            [{"name": "file1.txt", "isDir": False}],
        )

        # Make write_file raise an exception
        from openviking.storage.queuefs import semantic_processor

        mock_fs = MagicMock()
        mock_fs.ls = AsyncMock(return_value=[{"name": "file1.txt", "isDir": False}])
        mock_fs.write_file = AsyncMock(side_effect=Exception("Disk full"))

        monkeypatch.setattr(
            "openviking.storage.queuefs.semantic_processor.get_viking_fs",
            lambda: mock_fs,
        )

        await processor.on_dequeue(
            {
                "uri": "viking://test/memory",
                "context_type": "memory",
                "recursive": True,
            }
        )

        processor.report_success.assert_not_called()
        processor.report_error.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__])
