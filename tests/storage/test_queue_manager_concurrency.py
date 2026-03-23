# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Tests for QueueManager concurrency configuration."""

from unittest.mock import MagicMock, patch

import pytest

from openviking.storage.queuefs.queue_manager import QueueManager


class TestQueueManagerConcurrency:
    """Test that QueueManager respects max_concurrent configuration."""

    def test_semantic_queue_uses_configured_max_concurrent(self):
        """Bug #873: Semantic queue should use _max_concurrent_semantic config."""
        mock_agfs = MagicMock()
        max_semantic = 50

        manager = QueueManager(
            agfs=mock_agfs,
            timeout=10,
            mount_point="/queue",
            max_concurrent_embedding=10,
            max_concurrent_semantic=max_semantic,
        )

        # Create a mock queue for testing _start_queue_worker
        mock_queue = MagicMock()
        mock_queue.name = "Semantic"

        # Patch threading.Thread to capture the arguments
        with patch("threading.Thread") as mock_thread:
            mock_thread_instance = MagicMock()
            mock_thread.return_value = mock_thread_instance

            manager._start_queue_worker(mock_queue)

            # Verify Thread was called with correct max_concurrent
            call_args = mock_thread.call_args
            args = call_args[1]["args"]  # positional args passed to Thread
            max_concurrent_passed = args[2]

            assert max_concurrent_passed == max_semantic, (
                f"Expected max_concurrent={max_semantic} for Semantic queue, "
                f"but got {max_concurrent_passed}"
            )

    def test_embedding_queue_uses_configured_max_concurrent(self):
        """Embedding queue should use _max_concurrent_embedding config."""
        mock_agfs = MagicMock()
        max_embedding = 15

        manager = QueueManager(
            agfs=mock_agfs,
            timeout=10,
            mount_point="/queue",
            max_concurrent_embedding=max_embedding,
            max_concurrent_semantic=100,
        )

        mock_queue = MagicMock()
        mock_queue.name = "Embedding"

        with patch("threading.Thread") as mock_thread:
            mock_thread_instance = MagicMock()
            mock_thread.return_value = mock_thread_instance

            manager._start_queue_worker(mock_queue)

            call_args = mock_thread.call_args
            args = call_args[1]["args"]
            max_concurrent_passed = args[2]

            assert max_concurrent_passed == max_embedding, (
                f"Expected max_concurrent={max_embedding} for Embedding queue, "
                f"but got {max_concurrent_passed}"
            )

    def test_unknown_queue_uses_default_max_concurrent(self):
        """Unknown queues should default to max_concurrent=1."""
        mock_agfs = MagicMock()

        manager = QueueManager(
            agfs=mock_agfs,
            timeout=10,
            mount_point="/queue",
            max_concurrent_embedding=10,
            max_concurrent_semantic=100,
        )

        mock_queue = MagicMock()
        mock_queue.name = "UnknownQueue"

        with patch("threading.Thread") as mock_thread:
            mock_thread_instance = MagicMock()
            mock_thread.return_value = mock_thread_instance

            manager._start_queue_worker(mock_queue)

            call_args = mock_thread.call_args
            args = call_args[1]["args"]
            max_concurrent_passed = args[2]

            assert max_concurrent_passed == 1, (
                f"Expected max_concurrent=1 for unknown queue, "
                f"but got {max_concurrent_passed}"
            )


if __name__ == "__main__":
    pytest.main([__file__])
