import unittest

from message_store_open_telemetry.tracing import to_low_cardinality_subject


class ToLowCardinalitySubjectTest(unittest.TestCase):
    def test_redacts_trailing_object_id(self):
        self.assertEqual(
            to_low_cardinality_subject("import-podcast:command.61dc61d8a5073a4d0b036cc5"),
            "import-podcast:command.{id}",
        )
        self.assertEqual(
            to_low_cardinality_subject("import-podcast.61dc61d8a5073a4d0b036cc5"),
            "import-podcast.{id}",
        )

    def test_redacts_mid_subject_id_keeping_suffix(self):
        self.assertEqual(
            to_low_cardinality_subject("audio-converter.6a44e534ece4b38845d87f81.file"),
            "audio-converter.{id}.file",
        )

    def test_keeps_command_discriminator(self):
        self.assertEqual(
            to_low_cardinality_subject("notify-socket:command.61e7b88abf4bdd4eded8666b"),
            "notify-socket:command.{id}",
        )

    def test_redacts_uuid(self):
        self.assertEqual(
            to_low_cardinality_subject("job.550e8400-e29b-41d4-a716-446655440000.done"),
            "job.{id}.done",
        )

    def test_leaves_id_free_subject_untouched(self):
        self.assertEqual(
            to_low_cardinality_subject("platform.livestreams.heartbeats"),
            "platform.livestreams.heartbeats",
        )

    def test_does_not_redact_short_hex_tokens(self):
        # "deadbeef" is 8 chars — not ObjectId-length; must survive.
        self.assertEqual(
            to_low_cardinality_subject("file:command.deadbeef"),
            "file:command.deadbeef",
        )


if __name__ == "__main__":
    unittest.main()
