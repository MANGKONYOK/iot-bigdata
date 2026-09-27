"""Regression checks for the Doc 7.1 findings on src/spark_streaming.py (Issues #4 and #5)."""

import json
import os

import spark_streaming as ss


def event(event_id, room, event_time, temperature, humidity=40.0, received_at="2026-09-26 10:00:01"):
    return {
        "event_id": event_id, "device_id": f"dev-{room}", "room": room, "location_type": None,
        "event_time": event_time, "temperature": temperature, "humidity": humidity, "aqi": 50,
        "ac_status": 1, "latitude": 13.8, "longitude": 100.5, "_topic": f"t/{room}", "_qos": 0,
        "_received_at": received_at, "_landed_at": 0.0,
    }


def to_df(spark, rows):
    return spark.createDataFrame(rows, schema=ss.get_telemetry_schema())


def test_every_publisher_room_gets_a_zone(spark):
    rows = [event(str(i), r, "2026-09-26 10:00:01", 25.0)
            for i, r in enumerate(["living", "kitchen", "laundry", "gym", "bedroom", "terrace"])]
    zones = {r.room: r.spatial_zone for r in ss.apply_spatial_categorization(to_df(spark, rows)).collect()}
    assert zones == {"living": "Indoor", "kitchen": "Indoor", "laundry": "Indoor",
                     "gym": "Indoor", "bedroom": "Indoor", "terrace": "Outdoor"}


def test_one_average_per_zone_per_window(spark):
    # Doc 7.1 test 4: two Indoor rooms in one window must give ONE Indoor row.
    rows = [
        event("1", "living", "2026-09-26 10:00:01", 20.0),
        event("2", "kitchen", "2026-09-26 10:00:02", 30.0),
        event("3", "terrace", "2026-09-26 10:00:03", 40.0),
    ]
    out = {r.spatial_zone: r for r in ss.compute_windowed_aggregations(to_df(spark, rows)).collect()}
    assert set(out) == {"Indoor", "Outdoor"}
    assert out["Indoor"].avg_temperature == 25.0 and out["Indoor"].event_count == 2
    assert out["Indoor"].status == "OK" and out["Outdoor"].status == "ALERT"
    window = out["Indoor"].window
    assert (window.end - window.start).total_seconds() == 10


def test_alert_boundaries_are_strict(spark):
    rows = [
        event("1", "living", "2026-09-26 10:00:01", 35.0, 70.0),
        event("2", "living", "2026-09-26 10:00:11", 35.1, 40.0),
        event("3", "living", "2026-09-26 10:00:21", 20.0, 70.1),
    ]
    out = sorted(ss.compute_windowed_aggregations(to_df(spark, rows)).collect(), key=lambda r: r.window.start)
    assert [r.status for r in out] == ["OK", "ALERT", "ALERT"]


def test_malformed_event_time_falls_back_to_received_at(spark):
    # Doc 7.1 test 6: ISO 'T' timestamps raised CANNOT_PARSE_TIMESTAMP under ANSI mode.
    rows = [event("1", "living", "2026-09-26T10:00:05Z", 22.0, received_at="2026-09-26 10:00:05")]
    out = ss.compute_windowed_aggregations(to_df(spark, rows)).collect()
    assert len(out) == 1 and out[0].event_count == 1


def test_stream_survives_malformed_file_and_drops_late_event(spark, tmp_path):
    # Doc 7.1 tests 7, 8 and 9 through a real readStream on a landing directory.
    landing = tmp_path / "landing"
    landing.mkdir()
    batch_1 = [
        event("1", "living", "2026-09-26 10:00:01", 20.0),
        event("2", "kitchen", "2026-09-26 10:00:02", 22.0),
        event("3", "terrace", "2026-09-26 10:00:03", 45.0),
        event("4", "living", "not-a-time", 21.0, received_at="garbage"),
        event("5", "living", "2026-09-26 10:05:00", 23.0),
    ]
    for e in batch_1:
        (landing / f"{e['event_id']}.json").write_text(json.dumps(e, indent=2))

    def run():
        # foreachBatch rather than the memory sink: the memory sink cannot resume
        # from a checkpoint in update mode, and the second run must resume state.
        collected = []
        stream = (spark.readStream.schema(ss.get_telemetry_schema())
                  .option("multiLine", "true").json(str(landing)))
        query = (ss.compute_windowed_aggregations(stream).writeStream
                 .outputMode("update")
                 .foreachBatch(lambda df, _: collected.extend(df.collect()))
                 .option("checkpointLocation", str(tmp_path / "ckpt"))
                 .trigger(availableNow=True).start())
        query.awaitTermination()
        assert query.exception() is None
        return collected

    rows = run()
    indoor = [r for r in rows if r.spatial_zone == "Indoor" and r.window.start.minute == 0]
    assert indoor[0].event_count == 2  # the unparseable event has a NULL timestamp and is not counted

    # Watermark is now 10:04:30; an event for the 10:00:00 window is 4.5 minutes late.
    late = event("6", "living", "2026-09-26 10:00:04", 99.0)
    on_time = event("7", "living", "2026-09-26 10:05:05", 25.0)
    for e in (late, on_time):
        (landing / f"{e['event_id']}.json").write_text(json.dumps(e, indent=2))
    rows = run()
    assert [r for r in rows if r.window.start.minute == 5]  # the batch did run
    assert not [r for r in rows if r.window.start.minute == 0]  # the late event was dropped
