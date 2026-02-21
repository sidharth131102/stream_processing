import logging
import apache_beam as beam
from apache_beam.transforms.trigger import (
    AfterWatermark,
    AfterProcessingTime,
    AfterCount,
    AccumulationMode,
)


class ApplyWindows(beam.PTransform):
    def __init__(self, window_cfg: dict):
        self.cfg = window_cfg

    def expand(self, pcoll):
        window_type = self.cfg.get("type", "fixed")

        if window_type != "fixed":
            raise ValueError(f"Unsupported window type: {window_type}")

        # ----------------------------
        # Required settings
        # ----------------------------
        window_size = self.cfg["window_size_sec"]
        allowed_lateness = self.cfg.get("allowed_lateness_sec", 0)

        # ----------------------------
        # Accumulation mode
        # ----------------------------
        acc = self.cfg.get("accumulation_mode", "DISCARDING")
        if acc == "ACCUMULATING":
            accumulation_mode = AccumulationMode.ACCUMULATING
        else:
            accumulation_mode = AccumulationMode.DISCARDING

        # ----------------------------
        # Triggers
        # ----------------------------
        early_sec = self.cfg.get("early_trigger_sec")
        late_count = self.cfg.get("late_trigger_count")

        trigger = AfterWatermark(
            early=(
                AfterProcessingTime(early_sec)
                if early_sec is not None
                else None
            ),
            late=(
                AfterCount(late_count)
                if late_count is not None
                else None
            ),
        )
        logging.info(f"Configured windowing with size: {window_size}s, allowed lateness: {allowed_lateness}s, accumulation mode: {acc}, early trigger: {early_sec}s, late trigger: {late_count} elements")

        # ----------------------------
        # Apply windowing
        # ----------------------------
        return pcoll | beam.WindowInto(
            beam.window.FixedWindows(window_size),
            allowed_lateness=allowed_lateness,
            trigger=trigger,
            accumulation_mode=accumulation_mode,
        )
