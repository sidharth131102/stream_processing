import apache_beam as beam
import logging
import json

# Enable logging
logging.getLogger().setLevel(logging.INFO)

from pipeline.transforms.field_mapper import FieldMapper
from pipeline.transforms.null_defaults import NullDefaults
from pipeline.transforms.concatenation import Concatenation
from pipeline.transforms.business_rules import BusinessRules
from pipeline.transforms.enrichment import Enrichment
from pipeline.transforms.entity_extractor import EntityExtractor

# ✅ ADD
from pipeline.observability.metrics import PipelineMetrics


class TransformationEngine(beam.DoFn):
    def __init__(self, cfg):
        self.mapper = FieldMapper(cfg.get("field_mapping", {}))
        self.null_defaults = NullDefaults(cfg.get("null_defaults", []))
        self.concatenation = Concatenation(cfg.get("concatenations", []))
        self.rules = BusinessRules(cfg.get("business_rules", []))
        self.enrichment = Enrichment(cfg.get("enrichment", []))
        self.entities = EntityExtractor(cfg.get("entity_extraction", []))

    def process(self, event):
        logging.info(f"🔍 Transform input: type={type(event)}")
        original_event = event
        
        if isinstance(event, tuple):
            raise RuntimeError(
                "Unexpected tuple received in TransformationEngine. Dedup is misbehaving."
            )
        
        # 2. Handle raw string
        if isinstance(event, str):
            try:
                event = json.loads(event)
            except Exception as e:
                # ✅ ADD: METRICS
                PipelineMetrics.transform_errors.inc()
                PipelineMetrics.stage_error("transform").inc()

                # ✅ ADD: STRUCTURED LOGGING
                logging.error(json.dumps({
                    "severity": "ERROR",
                    "stage": "transform",
                    "error": "Invalid JSON string in transformation",
                }))

                # 🔒 EXISTING LOGIC (UNCHANGED)
                logging.error(f"❌ Invalid JSON: {str(original_event)[:100]}")
                yield beam.pvalue.TaggedOutput("dlq", {
                    "stage": "transform",
                    "error": "Invalid JSON string in transformation",
                    "raw": str(original_event)[:500]
                })
                return
        
        # 3. Final dict validation
        if not isinstance(event, dict):
            # ✅ ADD: METRICS
            PipelineMetrics.transform_errors.inc()
            PipelineMetrics.stage_error("transform").inc()

            # ✅ ADD: STRUCTURED LOGGING
            logging.error(json.dumps({
                "severity": "ERROR",
                "stage": "transform",
                "error": f"Expected dict, got {type(event)}",
            }))

            # 🔒 EXISTING LOGIC (UNCHANGED)
            logging.error(f"❌ Not dict: type={type(event)}")
            yield beam.pvalue.TaggedOutput("dlq", {
                "stage": "transform",
                "error": f"Expected dict, got {type(event)}",
                "raw": str(original_event)[:500]
            })
            return
        
        # 4. Apply transformations (SAFE)
        try:
            logging.info(f"✅ Transform processing dict with keys: {list(event.keys())}")
            event = self.mapper.apply(event)
            event = self.null_defaults.apply(event)
            event = self.concatenation.apply(event)
            event = self.rules.apply(event)
            event = self.enrichment.apply(event)
            event = self.entities.apply(event)
            logging.info(f"✅ Transform output: {list(event.keys())}")
            yield event

        except Exception as e:
            # ✅ ADD: METRICS
            PipelineMetrics.transform_errors.inc()
            PipelineMetrics.stage_error("transform").inc()

            # ✅ ADD: STRUCTURED LOGGING
            logging.error(json.dumps({
                "severity": "ERROR",
                "stage": "transform",
                "error": str(e),
            }))

            # 🔒 EXISTING LOGIC (UNCHANGED)
            logging.error(f"❌ Transform failed: {str(e)}")
            yield beam.pvalue.TaggedOutput("dlq", {
                "stage": "transform",
                "error": f"Transformation failed: {str(e)}",
                "raw": str(event)[:500]
            })
