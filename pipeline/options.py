from apache_beam.options.pipeline_options import PipelineOptions

class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Environment
        parser.add_argument(
            "--env",
            required=False,
            help="Environment name(optional, usually from pipeline.yaml)"
        )

        # Source
        parser.add_argument(
            "--subscription",
            required=True,
            help="Input Pub/Sub subscription"
        )

        # Config
        parser.add_argument(
            "--config_bucket",
            required=True,
            help="GCS bucket holding YAML configs"
        )

        # Job mode (future-proofing)
        parser.add_argument(
            "--job_mode",
            default="streaming",
            choices=["streaming", "backfill"],
            help="Execution mode"
        )

        # Backfill parameters (ignored for streaming)
        parser.add_argument("--backfill_start_ts")
        parser.add_argument("--backfill_end_ts")
