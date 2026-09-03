import time


class PipelineTimer:
    """Timer for measuring pipeline execution time."""

    def __init__(self):
        self.start_time = None

    def start(self):
        self.start_time = time.time()

    def elapsed_seconds(self):
        if self.start_time is None:
            return 0

        return round(
            time.time() - self.start_time,
            2
        )


class LayerMetrics:
    """Stores execution and data-quality metrics for one layer."""

    def __init__(self, layer_name):
        self.layer_name = layer_name

        self.start_time = None
        self.duration = None

        self.status = "NOT_STARTED"

        self.input_rows = 0
        self.output_rows = 0

        self.datasets_processed = 0

    def start(self):
        """Start measuring the layer."""
        self.start_time = time.time()
        self.status = "RUNNING"

    def set_input_rows(self, rows):
        """Store input row count."""
        self.input_rows = rows

    def set_output_rows(self, rows):
        """Store output row count."""
        self.output_rows = rows

    def set_datasets_processed(self, count):
        """Store number of datasets processed."""
        self.datasets_processed = count

    def success(self):
        """Mark the layer as successful."""
        if self.start_time is not None:
            self.duration = round(
                time.time() - self.start_time,
                2
            )

        self.status = "SUCCESS"

    def failed(self):
        """Mark the layer as failed."""
        if self.start_time is not None:
            self.duration = round(
                time.time() - self.start_time,
                2
            )

        self.status = "FAILED"

    def get_metrics(self):
        """Return all metrics as a dictionary."""
        return {
            "layer": self.layer_name,
            "status": self.status,
            "duration_seconds": self.duration,
            "input_rows": self.input_rows,
            "output_rows": self.output_rows,
            "datasets_processed": self.datasets_processed
        }
