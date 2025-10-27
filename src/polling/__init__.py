def __init__(self):
    """Initialize the incident poller."""
    self.settings = get_settings()
    self.query_generator = QueryGenerator()
    self.signoz_client = SigNozClient()
    self.log_transformer = LogTransformer()
    self.local_storage = LocalStorage()
    self.s3_storage = S3Storage()  # ← Add this
