import utils.gcputils as gcp


class ControlSheet:
    def __init__(
            self,
            url: str,
            worksheet_name: str,
            schema: list):
        self.url = url
        self.worksheet_name = worksheet_name
        self.schema = schema

    def get_df(self):
        self.df = gcp.spark_df_from_sheets(
            self.url,
            self.worksheet_name,
            self.schema
            )
        return None

    def write_to_history(self):
        # TODO
        pass

    def write_to_latest(self):
        # TODO
        pass
