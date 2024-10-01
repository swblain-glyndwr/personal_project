import json


class Config:
    """
    Performed on instantiation:
    - Load process parameters
    - Load process resources
    - Configure logging
    """
    def __init__(self, mode):
        self.mode = mode

        with open("config/params.json") as f:
            self.params = json.load(f)

        with open("config/resources.json") as f:
            self.resources = json.load(f)

    def set_logger(self):
        # TODO
        pass
