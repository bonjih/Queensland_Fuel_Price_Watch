import json


def load_config():
    with open('configs.json', 'r') as f:
        return json.load(f)


class ParamsDict(dict):
    """
    to load the params only once
    """
    _config = None

    def __init__(self):
        super().__init__()
        if ParamsDict._config is None:
            ParamsDict._config = load_config()
        self.config = ParamsDict._config

    def get_all_items(self):
        return list(self.config.items())

    def get_value(self, key):
        return self.config.get(key)

