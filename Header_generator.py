class Headers(object):
    def __init__(self):
        pass

    def load_headers(self, header_mapping):
        self.header_mapping = header_mapping

    def map_headers(self, header_mapping_keys, header_mapping):
        self.header_mapping = map(header_mapping_keys, header_mapping)


