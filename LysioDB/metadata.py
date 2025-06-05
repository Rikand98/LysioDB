from functools import lru_cache


class Metadata:
    def __init__(self, database):
        self.meta = database.meta

    @lru_cache(maxsize=128)
    def get_column_label(self, column):
        """Cached column label lookup."""
        return self.meta.column_names_to_labels.get(column, "")

    @lru_cache(maxsize=128)
    def get_value_labels(self, column):
        """Cached value label lookup."""
        return self.meta.variable_value_labels.get(column, {})
