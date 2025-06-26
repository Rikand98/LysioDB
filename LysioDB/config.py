class Config:
    def __init__(
        self,
        weight_column=None,
        minimum_count=None,
        question_prefixes=None,
        separator_m=None,
        separator_g=None,
        multi_response_pattern=None,
        ranking_pattern=None,
        grid_pattern=None,
        base_grid_pattern=None,
        single_choice_pattern=None,
        nan_values=None,
        question_map=None,
        area_map=None,
        year_map=None,
        template_map=None,
    ):
        # Constants
        self.WEIGHT_COLUMN = weight_column or "weight"
        self.MINIMUM_COUNT = minimum_count or 5
        self.QUESTION_PREFIXES = question_prefixes or ["Q"]
        self.SEPARATOR_M = separator_m or " = "
        self.SEPARATOR_G = separator_g or " - "
        self.MULTI_RESPONSE_PATTERN = multi_response_pattern or r"C\d+$"
        self.RANKING_PATTERN = ranking_pattern or r"M\d+$"
        self.GRID_PATTERN = grid_pattern or r"_[A]?\d+$"
        self.BASE_GRID_PATTERN = None
        question_prefix_base = r"^(" + "|".join(self.QUESTION_PREFIXES) + r")"
        if single_choice_pattern is None or single_choice_pattern == "":
            self.SINGLE_CHOICE_PATTERN = question_prefix_base + r"\d+*[a-zA-Z]?$"
        else:
            self.SINGLE_CHOICE_PATTERN = (
                question_prefix_base + f"{single_choice_pattern}$"
            )

        self.NAN_VALUES = nan_values or {999: None}

        self.question_map = question_map or {}
        self.area_map = area_map or {}
        self.year_map = year_map or {}
        self.template_map = template_map or {}
