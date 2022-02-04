import enum


class ExpectationResultType(str, enum.Enum):
    BOOLEAN_ONLY = "BOOLEAN_ONLY"
    BASIC = "BASIC"
    SUMMARY = "SUMMARY"
    COMPLETE = "COMPLETE"
