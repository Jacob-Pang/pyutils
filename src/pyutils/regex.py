def disjoint_regex(*regex) -> str:
    return '|'.join(regex)

def disjoint_token_regex(*tokens: str) -> str:
    return r"(?:" + disjoint_regex(*tokens) + ')'

# PATTERNS
DAYS = disjoint_token_regex(
    "Monday" , "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "monday" , "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
    "Mon" , "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"
)

MONTHS = disjoint_token_regex(
    "January", "February", "March", "April", "May", "June", "July", "August",
            "September", "October", "November", "December",
    "january", "february", "march", "april", "may", "june", "july", "august",
            "september", "october", "november", "december",
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
)

TIME = r"\d{1,2}(:\d+)+\s*(?:a.m.|p.m.|am|pm)*"

SINGLE_CHAR_ONLY = r"^(.)\1*$"

def n_day_n_month_n_year(delimiter: str) -> str:
    return r"\d{1,2}(\s*" + delimiter + r"\s*\d{1,4}){2}"

def n_day_s_month_n_year(delimiter: str) -> str:
    return r"\d{1,2}\s*" + delimiter + r"\s*" + MONTHS + r"\s*(" + \
        delimiter + r"\s*\d{2,4}){0,1}"

def s_month_n_day_n_year() -> str:
    return MONTHS + r"\s*\d{1,2}(,\s*\d{2,4}){0,1}"

def date_regex() -> str:
    return disjoint_regex(
        n_day_n_month_n_year(r"\/"),
        n_day_n_month_n_year(r"\-"),
        n_day_s_month_n_year(r"\/"),
        n_day_s_month_n_year(r"\-"),
        n_day_s_month_n_year(r" "),
        s_month_n_day_n_year()
    )

if __name__ == "__main__":
    pass
