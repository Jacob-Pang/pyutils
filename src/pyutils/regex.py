def disjoint_regex(*regex) -> str:
    return '|'.join(regex)

def disjoint_token_regex(*tokens: str) -> str:
    return r"(?:" + disjoint_regex(*tokens) + ')'

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

def dday_dmonth_dyear(delimiter: str = r"\/") -> str:
    return r"\d{1,2}(\s*" + delimiter + r"\s*\d{1,4}){2}"

def dday_smonth_dyear(delimiter: str = r"\/") -> str:
    return r"\d{1,2}\s*" + delimiter + r"\s*" + MONTHS + r"\s*(" + \
        delimiter + r"\s*\d{2,4}){0,1}"

def smonth_dday_dyear() -> str:
    return MONTHS + r"\s*\d{1,2}(,\s*\d{2,4}){0,1}"

def date_regex() -> str:
    return disjoint_regex(
        dday_dmonth_dyear(r"\/"),
        dday_dmonth_dyear(r"\-"),
        dday_smonth_dyear(r"\/"),
        dday_smonth_dyear(r"\-"),
        dday_smonth_dyear(r" "),
        smonth_dday_dyear()
    )

if __name__ == "__main__":
    pass
