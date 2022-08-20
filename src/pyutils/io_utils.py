import colorama

colorama.init()
TEMPORARY_LINES = 0

def flush_temporary_lines() -> None:
    global TEMPORARY_LINES
    
    for _ in range(TEMPORARY_LINES):
        # move up cursor and delete whole line
        print("\x1b[1A\x1b[2K", end='\r')
    
    TEMPORARY_LINES = 0

def temporary_print(message: str) -> None:
    global TEMPORARY_LINES

    flush_temporary_lines() # Remove existing temporary lines
    TEMPORARY_LINES = len(message.split("\n"))
    print(message)

if __name__ == "__main__":
    pass