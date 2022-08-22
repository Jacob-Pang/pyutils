import time

ID_COUNTER = 0

def generate_unique_id() -> str:
    global ID_COUNTER
    ID_COUNTER = (ID_COUNTER + 1) % 100
    
    return f"{int(time.time())}_{ID_COUNTER}"

if __name__ == "__main__":
    pass