import asyncio
import time

def wait_for(predicate: callable, timeout: float = None, revaluate_delay: float = .5) -> bool:
    start_time = time.time()
    predicate_value = False

    while not timeout or time.time() < start_time + timeout:
        predicate_value = predicate()

        if predicate_value:
            break

        time.sleep(revaluate_delay)
    
    return predicate_value

async def await_for(predicate: callable, timeout: float = None, revaluate_delay: float = .5) -> bool:
    start_time = time.time()
    predicate_value = False

    while not timeout or time.time() < start_time + timeout:
        predicate_value = predicate()

        if predicate_value:
            break

        asyncio.sleep(revaluate_delay)
    
    return predicate_value

if __name__ == "__main__":
    pass