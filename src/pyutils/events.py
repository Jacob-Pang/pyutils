import asyncio
import time

def wait_for(predicate: callable, timeout: float = None, revaluate_delay: float = .5, **kwargs) -> any:
    """ Busy waiting for event trigger captured by the predicate.

    params:
        predicate (callable): The predicate function to capture the occurrence of the event.
        timeout (float, opt): The maximum time to wait for the event to occur. Waits indefinitely if
                timeout is not specified.
        revaluate_delay (float, opt): The frequency in seconds at which to re-evaluate the predicate.
        **kwargs: Any keyword arguments to pass into the predicate function.
    
    returns:
        predicate_value (any): The return value of the predicate. Returns None if the predicate
                function has not been invoked (timeout is set to negative.)
    """
    start_time = time.time()
    predicate_value = None

    while not timeout or time.time() < start_time + timeout:
        predicate_value = predicate(**kwargs)

        if predicate_value:
            break

        time.sleep(revaluate_delay)
    
    return predicate_value

async def await_for(predicate: callable, timeout: float = None, revaluate_delay: float = .5, **kwargs) -> any:
    """ Async busy waiting for event trigger captured by the predicate.

    params:
        predicate (callable): The predicate function to capture the occurrence of the event.
        timeout (float, opt): The maximum time to wait for the event to occur. Waits indefinitely if
                timeout is not specified.
        revaluate_delay (float, opt): The frequency in seconds at which to re-evaluate the predicate.
        **kwargs: Any keyword arguments to pass into the predicate function.
    
    returns:
        predicate_value (any): The return value of the predicate. Returns None if the predicate
                function has not been invoked (timeout is set to negative.)
    """
    start_time = time.time()
    predicate_value = None

    while not timeout or time.time() < start_time + timeout:
        predicate_value = predicate(**kwargs)

        if predicate_value:
            break

        asyncio.sleep(revaluate_delay)
    
    return predicate_value

if __name__ == "__main__":
    pass