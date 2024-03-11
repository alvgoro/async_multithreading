import asyncio
import concurrent.futures
import time
import numpy as np
import pandas as pd

SLEEP_TIME = 5  # remove, just to debug

def sync_long_task(*args, **kwargs):
    
    # Simulate a heavy long process
    time.sleep(SLEEP_TIME)

    return args[0].sum()


async def async_task(*args, **kwargs):
    try:
        async with kwargs['semaphore']:
            result = await asyncio.to_thread(sync_long_task, args[0], args[1])

        return result
    except Exception as error:
        raise error


def sync_function(*args, **kwargs):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tasks = [async_task(*[row, args[1]], **{'semaphore': kwargs['semaphore']}) for _, row in args[0].iterrows()]
        
        results = loop.run_until_complete(asyncio.gather(*tasks,
                                                         return_exceptions=True))
        return results
    except Exception as error:
        raise error


def main(*args, **kwargs):

    MAX_THREADS = kwargs['max_threads']
    MAX_CONCURRENT_TASKS = kwargs['max_concurrent_tasks']
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    batches = np.array_split(args[0], MAX_THREADS)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS,
                                               thread_name_prefix='Thread_') as executor:

        # Call sync_function() for each batch of dataframe to be analyzied
        sync_functions = [executor.submit(sync_function,
                                          batch,
                                          args[1],
                                          **{'semaphore': semaphore}) for batch in batches]

        done, _ = concurrent.futures.wait(fs=sync_functions,
                                          return_when='ALL_COMPLETED')
    
    final_results = list()
    for future in done:
        for result in future.result():
            if isinstance(result, Exception):
                final_results += [result]
            else:
                final_results += [result]

    return final_results



if __name__ == "__main__":
    
    # Number of threads to use
    MAX_THREADS = 10
    # Número máximo de llamadas asíncronas (llamadas al servicio)
    MAX_CONCURRENT_TASKS = 100

    NCOLS = 100
    heavy_df = pd.DataFrame(np.random.rand(10000, NCOLS))
    small_df = pd.DataFrame(np.random.rand(50, NCOLS))
    
    start_time = time.time()
    small_df['Result'] = main(*[small_df, heavy_df], **{'max_threads': MAX_THREADS, 
                                                        'max_concurrent_tasks': MAX_CONCURRENT_TASKS})
    
    print(f'\nEstimated execution time (monothread, sync): {SLEEP_TIME*len(small_df)} seconds')
    print(f'\nExecution time: {round(time.time()-start_time, 3)} seconds')
