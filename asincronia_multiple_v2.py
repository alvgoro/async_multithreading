import asyncio
import concurrent.futures
import time
import numpy as np

SLEEP_TIME = 5  # remove, just to debug

def sync_long_task(*args, **kwargs):
    
    print(f'Async task\tThread: {kwargs["thread_id"]}\t> Task: {kwargs["idx"]}')

    time.sleep(SLEEP_TIME)

    return


async def async_task(*args, **kwargs):
    
    async with kwargs['semaphore']:
        asyncio.sleep(SLEEP_TIME)
        await asyncio.to_thread(sync_long_task, args[0], **{'thread_id': kwargs['thread_id'],
                                                            'idx': kwargs['idx']})

    return


def sync_function(*args, **kwargs):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tasks = [async_task([sample], **{'semaphore': kwargs['semaphore'],
                                       'thread_id': kwargs['thread_id'],
                                       'idx': idx}) for idx, sample in enumerate(args[0])]
        
        result = loop.run_until_complete(asyncio.gather(*tasks,
                                                        return_exceptions=True))

        return result
    except Exception as error:
        raise RuntimeError(error)


def main(*args, **kwargs):

    MAX_THREADS = kwargs['max_threads']
    MAX_CONCURRENT_TASKS = kwargs['max_concurrent_tasks']
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    batches = np.array_split(args[0], MAX_THREADS)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS,
                                               thread_name_prefix='Thread_') as executor:

        # Call sync_function() for each batch of our iterations
        sync_functions = [executor.submit(sync_function,
                                          batch,
                                          **{'semaphore': semaphore,
                                             'thread_id': idx+1}) for idx, batch in enumerate(batches)]

        done, _ = concurrent.futures.wait(fs=sync_functions,
                                          return_when='ALL_COMPLETED')
    
    final_results = list()
    for future in done:
        if not (future.cancelled() or future.exception()):
            final_results.append(future.result())
    
    return final_results



if __name__ == "__main__":
    
    # Total number of iterations (as a list)
    ITERATIONS = range(100)
    # Number of threads to use
    MAX_THREADS = 10
    # Número máximo de llamadas asíncronas (llamadas al servicio)
    MAX_CONCURRENT_TASKS = 100

    start_time = time.time()
    main(*[ITERATIONS], **{'max_threads': MAX_THREADS,
                            'max_concurrent_tasks': MAX_CONCURRENT_TASKS})
    
    print(f'\nEstimated execution time (monothread, sync): {SLEEP_TIME*len(ITERATIONS)} seconds')
    print(f'\nExecution time: {round(time.time()-start_time, 3)} seconds')