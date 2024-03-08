import asyncio
import concurrent.futures
import random
import time
import numpy as np

def sync_task(input, **kwargs):
    
    print(f'Async task\tHilo: {kwargs["thread_id"]}\t> Tarea: {kwargs["idx"]}')
    # Simulate a long variable computation
    time.sleep(random.randint(a=0, b=10))

    return


async def async_task(input, **kwargs):
    
    async with kwargs['semaphore']:
        await asyncio.to_thread(sync_task, input, **kwargs)

    return


def sync_function(my_input, **kwargs):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = list()
        tasks = [async_task(sample, **{'semaphore': kwargs['semaphore'],
                                       'thread_id': kwargs['thread_id'],
                                       'idx': idx}) for idx, sample in enumerate(my_input)]
        
        result = loop.run_until_complete(asyncio.gather(*tasks,
                                                        return_exceptions=True))

        return result
    except Exception as error:
        raise RuntimeError(error)


def main(iterations, **kwargs):

    MAX_THREADS = kwargs['max_threads']
    MAX_CONCURRENT_TASKS = kwargs['max_concurrent_tasks']
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    # Split iterations into 
    batches = np.array_split(iterations, MAX_THREADS)

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
    
    print('FINISH')

    return final_results



if __name__ == "__main__":
    
    # Number of total iterations
    my_input = range(100)
    # Number of threads to use
    MAX_THREADS = 10
    # Número máximo de llamadas asíncronas (llamadas al servicio)
    MAX_CONCURRENT_TASKS = 50

    main(my_input, **{'max_threads': MAX_THREADS,
                            'max_concurrent_tasks': MAX_CONCURRENT_TASKS})