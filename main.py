import os
import csv
import glob
import asyncio
import sys
import subprocess
from typing import List
from multiprocessing import Pool
import json

async def run_scraper(config_file):
    # Modify this command to match your environment if needed
    command = f"python {os.getcwd()}/scraper.py {config_file}"

    print(f'Running command: {command}')  # Console log the command being run

    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()

    print(f'[{config_file!r} exited with {proc.returncode}]')
    if stdout:
        print(f'[stdout]\n{stdout.decode(errors="ignore")}')
    if stderr:
        print(f'[stderr]\n{stderr.decode(errors="ignore")}')
    
    return stdout.decode(errors='ignore')

async def main():
    json_files = glob.glob("*.json")  # Find all json files in the current directory

    print(f'Found json files: {json_files}')  # Console log the found json files

    tasks = []
    for json_file in json_files:
        tasks.append(run_scraper(json_file))  # Create a task for each json file

    print('Starting tasks')  # Console log the start of tasks

    results = await asyncio.gather(*tasks)  # Run all tasks

    print('All tasks completed')  # Console log the completion of tasks

    # combine all results
    combined_results = []
    for result in results:
        if result.strip():  # make sure result is not empty
            try:
                decoded_result = result.encode('utf-8').decode('unicode_escape')  # decode result from bytes to string
                combined_results.extend(json.loads(decoded_result))
            except json.decoder.JSONDecodeError:
                print(f"Skipping invalid JSON: {result}")

    # Now output the data to a CSV
    if combined_results:
        # flatten the combined_results
        combined_results_flattened = [item for sublist in combined_results for item in sublist]

        keys = combined_results_flattened[0].keys()
        with open('scrape.csv', 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(combined_results_flattened)
        print('Data written to scrape.csv')  # Console log the writing of data
    else:
        print("No results to write.")

if __name__ == "__main__":
    print('Starting main')  # Console log the start of the main function
    asyncio.run(main())
    print('Finished main')  # Console log the finish of the main function
