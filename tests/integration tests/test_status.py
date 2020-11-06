import argparse
import requests
import json
import time
import toml


TEST_CONTAINER = 198
TEST_VM = 199

def main(cfg):


    backup_uri = 'https://{}:{}/barque/{}/backup'.format(
        cfg["host"],
        cfg["port"],
        TEST_CONTAINER
    )
    status_uri = 'https://{}:{}/{}/status'.format(
        cfg["host"],
        cfg["port"],
        TEST_CONTAINER
    )
    task_create_request = requests.post(
        status,
        auth=(cfg["username"], cfg["password"]),
        verify=False
    )

    if task_create_request.status_code == 202:
        print("Backup created successfully")
    elif task_create_request.status_code == 500:
        print("Critical failure, got 500")
    else:
        print("Failed to create backup, got response {}".format(task_create_request.text))
        return

    start_time = time.time()
    while (start_time + 60) <= time.time():
        task_status_request = requests.get(
            status_uri,
            auth=(cfg["username"], cfg["password"]),
            verify=False
        )

        if task_status_request.json()['198']['status'] == 'OK':
            print("Backup completed successfully.")
            break

        if task_status_request.json()['198']['status'] == 'error':
            print("Backup task failed!!")
            return

        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test Barque\'s backup functi'\
        'onality using ctid: 199')
    parser.add_argument(
        '-c',
        help='barque configuration file (.toml)',
        action='store'
    )
    cfg = toml.load("./config.toml")
    main(cfg)
