#!/usr/bin/env python3
#
import json
import datetime
import sys
import time
from collections import deque
from globus_sdk import NativeAppAuthClient, RefreshTokenAuthorizer, TransferClient, TransferData, DeleteData
from globus_sdk.exc import GlobusAPIError

# Test transfer of files from SRC: TACC Stampede2 endpoint to DST Connect personal endpoint on laptop

# File containing access and refresh tokens
TOKEN_FILE = "/home/scblack/.ssh/globus_tokens.json"
# Client Id for scblack
CLIENT_ID = "0259148a-8ae0-44b7-80b5-a4060e92dd3e"

# Destination endpoint,  Globus Personal connect for: scblack-test-laptop (owned by sb56773@utexas.edu)
# Default directory /~/data/globus
ENDPOINT_ID_DST = "4549fadc-7941-11ec-9f32-ed182a728dff"

# Source endpoint, TACC Stampede2 endpoint (owned by tacc@globusid.org)
# No default directory
ENDPOINT_ID_SRC = "7961b534-3f0e-11e7-bd15-22000b9a448b"


# Publicly available tutorial endpoint
# ENDPOINT_ID_SRC = "ddb59aef-6d04-11e5-ba46-22000b92c6ec"

# Code mostly taken from Globus python sdk examples and jpl-neid code
# https://github.com/globus/native-app-examples
# https://github.com/globus/globus-sdk-python

# Methods based on globus example example_copy_paste_refresh_token.py


def load_tokens_from_file(filepath):
    """Load a set of saved tokens."""
    with open(filepath, "r") as f:
        tokens = json.load(f)

    print("Loaded tokens from file. Tokens:")
    print("============================================================================================")
    print(json.dumps(tokens, indent=2, sort_keys=True))
    print("============================================================================================")
    globus_transfer_data = tokens["transfer.api.globus.org"]
    expires_at_epoch = globus_transfer_data["expires_at_seconds"]
    print("Transfer access token expires at epoch   : ", expires_at_epoch)
    expires_at_timestamp = datetime.datetime.fromtimestamp(expires_at_epoch)
    print("Transfer access token expires at datetime: ", expires_at_timestamp)
    print("============================================================================================")
    return tokens


def save_tokens_to_file(filepath, tokens):
    """Save a set of tokens for later use."""
    with open(filepath, "w") as f:
        json.dump(tokens, f)


def update_tokens_file_on_refresh(token_response):
    """
    Callback function passed into the RefreshTokenAuthorizer.
    Will be invoked any time a new access token is fetched.
    """
    save_tokens_to_file(TOKEN_FILE, token_response.by_resource_server)
    print("Saved tokens to file. Tokens:")
    print("============================================================================================")
    print(json.dumps(token_response.by_resource_server, indent=2, sort_keys=True))
    print("============================================================================================")


# Method to recursively list files
# See https://github.com/globus/globus-sdk-python/blob/main/docs/examples/recursive_ls.rst
def recursive_ls(tc, ep, dir_queue, max_depth):
    while dir_queue:
        abs_path, rel_path, depth = dir_queue.pop()
        path_prefix = rel_path + "/" if rel_path else ""
        ls_response = tc.operation_ls(ep, path=abs_path)
        if depth < max_depth:
            dir_queue.extend(
                (
                    ls_response["path"] + item["name"],
                    path_prefix + item["name"],
                    depth + 1,
                )
                for item in ls_response["DATA"]
                if item["type"] == "dir"
            )
        for item in ls_response["DATA"]:
            item["name"] = path_prefix + item["name"]
            print(item["type"], ": ", item["name"])


def main():
    # get/refresh the tokens
    tokens = load_tokens_from_file(TOKEN_FILE)
    transfer_tokens = tokens["transfer.api.globus.org"]

    # Create the authorizer
    auth_client = NativeAppAuthClient(client_id=CLIENT_ID)
    authorizer = RefreshTokenAuthorizer(
        transfer_tokens["refresh_token"],
        auth_client,
        access_token=transfer_tokens["access_token"],
        expires_at=transfer_tokens["expires_at_seconds"],
        on_refresh=update_tokens_file_on_refresh
    )
    print("Created authorizer from tokens.")
    print("============================================================================================")
    print("authorizer access_token:", authorizer.access_token)
    print("authorizer refresh_token:", authorizer.refresh_token)
    print("============================================================================================")

    # Use the authorizer to create a client
    transfer_client = TransferClient(authorizer=authorizer)

    # Get endpoints and set names
    ep_dst = transfer_client.get_endpoint(ENDPOINT_ID_DST)
    ep_dst_name = ep_dst["display_name"]
    ep_dst_default_dir = ep_dst["default_directory"]
    print("Destination endpoint. Name: {} Id: {}".format(ep_dst_name, ENDPOINT_ID_DST))
    print("                      Default dir:", ep_dst_default_dir)
    ep_src = transfer_client.get_endpoint(ENDPOINT_ID_SRC)
    ep_src_name = ep_src["display_name"]
    ep_src_default_dir = ep_src["default_directory"]
    print("Source endpoint. Name: {} Id: {}".format(ep_src_name, ENDPOINT_ID_SRC))
    print("                 Default dir:", ep_src_default_dir)
    print("============================================================================================")

    # Make sure we have valid source and destination directories
    ep_src_dir = ep_src_default_dir
    ep_dst_dir = ep_dst_default_dir
    if ep_src_dir is None:
        ep_src_dir = "/~/"
    if ep_dst_dir is None:
        ep_dst_dir = "/~/"

    # activate destination endpoint
    print("Activating destination endpoint. Name: {} Id: {}".format(ep_dst_name, ENDPOINT_ID_DST))
    try:
        transfer_client.endpoint_autoactivate(ENDPOINT_ID_DST)
    except GlobusAPIError as ex:
        print(ex)
        if ex.http_status == 401:
            sys.exit("Error activating destination endpoint. Most likely refresh token has expired.")
        else:
            raise ex

    # activate source endpoint
    print("Activating source endpoint. Name: {} Id: {}".format(ep_src_name, ENDPOINT_ID_SRC))
    try:
        transfer_client.endpoint_autoactivate(ENDPOINT_ID_SRC)
    except GlobusAPIError as ex:
        print(ex)
        if ex.http_status == 401:
            sys.exit("Error activating source endpoint. Most likely refresh token has expired.")
        else:
            raise ex

    # List all endpoints
    print("============================================================================================")
    print("My Endpoints:")
    for ep in transfer_client.endpoint_search(filter_scope="my-endpoints"):
        print("[EndpointId: {}] DisplayName: {} Default dir: {}".format(ep["id"], ep["display_name"],
                                                                        ep["default_directory"]))
    print("============================================================================================")

    # list files for destination endpoint
    print("============================================================================================")
    print("Listing files for destination endpoint:", ENDPOINT_ID_DST, " path: ", ep_dst_dir)
    for fentry in transfer_client.operation_ls(ENDPOINT_ID_DST, path=ep_dst_dir):
        print("file:", json.dumps(fentry, indent=2, sort_keys=True))
    print("============================================================================================")

    # list files for source endpoint
    ep_src_dir2 = "{}/globus_test".format(ep_src_dir)
    print("============================================================================================")
    print("Listing files for source endpoint:", ENDPOINT_ID_SRC, " path:", ep_src_dir2)
    for fentry in transfer_client.operation_ls(ENDPOINT_ID_SRC, path=ep_src_dir2):
        print("file:", json.dumps(fentry, indent=2, sort_keys=True))
    print("============================================================================================")

    # make a new dir on destination endpoint
    dst_dir = "{}/new_dir".format(ep_dst_dir)
    print("============================================================================================")
    print("Creating new directory on destination endpoint. Dir:", dst_dir)
    transfer_client.operation_mkdir(ENDPOINT_ID_DST, path=dst_dir)
    print("============================================================================================")

    # Now transfer files from source to destination
    # Build up data structure containing info needed for txfr
    src_dir = ep_src_dir2
    f1 = "file1.txt"
    f2 = "file2.txt"
    f3 = "file3.txt"
    f1_src_path = src_dir + "/" + f1
    f2_src_path = src_dir + "/" + f2
    f3_src_path = src_dir + "/" + f3
    f1_dst_path = dst_dir + "/" + f1
    f2_dst_path = dst_dir + "/" + f2
    f3_dst_path = dst_dir + "/" + f3
    files_to_transfer = [{'source': f1_src_path, 'dest': f1_dst_path},
                         {'source': f2_src_path, 'dest': f2_dst_path},
                         {'source': f3_src_path, 'dest': f3_dst_path}]

    txfr_data = TransferData(transfer_client, ENDPOINT_ID_SRC, ENDPOINT_ID_DST, label='', sync_level='size',
                             verify_checksum=False)
    for fentry in files_to_transfer:
        txfr_data.add_item(fentry['source'], fentry['dest'])

    # Start the txfr
    print("============================================================================================")
    print("Transferring files to destination dir:", dst_dir, " from source dir:", src_dir)
    txfr_response = transfer_client.submit_transfer(txfr_data)
    print("Transfer submit response:")
    print("============================================================================================")
    print(txfr_response)
    print("============================================================================================")
    txfr_task_id = txfr_response["task_id"]
    print("Transfer submit task_id:", txfr_task_id)
    # TEST
    # TEST Immediately cancel
    # TEST Attempt cancel immediately, so we can see what response looks like
    # TEST NOTE: This also allows us to see the response when a task fails because it was cancelled.
    # TEST       Response can be seen below when the task is retrieved after the short sleep.
    # task_cancel_resp = transfer_client.cancel_task(txfr_task_id)
    # print("Made immediate cancel request. Cancel response:")
    # print("============================================================================================")
    # print(task_cancel_resp)
    # print("============================================================================================")
    # CancelEnum
    #   COMPLETE - task was already done when request made.
    #     code = TaskComplete
    #   CANCELLED - request completed during call and task should no longer be ACTIVE.
    #             - note that task still may have completed successfully. Task status must be checked.
    #     code = Canceled
    #   ACCEPTED - request was accepted but did not complete during call.
    #     code = CancelAccepted
    #  {
    #    "DATA_TYPE": "result",
    #    "code": "Canceled",
    #    "message": "The task has been cancelled successfully.",
    #    "request_id": "4evUy9jR9",
    #    "resource": "/task/2eb8894a-8518-11ec-8fde-dfc5b31adbac/cancel"
    #  }
    #  {
    #    "DATA_TYPE": "result",
    #    "code": "TaskComplete",
    #    "message": "The task completed before the cancel request was processed.",
    #    "request_id": "cXIfEcTmw",
    #    "resource": "/task/2eb8894a-8518-11ec-8fde-dfc5b31adbac/cancel"
    #  }
    # TEST

    # Pause briefly, check status, then wait for it to finish
    print("Pausing 2 seconds before checking task status")
    time.sleep(2)
    txfr_task_response = transfer_client.get_task(task_id=txfr_task_id)
    print("Got transfer task. Task: ")
    print("============================================================================================")
    print(txfr_task_response)
    print("============================================================================================")
    print("Task status:", txfr_task_response["status"])
    print("============================================================================================")

    # wait for txfr to finish
    print("Waiting for transfer task to finish using timeout: 10 seconds, pollling_interval: 2")
    txfr_done = transfer_client.task_wait(txfr_task_id, timeout=10, polling_interval=2)
    if not txfr_done:
        print("Transfer task did not complete.")
        sys.exit(1)
    else:
        print("Transfer task completed.")
    print("============================================================================================")
    txfr_task_response = transfer_client.get_task(task_id=txfr_task_id)
    print("Transfer task status after transfer:", txfr_task_response["status"])

    # Attempt cancel even though task is done, so we can see what response looks like
    task_cancel_resp = transfer_client.cancel_task(txfr_task_id)
    print("Made cancel request. Cancel response:")
    print("============================================================================================")
    print(task_cancel_resp)
    print("============================================================================================")

    # rename a file on destination endpoint
    f1a = "file1a.txt"
    old_path = dst_dir + "/" + f1
    new_path = dst_dir + "/" + f1a
    print("============================================================================================")
    print("Renaming file on destination endpoint. Old path:", old_path + " New path:", new_path)
    transfer_client.operation_rename(ENDPOINT_ID_DST, oldpath=old_path, newpath=new_path)
    print("============================================================================================")

    # delete a file on destination endpoint
    print("============================================================================================")
    print("Deleting file3.txt on destination endpoint. Path:", new_path)
    del_data = DeleteData(transfer_client, ENDPOINT_ID_DST, recursive=False)
    del_data.add_item(f3_dst_path)
    del_response = transfer_client.submit_delete(del_data)
    print("Delete submit response:")
    print("============================================================================================")
    print(del_response)
    print("============================================================================================")
    del_task_id = del_response["task_id"]
    # Get delete task and print it out
    del_task_response = transfer_client.get_task(task_id=del_task_id)
    print("Got delete task. Task: ")
    print("============================================================================================")
    print(del_task_response)
    print("============================================================================================")
    print("Delete task status:", del_task_response["status"])

    # Wait for delete to finish
    print("Waiting for delete task to finish using timeout: 10 seconds, pollling_interval: 2")
    del_done = transfer_client.task_wait(del_task_id, timeout=10, polling_interval=2)
    if not del_done:
        print("Delete task did not complete.")
        sys.exit(1)
    else:
        print("Delete task completed.")
    print("============================================================================================")

    # Recursively list files on destination endpoint.
    # See https://github.com/globus/globus-sdk-python/blob/main/docs/examples/recursive_ls.rst
    dir_queue = deque()
    dir_queue.append((ep_dst_dir, "", 0))
    max_depth = 4
    print("Recursive listing on destination endpoint. Directory: {} Max depth: {}".format(ep_dst_dir, max_depth))
    recursive_ls(transfer_client, ENDPOINT_ID_DST, dir_queue, max_depth)


# ########################################
# Main
# ########################################
if __name__ == "__main__":
    main()
