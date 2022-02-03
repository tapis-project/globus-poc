#!/usr/bin/env python3
#

import json
import datetime

from globus_sdk import NativeAppAuthClient, RefreshTokenAuthorizer, TransferClient
from globus_sdk.exc import GlobusAPIError

# Client Id and endpoint for Globus Personal connect for: scblack-test-laptop
# Default directory /~/data/globus
CLIENT_ID = "0259148a-8ae0-44b7-80b5-a4060e92dd3e"
ENDPOINT_ID = "4549fadc-7941-11ec-9f32-ed182a728dff"

# Code mostly taken from Globus python sdk examples

# ###################################################3
# Methods based on globus example example_copy_paste_refresh_token.py


def load_tokens_from_file(filepath):
    """Load a set of saved tokens."""
    with open(filepath, "r") as f:
        tokens = json.load(f)

    print("Loaded tokens from file. Tokens:")
    print("============================================================================================")
    print(json.dumps(tokens, indent=4, sort_keys=True))
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
    print(json.dumps(token_response.by_resource_server, indent=4, sort_keys=True))
    print("============================================================================================")


def main():
    # get/refresh the tokens
    tokens = load_tokens_from_file("/home/scblack/.ssh/globus_tokens.json")
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
    print("authorizer access_token: " + authorizer.access_token)
    print("authorizer refresh_token: " + authorizer.refresh_token)
    print("============================================================================================")

    # Use the authorizer to create a client
    transfer_client = TransferClient(authorizer=authorizer)

    # activate the endpoint
    try:
        transfer_client.endpoint_autoactivate(ENDPOINT_ID)
    except GlobusAPIError as ex:
        print(ex)
        if ex.http_status == 401:
            sys.exit(
                "Error. Most likely refresh token has expired. "
                "Please delete tokens.json and try again."
            )
        else:
            raise ex

    # List all endpoints
    print("My Endpoints:")
    for ep in transfer_client.endpoint_search(filter_scope="my-endpoints"):
        print("[EndpintId: {}] DisplayName: {} Default dir: {}".format(ep["id"], ep["display_name"],
                                                                       ep["default_directory"]))

    # Make call to get endpoint default dir
    ep = transfer_client.get_endpoint(ENDPOINT_ID)
    # EP_DIR = "/~/data/globus"
    ep_dir = ep["default_directory"]
    # list files
    print("Listing files for endpoint:" + ENDPOINT_ID + " path: " + ep_dir)
    for fentry in transfer_client.operation_ls(ENDPOINT_ID, path=ep_dir):
        print("file: " + json.dumps(fentry, indent=4, sort_keys=True))


# ########################################
# Main
# ########################################
if __name__ == "__main__":
    main()
