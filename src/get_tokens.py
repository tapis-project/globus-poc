#!/usr/bin/env python3
#
import globus_sdk
CLIENT_ID = "0259148a-8ae0-44b7-80b5-a4060e92dd3e"

client = globus_sdk.NativeAppAuthClient(CLIENT_ID)
client.oauth2_start_flow(refresh_tokens=True)

authorize_url = client.oauth2_get_authorize_url()
print("Please go to this URL and login: {0}".format(authorize_url))

auth_code = input("Please enter the code you get after login here: ").strip()
token_response = client.oauth2_exchange_code_for_tokens(auth_code)
print("token_response from client.oauth2_exchange_code_for_tokens(auth_code):")
print("============================================================================================")
print(token_response)
print("============================================================================================")

globus_auth_data = token_response.by_resource_server["auth.globus.org"]
globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]

# most specifically, you want these tokens as strings
AUTH_TOKEN = globus_auth_data["access_token"]
TRANSFER_TOKEN = globus_transfer_data["access_token"]
print("Access token: ", AUTH_TOKEN)
print("Transfer token: ", TRANSFER_TOKEN)

# a GlobusAuthorizer is an auxiliary object we use to wrap the token. In
# more advanced scenarios, other types of GlobusAuthorizers give us
# expressive power
authorizer = globus_sdk.AccessTokenAuthorizer(TRANSFER_TOKEN)
tc = globus_sdk.TransferClient(authorizer=authorizer)

# high level interface; provides iterators for list responses
print("My Endpoints:")
for ep in tc.endpoint_search(filter_scope="my-endpoints"):
    print("[{}] {}".format(ep["id"], ep["display_name"]))

ENDPOINT_ID="4549fadc-7941-11ec-9f32-ed182a728dff"
# List tasks
#for tsk in tc.task_list():
#    print(tsk)

# List files
tc.operation_ls(ENDPOINT_ID)
