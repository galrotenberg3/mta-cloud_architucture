import oci
import io
import json
import logging
import traceback

from fdk import response

class NonHttpRequestError(Exception): pass
BUCKET_NAME = "CloudFuncBucket"
NON_HTTP_REQUEST_RESPONS = "Function loaded properly but not invoked via an HTTP request."

def _extract_params(raw_params: str):
    params = {}
    for segment in raw_params.split('&'):
        key, val = segment.split('=', 1)
        params[key] = val

    return params.get('operation', None), params.get('name', None), params.get('amount', 0)

def parse_request(ctx, data):
    url = ctx.RequestURL()
    method = ctx.Method()
    if method == 'GET': return (url, method, None, None, None)

    raw_params = data.read().decode()
    raw_params = raw_params.replace('+', ' ')  # replace '+' with spaces
    logging.getLogger().info(f"RAW_PARAMS: {raw_params}")
    op, name, amount = _extract_params(raw_params)
    return (url, method, op, name, amount)

def handle_request(os, operation, name, amount):
    handlers = {
        'create': handle_create,
        'update': handle_update,
        'delete': handle_delete,
    }
    handler = handlers.get(operation, None)
    if handler is None:
        raise Exception(f"unknown operation '{operation}'")
    return handler(os, name, amount)

def handle_create(os: oci.object_storage.ObjectStorageClient, name: str, amount: int=0):
    namespace = os.get_namespace().data
    os.put_object(namespace, BUCKET_NAME, name, json.dumps({"name": name, "amount": amount}).encode())
    return f"200 - Create operation done, item '{{name: {name}, amount: {amount}}}' created"

def handle_update(os: oci.object_storage.ObjectStorageClient, name: str, amount: int=0):
    try:
        namespace = os.get_namespace().data
        raw_obj = os.get_object(namespace, BUCKET_NAME, name)
        obj = json.loads(raw_obj.data.content.decode())
        handle_create(os, name, amount)
        return f"200 - Update operation done, updated item amount from '{obj['amount']}' to '{amount}'"
    except Exception as exp:
        return f"404 - Server could not trace item '{name}'"

def handle_delete(os: oci.object_storage.ObjectStorageClient, name: str, amount: int=0):
    try:
        namespace = os.get_namespace().data
        os.delete_object(namespace, BUCKET_NAME, name)
        return f"200 - Delete operation successfully done, removed item '{name}'"
    except:
        return f"404 - Server could not trace item '{name}'"

def handler(ctx, data: io.BytesIO=None):
    if None == ctx.RequestURL():
        return "Function loaded properly but not invoked via an HTTP request."

    signer = oci.auth.signers.get_resource_principals_signer()
    logging.getLogger().info(f"URI: {ctx.RequestURL()}, Method: {ctx.Method()}, Headers: {ctx.Headers()}, HttpHeaders: {ctx.HTTPHeaders()}")
    config = {
        # update with your tenancy's OCID
        "tenancy": "ocid1.bucket.oc1.il-jerusalem-1.aaaaaaaa3a5ysm6vx53nbtyhnttxbcvxl4bh6n5i7ime2q7sle2isngcriyq",
        # replace with the region you are using
        "region": "il-jerusalem-1"
    }
    try:
        object_storage = oci.object_storage.ObjectStorageClient(config, signer=signer)
        namespace = object_storage.get_namespace().data
        # update with your bucket name
        bucket_name = "CloudFuncBucket"
        url, request_type, op, name, amount = parse_request(ctx, data)
        if request_type == "GET":
            file_object_name = ctx.RequestURL()
            if file_object_name.endswith("/"):
                file_object_name += "index.html"

            # strip off the first character of the URI (i.e. the /)
            file_object_name = file_object_name[1:]

            obj = object_storage.get_object(namespace, bucket_name, file_object_name)
            return response.Response(
                ctx, response_data=obj.data.content,
                headers={"Content-Type": obj.headers['Content-type']}
            )
        elif request_type == "POST":
            req_res = handle_request(object_storage, op, name, amount)
            return response.Response(
                ctx, response_data=req_res, headers={"Content-Type": "text/plain"}
            )
        else:
            return "404 - {request_type} is not snsupported"
    except (Exception) as e:
        error = ''.join(traceback.format_exception(None, e, e.__traceback__))
        return response.Response(
            ctx, response_data=f"500 Server error - {error}",
            headers={"Content-Type": "text/plain"}
            )
