import enum
from ..models.base_models import APIResponse, EAPIResponseCode
from functools import wraps
from requests import Response
from ..config import ConfigClass


def catch_internal(api_namespace):
    '''
    decorator to catch internal server error.
    '''
    def decorator(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as exce:
                if ConfigClass.env == "test":
                    raise
                respon = APIResponse()
                respon.code = EAPIResponseCode.internal_error
                respon.result = None
                err = api_namespace + " " + str(exce)
                respon.error_msg = customized_error_template(
                    ECustomizedError.INTERNAL) % err
                return respon.json_response()
        return inner
    return decorator


class ECustomizedError(enum.Enum):
    '''
    Enum of customized errors
    '''
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    INVALID_FILE_AMOUNT = "INVALID_FILE_AMOUNT"
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    FORGED_TOKEN = "FORGED_TOKEN"
    TOKEN_EXPIRED = "TOKEN_EXPIRED"
    INVALID_TOKEN = "INVALID_TOKEN"
    INTERNAL = "INTERNAL"


def customized_error_template(customized_error: ECustomizedError):
    '''
    get error template
    '''
    return {
        "FILE_NOT_FOUND": "[File not found] %s.",
        "INVALID_FILE_AMOUNT": "[Invalid file amount] must greater than 0",
        "JOB_NOT_FOUND": "[Invalid Job ID] Not Found",
        "FORGED_TOKEN": "[Invalid Token] System detected forged token, \
                    a report has been submitted.",
        "TOKEN_EXPIRED": "[Invalid Token] Already expired.",
        "INVALID_TOKEN": "[Invalid Token] %s",
        "INTERNAL": "[Internal] %s"
    }.get(
        customized_error.name, "Unknown Error"
    )


def jsonrespon_handler(endpoint: str, response: Response):
    '''
    return json response when code starts with 2 , else riase an error
    '''
    if response.status_code // 200 == 1:
        return response.json()
    else:
        error_msg = "[Post Error %s] %s ------ Text: %s, Json: %s " % \
            str(response.status_code), endpoint, \
            str(response.text), str(response.json())
        raise Exception(error_msg)


class APIException(Exception):
    def __init__(self, status_code: int, error_msg: str):
        self.status_code = status_code
        self.content = {
            "code": self.status_code,
            "error_msg": error_msg,
            "result": "",
        }

