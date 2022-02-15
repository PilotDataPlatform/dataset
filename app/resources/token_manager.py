import jwt
from app.config import ConfigClass


def verify_token(token):
    '''
    verify download token with the download key
    '''
    try:
        res = jwt.decode(token, ConfigClass.DOWNLOAD_KEY, algorithms=['HS256'])
        return True, res
    except jwt.ExpiredSignatureError:
        return False, "expired"
    except Exception as e:
        return False, "invalid"


def generate_token(payload: dict):
    '''
    generate jwt token with the download key
    '''
    return jwt.encode(payload, key=ConfigClass.DOWNLOAD_KEY, algorithm='HS256').decode('utf-8')

