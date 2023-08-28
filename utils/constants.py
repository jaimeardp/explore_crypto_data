import os
from dotenv import load_dotenv

load_dotenv()

_suffix = "_me"

config_vars = {
    "AWS_ACCESS_KEY_ID": os.environ[f'aws_access_key_id{_suffix}'],
    "AWS_SECRET_ACCESS_KEY": os.environ[f'aws_secret_access_key{_suffix}'],
    "AWS_DEFAULT_REGION": "us-east-1",
    "BUCKET_NAME": f'{os.environ[f"BUCKET_NAME{_suffix}"]}',
    "SERVER_PROXY_0": f'{os.environ["SERVER_PROXY_0"]}',
    "SERVER_PROXY_1": f'{os.environ["SERVER_PROXY_1"]}',
    "SERVER_PROXY_2": f'{os.environ["SERVER_PROXY_2"]}',
    "SERVER_PROXY_3": f'{os.environ["SERVER_PROXY_3"]}',
    "SERVER_PROXY_4": f'{os.environ["SERVER_PROXY_4"]}',
    "USERNAME_PROXY": f'{os.environ["USERNAME_PROXIE"]}',
    "PASSWORD_PROXY": f'{os.environ["PASSWORD_PROXY"]}',
    "API_CRYPTO": f'{os.environ["API_CRYPTO"]}',
}

def read_ccloud_config(config_file):
    """
    convert a config file .properties into a dictionary
    """
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    print(conf)
    return conf