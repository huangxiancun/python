import os, hashlib
basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    PORT = 5000
    APIVERSION = ['v0.1.0']
    SESSION_TYPE = 'redis'
    SESSION_COOKIE_NAME = "FDISession"
    PERMANENT_SESSION_LIFETIME = 1800
    SESSION_REFRESH_EACH_REQUEST = True


    @staticmethod
    def init_app(app):
        init_password = "zhuiyikeji888;)"
        app.user = "yibot"
        app.passwd_token = hashlib.sha256(init_password).hexdigest()
        pass

class DevConfig(Config):
    DEBUG = True

class TestConfig(Config):
    TESTING = True

class ProdConfig(Config):
    PORT = 5000

class SQLConfig:
    USER = "root"
    PASSWD = "123456"
    #USER = "newuser"
    #PASSWD = "password"
    DB = "kg_schema"
config = {
    'dev': DevConfig,
    'test': TestConfig,
    'prod': ProdConfig,
    'default': DevConfig
}
