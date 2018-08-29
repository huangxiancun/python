from flask import Flask, request
from flask_session import Session
from config import config
import hashlib
from flask.json import jsonify

def create_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)
    Session(app)

    from dsl import dsl
    from multipledata import multiplestock
    from singledata import singlestock
    app.register_blueprint(dsl, url_prefix='/dsl')
    app.register_blueprint(multiplestock, url_prefix='/multiplestock')
    app.register_blueprint(singlestock, url_prefix='/singlestock')

    @app.route('/login/', methods=["POST"])
    def login():
        if request.method == "POST":
            if request.json is not None and "user" in request.json and "password" in request.json:
                user = request.json["user"]
                passwd = hashlib.sha256(request.json["password"]).hexdigest()
                if user == app.user and passwd == app.passwd_token:
                    return jsonify({
                    "respCode": "1000",
                    "token": passwd
                    }), 200

    @app.route('/orgamization/intro/')
    def org_intro():
        return 'org intro'

    #print app.url_map
    return app

