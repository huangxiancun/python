from flask import Blueprint, abort, request, current_app
from flask.json import jsonify

multiplestock = Blueprint('multiplestock', __name__)

@multiplestock.before_request
def bf_request():
    if "token" not in request.headers:
        return jsonify({
                "respCode": "0002",
                "msg": "Need attach token in headers"
            }), 200
    elif request.headers["token"] != current_app.passwd_token:
        return jsonify({
                "respCode": "0003",
                "msg": "invalid token"
            }), 200    
    elif request.method == "POST":
        if not request.json:
            abort(400)
        elif "version" in request.json and request.json["version"] not in current_app.config['APIVERSION']:
            return jsonify({
                "respCode": "0001",
                "msg": "illegal version"
            }), 200

@multiplestock.route("/data/", methods=["POST"])
def multiplestock_data():
    if not "version" in request.json:
        request.json["version"] == "v0.1.0"
    #TODO
