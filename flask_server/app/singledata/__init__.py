from flask import Blueprint, abort, request, current_app
from flask.json import jsonify
from singlestock_s import SingleStock as Singlestock_010

singlestock = Blueprint('singlestock', __name__)

@singlestock.before_request
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
        if not request.json or "stockId" not in request.json:
            abort(400)
        elif "version" in request.json and request.json["version"] not in current_app.config['APIVERSION']:
            return jsonify({
                "respCode": "0001",
                "msg": "illegal version"
            }), 200

@singlestock.route("/data/", methods=["POST"])
def singlestock_data():
    if not "version" in request.json:
        request.json["version"] = "v0.1.0"
    
    if request.json["version"] == "v0.1.0":
        sd = Singlestock_010(request.json)
        sd.return_data()
        sd.resp_code()
        return jsonify(sd.results),200
        #pass
