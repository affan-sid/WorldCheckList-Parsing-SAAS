from memory_profiler import profile
from flask import Flask, request, jsonify, abort
from config import load_env
from worldcheck import Parse_WorldCheck
import jwt
import os
app = Flask(__name__)

#pyinstaller -F -w main.py
#above command to create a .exe of this service
public_key = 'veryverysecret.....'
load_env('sql')  
Verify_Token_Flag = os.environ.get("VERIFY_TOKEN_REQUIRED")

def jwt_validation_middleware():
    if Verify_Token_Flag == "1":
        new_token = request.headers.get('Authorization')
        if new_token is None:
            return jsonify({'error': 'Missing JWT token'}), 401
        try:
            decoded_token = jwt.decode(new_token, public_key, options={'verify_signature': False}, algorithms=['HS256'])
            print(decoded_token)
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401

    return None

# Apply the middleware to specific routes or globally
@app.before_request
def before_request():
    error_response = jwt_validation_middleware()
    if error_response is not None:
        return error_response


@app.route('/parseWorldCheck', methods=['GET'])
def parse_Wcheck():
    Tenant_value = request.headers.get('TenantID')
    if Tenant_value is None:
        return jsonify({'error': 'Missing TenantID'}), 400  # Return a JSON response for missing TenantID
    parse_wcheck = Parse_WorldCheck(Tenant_value)
    if parse_wcheck:
        return jsonify({'message': 'Data processed'}), 200
    else:
        return jsonify({'message': 'Data Not Processed'}), 200

if __name__ == "__main__":
    app.run(debug=True, port=501)

#pyinstaller -F -w main.py
#above command to create a .exe of this service
