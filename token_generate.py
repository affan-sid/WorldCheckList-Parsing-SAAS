import jwt

payload_data = {
    "sub": "4242",
    "name": "Fernandez",
    "nickname": "Fer"
}
public_key = 'affann'
new_token = jwt.encode(
    payload=payload_data,
    key=public_key,
    algorithm='HS256'
)

print(new_token)