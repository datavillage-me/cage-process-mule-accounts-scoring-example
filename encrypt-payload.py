from Crypto.Cipher import PKCS1_OAEP
from Crypto.PublicKey import RSA
import base64

with open ("demo-keys/public.pem", "r") as pub_file:
    publicKey=pub_file.read()
key = RSA.importKey(str(publicKey))

payload=b"DK311448088022695900"
cipher = PKCS1_OAEP.new(key)
ciphertext = cipher.encrypt(payload)
print(base64.b64encode(ciphertext))

