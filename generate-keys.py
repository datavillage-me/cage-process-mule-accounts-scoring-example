from Crypto.PublicKey import RSA


key = RSA.generate(2048)
private_key = key.export_key()
with open("demo-keys/key.pem", "wb") as f:
    f.write(private_key)

public_key = key.publickey().export_key()
with open("demo-keys/public.pem", "wb") as f:
    f.write(public_key)

