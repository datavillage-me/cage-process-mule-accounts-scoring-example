from Crypto.PublicKey import RSA
private_key = RSA.generate(1024)
public_key = private_key.publickey()
print(private_key.exportKey(format='PEM'))
print(public_key.exportKey(format='PEM'))

with open ("demo-keys/private.pem", "w") as prv_file:
    print("{}".format(private_key.exportKey()), file=prv_file)

with open ("demo-keys/public.pem", "w") as pub_file:
    print("{}".format(public_key.exportKey()), file=pub_file)