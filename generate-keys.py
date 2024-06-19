import rsa

(publickey, privatekey) = rsa.newkeys(1024)
print(publickey)
print(privatekey)