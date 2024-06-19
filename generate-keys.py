import rsa

keys = rsa.newkeys(1024)
keys.exportKey()
keys.publickey().exportKey()
