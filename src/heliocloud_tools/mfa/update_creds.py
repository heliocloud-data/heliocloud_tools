# MIT License, 2023, JHUAPL

# simple tool to update .aws/credentials with user-prompted keys
# in a notebook, use %run update_creds.py

import time
import os

home=os.path.expanduser('~')
with open(f"{home}/.aws/credentials","r") as fin:
    lines=fin.readlines()

key=input("Paste AWS access key here:")
secret=input("Paste AWS secret key here:")

with open(f"{home}/.aws/credentials","w") as fout:
    date=time.time()
    fout.write(f"[default]\naws_access_key_id={key}\naws_secret_access_key={secret}\n\n")
    for line in lines:
        if line.startswith("[default]"):
            line = f"[prior-{date}]\n"
        fout.write(line)
print("~/.aws/credentials updated with new [default] credentials.")
