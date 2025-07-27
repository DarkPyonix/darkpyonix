# -*- coding: utf-8 -*-
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
### Alias : CertServer.init & Last Modded : 2022.02.21. ###
Coded with Python 3.10 Grammar by purplepig4657
Description : This is a generator script to generate a CertSercer-signed certificate.
Reference : [CA certificate] https://www.openssl.org/docs/manmaster/man5/x509v3_config.html
            [add subject, authority key] https://stackoverflow.com/questions/14972345/creating-self-signed-certificate-using-pyopenssl
                                         https://rohanc.me/valid-x509-certs-pyopenssl/
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
from os import mkdir, chmod, environ
import requests

from settings import *

isfile = path.isfile


# check root permission
if OS != "Windows":
    if 'SUDO_UID' not in environ.keys():
        print("ERROR: this program requires super user priv.")
        sys.exit(1)

# create rootCA certificate directory.
if not path.isdir(CERT_DIR):
    mkdir(CERT_DIR)

# check if rootCA certificate is exist. => if it does, skip the process.
if isfile(f"{CERT_DIR}/{PASS_FILE}") and isfile(f"{CERT_DIR}/{CERT_FILE}") and isfile(f"{CERT_DIR}/{KEY_FILE}"):
    print("INFO: root CA certificate already exists. Init process skipped.")
    sys.exit(0)
else:
    print(f"INFO: Certificate files not found. Create a directory called '{CERT_DIR}' automatically "
          f"in the same directory as this python file and generate '{CERT_FILE}' and '{KEY_FILE}' files.\n")


def set_certificate_passphrase():
    """ Get a passphrase for the certificate, and save it to a file. """
    # get rootCA certificate password.
    while True:
        __PASSPHRASE = getpass("Enter passphrase: ").replace(" ", "")
        if __PASSPHRASE == "":
            print("ERROR: passphrase cannot be empty.\n")
            continue
        elif '$' in __PASSPHRASE:
            print("ERROR: you should not use '$' in passphrase for bash auto input compatibility.\n")
            continue
        elif __PASSPHRASE == getpass("Enter passphrase again: ").replace(" ", ""):  # check passphrase is same.
            break
        else:
            print("ERROR: Passphrase is not same. retry.\n")

    # write rootCA certificate password to file.
    with open(f"{CERT_DIR}/{PASS_FILE}", 'w+', encoding='utf-8') as pass_file:
        pass_file.write(__PASSPHRASE)
    chmod(f"{CERT_DIR}/{PASS_FILE}", 0o600)  # can only root user read and write.
    return __PASSPHRASE


def proceed_certificate_authority_generation():
    """ Generate CertServer crt file and key file with a 4096bit RSA key. """
    __PASSPHRASE = set_certificate_passphrase()

    keypair = crypto.PKey()
    keypair.generate_key(TYPE_RSA, 4096)

    public_ip = requests.get(IP_API_URL).content.decode()
    country = input("\nEnter your Country Name: ")
    region = input("Enter your State: ")
    city = input("Enter your Location(City): ")

    crt = crypto.X509()
    crt.set_version(2)
    crt.set_serial_number(1)  # serial number must be unique, but we don't care. ^^
    crt.gmtime_adj_notBefore(0)  # start time from now
    crt.gmtime_adj_notAfter(ONE_YEAR * HOW_MANY_YEARS)  # end time

    subject = crt.get_subject()
    subject.CN = public_ip  # external ip
    subject.C = country
    subject.ST = region
    subject.L = city
    subject.O = ORGANIZATION
    subject.OU = UnitType.CERT
    crt.set_subject(crt.get_subject())
    crt.set_issuer(crt.get_subject())
    crt.set_pubkey(keypair)
    crt.add_extensions([  # add extensions; crt does not ues domain name, so need to add subject alternative name.
        # [set this certificate belongs to Certificate Authority(CA)]

        # This is a multivalued extension which indicates whether a certificate is a CA certificate.
        # The first value is CA followed by TRUE or FALSE. If CA is TRUE then an optional pathlen name followed
        # by a non-negative value can be included.
        crypto.X509Extension(b'basicConstraints', True, b'CA:TRUE'),
        # The SKID extension specification has a value with three choices. If the value is the word none then
        # no SKID extension will be included. If the value is the word hash, or by default for the x509, req,
        # and ca apps, the process specified in RFC 5280 section 4.2.1.2. (1) is followed: The keyIdentifier is
        # composed of the 160-bit SHA-1 hash of the value of the BIT STRING subjectPublicKey (excluding the tag,
        # length, and number of unused bits).
        crypto.X509Extension(b"subjectKeyIdentifier", False, b"hash", subject=crt),
        crypto.X509Extension(b"subjectAltName", False, f"IP:{public_ip}".encode('utf-8'))
    ])  # if the client's ip is not exists at crt ip list, the certificate will be disabled.
    # in this situation, authority key using a reference to CA, which is subject key
    # so, if add 'subjectKeyIdentifier' and 'authorityKeyIdentifier' extensions at the same time,
    # it will make error
    crt.add_extensions([
        # The AKID extension specification may have the value none indicating that no AKID shall be included.
        # Otherwise, it may have the value keyid or issuer or both of them, separated by ,. Either or both can have
        # the option always, indicated by putting a colon : between the value and this option. For self-signed
        # certificates the AKID is suppressed unless always is present. By default, the x509, req, and ca apps
        # behave as if none was given for self-signed certificates and keyid, issuer otherwise.
        crypto.X509Extension(b"authorityKeyIdentifier", False, b"keyid:always", issuer=crt)
    ])
    crt.sign(keypair, 'SHA256')  # sign with the CA(CS) private key.

    key_dump = crypto.dump_privatekey(FILETYPE_PEM, keypair, cipher='AES256', passphrase=__PASSPHRASE.encode('utf-8'))
    crt_dump = crypto.dump_certificate(FILETYPE_PEM, crt)
    with open(path.join(CERT_DIR, KEY_FILE), 'w+', encoding='utf-8') as ca_key_file, \
            open(path.join(CERT_DIR, CERT_FILE), 'w+', encoding='utf-8') as ca_crt_file:
        ca_key_file.write(key_dump.decode())
        ca_crt_file.write(crt_dump.decode())
        print("RESULT: Certificate Authority generated successfully.\n")
    chmod(path.join(CERT_DIR, KEY_FILE), 0o600)  # can only root user read and write
    chmod(path.join(CERT_DIR, CERT_FILE), 0o644)  # can any user read


if __name__ == '__main__':
    # set up the certificate password and create rootCA certificate.
    proceed_certificate_authority_generation()
