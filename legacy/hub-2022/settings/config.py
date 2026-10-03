# -*- coding: utf-8 -*-
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
### Alias : CertServer.settings & Last Modded : 2022.02.21. ###
Coded with Python 3.10 Grammar by purplepig4657
Description : CertServer Settings
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
from os import path
import sys
import platform
from getpass import getpass
from OpenSSL import crypto


IP_API_URL = "https://api.ipify.org"

TYPE_RSA = crypto.TYPE_RSA
SHA256 = 'SHA256'
FILETYPE_PEM = crypto.FILETYPE_PEM
ONE_YEAR = 365 * 24 * 60 * 60
HOW_MANY_YEARS = 65

OS = platform.system()

CERT_DIR = "cert" if OS == "Windows" else "/etc/certserver"
CERT_FILE = "rootCA.crt"
KEY_FILE = "rootCA.key"
PASS_FILE = "ssl.pass"


ORGANIZATION = "UntactOrder"


class UnitType(object):
    unit_text = "A % Instance"
    CERT = unit_text.replace('%', "CertServer")
    BRIDGE = unit_text.replace('%', "BridgeServer")
    POS = unit_text.replace('%', "OrderBroker")


class RootCA(object):
    """ RootCA Keypair Storage Object """

    def __init__(self):
        # check if root CA certificate is exist.
        if not path.isfile(f"{CERT_DIR}/{CERT_FILE}"):
            print(f"Certificate files not found. You must init(generate a certificate) first.")
            sys.exit(1)

        # ***** An error may occur in later times. *****
        # get a passphrase and key by an expedient way; waitress checks only part of the argv.
        #
        # check if redirection flag is set.
        if [i for i, arg in enumerate(sys.argv) if '--po=' in arg]:  # if --po= is in argv => redirect.
            __PASSPHRASE = input()
            __CA_ENCRYPTED_KEY = ""
            while True:
                try:
                    __CA_ENCRYPTED_KEY += input() + '\n'
                except EOFError:
                    break
            print("Passphrase entered by redirection.")
            print("Certificate Key entered by redirection.")
        elif OS == "Windows" and path.isfile(f"{CERT_DIR}/{PASS_FILE}"):  # if passphrase file is exist (windows only).
            with open(f"{CERT_DIR}/{PASS_FILE}", 'r', encoding='utf-8') as pass_file,\
                    open(f"{CERT_DIR}/{KEY_FILE}", 'r', encoding='utf-8') as ca_key_file:
                __PASSPHRASE = pass_file.read().replace('\n', '').replace('\r', '')
                __CA_ENCRYPTED_KEY = ca_key_file.read()
        else:  # formal input.
            __PASSPHRASE = getpass("Enter passphrase: ")
            __CA_ENCRYPTED_KEY = getpass("Enter certificate key: ") + '\n'
            while True:
                try:
                    # since some errors were found when I used getpass, I replace them with input.
                    # this is just a countermeasure that I added just in case, so please use redirection if possible.
                    __CA_ENCRYPTED_KEY += input() + '\n'
                except KeyboardInterrupt:
                    break

        self.__CA_KEY = crypto.load_privatekey(
            FILETYPE_PEM, __CA_ENCRYPTED_KEY, passphrase=__PASSPHRASE.encode('utf-8'))
        with open(path.join(CERT_DIR, CERT_FILE), 'r') as ca_crt_file:
            self.__CA_CRT = crypto.load_certificate(FILETYPE_PEM, ca_crt_file.read().encode('utf-8'))

    def set_issuer(self, crt: crypto.X509):
        """ Set root CA information."""
        crt.set_issuer(self.__CA_CRT.get_subject())

    def sign(self, crt: crypto.X509):
        """ Sign the crt with the CA(CS) private key. """
        crt.sign(self.__CA_KEY, SHA256)

    def set_authority_key_identifier(self, crt: crypto.X509):
        """ Set authority key identifier extension. """
        crt.add_extensions([
            # The AKID extension specification may have the value none indicating that no AKID shall be included.
            # Otherwise, it may have the value keyid or issuer or both of them, separated by ,. Either or both can have
            # the option always, indicated by putting a colon : between the value and this option. For self-signed
            # certificates the AKID is suppressed unless always is present. By default, the x509, req, and ca apps
            # behave as if none was given for self-signed certificates and keyid, issuer otherwise.
            crypto.X509Extension(b"authorityKeyIdentifier", False, b"keyid:always", issuer=self.__CA_CRT)
        ])
