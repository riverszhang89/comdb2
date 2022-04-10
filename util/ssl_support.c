/*
   Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

/* myself */
#include <ssl_support.h>

/* sys */
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>

/* openssl */
#define OPENSSL_THREAD_DEFINES
#include <openssl/opensslconf.h>

#include <openssl/conf.h>
#include <openssl/opensslv.h>
#include <openssl/crypto.h>
#include <openssl/pem.h>

/* bb */
#if SBUF2_SERVER
#  include <openssl/rand.h> /* RAND_pseudo_bytes() */
#  include "mem_util.h" /* subsystem malloc routines. */
#  include <mem_override.h> /* override malloc routines. */
#endif

#ifdef my_ssl_println
#  undef my_ssl_println
#endif
#ifdef my_ssl_eprintln
#  undef my_ssl_eprintln
#endif
#define my_ssl_println(fmt, ...)     \
    ssl_println("Generic", fmt, ##__VA_ARGS__)
#define my_ssl_eprintln(fmt, ...)    \
    ssl_eprintln("Generic", "%s: " fmt, __func__, ##__VA_ARGS__)

#if SBUF2_SERVER
/* server-side session id context */
static unsigned char sid_ctx[8];
#endif

int SBUF2_FUNC(ssl_new_ctx)(SSL_CTX **pctx, ssl_mode mode, const char *dir, char **pcert, X509 **px509cert, char **pkey,
                            EVP_PKEY **pevppkey, char **pca, X509 **px509ca, char **pcrl, X509_CRL **px509crl,
                            long sess_sz, const char *ciphers, double mintlsver, char *err, size_t n)
{
    SSL_CTX *myctx;
    char *buffer, *cert, *key, *ca, *crl;
    int rc = 0;
    int servermode;
    struct stat buf;
    long protocols = 0;
    int ii;
    FILE *fp = NULL;
    X509 *x509_cert = NULL, *x509_cert_chain = NULL, *x509_ca = NULL;
    EVP_PKEY *evp_pkey = NULL;
#if HAVE_CRL
    X509_CRL *x509_crl = NULL;
#endif

#if SBUF2_SERVER
    servermode = 1;
#else
    servermode = 0;
#endif
    myctx = NULL;
    X509_STORE *cs = NULL;
    cert = *pcert;
    key = *pkey;
    ca = *pca;
    crl = *pcrl;

    if (px509cert != NULL)
        x509_cert = *px509cert;
    if (px509ca != NULL)
        x509_ca = *px509ca;
    if (pevppkey != NULL)
        evp_pkey = *pevppkey;
#if HAVE_CRL
    if (px509crl != NULL)
        x509_crl = *px509crl;
#endif

    /* If we are told to verify peer, and cacert file is NULL,
       we explicitly make one with the default name so that
       ssl_new_ctx() would fail if it could not load the CA. */
    if (mode >= SSL_VERIFY_CA && *pca == NULL) {
        if (dir == NULL) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "A trusted CA certificate is required "
                         "to verify server certificates.");
            goto error;
        }
        ca = malloc(strlen(dir) + sizeof("/" DEFAULT_CA));
        if (ca == NULL) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Failed to allocate memory for cacert: %s.",
                         strerror(errno));
            goto error;
        }
        /* overflow-safe */
        sprintf(ca, "%s/%s", dir, DEFAULT_CA);
    }

    /* If we're given a directory, find files under it. */
    if (dir != NULL) {
        if (cert == NULL) {
            buffer = alloca(strlen(dir) + sizeof("/" DEFAULT_CERT));
            sprintf(buffer, "%s/%s", dir, DEFAULT_CERT);

            if ((rc = access(buffer, R_OK)) == 0) {
                cert = strdup(buffer);
                if (cert == NULL) {
                    ssl_sfeprint(err, n, my_ssl_eprintln,
                            "Failed to duplicate string: %s.",
                            strerror(errno));
                    goto error;
                }
            } else if (servermode) {
                ssl_sfeprint(err, n, my_ssl_eprintln,
                        "Could not find server certificate:%s.",
                        buffer);
                goto error;
            }
        }

        if (key == NULL) {
            buffer = alloca(strlen(dir) + sizeof("/" DEFAULT_KEY));
            sprintf(buffer, "%s/%s", dir, DEFAULT_KEY);

            if ((rc = access(buffer, R_OK)) == 0) {
                key = strdup(buffer);
                if (key == NULL) {
                    ssl_sfeprint(err, n, my_ssl_eprintln,
                            "Failed to duplicate string: %s.",
                            strerror(errno));
                    goto error;
                }
            } else if (servermode) {
                ssl_sfeprint(err, n, my_ssl_eprintln,
                        "Could not find server key:%s.",
                        buffer);
                goto error;
            }
        }

        if (ca == NULL) {
            buffer = alloca(strlen(dir) + sizeof("/" DEFAULT_CA));
            sprintf(buffer, "%s/%s", dir, DEFAULT_CA);

            if ((rc = access(buffer, R_OK)) == 0) {
                ca = strdup(buffer);
                if (ca == NULL) {
                    ssl_sfeprint(err, n, my_ssl_eprintln,
                                 "Failed to duplicate string: %s.",
                                 strerror(errno));
                    goto error;
                }
            }
        }

#if HAVE_CRL
        if (crl == NULL) {
            buffer = alloca(strlen(dir) + sizeof("/" DEFAULT_CRL));
            sprintf(buffer, "%s/%s", dir, DEFAULT_CRL);

            if ((rc = access(buffer, R_OK)) == 0) {
                crl = strdup(buffer);
                if (crl == NULL) {
                    ssl_sfeprint(err, n, my_ssl_eprintln,
                                 "Failed to duplicate string: %s.",
                                 strerror(errno));
                    goto error;
                }
            }
        }
#endif /* HAVE_CRL */
    }

    /* Test permissions. */

    /* Test read permission on certificate. */
    if (cert != NULL && (rc = access(cert, R_OK)) != 0) {
        ssl_sfeprint(err, n, my_ssl_eprintln,
                     "Failed to read certificate %s: %s.", cert,
                     strerror(errno));
        goto error;
    }

    if (key != NULL) {
        if ((rc = stat(key, &buf)) != 0) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Failed to access key %s: %s.", key, strerror(errno));
            goto error;
        }

        /* Key must to be owned by either us or root. */
        if (buf.st_uid != geteuid() && buf.st_uid != 0) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Key %s must be owned by root or "
                         "the effective user of the database process.",
                         key);
            rc = EACCES;
            goto error;
        }

        /* Key must be 0 to O/G if owned by us. */
        if (buf.st_uid == geteuid() && buf.st_mode & (S_IRWXG | S_IRWXO)) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Permissions for key %s are too open.", key);
            rc = EACCES;
            goto error;
        }

        /* Key must be 4 to G and 0 to O, if owned by root. */
        if (geteuid() != 0 &&
            buf.st_uid == 0 &&
            buf.st_mode & (S_IWGRP | S_IXGRP | S_IRWXO)) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Permissions for key %s are too open.", key);
            rc = EACCES;
            goto error;
        }
    }

    /* Test read permission on cacert. */
    if (ca != NULL && (rc = access(ca, R_OK)) != 0) {
        /* User has provided us with root CA. */
        ssl_sfeprint(err, n, my_ssl_eprintln, "Could not read cacert %s: %s.",
                     ca, strerror(errno));
        goto error;
    }

#if HAVE_CRL
    /* Test read permission on crl. */
    if (crl != NULL && (rc = access(crl, R_OK)) != 0) {
        /* User has provided us with root CA. */
        ssl_sfeprint(err, n, my_ssl_eprintln, "Could not read CRL %s: %s.", crl,
                     strerror(errno));
        goto error;
    }
#endif /* HAVE_CRL */

    /* Create SSL context. */
    rc = 1;
    ERR_clear_error();
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    myctx = SSL_CTX_new(TLS_method());
#else
    myctx = SSL_CTX_new(SSLv23_method());
#endif
    if (myctx == NULL) {
        ssl_sfliberrprint(err, n, my_ssl_eprintln,
                          "Failed to create SSL context");
        rc = ERR_get_error();
        goto error;
    }

    /* Make sure the obselete SSL v2 & v3 protocols are always disallowed. */
    if (mintlsver < 0)
        mintlsver = 0;

    #define XMACRO_SSL_NO_PROTOCOLS(a, b, c) {a,b,c},
    struct ssl_no_protocols ssl_no_protocols[] = {
        SSL_NO_PROTOCOLS
    };
    #undef XMACRO_SSL_NO_PROTOCOLS

    for (ii = 0; ii != sizeof(ssl_no_protocols) / sizeof(ssl_no_protocols[0]);
         ++ii) {
        if (ssl_no_protocols[ii].tlsver < mintlsver)
            protocols |= ssl_no_protocols[ii].opensslver;
    }

    /* Disable SSL protocols to prevent POODLE attack (CVE-2014-3566). */
    if (protocols != 0)
        SSL_CTX_set_options(myctx, protocols);

    /* We need the flag to be able to write as fast as possible.
       We let sbuf2/comdb2buf take care of uncomplete writes. */
    SSL_CTX_set_mode(myctx, SSL_MODE_ENABLE_PARTIAL_WRITE |
                            SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

    /* Adjust session cache size. */
    if (sess_sz < 0)
        sess_sz = SSL_SESSION_CACHE_MAX_SIZE_DEFAULT;
    SSL_CTX_sess_set_cache_size(myctx, sess_sz);

#if SBUF2_SERVER
    /* Set up session id context in server mode. */
    if (sess_sz > 0) {
        if (RAND_bytes(sid_ctx, sizeof(sid_ctx)) != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln,
                              "Failed to get random bytes");
            rc = ERR_get_error();
            goto error;
        }
        SSL_CTX_set_session_id_context(myctx, sid_ctx, sizeof(sid_ctx));
    }

    rc = SSL_CTX_set_cipher_list(myctx, ciphers);
    if (rc != 1) {
        ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to set cipher list");
        goto error;
    }
#endif

    if ((cs = SSL_CTX_get_cert_store(myctx)) == NULL) {
        ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to get cert store.");
        goto error;
    }

    if (cert != NULL || servermode == 1) {
        if (x509_cert == NULL) {
            fp = fopen(cert, "r");
            if (fp == NULL) {
                ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to open certificate");
                goto error;
            }

            x509_cert = PEM_read_X509_AUX(fp, NULL, NULL, NULL);
        }
        if (x509_cert == NULL) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to read certificate");
            goto error;
        }
        rc = SSL_CTX_use_certificate(myctx, x509_cert);
        if (rc != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to use certificate");
            goto error;
        }

        if (fp != NULL) {
            x509_cert_chain = PEM_read_X509(fp, NULL, NULL, NULL);
            while (x509_cert_chain != NULL) {
                rc = SSL_CTX_add0_chain_cert(myctx, x509_cert_chain);
                if (rc != 1) {
                    ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to add chain certificate");
                    goto error;
                }
                x509_cert_chain = PEM_read_X509(fp, NULL, NULL, NULL);
            }
            fclose(fp);
        }
        fp = NULL;
    }

    if (key != NULL || servermode == 1) {
        if (evp_pkey == NULL) {
            fp = fopen(key, "r");
            if (fp == NULL) {
                ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to open private key");
                goto error;
            }
            evp_pkey = PEM_read_PrivateKey(fp, NULL, NULL, NULL);
            fclose(fp);
            fp = NULL;
        }
        if (evp_pkey == NULL) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to read private key");
            goto error;
        }
        /* Use private key. Force error if no cert in server mode */
        rc = SSL_CTX_use_PrivateKey(myctx, evp_pkey);
        if (rc != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to use private key");
            goto error;
        }
    }

    if (key != NULL || cert != NULL) {
        rc = SSL_CTX_check_private_key(myctx);
        if (rc != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to validate private key");
            goto error;
        }
    }

    /* Load CA cert. */
    if (ca != NULL) {
        if (x509_ca == NULL) {
            fp = fopen(ca, "r");
            if (fp == NULL) {
                ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to open certificate");
                goto error;
            }
            x509_ca = PEM_read_X509_AUX(fp, NULL, NULL, NULL);
            fclose(fp);
            fp = NULL;
        }
        if (x509_ca == NULL) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to read certificate");
            goto error;
        }
        rc = X509_STORE_add_cert(cs, x509_ca);
        if (rc != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to load cacert");
            goto error;
        }

        if (servermode)
            SSL_CTX_add_client_CA(myctx, x509_ca);

        SSL_CTX_set_verify(myctx, SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE, NULL);
    }

#if HAVE_CRL
    /* Load the certificate revocation list (CRL). */
    if (crl != NULL) {
        if (x509_crl == NULL) {
            fp = fopen(crl, "r");
            if (fp == NULL) {
                ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to open CRL");
                goto error;
            }
            x509_crl = PEM_read_X509_CRL(fp, NULL, NULL, NULL);
            fclose(fp);
            fp = NULL;
        }
        if (x509_crl == NULL) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to read CRL");
            goto error;
        }
        rc = X509_STORE_add_crl(cs, x509_crl);
        if (rc != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to load CRL.");
            goto error;
        }
        rc = X509_STORE_set_flags(cs, X509_V_FLAG_CRL_CHECK | X509_V_FLAG_CRL_CHECK_ALL);
    }
#endif /* HAVE_CRL */

#ifndef OPENSSL_NO_ECDH
#if OPENSSL_VERSION_NUMBER >= 0x10002000L
    SSL_CTX_set_ecdh_auto(myctx, 1);
#else
    EC_KEY *ecdh = EC_KEY_new_by_curve_name(NID_X9_62_prime256v1);
    if (ecdh == NULL) {
        my_ssl_eprintln("prime256v1 isn't supported. ECDHE ciphers will be disabled.");
    } else {
        SSL_CTX_set_tmp_ecdh(myctx, ecdh);
        EC_KEY_free(ecdh);
    }
#endif
#endif /* OPENSSL_NO_ECDH */

    /* SSL success is 1. We want to return 0 upon success. */
    if (rc != 1) {
error:  if (myctx != NULL) {
            SSL_CTX_free(myctx);
            myctx = NULL;
        }

        if (fp != NULL)
            fclose(fp);

        X509_free(x509_cert_chain);
        if (px509cert == NULL || x509_cert != *px509cert)
            X509_free(x509_cert);
        if (px509ca == NULL || x509_ca != *px509ca)
            X509_free(x509_ca);
        if (pevppkey == NULL || evp_pkey != *pevppkey)
            EVP_PKEY_free(evp_pkey);
#if HAVE_CRL
        if (px509crl == NULL || x509_crl != *px509crl)
            X509_CRL_free(x509_crl);
#endif

        /* Free strdup()'d memory. */
        if (cert != *pcert)
            free(cert);
        if (key != *pkey)
            free(key);
        if (ca != *pca)
            free(ca);
        if (crl != *pcrl)
            free(crl);

        if (rc == 0)
            rc = 1;
    } else {
        rc = 0;
        *pctx = myctx;

        if (*pcert == NULL)
            *pcert = cert;
        if (*pkey == NULL)
            *pkey = key;
        if (*pca == NULL)
            *pca = ca;
        if (*pcrl == NULL)
            *pcrl = crl;

        /* If caller didn't request X509 certificates, free them. Otherwise let caller take ownership. */
        if (px509cert == NULL)
            X509_free(x509_cert);
        else if (*px509cert == NULL)
            *px509cert = x509_cert;

        if (pevppkey == NULL)
            EVP_PKEY_free(evp_pkey);
        else if (*pevppkey == NULL)
            *pevppkey = evp_pkey;

        if (px509ca == NULL)
            X509_free(x509_ca);
        else if (*px509ca == NULL)
            *px509ca = x509_ca;

#if HAVE_CRL
        if (px509crl == NULL)
            X509_CRL_free(x509_crl);
        else if (*px509crl == NULL)
            *px509crl = x509_crl;
#endif
    }

    return rc;
}
