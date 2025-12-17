"""
/*************************************************************************
 * 
 *  [2022] Identidad Technologies. 
 *  All Rights Reserved.
 * 
 * NOTICE:  All information contained herein is, and remains
 * the property of Identidad Technologies,
 * The intellectual and technical concepts contained
 * herein are proprietary to Identidad Technologies
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Identidad Technologies.
 */
"""

import jwt
import core.config as cfg
from core.logger import get_logger

logger = None
client = None
password = None
secret = None
id = None
issuer = None

__all__ = ['generate_token', 'validate_token']

def generate_token(auth_data):

    global logger
    global client
    global password
    global secret
    global id
    global issuer

    if auth_data['username'] == client and auth_data['password'] == password:
        #validar usuario y password contra el config
        payload_data = {
            "name": client, 
            "id": id,
            "iss":issuer
            }
        token = jwt.encode(payload_data, secret)
        return token
    else:
        return None

def validate_token(token):
    global logger
    global client
    global password
    global secret
    global id
    global issuer
    try:
        token = token.replace("bearer", "").replace('Bearer', '').strip()
        decoded = jwt.decode(token, secret, issuer=issuer, algorithms=["HS256"])        
        #validar datos del token para certtificarlo
        return "Ok"
    except jwt.ExpiredSignatureError:
        logger.debug('Error signature has expired')
        raise Exception("Error signature has expired")
    except jwt.InvalidTokenError:
        logger.debug('Error invalid token')
        raise Exception("Error invalid token")

def init_auth():

    global logger
    global client
    global password
    global secret
    global id
    global issuer
   
    logger = get_logger()
    logger.debug('-----------------Init Auth------------------------')
    client = cfg.get_parameter('Apollo_Auth', 'client')
    password = cfg.get_parameter('Apollo_Auth', 'password')
    secret = cfg.get_parameter('Apollo_Auth', 'secret')
    id = cfg.get_parameter('Apollo_Auth', 'id')
    issuer = cfg.get_parameter('Apollo_Auth', 'issuer')