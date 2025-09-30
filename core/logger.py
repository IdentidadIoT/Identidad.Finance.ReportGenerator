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

import logging
import os

__all__ = ['init_log', 'get_logger']

def init_log():
    try:
        dir_name = os.path.dirname(__file__)
        log_dir = os.path.join(dir_name, '../logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'debug.log')

        logger = logging.getLogger("sms_api")
        if not logger.handlers:  # Evita duplicados
            file_handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            logger.setLevel(logging.DEBUG)
            logger.addHandler(file_handler)

        return logger

    except Exception as err:
        print("Error initializing logger:", err)
        raise

def get_logger(name="sms_api"):
    return logging.getLogger(name)
