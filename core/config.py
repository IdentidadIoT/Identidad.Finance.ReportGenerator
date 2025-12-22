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


from configparser import ConfigParser
import os


__all__ = ['init_config', 'get_parameter']

_config = None


def get_parameter(header, value):
    try:
        return _config.get(header, value)
    except BaseException as err:
        print(f"Unexpected {err=}, {type(err)=}")
        return None   


def init_config():
    global _config

    dir_name = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(dir_name, '..', 'config', 'config.cfg') 
    config_file = os.path.abspath(config_file)

    if not os.path.exists(config_file):
        return 1
    
     # Read config
    _config = ConfigParser()
    # open the configuration file
    _config.read(config_file)

    return 0


if __name__ == '__main__':
    init_config()
    