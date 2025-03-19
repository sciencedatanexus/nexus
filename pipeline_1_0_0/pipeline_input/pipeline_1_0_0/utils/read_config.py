# coding=utf-8

# =============================================================================
# """
# .. module:: input_pipeline.utils.read_config.py
# .. moduleauthor:: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# .. version:: 1.0
#
# :Copyright: Jean-Francois Desvignes for Science Data Nexus, 2024
# :Contact: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# :Updated: 19/08/2024
# """
# =============================================================================

# Import the local functions module (file)
from configparser import ConfigParser


def read_db_config(section, filename):
    """ Parse the config file config.ini, first section postgresql, to retrieve connection settings """
    """ Read database configuration file and return a dictionary object
    :param filename: name of the configuration fil
    :param section: section of database configuration
    :return: a dictionary of database parameters
    """
    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))

    return db



