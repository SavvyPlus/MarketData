import os
import logging

import helpers

logger = logging.getLogger(__name__)


def unzip_pattern_handler(file_path, pattern=None, dest_folder=None):
    # HM01X_Data_002064_5559568190
    # pattern = 'HM01X_Data_.+\.txt'
    file_folder, file_name = os.path.split(file_path)
    dest_folder = file_folder if dest_folder is None else dest_folder

    if not pattern:
        logger.error('No pattern provided. Should use unzip_handler instead {}'.format(file_path))
        return (False, 0)

    helpers.unzip_matching_pattern(file_path, dest_folder, pattern)
    return (True, 0)
