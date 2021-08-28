from connarchitecture.abstract_transaction_handler import AbstractTransactionHandler
from connarchitecture.decorators import overrides
import os
import shutil
from datetime import datetime


class FileTransactionHandler(AbstractTransactionHandler):

    def __init__(self, **kwargs):
        AbstractTransactionHandler.__init__(self)
        self._archive_dir = kwargs['archive_dir']
        self._error_dir = kwargs['error_dir']
        self._create_dirs_if_not_exists(self._archive_dir)
        self._create_dirs_if_not_exists(self._error_dir)

    def component_name(self):
        return "FileTransactionHandler"

    @overrides(AbstractTransactionHandler)
    def on_event(self, file_path, success):
        self.log(file_path)
        '''
        if success:
            self._move(file_path, self._archive_dir)
        else:
            self._move(file_path, self._error_dir)
        '''

    def _create_dirs_if_not_exists(self, destination_dir):
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)

    def _move(self, file_path, destination_dir):
        if file_path and os.path.exists(file_path):
            self._create_dirs_if_not_exists(destination_dir)

            file_name = os.path.splitext(os.path.basename(file_path))[0] + f"_{datetime.now()}"
            destination_path = os.path.join(destination_dir, file_name)
            self.log(f"Archiving {file_path} into {destination_path}")
            shutil.move(file_path, destination_path)
