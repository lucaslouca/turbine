from connarchitecture.abstract_transaction_handler import AbstractTransactionHandler
from connarchitecture.decorators import overrides
from connarchitecture.poll_reference import PollReference
import os
import shutil


class FileTransactionHandler(AbstractTransactionHandler):

    def __init__(self, **kwargs):
        AbstractTransactionHandler.__init__(self)
        self._error_dir = kwargs['error_dir']
        self._create_dirs_if_not_exists(self._error_dir)

    def component_name(self):
        return "FileTransactionHandler"

    @overrides(AbstractTransactionHandler)
    def on_event(self, poll_reference: PollReference, success: bool):
        if not success:
            self.log_error(f"Something went wrong with {poll_reference}")
            if poll_reference.file:
                self._move(poll_reference, self._error_dir)

    def _create_dirs_if_not_exists(self, destination_dir):
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)

    def _move(self, poll_reference, destination_dir):
        if poll_reference.file and os.path.exists(str(poll_reference.file)):
            folder = destination_dir
            if poll_reference.year:
                folder = f'{folder}/{poll_reference.year}'
            if poll_reference.ticker:
                folder = f'{folder}/{poll_reference.ticker}'

            self._create_dirs_if_not_exists(folder)

            file_name = os.path.basename(poll_reference.file)
            destination_path = os.path.join(folder, file_name)
            self.log(f"Moving {poll_reference.file} into {destination_path}")
            shutil.move(poll_reference.file, destination_path)
