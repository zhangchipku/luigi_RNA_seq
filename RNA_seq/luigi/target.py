from luigi.local_target import LocalTarget, atomic_file
import os
from contextlib import contextmanager

class suffix_preserving_atomic_file(atomic_file):
    def generate_tmp_path(self, path):
        """
        make the temp file path, preserving the extensions
        :param path: the path of the target file
        :return: the path of the temp file
        """
        paths = os.path.splitext(path)
        return super().generate_tmp_path(paths[0]) + paths[1]


class BaseAtomicProviderLocalTarget(LocalTarget):
    # Allow some composability of atomic handling
    atomic_provider = atomic_file

    def open(self, mode='r'):
        """
        open the specified file
        :param mode: opening mode
        :return: local file to read or write
        """
        # leverage super() as well as modifying any code in LocalTarget
        # to use self.atomic_provider rather than atomic_file
        rwmode = mode.replace('b', '').replace('t', '')
        if rwmode == 'w':
            self.makedirs()
            return self.format.pipe_writer(self.atomic_provider(self.path))

        else:
            return super().open(mode=mode)

    @contextmanager
    def temporary_path(self):
        """
        Provide the path to the temp file
        :return: path of the temp file
        """
        # NB: unclear why LocalTarget doesn't use atomic_file in its implementation
        self.makedirs()
        with self.atomic_provider(self.path) as af:
            yield af.tmp_path


class SuffixPreservingLocalTarget(BaseAtomicProviderLocalTarget):
    """
    New luigi local target class that preserves the extension of temp file
    """
    atomic_provider = suffix_preserving_atomic_file
