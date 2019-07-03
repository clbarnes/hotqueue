"""
This module provides access to a redis server using a redis server embedded
in the module.  It provides enhanced redis bindings that are able to configure
run, and cleanup a redis server when they are accessed.

Attributes:
    __version__(str):
        The version of the redislite module.

    __redis_executable__(str):
        The full path to the embedded redis-server executable.

    __redis_server_version__(str):
        The version of the embedded redis-server built intot he module.

    __git_source_url__(str):
        The github web url for the source code used to generate this module.
        This will be an empty string if the module was not built from a github
        repo.

    __git_version__(str):
        Version number derived from the number of git revisions.
        This will be an empty string if not built from a git repo.

    __git_origin__(str):
        The git origin of the source repository the module was built from.
        This will be an empty string if not built from a git repo.

    __git_branch__(str):
        The git branch the module was built from.  This will be an empty string
        if not built from a git repo.

    __git_hash__(str):
        The git hash value for the code used to build this module.

Example:
  To access redis using a newly installed and configured redis server, then
  set and retrieve some data:

      >>> import redislite
      >>> connection = redislite.Redis()
      >>> connection.set('key', 'value')
      True
      >>> connection.get('key')
      'value'
      >>>
"""
import json
import os
from .hotqueue import HotQueue
from .serializers import PickleSerializer

__all__ = ['HotQueue', 'PickleSerializer']
__version__ = str('0.0.0')
__git_version__ = str("")
__git_origin__ = str("")
__git_branch__ = str("")
__git_hash__ = str("")
__source_url__ = str('')


_metadata_file = os.path.join(
    os.path.dirname(__file__),
    'package_metadata.json'
)


if os.path.exists(_metadata_file):  # pragma: no cover
    with open(_metadata_file) as fh:
        _package_metadata = json.load(fh)
        __version__ = str(_package_metadata['version'])
        __git_version__ = str(_package_metadata['git_version'])
        __git_origin__ = str(_package_metadata['git_origin'])
        __git_branch__ = str(_package_metadata['git_branch'])
        __git_hash__ = str(_package_metadata['git_hash'])
        __git_base_url__ = 'https://github.com/dhubbard/hotqueue'
        if __git_origin__.endswith('.git'):  # pragma: no cover
            __git_base_url__ = __git_origin__[:-4].strip('/')
        __source_url__ = __git_base_url__ + '/tree/' + __git_hash__


__version_info__ = tuple()
for item in __version__:
    try:
        item = int(item)
    except (ValueError, TypeError):
        pass
    __version_info__ += (item,)
