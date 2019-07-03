from copy import deepcopy

from .compat import pickle


def merge_kwargs(*dicts, **kwargs):
    out = dict()
    for d in dicts:
        out.update(d)
    out.update(kwargs)
    return out


class PickleSerializer(object):
    def __init__(self, protocol=pickle.HIGHEST_PROTOCOL, dumps_kwargs=None, loads_kwargs=None):
        self.protocol = protocol
        self.dumps_kwargs = deepcopy(dumps_kwargs) or dict()
        self.dumps_kwargs["protocol"] = self.dumps_kwargs.get("protocol", protocol)
        self.loads_kwargs = deepcopy(loads_kwargs) or dict()
        self.loads_kwargs["protocol"] = self.loads_kwargs.get("protocol", protocol)

    def dumps(self, obj, **kwargs):
        return pickle.dumps(obj, **merge_kwargs(self.dumps_kwargs, kwargs))

    def loads(self, s, **kwargs):
        return pickle.loads(s, **merge_kwargs(self.dumps_kwargs, kwargs))
