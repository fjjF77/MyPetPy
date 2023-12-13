import json
from dataclasses import dataclass
from enum import Enum

class ResourceStatus(Enum):
    CONNECTING = 'CONN'
    FREE = 'FREE'
    BUSY = 'BUSY'

    def serialize(self):
        return self.value

@dataclass
class ResourceInfo:
    cpu: float = 0
    gpu: int = 0 # count
    users: list = None
    process: int = 0

    def serialize(self):
        return {
            'cpu':self.cpu,
            'gpu':self.gpu,
            'users':self.users,
            'process':self.process,
        }

    @staticmethod
    def from_dict(dict):
        c = ResourceInfo()
        for k, v in dict.items():
            setattr(c, k, v)
        return c

@dataclass
class Resource:
    ip: str = None
    user: str = None
    status = ResourceStatus.CONNECTING
    info: ResourceInfo = None

    def __post_init__(self):
        self.id = f'{self.ip}_{self.user}'

    def __str__(self):
        return self.id

    def serialize(self):
        if self.info is None:
            return {
                'ip':self.ip,
                'user': self.user,
                'status': self.status.serialize(),
            }
        else:
            return {
                'ip': self.ip,
                'user': self.user,
                'status': self.status.serialize(),
                'info': self.info.serialize(),
            }

    @staticmethod
    def from_dict(dict):
        c = Resource()
        for k, v in dict.items():
            setattr(c, k, v)
        return c