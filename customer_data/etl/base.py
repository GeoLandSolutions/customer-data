from abc import ABC, abstractmethod

class BaseJurisdictionETL(ABC):
    def __init__(self, cfg):
        self.cfg = cfg

    @abstractmethod
    def extract(self, checkpoint_file=None):
        pass

    @abstractmethod
    def transform(self, data):
        pass

    @abstractmethod
    def load(self, data):
        pass 