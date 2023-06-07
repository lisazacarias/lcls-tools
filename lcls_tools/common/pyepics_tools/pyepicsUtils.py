from time import sleep

from epics import caget as epics_caget, caput as epics_caput
from psp.Pv import DEFAULT_TIMEOUT, Pv as pyca_pv
from pyca import DBE_ALARM, DBE_VALUE, flush_io, pend_event, pyexc

# These are the values that decide whether a PV is alarming (and if so, how)
EPICS_NO_ALARM_VAL = 0
EPICS_MINOR_VAL = 1
EPICS_MAJOR_VAL = 2
EPICS_INVALID_VAL = 3

DEBUG = True


class PVInvalidError(Exception):
    pass


class PV(pyca_pv):
    def __init__(self, pvname):
        super().__init__(pvname)
        self.pvname = pvname
        print(f"Connecting {self}, might take a while")
        self.connect()
        flush_io()
        pend_event(0.2)
        self.monitor(DBE_VALUE | DBE_ALARM)
        pend_event(.2)
    
    def __str__(self):
        return f"{self.pvname} PV Object"
    
    @property
    def severity(self) -> int:
        return self.data["severity"]
    
    def caget(self):
        while True:
            value = epics_caget(self.pvname)
            if value is not None:
                break
            print(f"{self.pvname} did not return a valid value, retrying")
            sleep(0.5)
        return value
    
    def caput(self, value):
        while True:
            status = epics_caput(self.pvname, value)
            if status == 1:
                break
            print(f"{self} caput did not execute successfully, retrying")
            sleep(0.5)
        return status
    
    def get(self, count=None, as_string=False, as_numpy=True,
            timeout=DEFAULT_TIMEOUT, with_ctrlvars=False, use_monitor=True,
            use_caget=False):
        
        if use_caget:
            return self.caget()
        
        else:
            attempt = 1
            while True:
                if attempt > 3:
                    raise PVInvalidError(f"{self} get failed more than 3 times")
                value = self.data["value"]
                if value is not None:
                    break
                print(f"{self} value is none, retrying")
                sleep(0.5)
                attempt += 1
            return value
    
    def put(self, value, wait=True, timeout=DEFAULT_TIMEOUT,
            use_complete=False, callback=None, callback_data=None, retry=True,
            use_caput=False):
        
        if use_caput:
            return self.caput(value)
        
        attempt = 1
        while True:
            if attempt > 3:
                raise PVInvalidError(f"{self} put failed more than 3 times")
            try:
                super().put(value, timeout=timeout)
                break
            except pyexc as e:
                attempt += 1
                print(e)
                print(f"{self} put failed, retrying")
                sleep(0.5)
