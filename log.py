# coding:utf-8
import logging
import logging.handlers
import logging.config
import threading
import sys
import socket
import os
from datetime import datetime
import webhook_transfer as w

#LOG_PATH = f'{os.getcwd()}/'
LOG_PATH = '/backup/'
CLI_LOG_NAME = 'blkbackup.log'
AUDIT_2_WEBHOOK = True
#AUDIT_2_WEBHOOK = False

def get_hostname():
    hostname = socket.gethostname()
    return hostname

def get_hostIP():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    return ip

def get_auditobj():
    auditobj = "StorService"
    return auditobj

def get_app():
    app = "blkbackup"
    return app

class MyLoggerAdapter(logging.LoggerAdapter):
    extra_dict = {
        "host": "",
        "hostip": "",
        "app": "",
        "auditobj": "",
        "severity": "",
        "sourceip": "",
        "msgdata": ""}

    def __init__(self,log_path,file_name):
        super().__init__(self.get_my_logger(log_path,file_name),self.extra_dict)


    def process(self, msg, kwargs):
        if 'extra' not in kwargs:
            kwargs["extra"] = self.extra
        return msg, kwargs


    def get_my_logger(self,log_path,file_name):
        handler_input = logging.handlers.RotatingFileHandler(filename=f'{log_path}{file_name}',
                                                             mode='a',
                                                             maxBytes=10 * 1024 * 1024, backupCount=20)
        fmt = logging.Formatter(
            '%(time)s %(host)s(%(hostip)s) %(app)s - - [auditObj="%(auditobj)s" severity="%(severity)s" sourceIP="%(sourceip)s"] %(msgdata)s',
            datefmt='%b %d %Y %H:%M:%S')
        handler_input.setFormatter(fmt)
        logger = logging.getLogger('blkbackup_logger')
        logger.addHandler(handler_input)
        logger.setLevel(logging.DEBUG)
        self.handler_input = handler_input
        return logger

    def remove_my_handler(self):
        if self.handler_input:
            self.logger.removeHandler(self.handler_input)

class Log(object):
    _instance_lock = threading.Lock()
    # _instance = None
    host = None 
    hostip = None
    app = None
    auditobj = None
    sourceip = None
    file_name = CLI_LOG_NAME
    log_path = LOG_PATH
    log_switch = True
    logger = None

    def __init__(self):
        pass

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            with Log._instance_lock:
                if not hasattr(cls, '_instance'):
                    Log._instance = super().__new__(cls)
                    Log._instance.logger = MyLoggerAdapter(cls.log_path,cls.file_name)

        return Log._instance

    # write to log file
    def write_to_log(self, level, msg, audit=False):
        logger = Log._instance.logger

        if not self.log_switch:
            logger.remove_my_handler()

        if not self.host:
            self.host = get_hostname()
        if not self.hostip:
            self.hostip = get_hostIP()
        if not self.app:
            self.app = get_app()
        if not self.auditobj:
            self.auditobj = get_auditobj()
        if not self.sourceip:
            self.sourceip = self.hostip
        self.time = datetime.now().astimezone().isoformat()
            
        logger.debug(
            "",
            extra={
                'time':self.time,
                'host': self.host,
                'hostip': self.hostip,
                'app': self.app,
                'auditobj': self.auditobj,
                'severity': level,
                'sourceip': self.sourceip,
                'msgdata': msg})
        
        if AUDIT_2_WEBHOOK & audit:
            timea=self.time
            workspacea=self.host+"("+self.hostip+")"
            infoa=msg
            typea=self.app
            resa=self.auditobj
            sourceIPa=self.hostip
            loglevela=level
            w.wh_interface(Time=timea,Workspace=workspacea,Reason=infoa,AuditResType=typea,ResName=resa,SourceIPs=sourceIPa,LogLevel=loglevela)
            #result = w.wh_interface(Time=timea,Workspace=workspacea,Reason=infoa,AuditResType=typea,ResName=resa,SourceIPs=sourceIPa,LogLevel=loglevela)
            #if result:
            #    logger = Log()
            #    logger.write_to_log("ERR", f"Tried to rewrite the data 10 times, but the data write failed!", False)

