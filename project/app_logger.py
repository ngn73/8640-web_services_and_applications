import logging
import config as cfg

class app_logger:
    
    def __init__(self, loggername:str):
        #read configuration
        log_active = cfg.LOGGING['active']
        log_filename = cfg.LOGGING['filename']
        log_level = (cfg.LOGGING['level']).upper()

        # Instance variables
        self.filename = log_filename
        self.level = log_level
        if(log_level == 'DEBUG'):
            self.log_level_int = 10
        elif(log_level == 'INFO'):
            self.log_level_int = 20
        elif(log_level == 'WARNING'):
            self.log_level_int = 30
        elif(log_level == 'ERROR'):
            self.log_level_int = 40
        elif(log_level == 'CRITICAL'):
            self.log_level_int = 50
        else:
            self.log_level_int = 0 #deferring to the root logger's level.

        self.logger = logging.getLogger(loggername)
        if(log_active == True):
            logging.basicConfig(format='%(asctime)s-%(name)s-%(levelname)s:%(message)s', filename=log_filename, encoding='utf-8', level=self.log_level_int)
        else:
            logging.disable()
        

    # record a log
    def logInfoMessage(self, str_message:str):
        self.logger.info(str_message)
        
    # record a log
    def logDebugMessage(self, str_message:str):
        self.logger.debug(str_message)

    # record a log
    def logWarningMessage(self, str_message:str):
        self.logger.warning(str_message)
        
    # record a log
    def logErrorMessage(self, str_message:str):
        self.logger.error(str_message)