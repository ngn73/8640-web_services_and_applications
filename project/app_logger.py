import logging
import config as cfg
import api.api_ntfy as ntfy # for sending mobile notifications

class app_logger:
    
    def __init__(self, loggername:str):
        flask_log = logging.getLogger('werkzeug')
        flask_log.disabled = True;  # suppress the excessive Flask logging
        
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
        
        #Also utilize the ntfy service to send notifications to my mobile for important events/errors.
        self.ntfy_client = ntfy.api_ntfy()


    # record a Info log
    def logInfoMessage(self, str_message:str, ntfy:bool=False):
        self.logger.info(str_message)
        #Send NTFY Notification if requested.
        if ntfy:
            self.ntfy_client.send_notification("tmdb_manager", str_message)
 

    # record a Debug log
    def logDebugMessage(self, str_message:str, ntfy:bool=False):
        self.logger.debug(str_message)
        #send NTFY Notification if requested.
        if ntfy:
            self.ntfy_client.send_notification("tmdb_manager", str_message)
  


    # record a Warning log
    def logWarningMessage(self, str_message:str, ntfy:bool=False):
        self.logger.warning(str_message)
        #send NTFY Notification if requested.
        if ntfy:
            self.ntfy_client.send_notification("tmdb_manager", str_message)
 
    # record an Error log
    def logErrorMessage(self, str_message:str, ntfy:bool=False):
        self.logger.error(str_message)
        #send NTFY Notification if requested.
        if ntfy:
            self.ntfy_client.send_notification("tmdb_manager", str_message)
