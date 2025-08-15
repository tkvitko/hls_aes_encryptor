import logging
import os
from logging import handlers


LOG_DIR = 'logs'
log_format = logging.Formatter('%(asctime)s %(module)s %(levelname)s %(message)s')
handler = handlers.TimedRotatingFileHandler(filename=os.path.join(LOG_DIR, 'streamer.log'),
                                            when='D',
                                            interval=1,
                                            backupCount=10)
handler.setFormatter(log_format)
logger = logging.getLogger('streamer')
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
