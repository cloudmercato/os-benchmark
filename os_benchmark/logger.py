import logging

# TODO: Make it selectable
# formatter = logging.Formatter('[%(asctime)s %(levelname)s]: %(message)s')
formatter = logging.Formatter('[%(asctime)s %(pathname)s:%(lineno)d %(levelname)s]: %(message)s')

handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger = logging.getLogger('osb')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

logging.getLogger("scapy").setLevel(logging.CRITICAL)
