import logging
from logging import StreamHandler


formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')


super_logger = logging.getLogger("super_logger")
super_logger.setLevel(logging.DEBUG)
handler = StreamHandler()

handler.setFormatter(fmt=formatter)
super_logger.addHandler(handler)


if __name__ == '__main__':
    pass
