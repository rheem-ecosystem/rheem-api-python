import configparser


# config_boundaries = []

def get_configurated_boundaries():
    config = configparser.ConfigParser()
    config.sections()
    config.read('/Users/rodrigopardomeza/PycharmProjects/pyrheem/config/init_config.ini')
    text_boundaries = config['OPERATORS']['boundary_types']
    return text_boundaries.split(",")
