class ConfigError(Exception):
    '''
    Custom exception thrown when there is an unrecoverable configuration error.
    For example, a required configuration key is not found.
    '''
    pass


class ExecutionError(Exception):
    '''
    Custom exception thrown when a command executed by Glusto results in an
    unrecoverable error.

    For example, all hosts are not in peer state or a volume canot be setup.

    '''
    pass
