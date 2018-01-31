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


class ExecutionParseError(Exception):
    '''
    Custom exception thrown when parsing a command executed by Glusto
    results in an unexpected error.

    For example, the output of a command when has to be parsed, can have three
    states. First, the output was as expected. Second, didn't get the expected
    ouput after the parsing result and Third, didn't get the expected result as
    the command itself failed.

    '''
    pass
