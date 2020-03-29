class IllegalStateException(RuntimeError):
    """Exception raised when the state is illegal"""

    def __repr__(self, message):
        return message


class IllegalArgumentException(ValueError):
    """Exception raised when an argument is illegal"""

    def __repr__(self, message):
        return message


class OptimisationWarning(RuntimeWarning):
    """Warning raised when the Optimisation does not converge"""

    def __repr__(self, message):
        return message


class OptimisationError(RuntimeError):
    """Error raised when the Optimisation does not converge"""

    def __repr__(self, message):
        return message


class NumericInstabilityWarning(RuntimeWarning):
    """Warning raised when the numeric instability is to be expected"""

    def __repr__(self, message):
        return message


class ArgumentWarning(RuntimeWarning):
    """Warning raised when an Argument is not as expected"""

    def __repr__(self, message):
        return message


class IllegalFunctionCallWarning(RuntimeWarning):
    """Warning raised when an Argument is not as expected"""

    def __repr__(self, message):
        return message
