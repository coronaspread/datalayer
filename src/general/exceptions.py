class IllegalStateException(RuntimeError):
    """Exception raised when the state is illegal"""


class IllegalArgumentException(ValueError):
    """Exception raised when an argument is illegal"""


class OptimisationWarning(RuntimeWarning):
    """Warning raised when the Optimisation does not converge"""


class OptimisationError(RuntimeError):
    """Error raised when the Optimisation does not converge"""


class NumericInstabilityWarning(RuntimeWarning):
    """Warning raised when the numeric instability is to be expected"""


class ArgumentWarning(RuntimeWarning):
    """Warning raised when an Argument is not as expected"""


class IllegalFunctionCallWarning(RuntimeWarning):
    """Warning raised when an Argument is not as expected"""

class InvalidDataModel(RuntimeWarning):
    """Warning raised when dataset does not have a valid model"""
