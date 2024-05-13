import atheris  # pragma: no cover
from typing import List  # pragma: no cover


def is_expected_exception(
    error_message_list: List[str], exception: Exception
):  # pragma: no cover
    """Checks if the message of a given exception matches any of the expected error messages.

     Args:
         error_message_list (List[str]): A list of error message substrings to check against the exception's message.
         exception (Exception): The exception object raised during execution.

    Returns:
      bool: True if the exception's message contains any of the substrings from the error_message_list, otherwise False.
    """
    for error in error_message_list:
        if error in str(exception):
            return True
    return False
