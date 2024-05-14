import atheris  # pragma: no cover
from typing import List  # pragma: no cover


@atheris.instrument_func
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


class EnhancedFuzzedDataProvider(atheris.FuzzedDataProvider):  # pragma: no cover
    """Extends atheris.FuzzedDataProvider to offer additional methods to make fuzz testing slightly more DRY."""

    def __init__(self, data):
        """Initializes the EnhancedFuzzedDataProvider with fuzzing data from the argument provided to TestOneInput.

        Args:
            data (bytes): The binary data used for fuzzing.
        """
        super().__init__(data)

    def ConsumeRemainingBytes(self) -> bytes:
        """Consume the remaining bytes in the bytes container.

        Returns:
          bytes: Zero or more bytes.
        """
        return self.ConsumeBytes(self.remaining_bytes())

    def ConsumeRandomBytes(self, max_length=None) -> bytes:
        """Consume a random count of bytes from the bytes container.

        Args:
          max_length (int, optional): The maximum length of the string. Defaults to the number of remaining bytes.

        Returns:
          bytes: Zero or more bytes.
        """
        if max_length is None:
            max_length = self.remaining_bytes()
        else:
            max_length = min(max_length, self.remaining_bytes())

        return self.ConsumeBytes(self.ConsumeIntInRange(0, max_length))

    def ConsumeRandomString(self, max_length=None) -> str:
        """Consume bytes to produce a Unicode string.

        Args:
          max_length (int, optional): The maximum length of the string. Defaults to the number of remaining bytes.

        Returns:
         str: A Unicode string.
        """
        if max_length is None:
            max_length = self.remaining_bytes()
        else:
            max_length = min(max_length, self.remaining_bytes())

        return self.ConsumeUnicode(self.ConsumeIntInRange(0, max_length))

    def ConsumeRandomInt(self, minimum=0, maximum=1234567890) -> int:
        """Consume bytes to produce an integer.

        Args:
          minimum (int, optional): The minimum value of the integer. Defaults to 0.
          maximum (int, optional): The maximum value of the integer. Defaults to 1234567890.

        Returns:
          int: An integer.
        """

        return self.ConsumeIntInRange(minimum, maximum)
