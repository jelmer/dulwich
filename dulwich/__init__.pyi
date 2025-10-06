# Type stubs for dulwich/__init__.py

from typing import Callable, Optional, TypeVar, Union

if True:  # sys.version_info >= (3, 10)
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

__version__: tuple[int, int, int]

__all__: list[str]

P = ParamSpec("P")
R = TypeVar("R")

def replace_me(
    since: Optional[Union[str, tuple[int, ...]]] = None,
    remove_in: Optional[Union[str, tuple[int, ...]]] = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...
