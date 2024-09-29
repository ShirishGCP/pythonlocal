import pytest
from even_number_checker.checker import is_even


@pytest.mark.parametrize("number, expected", [
    (4, True),
    (5, False),
])
def test_is_even(number, expected):
    assert is_even(number) == expected
