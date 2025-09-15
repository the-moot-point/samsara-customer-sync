from encompass_to_samsara.drivers_utils import generate_username


def test_generate_username_slugifies_input() -> None:
    username = generate_username("Élodie", "O'Connor-Sánchez 3rd", set())
    assert username == "eoconnorsanchez3rd-1"


def test_generate_username_increments_until_available() -> None:
    taken = {"jdoe-1", "jdoe-2"}
    assert generate_username("John", "Doe", taken) == "jdoe-3"


def test_generate_username_truncates_before_suffix() -> None:
    first = "A"
    last = "B" * 300

    base_for_two_digit_suffix = "a" + "b" * 186
    taken = {f"{base_for_two_digit_suffix}-{i}" for i in range(1, 10)}

    username = generate_username(first, last, taken)

    assert username == "a" + "b" * 185 + "-10"
    assert len(username) == 189
