from decimal import Decimal

from dispatch import publisher


def test__convert_to_json():
    assert publisher._convert_to_json({"d": Decimal("10.21")}) == '{"d": 10.21}'


def test_publish(queue, message):
    publisher.publish(message)
