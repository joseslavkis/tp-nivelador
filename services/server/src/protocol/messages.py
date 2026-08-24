from dataclasses import dataclass

UINT16_SIZE = 2
UINT32_SIZE = 4
UINT64_SIZE = 8
MAX_TEXT_LENGTH = 65535

MESSAGE_TYPE_BET = 1
MESSAGE_TYPE_END = 2
MESSAGE_TYPE_WINNER = 3
MESSAGE_TYPE_ERROR = 4


@dataclass(frozen=True)
class BetPayload:
    agency_id: int
    first_name: str
    last_name: str
    document: int
    birthdate: str
    number: int


def encode_bet(bet: BetPayload) -> bytes:
    return b"".join(
        [
            _encode_uint(bet.agency_id, UINT32_SIZE, "agency id"),
            _encode_text(bet.first_name, "first name"),
            _encode_text(bet.last_name, "last name"),
            _encode_uint(bet.document, UINT64_SIZE, "document"),
            _encode_text(bet.birthdate, "birthdate"),
            _encode_uint(bet.number, UINT32_SIZE, "number"),
        ]
    )


def decode_bet(payload: bytes) -> BetPayload:
    offset = 0

    agency_id, offset = _decode_uint(payload, offset, UINT32_SIZE, "agency id")
    first_name, offset = _decode_text(payload, offset, "first name")
    last_name, offset = _decode_text(payload, offset, "last name")
    document, offset = _decode_uint(payload, offset, UINT64_SIZE, "document")
    birthdate, offset = _decode_text(payload, offset, "birthdate")
    number, offset = _decode_uint(payload, offset, UINT32_SIZE, "number")

    if offset != len(payload):
        raise ValueError(f"bet payload has {len(payload) - offset} trailing bytes")

    return BetPayload(
        agency_id=agency_id,
        first_name=first_name,
        last_name=last_name,
        document=document,
        birthdate=birthdate,
        number=number,
    )


def _encode_uint(value: int, size: int, field: str) -> bytes:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    max_value = (1 << (size * 8)) - 1
    if not 0 <= value <= max_value:
        raise ValueError(f"{field} must be between 0 and {max_value}")
    return value.to_bytes(size, byteorder="big")


def _encode_text(value: str, field: str) -> bytes:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    encoded = value.encode("utf-8")
    if len(encoded) > MAX_TEXT_LENGTH:
        raise ValueError(f"{field} exceeds {MAX_TEXT_LENGTH} bytes")
    return len(encoded).to_bytes(UINT16_SIZE, byteorder="big") + encoded


def _decode_uint(
    payload: bytes, offset: int, size: int, field: str
) -> tuple[int, int]:
    encoded, offset = _read_bytes(payload, offset, size, field)
    return int.from_bytes(encoded, byteorder="big"), offset


def _decode_text(payload: bytes, offset: int, field: str) -> tuple[str, int]:
    encoded_length, offset = _read_bytes(
        payload, offset, UINT16_SIZE, f"{field} length"
    )
    length = int.from_bytes(encoded_length, byteorder="big")
    encoded, offset = _read_bytes(payload, offset, length, field)
    try:
        return encoded.decode("utf-8"), offset
    except UnicodeDecodeError as error:
        raise ValueError(f"{field} is not valid UTF-8") from error


def _read_bytes(
    payload: bytes, offset: int, size: int, field: str
) -> tuple[bytes, int]:
    end = offset + size
    if end > len(payload):
        raise ValueError(f"bet payload is missing {field}")
    return payload[offset:end], end
