import base64
import hashlib
import os
import shutil
import uuid
from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from enum import Enum


#
# Production:
#
# SECURITY_KEY = base64.b64decode(os.environ["ATHENA_AES_KEY"])
#
SECURITY_KEY = os.urandom(32)


#
# POC in-memory stores.
#
# Replace with Azure SQL / Redis / Key Vault later.
#
# assessment_id, table_name, column_name, transformed_value -> original_value
_PSEUDONYM_LOOKUP: dict[
    tuple[str, str, str, str],
    str,
] = {}

_TOKEN_LOOKUP: dict[
    tuple[str, str, str, str],
    str,
] = {}

# original_value -> transformed_value
_PSEUDONYM_FORWARD: dict[
    tuple[str, str, str, str],
    str,
] = {}

_TOKEN_FORWARD: dict[
    tuple[str, str, str, str],
    str,
] = {}

_PSEUDONYM_COUNTER = 1
_TOKEN_COUNTER = 1


class ReviewStatus(str, Enum):
    APPROVED = "Approved"
    MODIFIED = "Modified"
    EXCLUDED = "Excluded"

class SecurityControlType(str, Enum):
    """
    Technical security controls that can be applied to enterprise data.
    Exactly one control should be selected for each column.
    """

    ENCRYPT = "Encrypt"
    HASH = "Hash"
    MASK = "Mask"
    REDACT = "Redact"
    TOKENIZE = "Tokenize"
    PSEUDONYMIZE = "Pseudonymize"
    ANONYMIZE = "Anonymize"
    NO_ADDITIONAL_CONTROL = "No_Additional_Control"

# =============================================================================
# Public API
# =============================================================================

def apply_security_controls(
    assessment_id: str,
    table_name: str,
    dataframe: DataFrame,
    policies: dict[str, SecurityControlType],
) -> DataFrame:
    """
    Apply all configured security controls to a Spark DataFrame.
    """

    df = dataframe
    existing_cols = set(df.columns)  # hoist schema check - SCPAP001
    new_cols: dict = {}

    for column_name, security_control in policies.items():

        if column_name not in existing_cols:
            continue

        sc = security_control
        col_name = column_name

        if sc in (SecurityControlType.PSEUDONYMIZE, SecurityControlType.TOKENIZE):
            # Collect distinct values to the driver and build the forward mapping
            # here (not in workers), so _PSEUDONYM_LOOKUP / _TOKEN_LOOKUP are
            # populated on the driver and reversal works correctly.
            distinct_vals = [
                row[0] for row in df.select(col_name).distinct().collect()
                if row[0] is not None
            ]
            forward_map: dict = {}
            for v in distinct_vals:
                forward_map[str(v)] = _apply_single_value(
                    assessment_id=assessment_id,
                    table_name=table_name,
                    column_name=col_name,
                    security_control=sc,
                    value=str(v),
                )
            frozen_fwd = forward_map
            transform = udf(
                lambda value, _fwd=frozen_fwd: _fwd.get(str(value)) if value is not None else None,
                StringType(),
            )
        else:
            transform = udf(
                lambda value, _sc=sc, _col=col_name: _apply_single_value(
                    assessment_id=assessment_id,
                    table_name=table_name,
                    column_name=_col,
                    security_control=_sc,
                    value=value,
                ),
                StringType(),
            )

        new_cols[column_name] = transform(col(column_name))

    if new_cols:
        df = df.withColumns(new_cols)  # apply all at once - SCPAP004

    return df

def reverse_security_controls(
    assessment_id: str,
    table_name: str,
    dataframe: DataFrame,
    policies: dict[str, SecurityControlType],
) -> DataFrame:
    """
    Reverse all configured security controls.
    """

    df = dataframe
    existing_cols = set(df.columns)  # hoist schema check - SCPAP001
    new_cols: dict = {}

    for column_name, security_control in policies.items():

        if column_name not in existing_cols:
            continue

        sc = security_control
        col_name = column_name

        if sc in (SecurityControlType.PSEUDONYMIZE, SecurityControlType.TOKENIZE):
            # Read the reverse mapping from driver-side lookup tables,
            # populated during apply_security_controls.
            lookup = (
                _PSEUDONYM_LOOKUP
                if sc == SecurityControlType.PSEUDONYMIZE
                else _TOKEN_LOOKUP
            )
            rev_map: dict = {
                pseudonym: original
                for (aid, tname, cname, pseudonym), original in lookup.items()
                if aid == assessment_id and tname == table_name and cname == col_name
            }
            frozen_rev = rev_map
            reverse = udf(
                lambda value, _rev=frozen_rev: _rev.get(str(value), value) if value is not None else None,
                StringType(),
            )
        else:
            reverse = udf(
                lambda value, _sc=sc, _col=col_name: _reverse_single_value(
                    assessment_id=assessment_id,
                    table_name=table_name,
                    column_name=_col,
                    security_control=_sc,
                    value=value,
                ),
                StringType(),
            )

        new_cols[column_name] = reverse(col(column_name))

    if new_cols:
        df = df.withColumns(new_cols)  # apply all at once - SCPAP004

    return df

def _apply_single_value(
    assessment_id: str,
    table_name: str,
    column_name: str,
    security_control: SecurityControlType,
    value: Any,
) -> Any:
    """
    Applies the configured security control to a single value.
    """

    if value is None:
        return None

    dispatch = {
        SecurityControlType.ENCRYPT: _encrypt,
        SecurityControlType.HASH: _hash,
        SecurityControlType.MASK: _mask,
        SecurityControlType.REDACT: _redact,
        SecurityControlType.ANONYMIZE: _anonymize,
        SecurityControlType.NO_ADDITIONAL_CONTROL: lambda v: v,
        SecurityControlType.TOKENIZE: lambda v: _tokenize(
            assessment_id, table_name, column_name, v
        ),
        SecurityControlType.PSEUDONYMIZE: lambda v: _pseudonymize(
            assessment_id, table_name, column_name, v
        ),
    }

    handler = dispatch.get(security_control)

    if handler is None:
        raise ValueError(
            f"Unsupported security control: {security_control}"
        )

    return handler(value)


def _reverse_single_value(
    assessment_id: str,
    table_name: str,
    column_name: str,
    security_control: SecurityControlType,
    value: Any,
) -> Any:
    """
    Reverses the configured security control for a single value.

    Irreversible security controls (Hash, Mask, Redact, Anonymize)
    simply return the transformed value unchanged.
    """

    if value is None:
        return None

    dispatch: dict[
        str,
        Callable[[Any], Any],
    ] = {
        SecurityControlType.ENCRYPT: _decrypt,
        SecurityControlType.HASH: lambda v: v,
        SecurityControlType.MASK: lambda v: v,
        SecurityControlType.REDACT: lambda v: v,
        SecurityControlType.ANONYMIZE: lambda v: v,
        SecurityControlType.NO_ADDITIONAL_CONTROL: lambda v: v,
        SecurityControlType.TOKENIZE: lambda v: _reverse_tokenize(
            assessment_id,
            table_name,
            column_name,
            v,
        ),
        SecurityControlType.PSEUDONYMIZE: lambda v: _reverse_pseudonymize(
            assessment_id,
            table_name,
            column_name,
            v,
        ),
    }

    handler = dispatch.get(security_control)

    if handler is None:
        raise ValueError(
            f"Unsupported security control: {security_control}"
        )

    return handler(value)

# =============================================================================
# Encrypt
# =============================================================================

def _encrypt(value: Any) -> Any:
    """
    AES-256-GCM encryption.

    Returns:
        base64(nonce + ciphertext + tag)
    """

    from cryptography.hazmat.primitives.ciphers.aead import AESGCM as _AESGCM

    if value is None:
        return None

    aes = _AESGCM(SECURITY_KEY)

    nonce = os.urandom(12)

    ciphertext = aes.encrypt(
        nonce,
        str(value).encode("utf-8"),
        None,
    )

    return base64.b64encode(
        nonce + ciphertext
    ).decode("utf-8")


# =============================================================================
# Decrypt
# =============================================================================

def _decrypt(value: Any) -> Any:
    """
    Reverses _encrypt().
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM as _AESGCM

    if value is None:
        return None

    aes = _AESGCM(SECURITY_KEY)

    raw = base64.b64decode(value)

    nonce = raw[:12]
    ciphertext = raw[12:]

    plaintext = aes.decrypt(
        nonce=nonce,
        data=ciphertext,
        associated_data=None,
    )

    return plaintext.decode("utf-8")


# =============================================================================
# Hash
# =============================================================================

def _hash(value: Any) -> Any:
    """
    One-way SHA-256 hash.
    """

    if value is None:
        return None

    return hashlib.sha256(
        str(value).encode("utf-8")
    ).hexdigest()


# =============================================================================
# Mask
# =============================================================================

def _mask(value: Any) -> Any:
    """
    Preserve first and last character.

    JohnSmith
        ->
    J*******h
    """

    if value is None:
        return None

    text = str(value)

    if len(text) <= 2:
        return "*" * len(text)

    return (
        text[0]
        + "*" * (len(text) - 2)
        + text[-1]
    )


# =============================================================================
# Redact
# =============================================================================

def _redact(value: Any) -> Any:

    if value is None:
        return None

    return "[REDACTED]"


# =============================================================================
# Tokenize
# =============================================================================

def _tokenize(
    assessment_id: str,
    table_name: str,
    column_name: str,
    value: Any,
) -> Any:
    """
    Deterministic reversible tokenization.

    Same original value within the same
    assessment/table/column receives the same token.
    """

    global _TOKEN_COUNTER

    if value is None:
        return None

    original = str(value)

    forward_key = (
        assessment_id,
        table_name,
        column_name,
        original,
    )

    token = _TOKEN_FORWARD.get(forward_key)

    if token is None:

        token = f"TKN_{_TOKEN_COUNTER:06d}"
        _TOKEN_COUNTER += 1

        _TOKEN_FORWARD[forward_key] = token

        _TOKEN_LOOKUP[
            (
                assessment_id,
                table_name,
                column_name,
                token,
            )
        ] = original

    return token


# =============================================================================
# Reverse Tokenize
# =============================================================================

def _reverse_tokenize(
    assessment_id: str,
    table_name: str,
    column_name: str,
    value: Any,
) -> Any:
    """
    Restore original value from token.
    """

    if value is None:
        return None

    return _TOKEN_LOOKUP.get(
        (
            assessment_id,
            table_name,
            column_name,
            str(value),
        ),
        value,
    )


# =============================================================================
# Pseudonymize
# =============================================================================

def _pseudonymize(
    assessment_id: str,
    table_name: str,
    column_name: str,
    value: Any,
) -> Any:
    """
    Deterministic reversible pseudonymization.
    """

    global _PSEUDONYM_COUNTER

    if value is None:
        return None

    original = str(value)

    forward_key = (
        assessment_id,
        table_name,
        column_name,
        original,
    )

    pseudonym = _PSEUDONYM_FORWARD.get(forward_key)

    if pseudonym is None:

        pseudonym = f"USR_{_PSEUDONYM_COUNTER:06d}"
        _PSEUDONYM_COUNTER += 1

        _PSEUDONYM_FORWARD[forward_key] = pseudonym

        _PSEUDONYM_LOOKUP[
            (
                assessment_id,
                table_name,
                column_name,
                pseudonym,
            )
        ] = original

    return pseudonym


# =============================================================================
# Reverse Pseudonymize
# =============================================================================

def _reverse_pseudonymize(
    assessment_id: str,
    table_name: str,
    column_name: str,
    value: Any,
) -> Any:
    """
    Restore original value from pseudonym.
    """

    if value is None:
        return None

    return _PSEUDONYM_LOOKUP.get(
        (
            assessment_id,
            table_name,
            column_name,
            str(value),
        ),
        value,
    )


# =============================================================================
# Anonymize
# =============================================================================

def _anonymize(value: Any) -> Any:

    if value is None:
        return None

    return "ANONYMIZED"


# =============================================================================
# No Additional Control
# =============================================================================

def _no_additional_control(value: Any) -> Any:

    return value

def copy_security_control_module(output_dir: str) -> str:
    """
    Copies the security_control.py helper into the generated output directory.
    """

    source_path = __file__

    destination_path = os.path.join(
        output_dir,
        "security_control.py",
    )

    shutil.copy2(source_path, destination_path)

    return destination_path
