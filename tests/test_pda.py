"""Tests for Pump.fun bonding curve PDA derivation."""

from solders.pubkey import Pubkey

from utils import derive_bonding_curve_pda, validate_mint_address


# Verified mainnet Pump.fun mint / bonding curve pair
KNOWN_MINT = "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"
KNOWN_BONDING_CURVE = "7kAsAxtVQckxu5eoenWfyztb45pjb3raxKQEZd2ndi7J"
KNOWN_BUMP = 254


def test_validate_mint_address_accepts_valid_pubkey():
    pubkey = validate_mint_address(KNOWN_MINT)
    assert str(pubkey) == KNOWN_MINT


def test_validate_mint_address_rejects_invalid():
    try:
        validate_mint_address("not-a-valid-mint")
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_derive_bonding_curve_pda_deterministic():
    mint = Pubkey.from_string(KNOWN_MINT)
    pda1, bump1 = derive_bonding_curve_pda(mint)
    pda2, bump2 = derive_bonding_curve_pda(KNOWN_MINT)

    assert pda1 == pda2
    assert bump1 == bump2
    assert pda1 == KNOWN_BONDING_CURVE
    assert bump1 == KNOWN_BUMP


def test_derive_bonding_curve_pda_matches_solders():
    """Cross-check against solders find_program_address."""
    mint = Pubkey.from_string(KNOWN_MINT)
    program = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
    expected_pda, expected_bump = Pubkey.find_program_address(
        [b"bonding-curve", bytes(mint)],
        program,
    )
    pda, bump = derive_bonding_curve_pda(mint)
    assert pda == str(expected_pda)
    assert bump == expected_bump
