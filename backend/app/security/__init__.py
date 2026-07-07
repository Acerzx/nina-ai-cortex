"""
Security module — компоненты безопасности для N.I.N.A. AI Cortex.
"""

from app.security.vault import CredentialVault, VaultEntry

__all__ = [
    "CredentialVault",
    "VaultEntry",
]
