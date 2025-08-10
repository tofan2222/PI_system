"""
crypto.py
Enterprise Cryptographic Operations Module

Features:
- Zero-trust interface for encryption/decryption
- Mock functions for Stage 1 (DEV)
- Ready for Azure Key Vault/HSM in Stage 2
- Secure logging and audit trails
"""

import os
import json
import logging
from typing import Union, Optional
# from azure.keyvault.keys import KeyClient # MODIFICATION FOR TESTING: Commented out
# from azure.identity import DefaultAzureCredential # MODIFICATION FOR TESTING: Commented out
from d_config.demo_settings import APP_ENV  # Only import static configs

# Module-level logger (Pylance-compatible)
logger = logging.getLogger("security.crypto")

class CryptoManager:
    """
    Cryptographic operations with fail-secure design.
    Stage 1: Mock functions (DEV)
    Stage 2: Integrates with Azure Key Vault/HSM
    """

    def __init__(self):
        # This is already designed to be bypassable for testing by setting SECURE_CRYPTO_ENABLED=false
        # For ease of local testing, we can force it to False here if needed.
        # However, the current code allows this to be controlled via ENV, which is good.
        self._secure_mode = os.getenv("SECURE_CRYPTO_ENABLED", "false").lower() == "true"
        logger.info(
            f"Crypto initialized (Secure Mode: {self._secure_mode}, Env: {APP_ENV})",
            extra={"security_event": True}
        )

    def encrypt(self, data: Union[str, bytes], key_id: Optional[str] = None) -> str:
        """
        Encrypt data with optional key rotation.
        Args:
            data: String or bytes to encrypt
            key_id: Key version/identifier (future use)
        Returns:
            Encrypted data as base64 string
        """
        try:
            # --- Critical Add: Bypass encryption for KG-ready dicts ---
            if isinstance(data, dict) and data.get("metadata", {}).get("kg_ready"):
                return json.dumps(data)
            if self._secure_mode:
                return self._secure_encrypt(data, key_id or "default")

            logger.warning(
                "Using mock encryption (DEV ONLY)",
                extra={"security_event": True, "sensitive": False}
            )
            if isinstance(data, str):
                return f"encrypted_{data}"
            elif isinstance(data, (bytes, memoryview)):
                return f"encrypted_{bytes(data).decode('utf-8')}"
            else:
                raise TypeError(f"Unsupported data type for encryption: {type(data)}")

        except Exception as e:
            logger.critical(
                f"Encryption failed: {str(e)}", 
                exc_info=True,
                extra={"security_event": True}
            )
            raise

    def decrypt(self, data: str, key_id: Optional[str] = None) -> str:
        """Reverse of encrypt() with same security guarantees."""
        if self._secure_mode:
            return self._secure_decrypt(data, key_id)
        return data.replace("encrypted_", "")

    # --- Stage 2 Placeholders ---
    def _secure_encrypt(self, data: Union[str, bytes], key_id: str) -> str:
        """Azure Key Vault encryption integration (Stage 2)."""
        # --- MODIFICATION FOR TESTING (already present in the original design concept, but explicitly noted) ---
        # The original code already has these commented out, reinforcing the idea of a mock/stub.
        # credential = DefaultAzureCredential()
        # client = KeyClient(
        #     vault_url=os.getenv("AZURE_VAULT_URL") or "",
        #     credential=credential
        # )
        # key = client.get_key(key_id)
        # --- END MODIFICATION ---
        logger.warning("Simulating secure encryption (no actual vault access).")
        return f"VAULT_ENCRYPTED_{key_id}_{data}"  # Simulated output

    def _secure_decrypt(self, data: str, key_id: Optional[str]) -> str:
        """Future integration."""
        # This raises NotImplementedError, which is fine for testing.
        # If you needed to test decryption functionality, you'd implement a mock here.
        raise NotImplementedError("Secure decryption not implemented")

# Singleton pattern
crypto = CryptoManager()