/**
 * Credential encryption — AES-256-GCM.
 * Used for encrypting RefreshToken/AccessToken/ApiKey/AppSecret fields
 * in admin.BrandCredentials before writing to SQL.
 *
 * Phase 1 approach: app-side symmetric encryption.
 * Phase 2+: migrate to SQL Always Encrypted or Azure Key Vault references.
 *
 * Encrypted format: base64(iv || authTag || ciphertext)
 *   - iv: 12 bytes (96 bits, recommended for GCM)
 *   - authTag: 16 bytes (128 bits)
 *   - ciphertext: variable length
 */
const crypto = require('crypto');

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 12;
const AUTH_TAG_LENGTH = 16;

function getKey() {
  const hex = process.env.CRED_ENCRYPTION_KEY;
  if (!hex) throw new Error('CRED_ENCRYPTION_KEY env var is not set');
  if (hex.length !== 64) throw new Error('CRED_ENCRYPTION_KEY must be 64 hex chars (32 bytes)');
  return Buffer.from(hex, 'hex');
}

function encrypt(plaintext) {
  if (plaintext == null) return null;
  const key = getKey();
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);
  const enc = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
  const authTag = cipher.getAuthTag();
  return Buffer.concat([iv, authTag, enc]).toString('base64');
}

function decrypt(encoded) {
  if (encoded == null) return null;
  const key = getKey();
  const buf = Buffer.from(encoded, 'base64');
  const iv = buf.slice(0, IV_LENGTH);
  const authTag = buf.slice(IV_LENGTH, IV_LENGTH + AUTH_TAG_LENGTH);
  const ciphertext = buf.slice(IV_LENGTH + AUTH_TAG_LENGTH);
  const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
  decipher.setAuthTag(authTag);
  const dec = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
  return dec.toString('utf8');
}

module.exports = { encrypt, decrypt };
