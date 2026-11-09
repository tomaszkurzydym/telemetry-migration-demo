package demo.legacy

import java.nio.charset.StandardCharsets.UTF_8
import java.security.SecureRandom
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}

/** AES-256-GCM location encryption, same wire format as the Rust processor (contract §5). */
final class LegacyCrypto private (key: Array[Byte], val keyId: String) extends Serializable {
  require(key.length == 32)
  @transient private lazy val rng = new SecureRandom()

  private def aad(vin: String, eventId: String): Array[Byte] =
    s"${LegacyCrypto.AadPrefix}|$vin|$eventId".getBytes(UTF_8)

  /** Returns (ciphertext||tag b64, nonce b64, key_id). Fresh 12-byte nonce per call. */
  def encrypt(vin: String, eventId: String, plaintext: Array[Byte]): (String, String, String) = {
    val nonce = new Array[Byte](12)
    rng.nextBytes(nonce)
    val c = Cipher.getInstance("AES/GCM/NoPadding")
    c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, nonce))
    c.updateAAD(aad(vin, eventId))
    val ct = c.doFinal(plaintext)
    (Base64.getEncoder.encodeToString(ct), Base64.getEncoder.encodeToString(nonce), keyId)
  }

  /** Throws javax.crypto.AEADBadTagException on tampering / wrong key / wrong record. */
  def decrypt(vin: String, eventId: String, ciphertextB64: String, nonceB64: String): Array[Byte] = {
    val nonce = Base64.getDecoder.decode(nonceB64)
    require(nonce.length == 12, "nonce must be 12 bytes")
    val c = Cipher.getInstance("AES/GCM/NoPadding")
    c.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, nonce))
    c.updateAAD(aad(vin, eventId))
    c.doFinal(Base64.getDecoder.decode(ciphertextB64))
  }

  override def toString: String = s"LegacyCrypto(keyId=$keyId)"
}

object LegacyCrypto {
  val AadPrefix = "telemetry.location.v1"

  def fromBase64(keyB64: String, keyId: String): Either[String, LegacyCrypto] = {
    if (keyB64 == null || keyB64.trim.isEmpty) Left("LOCATION_ENC_KEY is not set")
    else {
      val raw = try Some(Base64.getDecoder.decode(keyB64.trim)) catch { case _: IllegalArgumentException => None }
      raw match {
        case None                       => Left("LOCATION_ENC_KEY is not valid standard base64")
        case Some(k) if k.length != 32 => Left(s"LOCATION_ENC_KEY must decode to 32 bytes (got ${k.length})")
        case Some(k)                    => Right(new LegacyCrypto(k, keyId))
      }
    }
  }

  def fromEnv(env: Map[String, String] = sys.env): Either[String, LegacyCrypto] =
    fromBase64(env.getOrElse("LOCATION_ENC_KEY", ""), env.get("LOCATION_ENC_KEY_ID").filter(_.trim.nonEmpty).getOrElse("local-dev-1"))
}
