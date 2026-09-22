package com.amex.lumi.ingestion.security;

import com.amex.lumi.ingestion.config.EncryptionProperties;
import com.amex.lumi.ingestion.exception.DecryptionException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FieldDecryptorTest {

    private final FieldDecryptor decryptor = new FieldDecryptor(new EncryptionProperties(TestCipher.KEY));

    @Test
    void decryptsValueEncryptedByBeam() {
        assertThat(decryptor.decrypt(TestCipher.encrypt("950000"))).isEqualTo("950000");
    }

    @Test
    void rejectsGarbage() {
        assertThatThrownBy(() -> decryptor.decrypt("not-encrypted")).isInstanceOf(DecryptionException.class);
    }

    @Test
    void rejectsValueFromAnotherKey() {
        String otherKeyValue = TestCipher.encrypt("950000", "ffffffffffffffffffffffffffffffff");

        assertThatThrownBy(() -> decryptor.decrypt(otherKeyValue)).isInstanceOf(DecryptionException.class);
    }

    @Test
    void rejectsUnknownVersion() {
        String v2 = TestCipher.encrypt("950000").replace("v1:", "v2:");

        assertThatThrownBy(() -> decryptor.decrypt(v2)).isInstanceOf(DecryptionException.class);
    }

    @Test
    void emptyValuesAreReturnedAsTheyAre() {
        assertThat(decryptor.decryptIfPresent(null)).isNull();
        assertThat(decryptor.decryptIfPresent(" ")).isEqualTo(" ");
    }

    @Test
    void keyMustBeThirtyTwoBytes() {
        assertThatThrownBy(() -> new FieldDecryptor(new EncryptionProperties("short")))
                .isInstanceOf(IllegalStateException.class);
    }
}
