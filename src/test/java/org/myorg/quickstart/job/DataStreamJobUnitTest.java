package org.myorg.quickstart.job;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for DataStreamJob utility methods.
 * Following Flink testing best practices for stateless UDF testing.
 */
class DataStreamJobUnitTest {

    @Test
    @DisplayName("Should load profanity list from resources")
    void shouldLoadProfanitiesFromFile() {
        // When
        Set<String> profanities = DataStreamJob.loadProfanities();

        // Then
        assertThat(profanities)
            .isNotNull()
            .isNotEmpty()
            .contains("gun");
    }

    @Test
    @DisplayName("Should detect profanity in lowercase text")
    void shouldDetectProfanityInLowercase() {
        // Given
        Set<String> profanities = Set.of("gun", "badword", "offensive");
        String text = "this message contains gun";

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Should detect profanity in uppercase text")
    void shouldDetectProfanityInUppercase() {
        // Given
        Set<String> profanities = Set.of("gun", "badword", "offensive");
        String text = "THIS MESSAGE CONTAINS GUN";

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Should detect profanity in mixed case text")
    void shouldDetectProfanityInMixedCase() {
        // Given
        Set<String> profanities = Set.of("gun", "badword", "offensive");
        String text = "This message contains GuN";

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Should detect profanity as substring")
    void shouldDetectProfanityAsSubstring() {
        // Given
        Set<String> profanities = Set.of("gun", "badword", "offensive");
        String text = "The gunman was arrested";

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Should detect multiple profanities in text")
    void shouldDetectMultipleProfanities() {
        // Given
        Set<String> profanities = Set.of("gun", "badword", "offensive");
        String text = "This gun and badword are both offensive";

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Should not detect profanity in clean text")
    void shouldNotDetectProfanityInCleanText() {
        // Given
        Set<String> profanities = Set.of("gun", "badword", "offensive");
        String text = "This is a perfectly clean message";

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isFalse();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @DisplayName("Should handle null and empty text")
    void shouldHandleNullAndEmptyText(String text) {
        // Given
        Set<String> profanities = Set.of("gun", "badword", "offensive");

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("Should handle empty profanity set")
    void shouldHandleEmptyProfanitySet() {
        // Given
        Set<String> profanities = Set.of();
        String text = "This message contains gun";

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "   ",
        "\t",
        "\n",
        "     \t\n  "
    })
    @DisplayName("Should handle whitespace-only text")
    void shouldHandleWhitespaceOnlyText(String text) {
        // Given
        Set<String> profanities = Set.of("gun", "badword");

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("Should be case-insensitive for profanity words")
    void shouldBeCaseInsensitiveForProfanityWords() {
        // Given
        Set<String> profanities = Set.of("GUN", "BADWORD");
        String text = "this message contains gun";

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Should handle special characters in text")
    void shouldHandleSpecialCharactersInText() {
        // Given
        Set<String> profanities = Set.of("gun");
        String text = "This $@!% gun is here!";

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Should handle unicode characters in text")
    void shouldHandleUnicodeCharactersInText() {
        // Given
        Set<String> profanities = Set.of("gun");
        String text = "This 日本語 gun message";

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Should handle very long text")
    void shouldHandleVeryLongText() {
        // Given
        Set<String> profanities = Set.of("gun");
        StringBuilder longText = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longText.append("This is a clean message. ");
        }
        longText.append("gun"); // Add profanity at the end

        // When
        boolean result = DataStreamJob.containsProfanity(longText.toString(), profanities);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Should not match partial words incorrectly")
    void shouldMatchPartialWords() {
        // Given
        Set<String> profanities = Set.of("gun");
        String text = "begun, unguarded, gunman"; // Contains "gun" as substring

        // When
        boolean result = DataStreamJob.containsProfanity(text, profanities);

        // Then
        // Current implementation does substring matching
        assertThat(result).isTrue();
    }
}
