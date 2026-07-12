package xml

import (
	"strings"
	"testing"
)

func TestWriteValueEscapesReservedCharacters(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "ampersand is escaped",
			input:    "p%X&word",
			expected: "p%X&amp;word",
		},
		{
			name:     "less-than is escaped",
			input:    "a<b",
			expected: "a&lt;b",
		},
		{
			name:     "greater-than is escaped",
			input:    "a>b",
			expected: "a&gt;b",
		},
		{
			name:     "generated password with mixed special chars",
			input:    "l%XubpKqz2y!QsKlsynEEE6#Thknj&fG",
			expected: "l%XubpKqz2y!QsKlsynEEE6#Thknj&amp;fG",
		},
		{
			name:     "plain value is unchanged",
			input:    "plainpassword",
			expected: "plainpassword",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sb strings.Builder
			(&xmlNode{}).writeValue(&sb, tc.input)
			if sb.String() != tc.expected {
				t.Errorf("writeValue() = %q, expected %q", sb.String(), tc.expected)
			}
		})
	}
}
