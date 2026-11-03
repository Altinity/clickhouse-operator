package xml

import (
	"strings"
	"testing"
)

func TestWriteValue(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		encoding valueEncoding // zero value is Escape
		expected string
	}{
		// Element text (Escape): reserved characters are escaped.
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
		{
			// CH multi-line settings must survive: tab, newline and CR are preserved.
			name:     "whitespace control chars are preserved",
			input:    "a\tb\nc\rd",
			expected: "a\tb\nc\rd",
		},
		{
			// Single-pass escaping: the '&' of a pre-escaped entity is escaped exactly
			// once (the replacer never reprocesses its own output).
			name:     "pre-escaped input is escaped once, not recursively",
			input:    "a&amp;b",
			expected: "a&amp;amp;b",
		},

		// Embedded values (Raw): a pre-rendered XML fragment (SetEmbed) must be
		// emitted verbatim — escaping it would turn markup into literal text and break
		// the generated config (e.g. CHK keeper_server/raft_configuration).
		{
			name:     "embedded xml fragment is emitted verbatim",
			encoding: Raw,
			input:    "<server>\n    <id>0</id>\n</server>",
			expected: "<server>\n    <id>0</id>\n</server>",
		},
		{
			name:     "embedded remove-attribute fragment is emitted verbatim",
			encoding: Raw,
			input:    `<tcp_port remove="1"/>`,
			expected: `<tcp_port remove="1"/>`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sb strings.Builder
			(&xmlNode{}).writeValue(&sb, tc.input, tc.encoding)
			if sb.String() != tc.expected {
				t.Errorf("writeValue(%q, encoding=%v) = %q, expected %q", tc.input, tc.encoding, sb.String(), tc.expected)
			}
		})
	}
}
