package xml

import (
	"strings"
	"testing"
)

// stubSetting is a setting that renders only when marked scalar or vector.
// The zero value matches a file-mapped source setting: it is kept in the tree
// and buildXML writes no tag for it.
type stubSetting struct {
	scalar bool
	vector bool
	value  string
	attrs  string
	embed  bool
	vec    []string
}

func (s stubSetting) String() string            { return s.value }
func (s stubSetting) IsEmpty() bool             { return false }
func (s stubSetting) IsScalar() bool            { return s.scalar }
func (s stubSetting) IsVector() bool            { return s.vector }
func (s stubSetting) Attributes() string        { return s.attrs }
func (s stubSetting) VectorOfStrings() []string { return s.vec }
func (s stubSetting) IsEmbed() bool             { return s.embed }

func TestBuildXMLOmitsEmptyParentsOfUnrenderedSettings(t *testing.T) {
	root := &xmlNode{}

	kafka := root.addChild("kafka")
	kafka.addChild("debug").value = stubSetting{scalar: true, value: "all"}
	kafka.addChild("sasl_password").value = stubSetting{}

	kafka2 := root.addChild("kafka2")
	kafka2.addChild("sasl_username").value = stubSetting{}
	kafka2.addChild("sasl_password").value = stubSetting{}

	var buf strings.Builder
	root.buildXML(&buf, 0, 4)
	got := buf.String()

	if strings.Contains(got, "kafka2") {
		t.Fatalf("empty parent was written:\n%s", got)
	}
	if strings.Contains(got, "sasl_password") {
		t.Fatalf("unrendered leaf was written:\n%s", got)
	}
	if !strings.Contains(got, "<debug>all</debug>") {
		t.Fatalf("scalar child missing:\n%s", got)
	}
	if !strings.Contains(got, "<kafka>") || !strings.Contains(got, "</kafka>") {
		t.Fatalf("parent of a rendered child missing:\n%s", got)
	}
}

func TestWriteTagNoValueKeepsAttributeWithoutChildren(t *testing.T) {
	n := &xmlNode{tag: "keep"}
	var buf strings.Builder
	n.writeTagNoValue(&buf, ` remove="1"`, 0, 4)
	if !strings.Contains(buf.String(), `<keep remove="1">`) {
		t.Fatalf("tag with an attribute was skipped:\n%s", buf.String())
	}
}

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
