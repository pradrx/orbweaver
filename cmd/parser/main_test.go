package main

import (
	"strings"
	"testing"
)

func TestExtractText(t *testing.T) {
	tests := []struct {
		name string
		html string
		want string
	}{
		{
			name: "paragraph text preserved",
			html: `<html><body><p>Hello world</p></body></html>`,
			want: "Hello world",
		},
		{
			name: "script text skipped",
			html: `<html><body><p>Before</p><script>alert("x")</script><p>After</p></body></html>`,
			want: "Before After",
		},
		{
			name: "style text skipped",
			html: `<html><body><style>.foo { color: red; }</style><p>Content</p></body></html>`,
			want: "Content",
		},
		{
			name: "noscript text skipped",
			html: `<html><body><noscript>Enable JS</noscript><p>Content</p></body></html>`,
			want: "Content",
		},
		{
			name: "nav text skipped",
			html: `<html><body><nav><a href="/">Home</a> <a href="/about">About</a></nav><p>Content here</p></body></html>`,
			want: "Content here",
		},
		{
			name: "header text skipped",
			html: `<html><body><header><h1>Site Name</h1><nav><a href="/">Home</a></nav></header><p>Article body</p></body></html>`,
			want: "Article body",
		},
		{
			name: "footer text skipped",
			html: `<html><body><p>Main content</p><footer>Copyright 2026 All rights reserved</footer></body></html>`,
			want: "Main content",
		},
		{
			name: "aside text skipped",
			html: `<html><body><p>Article</p><aside>Related: some sidebar content</aside></body></html>`,
			want: "Article",
		},
		{
			name: "menu text skipped",
			html: `<html><body><menu><li>Option 1</li><li>Option 2</li></menu><p>Content</p></body></html>`,
			want: "Content",
		},
		{
			name: "form text skipped",
			html: `<html><body><form><label>Name</label><input type="text"><button>Submit</button></form><p>Thanks</p></body></html>`,
			want: "Thanks",
		},
		{
			name: "inline anchor text preserved",
			html: `<html><body><p>See the <a href="/doc">documentation</a> for details.</p></body></html>`,
			want: "See the documentation for details.",
		},
		{
			name: "nested skip elements",
			html: `<html><body><nav><ul><li><a href="/">Home</a></li><li><a href="/about">About</a></li></ul></nav><p>Body text</p></body></html>`,
			want: "Body text",
		},
		{
			name: "mixed content page",
			html: `<html><body>
				<header><h1>My Site</h1><nav><a href="/">Home</a></nav></header>
				<main><article><h2>Title</h2><p>This is the real content.</p></article></main>
				<aside><h3>Related</h3><ul><li>Link 1</li></ul></aside>
				<footer><p>Copyright</p></footer>
			</body></html>`,
			want: "Title This is the real content.",
		},
		{
			name: "link-heavy page like crawler-test.com",
			html: `<html><body><nav>` +
				`<a href="/a">Link A</a> <a href="/b">Link B</a> <a href="/c">Link C</a>` +
				`</nav></body></html>`,
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractText(strings.NewReader(tt.html))
			if got != tt.want {
				t.Errorf("extractText() = %q, want %q", got, tt.want)
			}
		})
	}
}
