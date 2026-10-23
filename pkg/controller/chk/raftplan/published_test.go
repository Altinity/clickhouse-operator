package raftplan

import (
	"reflect"
	"testing"
)

func TestPublishedHostnames(t *testing.T) {
	tests := []struct {
		name  string
		data  map[string]string
		want  []string
		known bool
		fails bool
	}{
		{name: "no config map data"},
		{name: "no Raft section", data: map[string]string{"other.xml": "<clickhouse><logger/></clickhouse>"}},
		{name: "valid Raft section", data: map[string]string{
			"keeper.xml": "<clickhouse><keeper_server><raft_configuration><server><id>1</id><hostname>h0.ns.svc</hostname></server><server><hostname>h1.ns.svc</hostname></server></raft_configuration></keeper_server></clickhouse>",
			"users.xml":  "not xml <",
		}, want: []string{"h0.ns.svc", "h1.ns.svc"}, known: true},
		{name: "empty valid section", data: map[string]string{"raft.xml": "<raft_configuration></raft_configuration>"}, want: []string{}, known: true},
		{name: "duplicate hostname", data: map[string]string{"raft.xml": "<raft_configuration><server><hostname>h0</hostname></server><server><hostname>h0</hostname></server></raft_configuration>"}, fails: true},
		{name: "empty hostname", data: map[string]string{"raft.xml": "<raft_configuration><server><id>2</id></server></raft_configuration>"}, fails: true},
		{name: "malformed XML", data: map[string]string{"raft.xml": "<raft_configuration><server></raft_configuration>"}, fails: true},
		{name: "conflicting blocks", data: map[string]string{
			"a.xml": "<raft_configuration><server><hostname>h0</hostname></server></raft_configuration>",
			"b.xml": "<raft_configuration><server><hostname>h1</hostname></server></raft_configuration>",
		}, fails: true},
		{name: "identical blocks", data: map[string]string{
			"a.xml": "<raft_configuration><server><hostname>h0</hostname></server></raft_configuration>",
			"b.xml": "<raft_configuration><server><hostname>h0</hostname></server></raft_configuration>",
		}, want: []string{"h0"}, known: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, known, err := PublishedHostnames(tt.data)
			if (err != nil) != tt.fails {
				t.Fatalf("err = %v, want failure %v", err, tt.fails)
			}
			if tt.fails {
				return
			}
			if known != tt.known {
				t.Errorf("known=%v, want %v", known, tt.known)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("hosts=%v, want %v", got, tt.want)
			}
		})
	}
}
