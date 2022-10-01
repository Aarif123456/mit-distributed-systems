// Abdullah Arif

package raft

import (
	"testing"
)

func Test_Term(t *testing.T) {
	var (
		one = 1
		two = 2
	)

	tests := []struct {
		name    string
		term    *Term
		newTerm *Term
		want    *Term
	}{
		{
			name:    "Empty term can update each other",
			term:    &Term{},
			newTerm: &Term{},
			want:    &Term{},
		},
		{
			name: "Nil term can be updated",
			term: &Term{
				num: 0,
			},
			newTerm: &Term{
				num:      0,
				votedFor: &one,
			},
			want: &Term{
				num:      0,
				votedFor: &one,
			},
		},
		{
			name: "higher term wins can be updated",
			term: &Term{
				num:      0,
				votedFor: &two,
			},
			newTerm: &Term{
				num:      1,
				votedFor: &one,
			},
			want: &Term{
				num:      1,
				votedFor: &one,
			},
		},
		{
			name: "lower term loses can be updated",
			term: &Term{
				num:      1,
				votedFor: &one,
			},
			newTerm: &Term{
				num:      0,
				votedFor: &two,
			},
			want: &Term{
				num:      1,
				votedFor: &one,
			},
		},
		{
			name: "same term does not update if votedFor is not nil",
			term: &Term{
				num:      0,
				votedFor: &one,
			},
			newTerm: &Term{
				num:      0,
				votedFor: &two,
			},
			want: &Term{
				num:      0,
				votedFor: &one,
			},
		},
		{
			name: "same term does update if votedFor is nil",
			term: &Term{
				num:      0,
				votedFor: nil,
			},
			newTerm: &Term{
				num:      0,
				votedFor: &two,
			},
			want: &Term{
				num:      0,
				votedFor: &two,
			},
		},
		{
			name: "We never go back a term",
			term: &Term{
				num:      1,
				votedFor: nil,
			},
			newTerm: &Term{
				num:      0,
				votedFor: nil,
			},
			want: &Term{
				num:      1,
				votedFor: nil,
			},
		},
		{
			name: "We never go back a term",
			term: &Term{
				num:      1,
				votedFor: &one,
			},
			newTerm: &Term{
				num:      0,
				votedFor: &one,
			},
			want: &Term{
				num:      1,
				votedFor: &one,
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			changed := tc.term.VoteFor(tc.newTerm.num, tc.newTerm.votedFor)
			got := tc.term
			if *got != *tc.want {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
			if changed && *tc.newTerm != *tc.want {
				t.Fatalf("want %v, newTerm %v", tc.want, tc.newTerm)
			}
		})
	}
}
