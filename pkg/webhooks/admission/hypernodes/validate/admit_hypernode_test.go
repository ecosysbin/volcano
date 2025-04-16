/*
Copyright 2024 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package validate

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hypernodev1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
)

func TestValidateHyperNode(t *testing.T) {
	testCases := []struct {
		Name      string
		HyperNode hypernodev1alpha1.HyperNode
		ExpectErr bool
	}{
		{
			Name: "validate valid hypernode",
			HyperNode: hypernodev1alpha1.HyperNode{
				ObjectMeta: metav1.ObjectMeta{
					Name: "hypernode-1",
				},
				Spec: hypernodev1alpha1.HyperNodeSpec{
					Members: []hypernodev1alpha1.MemberSpec{
						{
							Type: hypernodev1alpha1.MemberTypeNode,
							Selector: hypernodev1alpha1.MemberSelector{
								ExactMatch: &hypernodev1alpha1.ExactMatch{Name: "node-1"},
							},
						},
					},
				},
			},
			ExpectErr: false,
		},
		{
			Name: "validate invalid hypernode with empty exactMatch",
			HyperNode: hypernodev1alpha1.HyperNode{
				ObjectMeta: metav1.ObjectMeta{
					Name: "hypernode-1",
				},
				Spec: hypernodev1alpha1.HyperNodeSpec{
					Members: []hypernodev1alpha1.MemberSpec{
						{
							Type: hypernodev1alpha1.MemberTypeNode,
							Selector: hypernodev1alpha1.MemberSelector{
								ExactMatch: &hypernodev1alpha1.ExactMatch{Name: ""},
							},
						},
					},
				},
			},
			ExpectErr: true,
		},
		{
			Name: "validate invalid hypernode with empty regexMatch",
			HyperNode: hypernodev1alpha1.HyperNode{
				ObjectMeta: metav1.ObjectMeta{
					Name: "hypernode-1",
				},
				Spec: hypernodev1alpha1.HyperNodeSpec{
					Members: []hypernodev1alpha1.MemberSpec{
						{
							Type: hypernodev1alpha1.MemberTypeNode,
							Selector: hypernodev1alpha1.MemberSelector{
								RegexMatch: &hypernodev1alpha1.RegexMatch{
									Pattern: "",
								},
							},
						},
					},
				},
			},
			ExpectErr: true,
		},
		{
			Name: "validate invalid hypernode with invalid regexMatch",
			HyperNode: hypernodev1alpha1.HyperNode{
				ObjectMeta: metav1.ObjectMeta{
					Name: "hypernode-1",
				},
				Spec: hypernodev1alpha1.HyperNodeSpec{
					Members: []hypernodev1alpha1.MemberSpec{
						{
							Type: hypernodev1alpha1.MemberTypeNode,
							Selector: hypernodev1alpha1.MemberSelector{
								RegexMatch: &hypernodev1alpha1.RegexMatch{
									Pattern: "a(b",
								},
							},
						},
					},
				},
			},
			ExpectErr: true,
		},
		{
			Name: "validate invalid hypernode with both regexMatch and exactMatch",
			HyperNode: hypernodev1alpha1.HyperNode{
				ObjectMeta: metav1.ObjectMeta{
					Name: "hypernode-1",
				},
				Spec: hypernodev1alpha1.HyperNodeSpec{
					Members: []hypernodev1alpha1.MemberSpec{
						{
							Type: hypernodev1alpha1.MemberTypeNode,
							Selector: hypernodev1alpha1.MemberSelector{
								RegexMatch: &hypernodev1alpha1.RegexMatch{
									Pattern: "node.*",
								},
								ExactMatch: &hypernodev1alpha1.ExactMatch{Name: "node-1"},
							},
						},
					},
				},
			},
			ExpectErr: true,
		},
		{
			Name: "validate invalid hypernode with neither regexMatch nor exactMatch",
			HyperNode: hypernodev1alpha1.HyperNode{
				ObjectMeta: metav1.ObjectMeta{
					Name: "hypernode-1",
				},
				Spec: hypernodev1alpha1.HyperNodeSpec{
					Members: []hypernodev1alpha1.MemberSpec{
						{
							Type:     hypernodev1alpha1.MemberTypeNode,
							Selector: hypernodev1alpha1.MemberSelector{},
						},
					},
				},
			},
			ExpectErr: true,
		},
		{
			Name: "validate none members",
			HyperNode: hypernodev1alpha1.HyperNode{
				ObjectMeta: metav1.ObjectMeta{
					Name: "hypernode-1",
				},
				Spec: hypernodev1alpha1.HyperNodeSpec{},
			},
			ExpectErr: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			err := validateHyperNode(&testCase.HyperNode)
			if !testCase.ExpectErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			} else if testCase.ExpectErr && err == nil {
				t.Errorf("expected error but got nil")
			} else {
				t.Logf("error: %v", err)
			}
		})
	}

}

func TestGetMembers(t *testing.T) {
	type testCase struct {
		name     string
		selector topologyv1alpha1.MemberSelector
		nodes    []*corev1.Node
		expected sets.Set[string]
	}

	testCases := []testCase{
		{
			name: "Label match success with MatchLabels",
			selector: topologyv1alpha1.MemberSelector{
				LabelMatch: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"role": "worker",
					},
				},
			},
			nodes: []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"role": "worker",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node2",
						Labels: map[string]string{
							"role": "master",
						},
					},
				},
			},
			expected: sets.New[string]("node1"),
		},
		{
			name: "Label match failure with MatchLabels",
			selector: topologyv1alpha1.MemberSelector{
				LabelMatch: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"role": "invalid-role",
					},
				},
			},
			nodes: []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"role": "worker",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node2",
						Labels: map[string]string{
							"role": "master",
						},
					},
				},
			},
			expected: sets.New[string](),
		},
		{
			name: "Label selector is nil",
			selector: topologyv1alpha1.MemberSelector{
				LabelMatch: nil,
			},
			nodes: []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"role": "worker",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node2",
						Labels: map[string]string{
							"role": "master",
						},
					},
				},
			},
			expected: sets.New[string](),
		},
		{
			name: "MatchExpressions In operator match success",
			selector: topologyv1alpha1.MemberSelector{
				LabelMatch: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "role",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"worker"},
						},
					},
				},
			},
			nodes: []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"role": "worker",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node2",
						Labels: map[string]string{
							"role": "master",
						},
					},
				},
			},
			expected: sets.New[string]("node1"),
		},
		{
			name: "MatchExpressions In operator match failure",
			selector: topologyv1alpha1.MemberSelector{
				LabelMatch: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "role",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"invalid-role"},
						},
					},
				},
			},
			nodes: []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"role": "worker",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node2",
						Labels: map[string]string{
							"role": "master",
						},
					},
				},
			},
			expected: sets.New[string](),
		},
		{
			name: "MatchExpressions NotIn operator match success",
			selector: topologyv1alpha1.MemberSelector{
				LabelMatch: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "role",
							Operator: metav1.LabelSelectorOpNotIn,
							Values:   []string{"master"},
						},
					},
				},
			},
			nodes: []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"role": "worker",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node2",
						Labels: map[string]string{
							"role": "master",
						},
					},
				},
			},
			expected: sets.New[string]("node1"),
		},
		{
			name: "MatchExpressions Exists operator match success",
			selector: topologyv1alpha1.MemberSelector{
				LabelMatch: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "role",
							Operator: metav1.LabelSelectorOpExists,
						},
					},
				},
			},
			nodes: []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"role": "worker",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node2",
						Labels: map[string]string{},
					},
				},
			},
			expected: sets.New[string]("node1"),
		},
		{
			name: "MatchExpressions DoesNotExist operator match success",
			selector: topologyv1alpha1.MemberSelector{
				LabelMatch: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "role",
							Operator: metav1.LabelSelectorOpDoesNotExist,
						},
					},
				},
			},
			nodes: []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"role": "worker",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node2",
						Labels: map[string]string{},
					},
				},
			},
			expected: sets.New[string]("node2"),
		},
	}

	hyperNodesInfor := HyperNodesInfo{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := hyperNodesInfor.getMembers(tc.selector, tc.nodes)
			if !result.Equal(tc.expected) {
				t.Errorf("Test %s failed: Expected %v, but got %v", tc.name, tc.expected, result)
			}
		})
	}
}
